"""Optionally convert, verify and clean up TUFLOW FLT/ASC rasters.

The default is validation only. With --convert-missing, create absent GeoTIFFs
using the existing ryan_library GDAL translation function before verification.
Every source is paired with a .tif or .tiff
having the same stem and relative directory. Compare raster cells exactly,
including NoData positions, dimensions and grid transform. A declared source
CRS must be preserved in the TIFF; an unknown source CRS is acceptable with a
warning. No resampling or floating-point tolerance is used. Regenerable
statistics and auxiliary metadata are not compared. Only matching sources
become deletion candidates. The source FLT's .hdr is also eligible; .prj and
auxiliary files are always retained. The TIFF must be strictly newer.

Edit ROOT_DIR/TIF_ROOT_DIR below or use --root/--tif-root. To run all steps,
provide --convert-missing --delete, then type DELETE when prompted. Existing
GeoTIFFs are never overwritten, and a failed comparison retains the source.
Do not run on active outputs; file metadata snapshots do not lock files.
"""

# Pyright: Rasterio currently has no bundled type stubs; follow the existing GIS module boundary.
# pyright: reportMissingTypeStubs=false, reportUnknownArgumentType=false
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownParameterType=false, reportAttributeAccessIssue=false

from __future__ import annotations

import argparse
import csv
import os
import shutil
import sys
from uuid import uuid4
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio
from rasterio.errors import RasterioError
from rich.progress import BarColumn, Progress, TaskID, TaskProgressColumn, TextColumn, TimeRemainingColumn

from ryan_library.functions.file_verification import (
    FileSnapshot,
    has_linked_component,
    snapshot_file,
    snapshot_is_unchanged,
)

# ---------------------------------------------------------------------------
# User settings (CLI arguments may override the first three)
# ---------------------------------------------------------------------------
ROOT_DIR = Path.cwd()
TIF_ROOT_DIR: Path | None = None  # None means TIFs alongside FLT/ASC files
DELETE_FILES = False  # Verification/proposal only unless set True or --delete
CONVERT_MISSING = False  # Opt in to creating absent GeoTIFFs with the shared GDAL converter
DELETE_FLT_HEADERS = True  # Include .hdr paired with a verified .flt
SHOW_PROGRESS = True
MAX_WINDOW_CELLS = 1_000_000  # Approximate upper bound on cells per read
REPORT_PREFIX = "tuflow_raster_conversion"


@dataclass(frozen=True)
class Comparison:
    source: FileSnapshot
    tif: FileSnapshot | None
    status: str
    reason: str
    header: FileSnapshot | None = None

    @property
    def eligible(self) -> bool:
        return self.status in {"MATCH", "MATCH_CRS_UNVERIFIED"}


def snapshot(path: Path) -> FileSnapshot:
    """Record inexpensive metadata for a file without reading its contents."""
    return snapshot_file(path)


def unchanged(record: FileSnapshot) -> bool:
    """Reject deletion when either file changed after validation."""
    return snapshot_is_unchanged(record) and not has_linked_component(record.path)


def discover_sources(root: Path) -> list[Path]:
    """Walk folders without following symlink directories; collect FLT/ASC only."""
    sources: list[Path] = []
    for parent, directories, files in os.walk(root, followlinks=False):
        directories[:] = sorted(name for name in directories if not has_linked_component(Path(parent) / name))
        for name in sorted(files):
            path = Path(parent) / name
            if path.suffix.casefold() in {".asc", ".flt"} and not has_linked_component(path):
                sources.append(path)
    return sorted(sources, key=lambda p: str(p).casefold())


def paired_file(directory: Path, stem: str, suffixes: set[str]) -> tuple[Path | None, str | None]:
    """Find an unambiguous companion, ignoring filename case."""
    if has_linked_component(directory):
        return None, "Matching directory contains a symlink or junction"
    try:
        matches = [
            child
            for child in directory.iterdir()
            if child.stem.casefold() == stem.casefold() and child.suffix.casefold() in suffixes and child.is_file()
        ]
    except FileNotFoundError:
        return None, "Destination directory does not exist"
    if not matches:
        return None, "Matching file not found"
    if len(matches) > 1:
        return None, "Ambiguous matching files: " + ", ".join(str(path) for path in sorted(matches))
    if has_linked_component(matches[0]):
        return None, "Matching file contains a symlink or junction"
    return matches[0], None


def crs_warning(*, source_missing: bool, tif_missing: bool) -> str | None:
    """Warn only when the source CRS is unknown; missing target CRS is an error."""
    if source_missing and tif_missing:
        return "Both rasters lack CRS metadata; spatial reference cannot be verified"
    if source_missing:
        return "Source raster lacks CRS metadata; GeoTIFF CRS cannot be independently verified"
    return None


def raster_metadata_mismatch(original: rasterio.io.DatasetReader, converted: rasterio.io.DatasetReader) -> str | None:
    """Check non-regenerable geometry, CRS and value interpretation metadata."""
    if (original.width, original.height, original.count) != (
        converted.width,
        converted.height,
        converted.count,
    ):
        return "Raster width, height or band count differs"
    if original.transform != converted.transform:
        return "Grid geotransform differs"
    if original.scales != converted.scales or original.offsets != converted.offsets:
        return "Raster band value scales or offsets differ"
    if original.crs is not None:
        if converted.crs is None:
            return f"GeoTIFF is missing the declared source CRS: {original.crs}"
        # Strict CRS equality is intentional: do not ignore axis-order differences
        # (for example, OGC:CRS84 versus EPSG:4326) or normalise away discrepancies.
        if original.crs != converted.crs:
            return f"Declared CRS definitions differ: source {original.crs}, GeoTIFF {converted.crs}"
    return None


def compare_rasters(
    source: Path, tif: Path, progress: Progress | None = None, task_id: TaskID | None = None
) -> tuple[str | None, str | None]:
    """Return (mismatch, CRS warning), checking all valid cells exactly.

    A raster may be compressed or use another NoData *sentinel*, provided the
    invalid-pixel locations and all valid pixel values are identical.
    An absent source CRS creates a warning. A declared source CRS must be
    preserved, so a missing or conflicting GeoTIFF CRS fails the comparison.
    Grid origin/resolution and band scale/offset metadata must match exactly.
    Statistics tags and auxiliary sidecars are intentionally excluded.
    """
    with rasterio.open(source) as original, rasterio.open(tif) as converted:
        metadata_difference = raster_metadata_mismatch(original, converted)
        if metadata_difference is not None:
            return metadata_difference, None
        warning = crs_warning(source_missing=original.crs is None, tif_missing=converted.crs is None)

        rows = max(1, min(original.height, MAX_WINDOW_CELLS // max(1, original.width)))
        if progress is not None and task_id is not None:
            progress.update(task_id, total=original.height * original.count, completed=0, description=source.name)

        for band in range(1, original.count + 1):
            for y in range(0, original.height, rows):
                height = min(rows, original.height - y)
                window = ((y, y + height), (0, original.width))
                before = original.read(band, window=window, masked=True)
                after = converted.read(band, window=window, masked=True)
                valid_before = ~np.ma.getmaskarray(before)
                valid_after = ~np.ma.getmaskarray(after)
                if not np.array_equal(valid_before, valid_after):
                    return f"NoData mask differs at band {band}, rows {y}-{y + height - 1}", None

                left = np.asarray(before.data)
                right = np.asarray(after.data)
                equal = np.equal(left, right) | (np.isnan(left) & np.isnan(right))
                differing = valid_before & ~equal
                if np.any(differing):
                    local_row, column = np.argwhere(differing)[0]
                    message = (
                        f"Pixel values differ at band {band}, row {y + int(local_row)}, column {int(column)}: "
                        f"{left[local_row, column]!r} != {right[local_row, column]!r}"
                    )
                    return message, None
                if progress is not None and task_id is not None:
                    progress.advance(task_id, height)
    return None, warning


def publish_staged_tif(staged: Path, destination: Path) -> None:
    """Publish a completed TIFF without replacing an existing destination.

    Hard linking is atomic and avoids copying large rasters. On filesystems
    such as network shares where it is unavailable, fall back to exclusive
    creation and remove any partial copy if an I/O error occurs.
    """
    try:
        os.link(staged, destination)
    except OSError as link_error:
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(f"Destination already exists: {destination}") from link_error
        created = False
        try:
            with destination.open("xb") as output:
                created = True
                with staged.open("rb") as raster:
                    shutil.copyfileobj(raster, output, length=8 * 1024 * 1024)
                output.flush()
                os.fsync(output.fileno())
        except OSError:
            if created:
                destination.unlink(missing_ok=True)
            raise


def create_missing_tif(source: Path, destination_dir: Path) -> Path:
    """Translate one missing FLT/ASC GeoTIFF without overwriting any existing output.

    The shared GDAL translator creates a temporary GeoTIFF beside the target.
    Publish the completed file using a no-clobber hard link when supported,
    with an exclusive-create copy fallback for filesystems without hard links.
    An interrupted or failed conversion never makes its source eligible for
    deletion. Only the subsequent independent raster comparison does that.
    """
    destination = destination_dir / f"{source.stem}.tif"
    if has_linked_component(source) or has_linked_component(destination):
        raise OSError("Source or output location contains a symlink or junction")
    destination_dir.mkdir(parents=True, exist_ok=True)
    if has_linked_component(destination):
        raise OSError("Output location contains a symlink or junction")
    existing, error = paired_file(destination_dir, source.stem, {".tif", ".tiff"})
    if existing is not None or error != "Matching file not found":
        raise FileExistsError(f"Cannot safely create {destination}: {error or existing}")
    if destination.exists() or destination.is_symlink():
        raise FileExistsError(f"Destination already exists: {destination}")

    staged = destination_dir / f".{source.stem}.{uuid4().hex}.conversion.tif"
    original = snapshot(source)
    header: FileSnapshot | None = None
    if source.suffix.casefold() == ".flt":
        hdr, hdr_error = paired_file(source.parent, source.stem, {".hdr"})
        if hdr_error not in (None, "Matching file not found"):
            raise OSError(f"Cannot inspect FLT header: {hdr_error}")
        if hdr is not None:
            header = snapshot(hdr)

    try:
        # Keep GDAL translation logic in one place; neither resample nor rewrite CRS.
        from ryan_library.functions.gdal.raster_processing import translate_to_geotiff

        translate_to_geotiff(source, staged, profile="tuflow")
        if not unchanged(original) or (header is not None and not unchanged(header)):
            raise RuntimeError("Source FLT/ASC or its header changed during conversion")

        # Do not publish a file when a counterpart appeared during conversion.
        existing, error = paired_file(destination_dir, source.stem, {".tif", ".tiff"})
        if existing is not None or error != "Matching file not found":
            raise FileExistsError(f"Counterpart appeared during conversion: {error or existing}")
        publish_staged_tif(staged, destination)
        print(f"CREATED: {destination}")
        return destination
    finally:
        staged.unlink(missing_ok=True)
        Path(f"{staged}.aux.xml").unlink(missing_ok=True)
        Path(f"{staged}.msk").unlink(missing_ok=True)


def inspect_source(
    source: Path,
    root: Path,
    tif_root: Path,
    progress: Progress | None = None,
    raster_task: TaskID | None = None,
    *,
    convert_missing: bool = False,
) -> Comparison:
    """Optionally create missing GeoTIFF and verify it against its source."""
    original = snapshot(source)
    destination_dir = tif_root / source.parent.relative_to(root)
    candidate, error = paired_file(destination_dir, source.stem, {".tif", ".tiff"})
    if candidate is None and convert_missing and error in {"Matching file not found", "Destination directory does not exist"}:
        try:
            candidate = create_missing_tif(source, destination_dir)
        except (OSError, ValueError, RuntimeError, RasterioError) as exc:
            return Comparison(original, None, "CONVERSION_FAILED", f"Could not create GeoTIFF: {exc}")
    if candidate is None:
        return Comparison(original, None, "MISSING_OR_AMBIGUOUS_TIF", error or "No GeoTIFF")

    converted = snapshot(candidate)
    if converted.mtime_ns <= original.mtime_ns:
        return Comparison(original, converted, "TIF_NOT_NEWER", "TIF modification time is not strictly newer")

    header: FileSnapshot | None = None
    if source.suffix.casefold() == ".flt" and DELETE_FLT_HEADERS:
        hdr, hdr_error = paired_file(source.parent, source.stem, {".hdr"})
        if hdr_error and hdr is None and hdr_error != "Matching file not found":
            return Comparison(original, converted, "ERROR", f"FLT header: {hdr_error}")
        if hdr is not None:
            header = snapshot(hdr)
            if converted.mtime_ns <= header.mtime_ns:
                return Comparison(original, converted, "TIF_NOT_NEWER", "TIF is not newer than its FLT header", header)

    try:
        difference, warning = compare_rasters(source, candidate, progress, raster_task)
    except (OSError, ValueError, RasterioError) as exc:
        return Comparison(original, converted, "ERROR", f"Raster read failed: {exc}")

    status = "MISMATCH" if difference is not None else "MATCH_CRS_UNVERIFIED" if warning else "MATCH"
    result = Comparison(original, converted, status, difference or warning or "", header)
    if result.eligible and not all(unchanged(item) for item in (original, converted, header) if item is not None):
        return Comparison(original, converted, "CHANGED_DURING_CHECK", "File metadata changed during comparison")
    return result


def unique_report_path(root: Path, stem: str) -> Path:
    """Avoid overwriting previous scan reports."""
    for index in range(10_000):
        suffix = "" if index == 0 else f"_{index:03}"
        candidate = root / f"{REPORT_PREFIX}_{stem}{suffix}.csv"
        if not candidate.exists():
            return candidate
    message = f"No available filename for {stem}"
    raise FileExistsError(message)


def write_csv(path: Path, headings: tuple[str, ...], rows: list[dict[str, str]]) -> None:
    """Create an independent CSV; never truncate an existing report."""
    with path.open("x", encoding="utf-8-sig", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=headings)
        writer.writeheader()
        writer.writerows(rows)


def write_reports(results: list[Comparison], output_dir: Path) -> tuple[Path, Path, Path]:
    """Write deletion candidates, actual failures, and CRS uncertainty separately."""
    candidates: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []
    warnings: list[dict[str, str]] = []
    for result in results:
        tif = str(result.tif.path) if result.tif else ""
        if result.eligible:
            candidates.append(
                {
                    "delete_path": str(result.source.path),
                    "retained_tif": tif,
                    "type": "raster",
                    "status": result.status,
                    "warning": result.reason,
                }
            )
            if result.header is not None:
                candidates.append(
                    {
                        "delete_path": str(result.header.path),
                        "retained_tif": tif,
                        "type": "flt_header",
                        "status": result.status,
                        "warning": result.reason,
                    }
                )
            if result.status == "MATCH_CRS_UNVERIFIED":
                warnings.append(
                    {"source": str(result.source.path), "tif": tif, "status": result.status, "reason": result.reason}
                )
        else:
            failures.append(
                {"source": str(result.source.path), "tif": tif, "status": result.status, "reason": result.reason}
            )
    proposal = unique_report_path(output_dir, "delete_candidates")
    errors = unique_report_path(output_dir, "errors")
    warning_report = unique_report_path(output_dir, "warnings")
    write_csv(proposal, ("delete_path", "retained_tif", "type", "status", "warning"), candidates)
    write_csv(errors, ("source", "tif", "status", "reason"), failures)
    write_csv(warning_report, ("source", "tif", "status", "reason"), warnings)
    return proposal, errors, warning_report


def perform_deletions(results: list[Comparison]) -> tuple[int, int]:
    """Delete only verified, unchanged sources; do not delete retained TIFs."""
    deleted = failed = 0
    for result in results:
        if not result.eligible or result.tif is None:
            continue
        checked = (result.source, result.tif, result.header)
        if not all(unchanged(item) for item in checked if item is not None):
            print(f"SKIPPED (changed since validation): {result.source.path}")
            failed += 1
            continue
        try:
            result.source.path.unlink()
            deleted += 1
            print(f"DELETED: {result.source.path}")
        except OSError as exc:
            failed += 1
            print(f"FAILED to delete {result.source.path}: {exc}")
            continue
        if result.header is not None:
            # Check again after the FLT removal. If it changed, leave it alone.
            if unchanged(result.header) and unchanged(result.tif):
                try:
                    result.header.path.unlink()
                    deleted += 1
                    print(f"DELETED: {result.header.path}")
                except OSError as exc:
                    failed += 1
                    print(f"FAILED to delete {result.header.path}: {exc}")
            else:
                failed += 1
                print(f"SKIPPED changed FLT header: {result.header.path}")
    return deleted, failed


def finish_deletion(results: list[Comparison], source_count: int, *, allow_delete: bool) -> int:
    """Require an explicit opt-in and final confirmation before any deletion."""
    matched = sum(result.eligible for result in results)
    if not allow_delete:
        print("Validation only. No files deleted. Use --delete and type DELETE to allow removal.")
        return 0 if matched == source_count else 1
    if not matched:
        print("No verified candidates to delete.")
        return 1 if source_count else 0
    try:
        answer = input("Type DELETE to remove verified FLT/ASC files (and applicable .hdr): ").strip()
    except EOFError:
        answer = ""
    if answer != "DELETE":
        print("Deletion cancelled.")
        return 1
    deleted, failed = perform_deletions(results)
    print(f"Files deleted: {deleted}; deletions skipped/failed: {failed}")
    return 1 if failed or matched != source_count else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT_DIR, help="Source directory to scan recursively")
    parser.add_argument("--tif-root", type=Path, default=TIF_ROOT_DIR, help="Matching TIF folder tree (same layout)")
    parser.add_argument("--delete", action="store_true", help="Allow deletion after typing DELETE")
    parser.add_argument("--convert-missing", action="store_true", help="Create missing GeoTIFFs before verification")
    parser.add_argument("--no-progress", action="store_true", help="Disable live raster progress")
    args = parser.parse_args(argv)

    root: Path = args.root.expanduser().absolute()
    tif_root: Path = args.tif_root.expanduser().absolute() if args.tif_root is not None else root
    if not root.is_dir() or has_linked_component(root):
        parser.error(f"Source directory is missing or contains a symlink/junction: {root}")
    if not tif_root.is_dir() or has_linked_component(tif_root):
        parser.error(f"TIFF directory is missing or contains a symlink/junction: {tif_root}")

    sources = discover_sources(root)
    print(f"Scanning {len(sources)} FLT/ASC raster(s) under {root}")
    results: list[Comparison] = []
    progress_enabled = SHOW_PROGRESS and not args.no_progress and sys.stdout.isatty()
    with Progress(
        TextColumn("{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        disable=not progress_enabled,
    ) as progress:
        file_task = progress.add_task("Rasters", total=len(sources))
        cell_task = progress.add_task("Raster rows", total=1)
        for source in sources:
            try:
                result = inspect_source(
                    source,
                    root,
                    tif_root,
                    progress if progress_enabled else None,
                    cell_task,
                    convert_missing=CONVERT_MISSING or args.convert_missing,
                )
            except OSError as exc:
                result = Comparison(FileSnapshot(source, -1, -1), None, "ERROR", f"File inspection failed: {exc}")
            results.append(result)
            progress.advance(file_task)
            if not result.eligible:
                print(f"{result.status}: {source}: {result.reason}")

    try:
        proposal, errors, warnings = write_reports(results, root)
    except OSError as exc:
        print(f"Could not write reports; deletion cancelled: {exc}", file=sys.stderr)
        return 2
    matched = sum(result.eligible for result in results)
    unverified_crs = sum(result.status == "MATCH_CRS_UNVERIFIED" for result in results)
    print(f"Verified raster contents: {matched}/{len(results)}; excluded: {len(results) - matched}")
    print(f"Matched with unverified CRS: {unverified_crs} (eligible; review warning report)")
    print(f"Deletion proposal: {proposal}")
    print(f"Error report: {errors}")
    print(f"CRS warning report: {warnings}")
    return finish_deletion(results, len(sources), allow_delete=DELETE_FILES or args.delete)


if __name__ == "__main__":
    raise SystemExit(main())
