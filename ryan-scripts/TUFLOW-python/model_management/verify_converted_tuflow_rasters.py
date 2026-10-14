"""Verify converted TUFLOW FLT/ASC rasters against newer GeoTIFFs.

The default is validation only. Every source is paired with a .tif or .tiff
having the same stem and relative directory. Compare raster cells exactly,
including NoData positions, dimensions, grid transform and CRS. No resampling
or floating-point tolerance is used. Only verified sources become deletion
candidates. The source FLT's .hdr is also eligible; .prj and auxiliary files
are always retained. File timestamps must show that the TIFF is strictly newer.

Edit ROOT_DIR/TIF_ROOT_DIR below or use --root/--tif-root. To delete, supply
--delete (or set DELETE_FILES) and then type DELETE at the confirmation prompt.
Do not run on active outputs; file metadata snapshots do not lock files.
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import rasterio
from rasterio.windows import Window
from rich.progress import BarColumn, Progress, TaskProgressColumn, TextColumn, TimeRemainingColumn

# ---------------------------------------------------------------------------
# User settings (CLI arguments may override the first three)
# ---------------------------------------------------------------------------
ROOT_DIR = Path.cwd()
TIF_ROOT_DIR: Path | None = None  # None means TIFs alongside FLT/ASC files
DELETE_FILES = False  # Verification/proposal only unless set True or --delete
DELETE_FLT_HEADERS = True  # Include .hdr paired with a verified .flt
SHOW_PROGRESS = True
MAX_WINDOW_CELLS = 1_000_000  # Approximate upper bound on cells per read
REPORT_PREFIX = "tuflow_raster_conversion"


@dataclass(frozen=True)
class FileSnapshot:
    path: Path
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class Comparison:
    source: FileSnapshot
    tif: FileSnapshot | None
    status: str
    reason: str
    header: FileSnapshot | None = None

    @property
    def eligible(self) -> bool:
        return self.status == "MATCH"


def snapshot(path: Path) -> FileSnapshot:
    """Record inexpensive metadata for a file without reading its contents."""
    stat = path.stat()
    return FileSnapshot(path=path, size=stat.st_size, mtime_ns=stat.st_mtime_ns)


def unchanged(record: FileSnapshot) -> bool:
    """Reject deletion when either file changed after validation."""
    try:
        current = snapshot(record.path)
    except OSError:
        return False
    return record == current and not record.path.is_symlink()


def discover_sources(root: Path) -> list[Path]:
    """Walk folders without following symlink directories; collect FLT/ASC only."""
    sources: list[Path] = []
    for parent, directories, files in os.walk(root, followlinks=False):
        directories[:] = sorted(
            name for name in directories if not (Path(parent) / name).is_symlink()
        )
        for name in sorted(files):
            path = Path(parent) / name
            if path.suffix.casefold() in {".asc", ".flt"} and not path.is_symlink():
                sources.append(path)
    return sorted(sources, key=lambda p: str(p).casefold())


def paired_file(directory: Path, stem: str, suffixes: set[str]) -> tuple[Path | None, str | None]:
    """Find an unambiguous companion, ignoring filename case."""
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
    if matches[0].is_symlink():
        return None, "Matching file is a symlink"
    return matches[0], None


def compare_rasters(source: Path, tif: Path, progress: Progress | None = None, task_id: int | None = None) -> str | None:
    """Return None if all valid pixel values and spatial metadata match exactly.

    A raster may be compressed or use another NoData *sentinel*, provided the
    invalid-pixel locations and all valid pixel values are identical.
    """
    with rasterio.open(source) as original, rasterio.open(tif) as converted:
        if (original.width, original.height, original.count) != (
            converted.width,
            converted.height,
            converted.count,
        ):
            return "Raster width, height or band count differs"
        if original.transform != converted.transform:
            return "Grid geotransform differs"
        if original.crs != converted.crs:
            return "CRS differs (including a missing CRS)"

        rows = max(1, min(original.height, MAX_WINDOW_CELLS // max(1, original.width)))
        if progress is not None and task_id is not None:
            progress.update(task_id, total=original.height * original.count, completed=0, description=source.name)

        for band in range(1, original.count + 1):
            for y in range(0, original.height, rows):
                height = min(rows, original.height - y)
                window = Window(0, y, original.width, height)
                before = original.read(band, window=window, masked=True)
                after = converted.read(band, window=window, masked=True)
                valid_before = ~np.ma.getmaskarray(before)
                valid_after = ~np.ma.getmaskarray(after)
                if not np.array_equal(valid_before, valid_after):
                    return f"NoData mask differs at band {band}, rows {y}-{y + height - 1}"

                left = np.asarray(before.data)
                right = np.asarray(after.data)
                equal = np.equal(left, right) | (np.isnan(left) & np.isnan(right))
                differing = valid_before & ~equal
                if np.any(differing):
                    local_row, column = np.argwhere(differing)[0]
                    return (
                        f"Pixel values differ at band {band}, row {y + int(local_row)}, column {int(column)}: "
                        f"{left[local_row, column]!r} != {right[local_row, column]!r}"
                    )
                if progress is not None and task_id is not None:
                    progress.advance(task_id, height)
    return None


def inspect_source(
    source: Path,
    root: Path,
    tif_root: Path,
    progress: Progress | None = None,
    raster_task: int | None = None,
) -> Comparison:
    """Verify a single source and the newer, same-stem GeoTIFF."""
    original = snapshot(source)
    destination_dir = tif_root / source.parent.relative_to(root)
    candidate, error = paired_file(destination_dir, source.stem, {".tif", ".tiff"})
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
                return Comparison(
                    original, converted, "TIF_NOT_NEWER", "TIF is not newer than its FLT header", header
                )

    try:
        difference = compare_rasters(source, candidate, progress, raster_task)
    except (OSError, ValueError, rasterio.errors.RasterioError) as exc:
        return Comparison(original, converted, "ERROR", f"Raster read failed: {exc}")

    result = Comparison(original, converted, "MATCH" if difference is None else "MISMATCH", difference or "", header)
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


def write_reports(results: list[Comparison], output_dir: Path) -> tuple[Path, Path]:
    """Write deletion candidate list and non-matches, including detailed reasons."""
    candidates: list[dict[str, str]] = []
    failures: list[dict[str, str]] = []
    for result in results:
        tif = str(result.tif.path) if result.tif else ""
        if result.eligible:
            candidates.append({"delete_path": str(result.source.path), "retained_tif": tif, "type": "raster"})
            if result.header is not None:
                candidates.append({"delete_path": str(result.header.path), "retained_tif": tif, "type": "flt_header"})
        else:
            failures.append(
                {"source": str(result.source.path), "tif": tif, "status": result.status, "reason": result.reason}
            )
    proposal = unique_report_path(output_dir, "delete_candidates")
    errors = unique_report_path(output_dir, "errors")
    write_csv(proposal, ("delete_path", "retained_tif", "type"), candidates)
    write_csv(errors, ("source", "tif", "status", "reason"), failures)
    return proposal, errors


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
    parser.add_argument("--no-progress", action="store_true", help="Disable live raster progress")
    args = parser.parse_args(argv)

    root: Path = args.root.expanduser().resolve()
    tif_root: Path = args.tif_root.expanduser().resolve() if args.tif_root is not None else root
    if not root.is_dir() or root.is_symlink():
        parser.error(f"Source directory is missing or a symlink: {root}")
    if not tif_root.is_dir() or tif_root.is_symlink():
        parser.error(f"TIFF directory is missing or a symlink: {tif_root}")

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
                result = inspect_source(source, root, tif_root, progress if progress_enabled else None, cell_task)
            except OSError as exc:
                result = Comparison(FileSnapshot(source, -1, -1), None, "ERROR", f"File inspection failed: {exc}")
            results.append(result)
            progress.advance(file_task)
            if not result.eligible:
                print(f"{result.status}: {source}: {result.reason}")

    try:
        proposal, errors = write_reports(results, root)
    except OSError as exc:
        print(f"Could not write reports; deletion cancelled: {exc}", file=sys.stderr)
        return 2
    matched = sum(result.eligible for result in results)
    print(f"Verified: {matched}/{len(results)}; excluded: {len(results) - matched}")
    print(f"Deletion proposal: {proposal}")
    print(f"Error report: {errors}")
    return finish_deletion(results, len(sources), allow_delete=DELETE_FILES or args.delete)


if __name__ == "__main__":
    raise SystemExit(main())
