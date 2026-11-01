"""Audit and optionally remove byte-identical files in TUFLOW check/checks folders.

Run from the intended result/model root, or configure ROOT_DIR below. Choose ONE
mode using the editable constants below (no command-line arguments required):

1. SCAN (default): RESTORE_FROM_MANIFEST = False and
   APPLY_PROPOSAL_MANIFEST = False. Read and hash candidate files, write a dated
   proposal CSV and TXT report. Set DRY_RUN = True to stop before deletion.
   With DRY_RUN = False, type DELETE to proceed after reviewing the proposal.
2. APPLY SAVED PROPOSAL: APPLY_PROPOSAL_MANIFEST = True (keep
   RESTORE_FROM_MANIFEST = False). Set
   PROPOSAL_MANIFEST_FILE to the exact CSV produced by an earlier SCAN (including
   any _001 suffix). No full folder scan or new SHA-256 hash pass occurs. The
   proposal must include original scan timestamps and total sizes, and all
   listed files must still match their recorded size, suffix and modification
   time. Both preflight and the deletion loop verify file metadata. This mode
   TRUSTS the proposal's original SHA-256 hashes: metadata checks cannot detect
   content changes made while preserving timestamps. Only apply proposals from
   a trusted source. DRY_RUN and DELETE confirmation still apply.
3. RESTORE: RESTORE_FROM_MANIFEST = True (keep APPLY_PROPOSAL_MANIFEST = False).
   Set MANIFEST_FILE to the recovery CSV created by the deletion run. Type
   RESTORE to recover missing originals; existing originals are never overwritten.

Setting both mode switches True is an error; no operation is performed.

All three output filenames default to a YYYY-MM-DD suffix using the local date
at script startup. Set filenames explicitly to use another run's files.
Proposal/report name collisions are numbered rather than overwritten. A
recovery manifest is never overwritten. Planned deletion and remaining sizes
are reported in decimal GB (1 GB = 1,000,000,000 bytes).

A .DUPLICATE_REMOVED.txt placeholder is an audit record, NOT a replacement for
a deleted file path. Deleting byte-identical rasters or GIS files may still
break TUFLOW/QGIS references; review all proposed paths before confirming.
"""

from __future__ import annotations

import csv
import os
import shutil
import sys
from collections import defaultdict
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from datetime import date
from functools import partial
from pathlib import Path
from time import monotonic

from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)

from ryan_library.functions.file_verification import (
    FileSnapshot,
    has_linked_component,
    snapshot_is_unchanged,
)
from ryan_library.functions.file_verification import hash_file as shared_hash_file

# delete duplicate files below this script in TUFLOW check folders.

# ---------------------------------------------------------------------------
# Hard-coded constants
# ---------------------------------------------------------------------------

ROOT_DIR = Path()
CHECK_FOLDER_NAMES = ("check", "checks")

RUN_DATE = date.today().isoformat()  # Local date, YYYY-MM-DD; shared by all three outputs.
REPORT_FILE = Path(f"duplicate_files_to_delete_{RUN_DATE}.txt")
MANIFEST_FILE = Path(f"duplicate_files_manifest_{RUN_DATE}.csv")
PROPOSAL_MANIFEST_FILE = Path(f"duplicate_files_proposed_{RUN_DATE}.csv")

HASH_CHUNK_SIZE = 1024 * 1024  # 1 MiB

# Rich byte-based hash progress; defaults to live updates in interactive terminals.
# Disable for quiet/headless operation. Does not affect hashing or deletion.
SHOW_HASH_PROGRESS = True
HASH_PROGRESS_REFRESH_PER_SECOND = 2.0

# True: generate/validate reports only, never delete or restore files.
# False: deletion still requires typing DELETE; restoration requires RESTORE.
DRY_RUN = False

# True  = compare duplicates across all discovered check folders.
# False = compare duplicates only within each individual check folder.
COMPARE_ACROSS_CHECK_FOLDERS = True

# Include raster and overview duplicates. Removing original filenames may break
# path-based GIS/TUFLOW references. Set False to exclude these files.
INCLUDE_RASTER_FILES = True
RASTER_SUFFIXES = {".tif", ".tiff", ".ovr"}

# Restore mode (highest priority). Set MANIFEST_FILE to the exact dated recovery
# CSV from the deletion run, then type RESTORE. Existing files are never replaced.
RESTORE_FROM_MANIFEST = False

# Apply mode (explicit opt-in). Reuse a previously saved proposal CSV with no
# whole-tree scan or SHA-256 pass. Set PROPOSAL_MANIFEST_FILE above to that EXACT
# filename, including a same-day _001 suffix if present. The file must contain
# scan-time modification timestamps; older proposal CSVs need a fresh SCAN.
# Paths/sizes/timestamps are rechecked, but original SHA-256 values are TRUSTED.
APPLY_PROPOSAL_MANIFEST = False

# Example:
#   deleted file: result_001.tif
#   placeholder:  result_001.tif.DUPLICATE_REMOVED.txt
#
# If multiple identical files are deleted from one folder, only one placeholder
# is written for that unique content hash in that folder.
PLACEHOLDER_SUFFIX = ".DUPLICATE_REMOVED.txt"


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FileRecord:
    check_folder: Path
    path: Path
    suffix: str
    size_bytes: int
    mtime_ns: int
    content_hash: str | None = None


@dataclass(frozen=True)
class DuplicateGroup:
    suffix: str
    size_bytes: int
    content_hash: str
    keep_file: Path
    delete_files: tuple[Path, ...]


@dataclass(frozen=True)
class DeleteAction:
    delete_file: Path
    keep_file: Path
    delete_mtime_ns: int
    keep_mtime_ns: int
    suffix: str
    size_bytes: int
    content_hash: str


@dataclass(frozen=True)
class PlaceholderAction:
    check_folder: Path
    placeholder_file: Path
    keep_file: Path
    deleted_files: tuple[Path, ...]
    suffix: str
    size_bytes: int
    content_hash: str


@dataclass(frozen=True)
class ScanTotals:
    """Original scan scope, retained in the proposal for later apply reports."""

    file_count: int
    total_bytes: int


def storage_summary_lines(totals: ScanTotals, actions: list[DeleteAction], *, actual: bool = False) -> list[str]:
    """Return decimal-GB deletion and remaining estimates for the original scan scope.

    For APPLY, totals refer to the earlier proposal scan, not a new disk scan.
    Deleted bytes are computed from actions, not a second read of file contents.
    """
    delete_bytes = sum(action.size_bytes for action in actions)
    remaining_bytes = max(0, totals.total_bytes - delete_bytes)
    fraction = 100 * delete_bytes / totals.total_bytes if totals.total_bytes else 0.0
    verb = "Actually deleted" if actual else "Proposed for deletion"

    def as_gb(value: int) -> str:
        return f"{value / 1_000_000_000:.3f} GB ({value:,} bytes)"

    return [
        f"Total at original scan: {as_gb(totals.total_bytes)} across {totals.file_count:,} files",
        f"{verb}: {as_gb(delete_bytes)} across {len(actions):,} files ({fraction:.1f}% of original total)",
        f"Estimated remaining: {as_gb(remaining_bytes)} ({100.0 - fraction:.1f}% of original total)",
    ]


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------


def absolute_path(path: Path) -> Path:
    """Return an absolute path without requiring the path to exist.

    Works for normal drives and UNC paths.
    """
    return path.expanduser().absolute()


def safe_display_path(path: Path) -> str:
    """Return a readable path string.

    Do not use Path.as_posix() because it can make Windows and UNC paths less clear.
    """
    return str(path)


def relative_or_absolute_path_from_placeholder_to_kept_file(
    placeholder_file: Path,
    keep_file: Path,
) -> str:
    """Return the kept file path relative to the placeholder file's folder where possible.

    Handles:
    - normal local drives
    - UNC paths
    - same drive / same UNC share
    - different drive / different UNC share

    On Windows, os.path.relpath raises ValueError when paths are on different
    drives. In that case, this returns the absolute kept-file path instead.
    """
    placeholder_folder = absolute_path(placeholder_file.parent)
    kept_file_absolute = absolute_path(keep_file)

    try:
        return os.path.relpath(
            kept_file_absolute,
            start=placeholder_folder,
        )
    except ValueError:
        return safe_display_path(kept_file_absolute)


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------


def find_check_folders(root_dir: Path) -> list[Path]:
    """Find check/checks directories without stat-ing every file under the root."""
    root_dir = absolute_path(root_dir)
    targets = {name.casefold() for name in CHECK_FOLDER_NAMES}
    matches = [
        parent / name
        for parent, directory_names, _ in root_dir.walk()
        for name in directory_names
        if name.casefold() in targets
    ]
    return sorted(matches, key=lambda path: str(path).casefold())


def is_placeholder_file(path: Path) -> bool:
    """Return True if path is one of this script's placeholder files."""
    return path.name.endswith(PLACEHOLDER_SUFFIX)


def iter_files_in_check_folders(check_folders: list[Path]) -> list[FileRecord]:
    """List each path once, assigning nested check files to their closest check folder.

    A parent check folder's recursive scan also visits nested check/checks folders.
    Without this guard, one file can appear twice and be labelled a duplicate of itself.
    """
    records: list[FileRecord] = []
    seen_paths: set[Path] = set()

    # Scan the most specific check folders first so nested files belong to them.
    for check_folder in sorted(check_folders, key=lambda folder: (-len(folder.parts), str(folder).casefold())):
        resolved_check_folder = absolute_path(check_folder)

        for path in sorted(resolved_check_folder.rglob("*"), key=lambda p: str(p).lower()):
            if not path.is_file():
                continue

            file_path = absolute_path(path)
            if file_path in seen_paths:
                continue
            seen_paths.add(file_path)

            if is_placeholder_file(path):
                continue
            if not INCLUDE_RASTER_FILES and (
                path.suffix.lower() in RASTER_SUFFIXES or path.name.lower().endswith((".tif.aux.xml", ".tiff.aux.xml"))
            ):
                continue

            file_stat = path.stat()
            records.append(
                FileRecord(
                    check_folder=resolved_check_folder,
                    path=file_path,
                    suffix=path.suffix.lower(),
                    size_bytes=file_stat.st_size,
                    mtime_ns=file_stat.st_mtime_ns,
                )
            )

    return sorted(records, key=lambda r: str(r.path).lower())


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------


def file_hash(path: Path, *, on_chunk: Callable[[int], None] | None = None) -> str:
    """Hash a file using SHA-256; optionally report bytes read during the same pass."""
    return shared_hash_file(path, chunk_size=HASH_CHUNK_SIZE, on_chunk=on_chunk).hex()


def matches_snapshot(path: Path, *, size_bytes: int, mtime_ns: int) -> bool:
    """Compare inexpensive metadata with the original scan; never rehash."""
    record = FileSnapshot(path=path, size=size_bytes, mtime_ns=mtime_ns)
    return not has_linked_component(path) and snapshot_is_unchanged(record)


def hash_progress_display() -> Progress:
    """Build a byte-weighted Rich display, as used by other repo dashboards."""
    return Progress(
        TextColumn("[cyan]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
        TextColumn("{task.fields[files_done]}/{task.fields[files_total]} files"),
        refresh_per_second=HASH_PROGRESS_REFRESH_PER_SECOND,
        transient=False,
        disable=not sys.stdout.isatty(),
    )


def hash_candidate_record(record: FileRecord, *, on_chunk: Callable[[int], None] | None = None) -> FileRecord:
    """Hash a candidate once and reject it if the source changed during that read."""
    digest = file_hash(record.path, on_chunk=on_chunk) if on_chunk is not None else file_hash(record.path)
    if not matches_snapshot(record.path, size_bytes=record.size_bytes, mtime_ns=record.mtime_ns):
        print(f"Changed during hashing, excluded from deletion: {record.path}")
        return record
    return FileRecord(
        check_folder=record.check_folder,
        path=record.path,
        suffix=record.suffix,
        size_bytes=record.size_bytes,
        mtime_ns=record.mtime_ns,
        content_hash=digest,
    )


def add_hashes_to_candidates(records: list[FileRecord]) -> list[FileRecord]:
    """Hash only same-suffix, same-size candidates and report byte-level progress.

    Counts exclude files with no possible size/suffix match. Updates occur within
    the original read loop: never read the files a second time for progress.
    """
    by_size: dict[object, list[FileRecord]] = defaultdict(list)
    for record in records:
        if COMPARE_ACROSS_CHECK_FOLDERS:
            key = (record.suffix, record.size_bytes)
        else:
            key = (record.check_folder, record.suffix, record.size_bytes)
        by_size[key].append(record)

    candidates = [record for group in by_size.values() if len(group) > 1 for record in group]
    total_bytes = sum(record.size_bytes for record in candidates)
    total_files = len(candidates)
    output: list[FileRecord] = []
    completed_files = 0
    started = monotonic()

    progress = hash_progress_display() if SHOW_HASH_PROGRESS and candidates else None
    with progress if progress is not None else nullcontext():
        task_id = (
            progress.add_task("SHA-256", total=total_bytes, files_done=0, files_total=total_files)
            if progress is not None
            else None
        )
        for group in by_size.values():
            if len(group) == 1:
                output.extend(group)
                continue
            for record in group:
                if progress is not None and task_id is not None:
                    name = record.path.name
                    short_name = name if len(name) <= 32 else name[:29] + "..."
                    progress.update(task_id, description=short_name)
                    output.append(hash_candidate_record(record, on_chunk=partial(progress.advance, task_id)))
                else:
                    output.append(hash_candidate_record(record))
                completed_files += 1
                if progress is not None and task_id is not None:
                    progress.update(task_id, files_done=completed_files)

        if progress is not None and task_id is not None:
            progress.update(task_id, description="SHA-256 complete", refresh=True)

    if candidates:
        print(
            f"SHA-256 complete: {completed_files}/{total_files} candidate files, "
            f"{total_bytes / (1024**3):.2f} GiB in {monotonic() - started:.1f}s."
        )
    return sorted(output, key=lambda record: str(record.path).lower())


def find_duplicate_groups(records: list[FileRecord]) -> list[DuplicateGroup]:
    """Find duplicate files by suffix, file size, then content hash.

    The first file alphabetically is kept.
    """
    by_hash: dict[object, list[FileRecord]] = defaultdict(list)

    for record in records:
        if record.content_hash is None:
            continue
        if COMPARE_ACROSS_CHECK_FOLDERS:
            key = (record.suffix, record.size_bytes, record.content_hash)
        else:
            key = (record.check_folder, record.suffix, record.size_bytes, record.content_hash)
        by_hash[key].append(record)

    duplicate_groups: list[DuplicateGroup] = []

    for group in by_hash.values():
        if len(group) < 2:
            continue

        sorted_group = sorted(group, key=lambda r: str(r.path).lower())

        keep_file = sorted_group[0].path
        delete_files = tuple(record.path for record in sorted_group[1:])

        duplicate_groups.append(
            DuplicateGroup(
                suffix=sorted_group[0].suffix,
                size_bytes=sorted_group[0].size_bytes,
                content_hash=sorted_group[0].content_hash or "",
                keep_file=keep_file,
                delete_files=delete_files,
            )
        )

    return sorted(duplicate_groups, key=lambda g: str(g.keep_file).lower())


def build_delete_actions(duplicate_groups: list[DuplicateGroup], records: list[FileRecord]) -> list[DeleteAction]:
    """Build delete actions with the metadata recorded during initial scan."""
    actions: list[DeleteAction] = []
    by_path = {record.path: record for record in records}

    for group in duplicate_groups:
        for delete_file in group.delete_files:
            if delete_file == group.keep_file:
                message = f"Refusing to delete a file that is also its retained copy: {delete_file}"
                raise ValueError(message)
            actions.append(
                DeleteAction(
                    delete_file=delete_file,
                    keep_file=group.keep_file,
                    delete_mtime_ns=by_path[delete_file].mtime_ns,
                    keep_mtime_ns=by_path[group.keep_file].mtime_ns,
                    suffix=group.suffix,
                    size_bytes=group.size_bytes,
                    content_hash=group.content_hash,
                )
            )

    return sorted(actions, key=lambda action: str(action.delete_file).lower())


# ---------------------------------------------------------------------------
# Placeholder handling
# ---------------------------------------------------------------------------


def build_check_folder_lookup(records: list[FileRecord]) -> dict[Path, Path]:
    """Map each scanned file to its containing check folder."""
    return {record.path: record.check_folder for record in records}


def should_write_placeholder(
    action: DeleteAction,
    check_folder_by_file: dict[Path, Path],
) -> bool:
    """Return True when a deleted file belongs to a discovered check folder."""
    return action.delete_file in check_folder_by_file


def placeholder_path_for_representative_deleted_file(delete_file: Path) -> Path:
    """Return the placeholder path based on a representative deleted file.

    Example:
        result_001.tif -> result_001.tif.DUPLICATE_REMOVED.txt

    If many identical files are deleted from the same folder, only the first
    alphabetically is used to name the placeholder.
    """
    return delete_file.with_name(delete_file.name + PLACEHOLDER_SUFFIX)


def build_placeholder_actions(
    actions: list[DeleteAction],
    records: list[FileRecord],
) -> list[PlaceholderAction]:
    """Build one placeholder action per unique deleted content per check folder.

    This prevents writing one placeholder per deleted filename.

    Identical files deleted within the same folder also get a placeholder.
    Different content hashes in a folder get different placeholders.
    """
    check_folder_by_file = build_check_folder_lookup(records)

    grouped: dict[tuple[Path, str, int, str, Path], list[DeleteAction]] = defaultdict(list)

    for action in actions:
        if not should_write_placeholder(action, check_folder_by_file):
            continue

        delete_check_folder = check_folder_by_file[action.delete_file]

        key = (
            delete_check_folder,
            action.suffix,
            action.size_bytes,
            action.content_hash,
            action.keep_file,
        )
        grouped[key].append(action)

    placeholder_actions: list[PlaceholderAction] = []

    for (
        check_folder,
        suffix,
        size_bytes,
        content_hash,
        keep_file,
    ), grouped_actions in grouped.items():
        sorted_actions = sorted(grouped_actions, key=lambda a: str(a.delete_file).lower())
        deleted_files = tuple(action.delete_file for action in sorted_actions)

        representative_deleted_file = deleted_files[0]
        placeholder_file = placeholder_path_for_representative_deleted_file(representative_deleted_file)

        placeholder_actions.append(
            PlaceholderAction(
                check_folder=check_folder,
                placeholder_file=placeholder_file,
                keep_file=keep_file,
                deleted_files=deleted_files,
                suffix=suffix,
                size_bytes=size_bytes,
                content_hash=content_hash,
            )
        )

    return sorted(
        placeholder_actions,
        key=lambda a: str(a.placeholder_file).lower(),
    )


def placeholder_text(action: PlaceholderAction) -> str:
    """Create placeholder file text."""
    kept_file_reference = relative_or_absolute_path_from_placeholder_to_kept_file(
        placeholder_file=action.placeholder_file,
        keep_file=action.keep_file,
    )

    lines: list[str] = [
        "Duplicate file content removed.",
        "",
        "The following original files were deleted because their content",
        "was identical to a retained file (in this or another check folder).",
        "This record does not preserve the original paths for GIS references.",
        "",
        "Remaining file reference:",
        f"  {kept_file_reference}",
        "",
        f"Deleted files in this group: {len(action.deleted_files)}",
        "",
        "Duplicate content details:",
        f"  Suffix: {action.suffix or '[no suffix]'}",
        f"  Size: {action.size_bytes:,} bytes",
        f"  SHA-256: {action.content_hash}",
        "",
        "Deleted file name(s) from this folder:",
    ]

    for deleted_file in action.deleted_files:
        lines.append(f"  - {deleted_file.name}")

    lines.extend(
        [
            "",
            "Note:",
            "  This path is relative where possible.",
            "  If the remaining file is on another drive or UNC share, an absolute path is used.",
            "",
        ]
    )

    return "\n".join(lines)


def write_placeholder_file(action: PlaceholderAction) -> bool:
    """Write one placeholder file.

    Returns True if written successfully.
    """
    text = placeholder_text(action)

    if DRY_RUN:
        print(f"DRY RUN - would write placeholder: {action.placeholder_file}")
        return True

    try:
        with action.placeholder_file.open("x", encoding="utf-8") as file:
            file.write(text)
        print(f"Wrote placeholder: {action.placeholder_file}")
        return True
    except FileExistsError:
        if action.placeholder_file.read_text(encoding="utf-8") == text:
            print(f"Placeholder already correct: {action.placeholder_file}")
            return True
        print(f"Existing placeholder differs; not overwriting: {action.placeholder_file}")
        return False
    except OSError as error:
        print(f"Failed to write placeholder {action.placeholder_file}: {error}")
        return False


def write_placeholder_files(placeholder_actions: list[PlaceholderAction]) -> None:
    """Write placeholder files."""
    for action in placeholder_actions:
        write_placeholder_file(action)


# ---------------------------------------------------------------------------
# Deletion manifest and restoration
# ---------------------------------------------------------------------------

MANIFEST_FIELDS: tuple[str, ...] = ("original_path", "kept_path", "suffix", "size_bytes", "sha256", "status")
# Additional scan-time evidence is required for safe no-rehash proposal application.
# Recovery manifests intentionally keep their original six-column schema.
PROPOSAL_FIELDS: tuple[str, ...] = (
    *MANIFEST_FIELDS,
    "delete_mtime_ns",
    "keep_mtime_ns",
    "scan_file_count",
    "scan_total_bytes",
)


def manifest_rows(
    actions: list[DeleteAction],
    *,
    successful_actions: list[DeleteAction] | None = None,
    status: str = "planned",
) -> list[dict[str, str]]:
    """Create an explicit old-path -> retained-path mapping for every deletion."""
    successful_paths = {action.delete_file for action in successful_actions or []}
    rows: list[dict[str, str]] = []
    for action in actions:
        action_status = status
        if successful_actions is not None:
            action_status = "deleted" if action.delete_file in successful_paths else "failed"
        rows.append(
            {
                "original_path": str(absolute_path(action.delete_file)),
                "kept_path": str(absolute_path(action.keep_file)),
                "suffix": action.suffix,
                "size_bytes": str(action.size_bytes),
                "sha256": action.content_hash,
                "status": action_status,
            }
        )
    return rows


def write_manifest(path: Path, rows: list[dict[str, str]], *, exclusive: bool = False) -> None:
    """Persist recovery information before any destructive changes."""
    path = absolute_path(path)
    with path.open("x" if exclusive else "w", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=MANIFEST_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def available_output_path(path: Path, index: int) -> Path:
    """Give each new report/proposal a distinct name without replacing prior evidence."""
    path = absolute_path(path)
    if index == 0:
        return path
    return path.with_name(f"{path.stem}_{index:03d}{path.suffix}")


def write_proposal_manifest(path: Path, actions: list[DeleteAction], totals: ScanTotals) -> Path:
    """Write a reusable proposal with original scan times and total scanned bytes.

    Metadata is repeated per action so the CSV remains a standalone artifact.
    An older six-column proposal cannot be applied without a fresh scan.
    """
    rows = [
        {
            **row,
            "delete_mtime_ns": str(action.delete_mtime_ns),
            "keep_mtime_ns": str(action.keep_mtime_ns),
            "scan_file_count": str(totals.file_count),
            "scan_total_bytes": str(totals.total_bytes),
        }
        for row, action in zip(manifest_rows(actions), actions, strict=True)
    ]
    for index in range(10000):
        candidate = available_output_path(path, index)
        try:
            with candidate.open("x", newline="", encoding="utf-8") as output:
                writer = csv.DictWriter(output, fieldnames=PROPOSAL_FIELDS)
                writer.writeheader()
                writer.writerows(rows)
        except FileExistsError:
            continue
        return candidate
    message = f"Too many existing proposal manifests for {path}"
    raise FileExistsError(message)


def update_manifest(path: Path, rows: list[dict[str, str]]) -> None:
    """Replace a manifest after deleting/restoring without leaving a partial CSV."""
    path = absolute_path(path)
    temp_path = path.with_name(path.name + ".tmp")
    try:
        write_manifest(temp_path, rows, exclusive=True)
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def read_manifest(path: Path) -> list[dict[str, str]]:
    """Load and validate the recovery manifest's required fields."""
    with absolute_path(path).open(newline="", encoding="utf-8") as source:
        reader = csv.DictReader(source)
        if reader.fieldnames is None or not set(MANIFEST_FIELDS).issubset(reader.fieldnames):
            message = "The manifest is missing required recovery columns."
            raise ValueError(message)
        rows: list[dict[str, str]] = []
        for raw_row in reader:
            row: dict[str, str] = {}
            for field in MANIFEST_FIELDS:
                value = raw_row.get(field)
                if not isinstance(value, str):
                    message = f"Invalid manifest field: {field}"
                    raise ValueError(message)
                row[field] = value
            for field in PROPOSAL_FIELDS[len(MANIFEST_FIELDS) :]:
                if field in reader.fieldnames:
                    value = raw_row.get(field)
                    if not isinstance(value, str):
                        message = f"Invalid proposal field: {field}"
                        raise ValueError(message)
                    row[field] = value
            if row["original_path"]:
                rows.append(row)
        return rows


def matches_record(path: Path, *, size_bytes: int, content_hash: str) -> bool:
    """Verify that the file still matches the scanned content before mutation."""
    try:
        return path.is_file() and path.stat().st_size == size_bytes and file_hash(path) == content_hash
    except OSError:
        return False


def check_folder_for_proposal_path(path: Path) -> Path:
    """Check real containment and return the nearest check/checks parent.

    A proposal is editable input. Canonicalise ONLY for the security check so
    embedded '..' components, links and junctions cannot escape ROOT_DIR.
    The original path spelling is retained for reporting and actual operations.
    """
    root = absolute_path(ROOT_DIR).resolve(strict=True)
    actual_path = path.resolve(strict=True)
    if not actual_path.is_relative_to(root):
        message = f"Proposal file is outside ROOT_DIR: {path}"
        raise ValueError(message)
    names = {name.casefold() for name in CHECK_FOLDER_NAMES}
    for parent in path.parents:
        if parent.name.casefold() not in names:
            continue
        actual_parent = parent.resolve(strict=True)
        # A lexical 'check/..' parent is not evidence that the target is
        # physically inside that check folder.
        if actual_parent.is_relative_to(root) and actual_path.is_relative_to(actual_parent):
            return parent
    message = f"Proposal file is not inside a check/checks folder: {path}"
    raise ValueError(message)


def actions_from_proposal(path: Path) -> tuple[list[DeleteAction], list[FileRecord], ScanTotals]:  # noqa: C901
    """Rebuild a trusted, unchanged proposal with no SHA-256 or full folder scan.

    Reject legacy proposals without scan-time mtimes/totals; keeping the same
    size alone is insufficient to establish that a candidate was unchanged.
    """
    rows = read_manifest(path)
    if not rows:
        return [], [], ScanTotals(file_count=0, total_bytes=0)
    actions: list[DeleteAction] = []
    records_by_path: dict[Path, FileRecord] = {}
    seen_originals: set[Path] = set()
    seen_retained: set[Path] = set()
    # A retained file may appear on multiple rows; every row must agree about
    # its identity, contents and original metadata before anything is deleted.
    retained_evidence: dict[Path, tuple[int, int, str, str]] = {}
    totals: ScanTotals | None = None
    metadata = PROPOSAL_FIELDS[len(MANIFEST_FIELDS) :]

    for row in rows:
        if row["status"] != "planned":
            message = f"Proposal row is not planned: {row['original_path']}"
            raise ValueError(message)
        if any(field not in row for field in metadata):
            message = "Proposal lacks original modification times / totals. Run a new SCAN first."
            raise ValueError(message)

        current_totals = ScanTotals(file_count=int(row["scan_file_count"]), total_bytes=int(row["scan_total_bytes"]))
        if totals is None:
            totals = current_totals
        elif totals != current_totals:
            message = "Proposal has inconsistent original scan totals."
            raise ValueError(message)
        if current_totals.file_count < 0 or current_totals.total_bytes < 0:
            message = "Proposal has negative scan totals."
            raise ValueError(message)

        original = absolute_path(Path(row["original_path"]))
        kept = absolute_path(Path(row["kept_path"]))
        # Compare physical identities, not spellings: paths with '..' can
        # otherwise name a retained file as its own deletion target.
        canonical_original = original.resolve(strict=True)
        canonical_kept = kept.resolve(strict=True)
        if canonical_original == canonical_kept or canonical_original in seen_originals:
            message = f"Proposal has a self-reference or duplicate deletion path: {original}"
            raise ValueError(message)
        seen_originals.add(canonical_original)
        seen_retained.add(canonical_kept)

        size_bytes = int(row["size_bytes"])
        delete_mtime_ns = int(row["delete_mtime_ns"])
        keep_mtime_ns = int(row["keep_mtime_ns"])
        suffix = row["suffix"]
        content_hash = row["sha256"].casefold()
        if size_bytes < 0 or len(content_hash) != 64 or any(c not in "0123456789abcdef" for c in content_hash):
            message = f"Proposal has an invalid size or SHA-256 value: {original}"
            raise ValueError(message)
        if has_linked_component(original) or has_linked_component(kept):
            message = f"Proposal contains a symlink, junction or linked file: {original} -> {kept}"
            raise ValueError(message)
        original_check = check_folder_for_proposal_path(original)
        kept_check = check_folder_for_proposal_path(kept)

        original_stat = original.stat()
        kept_stat = kept.stat()
        if (
            original_stat.st_size != size_bytes
            or kept_stat.st_size != size_bytes
            or original_stat.st_mtime_ns != delete_mtime_ns
            or kept_stat.st_mtime_ns != keep_mtime_ns
            or original.suffix.lower() != suffix
            or kept.suffix.lower() != suffix
        ):
            message = f"Proposal file changed since the original scan: {original} -> {kept}"
            raise ValueError(message)

        retained_fingerprint = (size_bytes, keep_mtime_ns, suffix, content_hash)
        previous_fingerprint = retained_evidence.setdefault(canonical_kept, retained_fingerprint)
        if previous_fingerprint != retained_fingerprint:
            message = f"Proposal has contradictory retained-file evidence: {kept}"
            raise ValueError(message)

        records_by_path[canonical_original] = FileRecord(
            check_folder=original_check,
            path=original,
            suffix=suffix,
            size_bytes=size_bytes,
            mtime_ns=delete_mtime_ns,
            content_hash=content_hash,
        )
        records_by_path.setdefault(
            canonical_kept,
            FileRecord(
                check_folder=kept_check,
                path=kept,
                suffix=suffix,
                size_bytes=size_bytes,
                mtime_ns=keep_mtime_ns,
                content_hash=content_hash,
            ),
        )
        actions.append(
            DeleteAction(
                delete_file=original,
                keep_file=kept,
                delete_mtime_ns=delete_mtime_ns,
                keep_mtime_ns=keep_mtime_ns,
                suffix=suffix,
                size_bytes=size_bytes,
                content_hash=content_hash,
            )
        )

    if seen_originals & seen_retained:
        message = "Proposal would delete a file used as a retained copy."
        raise ValueError(message)
    if totals is None:
        message = "Missing scan totals in proposal."
        raise ValueError(message)
    if totals.file_count < len(records_by_path) or totals.total_bytes < sum(
        record.size_bytes for record in records_by_path.values()
    ):
        message = "Proposal scan totals are smaller than the listed files."
        raise ValueError(message)
    return sorted(actions, key=lambda action: str(action.delete_file).lower()), list(records_by_path.values()), totals


def duplicate_groups_from_actions(actions: list[DeleteAction]) -> list[DuplicateGroup]:
    """Group proposal actions for report and placeholder generation."""
    grouped: dict[tuple[str, int, str, Path], list[Path]] = defaultdict(list)
    for action in actions:
        grouped[(action.suffix, action.size_bytes, action.content_hash, action.keep_file)].append(action.delete_file)
    groups = [
        DuplicateGroup(
            suffix=suffix,
            size_bytes=size_bytes,
            content_hash=content_hash,
            keep_file=keep_file,
            delete_files=tuple(sorted(delete_files, key=lambda item: str(item).lower())),
        )
        for (suffix, size_bytes, content_hash, keep_file), delete_files in grouped.items()
    ]
    return sorted(groups, key=lambda group: str(group.keep_file).lower())


def restore_manifest_entry(row: dict[str, str]) -> None:
    """Restore a single missing original without overwriting another writer's path."""
    original = Path(row["original_path"])
    retained = Path(row["kept_path"])
    size_bytes = int(row["size_bytes"])
    if not matches_record(retained, size_bytes=size_bytes, content_hash=row["sha256"]):
        message = f"Retained file missing or changed: {retained}"
        raise ValueError(message)

    original.parent.mkdir(parents=True, exist_ok=True)
    created = False
    try:
        with retained.open("rb") as source, original.open("xb") as destination:
            created = True
            shutil.copyfileobj(source, destination, length=HASH_CHUNK_SIZE)
        if not matches_record(original, size_bytes=size_bytes, content_hash=row["sha256"]):
            message = f"Restored file failed hash verification: {original}"
            raise ValueError(message)
    except Exception:
        # Never remove a file created by another process if exclusive open failed.
        if created:
            original.unlink(missing_ok=True)
        raise


def restore_files_from_manifest(path: Path) -> tuple[int, int]:
    """Restore missing original paths listed in a completed or pending manifest."""
    rows = read_manifest(path)
    candidates = [row for row in rows if row["status"] in {"deleted", "pending"}]
    print(f"Recovery manifest: {absolute_path(path)}")
    print(f"Eligible entries: {len(candidates)}")
    if DRY_RUN:
        for row in candidates:
            original = Path(row["original_path"])
            if not original.exists():
                print(f"DRY RUN - would restore: {original} <- {row['kept_path']}")
        return 0, 0

    print("Restoration copies verified retained content back to missing original filenames.")
    print("Type RESTORE to continue; existing files will never be overwritten.")
    if input("> ").strip() != "RESTORE":
        print("Restoration cancelled.")
        return 0, 0

    restored = 0
    failed = 0
    for row in candidates:
        original = Path(row["original_path"])
        if original.exists():
            print(f"Existing original, skipped: {original}")
            continue
        try:
            restore_manifest_entry(row)
            row["status"] = "restored"
            restored += 1
            print(f"Restored: {original}")
        except (OSError, ValueError) as error:
            failed += 1
            print(f"Failed to restore {original}: {error}")
    update_manifest(path, rows)
    print(f"Restored: {restored}; failed: {failed}")
    return restored, failed


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def write_report(
    duplicate_groups: list[DuplicateGroup],
    actions: list[DeleteAction],
    placeholder_actions: list[PlaceholderAction],
    report_file: Path,
    proposal_file: Path,
    totals: ScanTotals,
) -> Path:
    """Write a human-readable report of proposed deletions and remaining space."""
    lines: list[str] = [
        "Duplicate file report",
        "=" * 80,
        "",
        f"Root folder: {absolute_path(ROOT_DIR)}",
        f"Compare across check folders: {COMPARE_ACROSS_CHECK_FOLDERS}",
        f"Duplicate groups found: {len(duplicate_groups)}",
        f"Files marked for deletion: {len(actions)}",
        f"Placeholder files to write: {len(placeholder_actions)}",
        f"Proposal CSV manifest: {absolute_path(proposal_file)}",
        "",
        "Storage summary (original scan; decimal GB):",
        *storage_summary_lines(totals, actions),
        "",
        "Placeholder policy:",
        "  One placeholder per unique deleted content per affected check folder.",
        "  The kept copy may be in the same folder; original paths will disappear.",
        "",
    ]

    for index, group in enumerate(duplicate_groups, start=1):
        lines.extend(
            [
                "-" * 80,
                f"Group {index}",
                f"Suffix: {group.suffix or '[no suffix]'}",
                f"Size: {group.size_bytes:,} bytes",
                f"SHA-256: {group.content_hash}",
                "",
                f"KEEP:   {group.keep_file}",
                "DELETE:",
            ]
        )

        for delete_file in group.delete_files:
            lines.append(f"        {delete_file}")

        lines.append("")

    if placeholder_actions:
        lines.extend(
            [
                "=" * 80,
                "Placeholder files",
                "=" * 80,
                "",
            ]
        )

        for index, action in enumerate(placeholder_actions, start=1):
            kept_file_reference = relative_or_absolute_path_from_placeholder_to_kept_file(
                placeholder_file=action.placeholder_file,
                keep_file=action.keep_file,
            )

            lines.extend(
                [
                    "-" * 80,
                    f"Placeholder {index}",
                    f"Folder: {action.check_folder}",
                    f"Placeholder: {action.placeholder_file}",
                    f"Kept file reference: {kept_file_reference}",
                    f"SHA-256: {action.content_hash}",
                    "Deleted file name(s):",
                ]
            )

            for deleted_file in action.deleted_files:
                lines.append(f"        {deleted_file.name}")

            lines.append("")

    for index in range(10000):
        candidate = available_output_path(report_file, index)
        try:
            with candidate.open("x", encoding="utf-8") as output:
                output.write("\n".join(lines))
        except FileExistsError:
            continue
        return candidate
    message = f"Too many existing reports for {report_file}"
    raise FileExistsError(message)


def print_summary(
    check_folders: list[Path],
    records: list[FileRecord],
    duplicate_groups: list[DuplicateGroup],
    actions: list[DeleteAction],
    placeholder_actions: list[PlaceholderAction],
    report_file: Path,
    proposal_file: Path,
    totals: ScanTotals,
) -> None:
    """Print scan counts, file paths, and planned/remaining storage sizes."""
    print()
    print("Summary")
    print("=" * 80)
    print(f"Root folder:                    {absolute_path(ROOT_DIR)}")
    print(f"Check folders found:             {len(check_folders)}")
    print(f"Files scanned:                   {len(records)}")
    print(f"Compare across check folders:     {COMPARE_ACROSS_CHECK_FOLDERS}")
    print(f"Include raster files:            {INCLUDE_RASTER_FILES}")
    print(f"Duplicate groups found:          {len(duplicate_groups)}")
    print(f"Files proposed for deletion:     {len(actions)}")
    print(f"Placeholder files to write:      {len(placeholder_actions)}")
    print(f"Report written to:               {absolute_path(report_file)}")
    print(f"Proposal manifest:                {absolute_path(proposal_file)}")
    print(f"Recovery manifest:                {absolute_path(MANIFEST_FILE)}")
    for line in storage_summary_lines(totals, actions):
        print(line)
    print()


# ---------------------------------------------------------------------------
# Deletion
# ---------------------------------------------------------------------------


def confirm_deletion() -> bool:
    """Ask user to confirm deletion."""
    print("Deletion requires confirmation.")
    print("Type DELETE to delete duplicate files and write placeholders.")
    response = input("> ").strip()

    return response == "DELETE"


def delete_duplicate_file(action: DeleteAction) -> bool:
    """Delete one duplicate file.

    Returns True only if the file was actually deleted.
    """
    path = action.delete_file
    # This inexpensive guard also catches aliases containing '..' (and
    # case-only differences on Windows). Proposal mode checks resolved
    # identities separately before any deletion is planned.
    if os.path.normcase(os.path.normpath(str(path))) == os.path.normcase(os.path.normpath(str(action.keep_file))):
        print(f"Refusing self-deletion: {path}")
        return False

    if DRY_RUN:
        print(f"DRY RUN - would delete: {path}")
        return False

    try:
        path.unlink()
        print(f"Deleted: {path}")
        return True
    except FileNotFoundError:
        print(f"Already missing: {path}")
        return False
    except PermissionError:
        print(f"Permission denied: {path}")
        return False
    except OSError as error:
        print(f"Failed to delete {path}: {error}")
        return False


def delete_duplicate_files(actions: list[DeleteAction]) -> list[DeleteAction]:
    """Use cheap metadata checks before each unlink; no repeated SHA-256 reads."""
    successful_actions: list[DeleteAction] = []

    for action in actions:
        # Recheck the retained file for EACH deletion, addressing changes on a
        # shared drive without rereading gigabytes of content.
        if not matches_snapshot(action.keep_file, size_bytes=action.size_bytes, mtime_ns=action.keep_mtime_ns):
            print(f"Retained copy missing or changed; skipping: {action.delete_file}")
            continue
        if not matches_snapshot(action.delete_file, size_bytes=action.size_bytes, mtime_ns=action.delete_mtime_ns):
            print(f"Duplicate missing or changed; skipping: {action.delete_file}")
            continue
        if delete_duplicate_file(action):
            successful_actions.append(action)

    return successful_actions


def cleanup_targets_are_safe(placeholder_actions: list[PlaceholderAction]) -> bool:
    """Reject conflicting placeholder paths and existing recovery manifests."""
    if MANIFEST_FILE.exists():
        print(f"Refusing deletion: recovery manifest already exists: {absolute_path(MANIFEST_FILE)}")
        print("Archive/rename the previous manifest before another deletion run.")
        return False

    for placeholder in placeholder_actions:
        if placeholder.placeholder_file.exists() and (
            placeholder.placeholder_file.read_text(encoding="utf-8") != placeholder_text(placeholder)
        ):
            print(f"Refusing deletion: conflicting placeholder: {placeholder.placeholder_file}")
            return False
    return True


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def execute_deletion(
    actions: list[DeleteAction],
    records: list[FileRecord],
    planned_placeholder_actions: list[PlaceholderAction],
    totals: ScanTotals,
    report_file: Path,
) -> None:
    """Confirm and delete with metadata checks and a durable recovery manifest.

    Report actual deleted bytes separately from proposed bytes. Append final
    outcomes to the current run's TXT report; retained sizes use the original
    scan as the baseline. An interruption leaves a pending recovery manifest
    that can be used to restore only originals actually missing.
    """
    if DRY_RUN:
        print("DRY_RUN is enabled. No files were deleted and no placeholders were written.")
        return

    if not cleanup_targets_are_safe(planned_placeholder_actions):
        return
    if not confirm_deletion():
        print("Deletion cancelled. No files were deleted.")
        return

    # Persist the pending recovery mapping BEFORE deleting anything.
    try:
        write_manifest(MANIFEST_FILE, manifest_rows(actions, status="pending"), exclusive=True)
    except FileExistsError:
        print(f"Refusing deletion: recovery manifest appeared meanwhile: {absolute_path(MANIFEST_FILE)}")
        return
    successful_actions = delete_duplicate_files(actions)
    update_manifest(MANIFEST_FILE, manifest_rows(actions, successful_actions=successful_actions))

    successful_placeholder_actions = build_placeholder_actions(actions=successful_actions, records=records)
    if successful_placeholder_actions:
        write_placeholder_files(successful_placeholder_actions)

    print()
    print("Finished")
    print("=" * 80)
    print(f"Duplicate files requested for deletion: {len(actions)}")
    print(f"Duplicate files deleted:                 {len(successful_actions)}")
    print(f"Duplicate files failed:                  {len(actions) - len(successful_actions)}")
    print(f"Placeholder groups written/planned:       {len(successful_placeholder_actions)}")
    print(f"Recovery manifest:                       {absolute_path(MANIFEST_FILE)}")
    actual_lines = storage_summary_lines(totals, successful_actions, actual=True)
    for line in actual_lines:
        print(line)

    # The report was exclusively created for this run before confirmation. Add
    # actual outcomes to the same file rather than creating/overwriting another
    # user's report. The recovery manifest remains authoritative for each row.
    try:
        with report_file.open("a", encoding="utf-8") as report:
            report.write("\n\nActual deletion results\n")
            report.write("=" * 80 + "\n")
            report.write(f"Requested files: {len(actions)}\n")
            report.write(f"Successfully deleted: {len(successful_actions)}\n")
            report.write(f"Failed/skipped: {len(actions) - len(successful_actions)}\n")
            report.write("\n".join(actual_lines) + "\n")
            report.write(f"Recovery manifest: {absolute_path(MANIFEST_FILE)}\n")
    except OSError as error:
        print(f"WARNING: could not append actual outcomes to report {report_file}: {error}")


def main() -> None:
    """Dispatch restore, saved-proposal application, or fresh scan.

    Only one non-scan mode may be enabled. A conflict fails before reading or
    modifying manifests and before any deletion or restoration.
    """
    if RESTORE_FROM_MANIFEST and APPLY_PROPOSAL_MANIFEST:
        print("Refusing to run: RESTORE_FROM_MANIFEST and APPLY_PROPOSAL_MANIFEST are both True.")
        print("Enable exactly one operation, or set both False for a fresh scan.")
        return

    if RESTORE_FROM_MANIFEST:
        restore_files_from_manifest(MANIFEST_FILE)
        return

    if APPLY_PROPOSAL_MANIFEST:
        print(f"Loading trusted proposal: {absolute_path(PROPOSAL_MANIFEST_FILE)}")
        print("No full folder scan or SHA-256 pass; original content hashes are TRUSTED.")
        try:
            actions, records, totals = actions_from_proposal(PROPOSAL_MANIFEST_FILE)
        except (OSError, ValueError) as error:
            print(f"Refusing deletion: invalid or stale proposal manifest: {error}")
            return
        if not actions:
            print("The proposal manifest contains no deletion actions.")
            return

        groups = duplicate_groups_from_actions(actions)
        placeholders = build_placeholder_actions(actions, records)
        report_file = write_report(
            duplicate_groups=groups,
            actions=actions,
            placeholder_actions=placeholders,
            report_file=REPORT_FILE,
            proposal_file=PROPOSAL_MANIFEST_FILE,
            totals=totals,
        )
        print(f"Application report: {absolute_path(report_file)}")
        for line in storage_summary_lines(totals, actions):
            print(line)
        execute_deletion(actions, records, placeholders, totals, report_file)
        return

    check_folders = find_check_folders(ROOT_DIR)

    if not check_folders:
        print(f"No folders named {CHECK_FOLDER_NAMES!r} found under {absolute_path(ROOT_DIR)}")
        return

    records = iter_files_in_check_folders(check_folders)
    totals = ScanTotals(file_count=len(records), total_bytes=sum(record.size_bytes for record in records))

    if not records:
        print("Check folders found, but no files were found under them.")
        return

    print(f"Found {len(check_folders)} check folder(s).")
    print(f"Found {len(records)} file(s) to scan.")
    print("Checking file sizes and hashing candidate duplicates...")

    hashed_records = add_hashes_to_candidates(records)
    duplicate_groups = find_duplicate_groups(hashed_records)
    actions = build_delete_actions(duplicate_groups, hashed_records)

    planned_placeholder_actions = build_placeholder_actions(
        actions=actions,
        records=records,
    )

    proposal_file = write_proposal_manifest(PROPOSAL_MANIFEST_FILE, actions, totals)
    report_file = write_report(
        duplicate_groups=duplicate_groups,
        actions=actions,
        placeholder_actions=planned_placeholder_actions,
        report_file=REPORT_FILE,
        proposal_file=proposal_file,
        totals=totals,
    )

    print_summary(
        check_folders=check_folders,
        records=records,
        duplicate_groups=duplicate_groups,
        actions=actions,
        placeholder_actions=planned_placeholder_actions,
        report_file=report_file,
        proposal_file=proposal_file,
        totals=totals,
    )

    if not duplicate_groups:
        print("No duplicate file contents found.")
        return

    execute_deletion(actions, records, planned_placeholder_actions, totals, report_file)


if __name__ == "__main__":
    main()
