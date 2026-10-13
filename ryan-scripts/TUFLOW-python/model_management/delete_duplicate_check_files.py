"""Find and optionally delete byte-identical files in TUFLOW check folders.

Run from the model/result root or set ROOT_DIR. Cross-folder comparison and
raster scanning are enabled by default. Review the proposed deletion list and
type DELETE to confirm actual removal; DRY_RUN = True remains available.

One placeholder is retained per unique deleted content group in each affected
check folder, even when the retained file is in that folder. Placeholders are
audit records, not usable replacements for original file paths.

To recover files deleted by this version, set RESTORE_FROM_MANIFEST = True and
follow the separate restore confirmation. Restoration never overwrites files.
"""

from __future__ import annotations

import csv
import hashlib
import os
import shutil
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

# delete duplicate files below this script in TUFLOW check folders.

# ---------------------------------------------------------------------------
# Hard-coded constants
# ---------------------------------------------------------------------------

ROOT_DIR = Path()
CHECK_FOLDER_NAME = "check"

REPORT_FILE = Path("duplicate_files_to_delete.txt")
MANIFEST_FILE = Path("duplicate_files_manifest.csv")
PROPOSAL_MANIFEST_FILE = Path("duplicate_files_proposed.csv")

HASH_CHUNK_SIZE = 1024 * 1024  # 1 MiB

# Even when False, deletion still requires typing DELETE.
DRY_RUN = False

# True  = compare duplicates across all discovered check folders.
# False = compare duplicates only within each individual check folder.
COMPARE_ACROSS_CHECK_FOLDERS = True

# Include raster and overview duplicates. Removing original filenames may break
# path-based GIS/TUFLOW references. Set False to exclude these files.
INCLUDE_RASTER_FILES = True
RASTER_SUFFIXES = {".tif", ".tiff", ".ovr"}

# When True, restore files from the last completed MANIFEST_FILE rather than
# scanning for duplicates. Restore requires typing RESTORE when DRY_RUN is False.
RESTORE_FROM_MANIFEST = False

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
    """Find folders named CHECK_FOLDER_NAME under root_dir."""
    root_dir = absolute_path(root_dir)

    return sorted(
        (path for path in root_dir.rglob("*") if path.is_dir() and path.name.lower() == CHECK_FOLDER_NAME.lower()),
        key=lambda p: str(p).lower(),
    )


def is_placeholder_file(path: Path) -> bool:
    """Return True if path is one of this script's placeholder files."""
    return path.name.endswith(PLACEHOLDER_SUFFIX)


def iter_files_in_check_folders(check_folders: list[Path]) -> list[FileRecord]:
    """Return file records for all normal files inside discovered check folders."""
    records: list[FileRecord] = []

    for check_folder in check_folders:
        resolved_check_folder = absolute_path(check_folder)

        for path in sorted(resolved_check_folder.rglob("*"), key=lambda p: str(p).lower()):
            if not path.is_file():
                continue

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
                    path=absolute_path(path),
                    suffix=path.suffix.lower(),
                    size_bytes=file_stat.st_size,
                    mtime_ns=file_stat.st_mtime_ns,
                )
            )

    return sorted(records, key=lambda r: str(r.path).lower())


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------


def file_hash(path: Path) -> str:
    """Hash a file using SHA-256."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        while chunk := file.read(HASH_CHUNK_SIZE):
            digest.update(chunk)

    return digest.hexdigest()


def matches_snapshot(path: Path, *, size_bytes: int, mtime_ns: int) -> bool:
    """Compare inexpensive metadata with the original scan; never rehash."""
    try:
        stat = path.stat()
        return stat.st_size == size_bytes and stat.st_mtime_ns == mtime_ns
    except OSError:
        return False


def add_hashes_to_candidates(records: list[FileRecord]) -> list[FileRecord]:
    """Hash only files that have at least one same-suffix, same-size candidate.

    This avoids hashing files that cannot be duplicates.
    """
    by_size: dict[object, list[FileRecord]] = defaultdict(list)

    for record in records:
        if COMPARE_ACROSS_CHECK_FOLDERS:
            key = (record.suffix, record.size_bytes)
        else:
            key = (record.check_folder, record.suffix, record.size_bytes)
        by_size[key].append(record)

    output: list[FileRecord] = []

    for group in by_size.values():
        if len(group) == 1:
            output.extend(group)
            continue

        for record in group:
            digest = file_hash(record.path)
            if not matches_snapshot(record.path, size_bytes=record.size_bytes, mtime_ns=record.mtime_ns):
                print(f"Changed during hashing, excluded from deletion: {record.path}")
                output.append(record)
                continue
            output.append(
                FileRecord(
                    check_folder=record.check_folder,
                    path=record.path,
                    suffix=record.suffix,
                    size_bytes=record.size_bytes,
                    mtime_ns=record.mtime_ns,
                    content_hash=digest,
                )
            )

    return sorted(output, key=lambda r: str(r.path).lower())


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


def write_proposal_manifest(path: Path, rows: list[dict[str, str]]) -> Path:
    """Write the first available numbered CSV, using exclusive file creation."""
    for index in range(10000):
        candidate = available_output_path(path, index)
        try:
            write_manifest(candidate, rows, exclusive=True)
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
            if row["original_path"]:
                rows.append(row)
        return rows


def matches_record(path: Path, *, size_bytes: int, content_hash: str) -> bool:
    """Verify that the file still matches the scanned content before mutation."""
    try:
        return path.is_file() and path.stat().st_size == size_bytes and file_hash(path) == content_hash
    except OSError:
        return False


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
) -> Path:
    """Write a human-readable duplicate deletion report without overwriting it."""
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
) -> None:
    """Print a console summary."""
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


def main() -> None:
    if RESTORE_FROM_MANIFEST:
        restore_files_from_manifest(MANIFEST_FILE)
        return

    check_folders = find_check_folders(ROOT_DIR)

    if not check_folders:
        print(f"No folders named {CHECK_FOLDER_NAME!r} found under {absolute_path(ROOT_DIR)}")
        return

    records = iter_files_in_check_folders(check_folders)

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

    proposal_file = write_proposal_manifest(PROPOSAL_MANIFEST_FILE, manifest_rows(actions))
    report_file = write_report(
        duplicate_groups=duplicate_groups,
        actions=actions,
        placeholder_actions=planned_placeholder_actions,
        report_file=REPORT_FILE,
        proposal_file=proposal_file,
    )

    print_summary(
        check_folders=check_folders,
        records=records,
        duplicate_groups=duplicate_groups,
        actions=actions,
        placeholder_actions=planned_placeholder_actions,
        report_file=report_file,
        proposal_file=proposal_file,
    )

    if not duplicate_groups:
        print("No duplicate file contents found.")
        return

    if DRY_RUN:
        print("DRY_RUN is enabled. No files were deleted and no placeholders were written.")
        return

    if not cleanup_targets_are_safe(planned_placeholder_actions):
        return

    if not confirm_deletion():
        print("Deletion cancelled. No files were deleted.")
        return

    # The pending manifest is durable before any file is removed.
    try:
        write_manifest(MANIFEST_FILE, manifest_rows(actions, status="pending"), exclusive=True)
    except FileExistsError:
        print(f"Refusing deletion: recovery manifest appeared meanwhile: {absolute_path(MANIFEST_FILE)}")
        return
    successful_actions = delete_duplicate_files(actions)
    update_manifest(MANIFEST_FILE, manifest_rows(actions, successful_actions=successful_actions))

    successful_placeholder_actions = build_placeholder_actions(
        actions=successful_actions,
        records=records,
    )

    if successful_placeholder_actions:
        write_placeholder_files(successful_placeholder_actions)

    failed_count = len(actions) - len(successful_actions)

    print()
    print("Finished")
    print("=" * 80)
    print(f"Duplicate files requested for deletion: {len(actions)}")
    print(f"Duplicate files deleted:                 {len(successful_actions)}")
    print(f"Duplicate files failed:                  {failed_count}")
    print(f"Placeholder groups written/planned:       {len(successful_placeholder_actions)}")
    print(f"Recovery manifest:                       {absolute_path(MANIFEST_FILE)}")


if __name__ == "__main__":
    main()
