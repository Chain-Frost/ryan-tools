from __future__ import annotations

import hashlib
import os
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

# delete duplicate files below this script in TUFLOW check folders.

# ---------------------------------------------------------------------------
# Hard-coded constants
# ---------------------------------------------------------------------------

ROOT_DIR = Path(".")
CHECK_FOLDER_NAME = "check"

REPORT_FILE = Path("duplicate_files_to_delete.txt")

HASH_CHUNK_SIZE = 1024 * 1024  # 1 MiB

# Even when False, deletion still requires typing DELETE.
DRY_RUN = False

# True  = compare duplicates across all discovered check folders.
# False = compare duplicates only within each individual check folder.
COMPARE_ACROSS_CHECK_FOLDERS = True

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
    """
    Return an absolute path without requiring the path to exist.

    Works for normal drives and UNC paths.
    """
    return path.expanduser().absolute()


def safe_display_path(path: Path) -> str:
    """
    Return a readable path string.

    Do not use Path.as_posix() because it can make Windows and UNC paths less clear.
    """
    return str(path)


def relative_or_absolute_path_from_placeholder_to_kept_file(
    placeholder_file: Path,
    keep_file: Path,
) -> str:
    """
    Return the kept file path relative to the placeholder file's folder where possible.

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
        (
            path
            for path in root_dir.rglob("*")
            if path.is_dir() and path.name.lower() == CHECK_FOLDER_NAME.lower()
        ),
        key=lambda p: str(p).lower(),
    )


def is_placeholder_file(path: Path) -> bool:
    """Return True if path is one of this script's placeholder files."""
    return path.name.endswith(PLACEHOLDER_SUFFIX)


def iter_files_in_check_folders(check_folders: list[Path]) -> list[FileRecord]:
    """Return file records for all normal files inside discovered check folders."""
    records: list[FileRecord] = []

    for check_folder in check_folders:
        check_folder = absolute_path(check_folder)

        for path in sorted(check_folder.rglob("*"), key=lambda p: str(p).lower()):
            if not path.is_file():
                continue

            if is_placeholder_file(path):
                continue

            records.append(
                FileRecord(
                    check_folder=check_folder,
                    path=absolute_path(path),
                    suffix=path.suffix.lower(),
                    size_bytes=path.stat().st_size,
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


def add_hashes_to_candidates(records: list[FileRecord]) -> list[FileRecord]:
    """
    Hash only files that have at least one same-suffix, same-size candidate.

    This avoids hashing files that cannot be duplicates.
    """
    if COMPARE_ACROSS_CHECK_FOLDERS:
        size_group_key = lambda r: (r.suffix, r.size_bytes)
    else:
        size_group_key = lambda r: (r.check_folder, r.suffix, r.size_bytes)

    by_size: dict[object, list[FileRecord]] = defaultdict(list)

    for record in records:
        by_size[size_group_key(record)].append(record)

    output: list[FileRecord] = []

    for group in by_size.values():
        if len(group) == 1:
            output.extend(group)
            continue

        for record in group:
            output.append(
                FileRecord(
                    check_folder=record.check_folder,
                    path=record.path,
                    suffix=record.suffix,
                    size_bytes=record.size_bytes,
                    content_hash=file_hash(record.path),
                )
            )

    return sorted(output, key=lambda r: str(r.path).lower())


def find_duplicate_groups(records: list[FileRecord]) -> list[DuplicateGroup]:
    """
    Find duplicate files by suffix, file size, then content hash.

    The first file alphabetically is kept.
    """
    if COMPARE_ACROSS_CHECK_FOLDERS:
        hash_group_key = lambda r: (r.suffix, r.size_bytes, r.content_hash)
    else:
        hash_group_key = lambda r: (r.check_folder, r.suffix, r.size_bytes, r.content_hash)

    by_hash: dict[object, list[FileRecord]] = defaultdict(list)

    for record in records:
        if record.content_hash is None:
            continue

        by_hash[hash_group_key(record)].append(record)

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


def build_delete_actions(duplicate_groups: list[DuplicateGroup]) -> list[DeleteAction]:
    """Create one delete action per duplicate file."""
    actions: list[DeleteAction] = []

    for group in duplicate_groups:
        for delete_file in group.delete_files:
            actions.append(
                DeleteAction(
                    delete_file=delete_file,
                    keep_file=group.keep_file,
                    suffix=group.suffix,
                    size_bytes=group.size_bytes,
                    content_hash=group.content_hash,
                )
            )

    return sorted(actions, key=lambda a: str(a.delete_file).lower())


# ---------------------------------------------------------------------------
# Placeholder handling
# ---------------------------------------------------------------------------

def build_check_folder_lookup(records: list[FileRecord]) -> dict[Path, Path]:
    """Map each scanned file to its containing check folder."""
    return {
        record.path: record.check_folder
        for record in records
    }


def should_write_placeholder(
    action: DeleteAction,
    check_folder_by_file: dict[Path, Path],
) -> bool:
    """
    Return True when the deleted file and kept file are in different check folders.

    Placeholder files are only required when the remaining source file is elsewhere.
    """
    delete_check_folder = check_folder_by_file.get(action.delete_file)
    keep_check_folder = check_folder_by_file.get(action.keep_file)

    if delete_check_folder is None or keep_check_folder is None:
        return False

    return delete_check_folder != keep_check_folder


def placeholder_path_for_representative_deleted_file(delete_file: Path) -> Path:
    """
    Return the placeholder path based on a representative deleted file.

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
    """
    Build one placeholder action per unique deleted content per check folder.

    This prevents writing one placeholder per deleted filename.

    Example:
        If 10 identical files are deleted from one folder because the same
        content is kept in another folder, this creates 1 placeholder action.

        If the same folder also has deleted files with another content hash,
        this creates another placeholder action.
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
        placeholder_file = placeholder_path_for_representative_deleted_file(
            representative_deleted_file
        )

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
        "One or more files that were previously in this folder were deleted",
        "because identical content exists in another check folder.",
        "",
        "Remaining file reference:",
        f"  {kept_file_reference}",
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
    """
    Write one placeholder file.

    Returns True if written successfully.
    """
    text = placeholder_text(action)

    if DRY_RUN:
        print(f"DRY RUN - would write placeholder: {action.placeholder_file}")
        return True

    try:
        action.placeholder_file.write_text(text, encoding="utf-8")
        print(f"Wrote placeholder: {action.placeholder_file}")
        return True
    except OSError as error:
        print(f"Failed to write placeholder {action.placeholder_file}: {error}")
        return False


def write_placeholder_files(placeholder_actions: list[PlaceholderAction]) -> None:
    """Write placeholder files."""
    for action in placeholder_actions:
        write_placeholder_file(action)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def write_report(
    duplicate_groups: list[DuplicateGroup],
    actions: list[DeleteAction],
    placeholder_actions: list[PlaceholderAction],
    report_file: Path,
) -> None:
    """Write a human-readable duplicate deletion report."""
    lines: list[str] = [
        "Duplicate file report",
        "=" * 80,
        "",
        f"Root folder: {absolute_path(ROOT_DIR)}",
        f"Compare across check folders: {COMPARE_ACROSS_CHECK_FOLDERS}",
        f"Duplicate groups found: {len(duplicate_groups)}",
        f"Files marked for deletion: {len(actions)}",
        f"Placeholder files to write: {len(placeholder_actions)}",
        "",
        "Placeholder policy:",
        "  One placeholder is written per unique deleted file content per check folder.",
        "  Multiple identical deleted files in the same folder share one placeholder.",
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

    report_file = absolute_path(report_file)
    report_file.write_text("\n".join(lines), encoding="utf-8")


def print_summary(
    check_folders: list[Path],
    records: list[FileRecord],
    duplicate_groups: list[DuplicateGroup],
    actions: list[DeleteAction],
    placeholder_actions: list[PlaceholderAction],
) -> None:
    """Print a console summary."""
    print()
    print("Summary")
    print("=" * 80)
    print(f"Root folder:                    {absolute_path(ROOT_DIR)}")
    print(f"Check folders found:             {len(check_folders)}")
    print(f"Files scanned:                   {len(records)}")
    print(f"Compare across check folders:     {COMPARE_ACROSS_CHECK_FOLDERS}")
    print(f"Duplicate groups found:          {len(duplicate_groups)}")
    print(f"Files proposed for deletion:     {len(actions)}")
    print(f"Placeholder files to write:      {len(placeholder_actions)}")
    print(f"Report written to:               {absolute_path(REPORT_FILE)}")
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
    """
    Delete one duplicate file.

    Returns True only if the file was deleted or was already missing.
    """
    path = action.delete_file

    if DRY_RUN:
        print(f"DRY RUN - would delete: {path}")
        return True

    try:
        path.unlink()
        print(f"Deleted: {path}")
        return True
    except FileNotFoundError:
        print(f"Already missing: {path}")
        return True
    except PermissionError:
        print(f"Permission denied: {path}")
        return False
    except OSError as error:
        print(f"Failed to delete {path}: {error}")
        return False


def delete_duplicate_files(actions: list[DeleteAction]) -> list[DeleteAction]:
    """
    Delete duplicate files.

    Returns the actions that successfully deleted or skipped already-missing files.
    """
    successful_actions: list[DeleteAction] = []

    for action in actions:
        if delete_duplicate_file(action):
            successful_actions.append(action)

    return successful_actions


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
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
    actions = build_delete_actions(duplicate_groups)

    planned_placeholder_actions = build_placeholder_actions(
        actions=actions,
        records=records,
    )

    write_report(
        duplicate_groups=duplicate_groups,
        actions=actions,
        placeholder_actions=planned_placeholder_actions,
        report_file=REPORT_FILE,
    )

    print_summary(
        check_folders=check_folders,
        records=records,
        duplicate_groups=duplicate_groups,
        actions=actions,
        placeholder_actions=planned_placeholder_actions,
    )

    if not duplicate_groups:
        print("No duplicate file contents found.")
        return

    if DRY_RUN:
        print("DRY_RUN is enabled. No files were deleted and no placeholders were written.")
        return

    if not confirm_deletion():
        print("Deletion cancelled. No files were deleted.")
        return

    successful_actions = delete_duplicate_files(actions)

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
    print(f"Duplicate files deleted/skipped:         {len(successful_actions)}")
    print(f"Duplicate files failed:                  {failed_count}")
    print(f"Placeholder files written/planned:        {len(successful_placeholder_actions)}")


if __name__ == "__main__":
    main()