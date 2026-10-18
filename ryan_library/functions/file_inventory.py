"""Document identification and reusable file inventory."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TypedDict

from ryan_library.functions.file_utils import find_files_parallel
from ryan_library.functions.file_verification import hash_file, snapshot_file

DOCUMENT_PATTERN = re.compile(r"^(?P<number>[A-Z0-9]{1,15}(?:-[A-Z0-9]{1,15}){1,7})(?=$|[_ .-])", re.IGNORECASE)
REVISION_PATTERN = re.compile(r"^_(?:(?:REV|REVISION)[_ -]*)?(?P<revision>\d+[A-Z]?)(?=$|[_ .-])", re.IGNORECASE)


class InventoryRow(TypedDict):
    filename: str
    document_number: str
    revision: str
    description: str
    filepath: str
    modified_utc: str
    file_size_bytes: int | None
    sha256: str
    error: str


INVENTORY_COLUMNS: tuple[str, ...] = (
    "filename", "document_number", "revision", "description", "filepath",
    "modified_utc", "file_size_bytes", "sha256", "error",
)


@dataclass(frozen=True, slots=True)
class DocumentIdentity:
    """Best-effort document identity, with no guesses for other filenames."""

    number: str | None
    revision: str | None
    description: str


def parse_document_filename(filename: str) -> DocumentIdentity:
    """Parse a hyphenated document number, optional revision, and description."""
    stem = Path(filename).stem
    match = DOCUMENT_PATTERN.match(stem)
    if match is None:
        return DocumentIdentity(number=None, revision=None, description=stem)
    remainder = stem[match.end():]
    revision_match = REVISION_PATTERN.match(remainder)
    revision = revision_match.group("revision") if revision_match else None
    if revision_match:
        remainder = remainder[revision_match.end():]
    return DocumentIdentity(match.group("number"), revision, remainder.strip(" _-"))


def collect_file_inventory(
    root_directories: list[Path],
    *,
    patterns: list[str] | None = None,
    recursive: bool = True,
    include_sha256: bool = False,
) -> list[InventoryRow]:
    """Reuse shared threaded discovery and filesystem snapshots; return sorted records."""
    if not root_directories:
        message = "At least one root directory is required"
        raise ValueError(message)
    roots = [Path(root).expanduser().absolute() for root in root_directories]
    invalid = [root for root in roots if not root.is_dir()]
    if invalid:
        message = f"Invalid root directory: {invalid[0]}"
        raise NotADirectoryError(message)
    discovered = find_files_parallel(
        root_dirs=roots,
        patterns=patterns if patterns is not None else ["*"],
        recursive_search=recursive,
        report_level=None,
        print_found_folder=False,
    )
    files = sorted({path.absolute() for path in discovered}, key=lambda path: str(path).casefold())
    rows: list[InventoryRow] = []
    for path in files:
        document = parse_document_filename(path.name)
        row: InventoryRow = {
            "filename": path.name,
            "document_number": document.number or "",
            "revision": document.revision or "",
            "description": document.description,
            "filepath": str(path),
            "modified_utc": "",
            "file_size_bytes": None,
            "sha256": "",
            "error": "",
        }
        try:
            snapshot = snapshot_file(path)
            row["file_size_bytes"] = snapshot.size
            row["modified_utc"] = datetime.fromtimestamp(
                snapshot.mtime_ns / 1_000_000_000, tz=UTC
            ).isoformat(timespec="seconds")
            if include_sha256:
                row["sha256"] = hash_file(path).hex()
        except (OSError, ValueError) as error:
            row["error"] = f"{type(error).__name__}: {error}"
        rows.append(row)
    return rows
