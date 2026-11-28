"""Document identity and reusable inventory regression tests."""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from pathlib import Path

import pytest

from ryan_library.functions.file_inventory import (
    INVENTORY_COLUMNS,
    collect_file_inventory,
    parse_document_filename,
)


@pytest.mark.parametrize(
    ("filename", "expected"),
    [
        ("RP26188-PM-SCN-0001_1 - Hydrology.docx", ("RP26188-PM-SCN-0001", "1", "Hydrology")),
        ("ABC-123_2A_description.pdf", ("ABC-123", "2A", "description")),
        ("ABC-123.pdf", ("ABC-123", None, "")),
        ("ordinary report.pdf", (None, None, "ordinary report")),
        ("ABC-123_REV03_misc.txt", ("ABC-123", "03", "misc")),
    ],
)
def test_document_identity(filename: str, expected: tuple[str | None, str | None, str]) -> None:
    result = parse_document_filename(filename)
    assert (result.number, result.revision, result.description) == expected


def test_inventory_hash_and_metadata(tmp_path: Path) -> None:
    subdirectory = tmp_path / "sub"
    subdirectory.mkdir()
    path = subdirectory / "ABC-123_2_design.pdf"
    path.write_bytes(b"example")
    (tmp_path / "note.txt").write_text("plain", encoding="utf-8")
    rows = collect_file_inventory([tmp_path], patterns=["*.pdf"], include_sha256=True)
    assert len(rows) == 1
    row = rows[0]
    assert list(row) == list(INVENTORY_COLUMNS)
    assert row["filename"] == path.name
    assert row["document_number"] == "ABC-123"
    assert row["revision"] == "2"
    assert row["sha256"] == sha256(b"example").hexdigest()
    assert row["file_size_bytes"] == 7
    assert datetime.fromisoformat(row["modified_utc"]).tzinfo is not None
    assert not row["error"]


def test_nonrecursive_inventory(tmp_path: Path) -> None:
    (tmp_path / "top.txt").write_text("a", encoding="utf-8")
    subdirectory = tmp_path / "child"
    subdirectory.mkdir()
    (subdirectory / "nested.txt").write_text("b", encoding="utf-8")
    assert [row["filename"] for row in collect_file_inventory([tmp_path], recursive=False)] == ["top.txt"]


def test_empty_roots() -> None:
    with pytest.raises(ValueError):
        collect_file_inventory([])


def test_invalid_root(tmp_path: Path) -> None:
    with pytest.raises(NotADirectoryError):
        collect_file_inventory([tmp_path / "absent"])
