"""Bounded read-only RORB MCP contracts."""

import json
from pathlib import Path

from ryan_library.mcp.rorb import inspect_rorb_file, rorb_reader_guidance_resource


def test_unknown_input_retains_bounded_raw_evidence(tmp_path: Path) -> None:
    path = tmp_path / "unknown.par"
    content = "unrecognised parameter layout\n" + ("x" * 1000 + "\n") * 25
    path.write_text(content)
    result = inspect_rorb_file(str(path), section="raw", offset=1, limit=2)
    assert result["status"] == "unsupported"
    assert result["returned_count"] == 2
    assert result["next_offset"] == 3
    assert result["values_truncated"] is True
    assert len(json.dumps(result)) < 60000
    assert path.read_text() == content


def test_errors_and_zero_rows_are_bounded(tmp_path: Path) -> None:
    path = tmp_path / "unknown.par"
    path.write_text("unknown\n")
    assert "error" in inspect_rorb_file(str(path), limit=21)
    assert "error" in inspect_rorb_file(str(path), offset=-1)
    assert "error" in inspect_rorb_file(str(path), section="absent")
    result = inspect_rorb_file(str(path), section="raw", limit=0)
    assert result["returned_count"] == 0
    assert result["next_offset"] is None
    assert "error" in inspect_rorb_file(str(tmp_path / "missing"))


def test_guidance_separates_engine_support_from_format_parsing() -> None:
    guidance = json.loads(rorb_reader_guidance_resource())
    assert guidance["tool"] == "inspect_rorb_file"
    assert any("engine compatibility" in s for s in guidance["semantics"])
