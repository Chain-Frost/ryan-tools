"""Integration checks for the copied file-inventory wrapper."""

from __future__ import annotations

import runpy
from pathlib import Path
from typing import Any

import pandas as pd


def _wrapper() -> dict[str, Any]:
    path = Path(__file__).parents[2] / "ryan-scripts" / "file-management-python" / "file_inventory.py"
    return runpy.run_path(str(path))


def test_file_inventory_wrapper_writes_csv_and_excel(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    (source / "ABC-123_2_design.pdf").write_bytes(b"example")
    output = tmp_path / "results"
    wrapper = _wrapper()
    parse = wrapper["_parse_cli_arguments"]
    main = wrapper["main"]
    arguments = parse([str(source), "--output-directory", str(output), "--no-pause"])
    assert main(arguments) == 0
    csv_path = output / "file_inventory.csv"
    xlsx_path = output / "file_inventory.xlsx"
    assert csv_path.exists()
    assert xlsx_path.exists()
    frame = pd.read_csv(csv_path, dtype={"revision": str})
    assert frame["document_number"].iloc[0] == "ABC-123"
    assert frame["revision"].iloc[0] == "2"
    assert frame["file_size_bytes"].iloc[0] == 7


def test_file_inventory_wrapper_rejects_bad_filename(tmp_path: Path) -> None:
    wrapper = _wrapper()
    parse = wrapper["_parse_cli_arguments"]
    main = wrapper["main"]
    arguments = parse([str(tmp_path), "--basename", "../outside", "--no-pause"])
    assert main(arguments) == 2
