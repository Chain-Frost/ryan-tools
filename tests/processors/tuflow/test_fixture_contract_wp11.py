"""Pinned, explicit supported/empty/unsupported/malformed outcomes (issue #228)."""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from ryan_library.functions.excel_export import ExcelExporter
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

from tests.processors.tuflow.fixture_contract_assertions import assert_declared_outcome

CONTRACT = Path(__file__).with_name("fixture_contract_wp11.json")
VALID_OUTCOMES = {"supported", "empty", "unsupported", "malformed"}


def load_contract() -> dict[str, Any]:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))


def test_contract_is_bounded_explicit_and_unambiguous() -> None:
    manifest = load_contract()
    assert manifest["schema_version"] == 1
    cases: list[dict[str, Any]] = manifest["cases"]
    assert 1 <= len(cases) <= 64, "The first-delivery manifest must remain deliberately bounded"
    ids = [case["id"] for case in cases]
    assert len(ids) == len(set(ids))
    assert {case["outcome"] for case in cases} == VALID_OUTCOMES
    assert all(case.get("reason") and case["id"] == Path(case["id"]).name for case in cases)
    for case in cases:
        if case["outcome"] == "supported":
            assert case.get("minimum_rows", 0) >= 1
            assert case.get("columns")
        else:
            assert "minimum_rows" not in case
            assert "values" not in case


@pytest.fixture(scope="module")
def fixture_root() -> Path:
    # The repository conftest already mandates an initialised, pinned test-data submodule.
    return Path(__file__).parents[2] / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset"


@pytest.mark.parametrize("case", load_contract()["cases"], ids=lambda item: item["id"])
def test_real_fixture_matches_declared_outcome(fixture_root: Path, case: dict[str, Any]) -> None:
    matches = list(fixture_root.rglob(case["id"]))
    assert len(matches) == 1, f"Fixture ID {case['id']} must resolve to exactly one pinned file; got {matches}"
    processor = BaseProcessor.from_file(matches[0])
    processor.process()
    assert_declared_outcome(processor, case)

def _write_small_po(path: Path, q_offset: float) -> None:
    # Independent expected 2-row PO header and signed/zero hydraulic source values.
    with path.open("x", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(["", "Time", "H", "Q", "V", "H", "Q", "V"])
        writer.writerow(["", "Time", "L001", "L001", "L001", "L002", "L002", "L002"])
        for minute, q in enumerate((-1.0, 0.0, 2.0)):
            writer.writerow(["", minute / 60, 10 + minute, q + q_offset, 0.1 + minute * 0.1, 20 + minute, 1.0, 0.5])


def test_synthetic_public_po_collection_and_export_roundtrip(tmp_path: Path) -> None:
    first = tmp_path / "M11_01p_00120m_TP01_PO.csv"
    second = tmp_path / "M11_01p_00120m_TP02_PO.csv"
    _write_small_po(first, 0.0)
    _write_small_po(second, 0.5)
    collection = ProcessorCollection()
    for path in (first, second):
        processor = BaseProcessor.from_file(path, entity_filter=["L001"], include_path_columns=False)
        processor.process()
        assert processor.processed
        assert len(processor.df) == 9  # 3 times x 3 measurements, only selected L001.
        collection.add_processor(processor)
    combined = collection.po_combine()
    assert len(combined) == 18
    assert set(combined["internalName"].astype(str)) == {
        first.stem.removesuffix("_PO"),
        second.stem.removesuffix("_PO"),
    }
    assert set(combined["Location"].astype(str)) == {"L001"}
    assert set(combined["Type"].astype(str)) == {"H", "Q", "V"}
    q_filter = (combined["internalName"] == first.stem.removesuffix("_PO")) & (combined["Type"] == "Q")
    first_q = combined.loc[q_filter].sort_values("Time")
    assert first_q["Value"].tolist() == [-1.0, 0.0, 2.0]
    assert first_q["AbsValue"].tolist() == [1.0, 0.0, 2.0]
    assert first_q["Time"].to_numpy() == pytest.approx([0.0, 1 / 60, 2 / 60])

    # Real safe exporter, not a mocked save, with independently checked readback.
    ExcelExporter().save_to_excel(
        data_frame=combined,
        file_name="wp11_source_values",
        sheet_name="PO",
        output_directory=tmp_path,
        export_mode="excel",
    )
    workbook = tmp_path / "wp11_source_values.xlsx"
    assert workbook.is_file()
    excel: pd.DataFrame = pd.read_excel(workbook, sheet_name="PO")  # pyright: ignore[reportUnknownMemberType]
    assert len(excel) == len(combined)
    assert excel["Value"].astype(float).tolist() == pytest.approx(combined["Value"].astype(float).tolist())
    assert excel["Time"].astype(float).tolist() == pytest.approx(combined["Time"].astype(float).tolist())

    pytest.importorskip("pyarrow", reason="Parquet readback requires the supported optional engine")
    parquet = tmp_path / "wp11_source_values.parquet"
    combined.to_parquet(parquet, index=False)
    restored = pd.read_parquet(parquet)
    assert restored["Value"].astype(float).tolist() == pytest.approx(combined["Value"].astype(float).tolist())
    assert restored["internalName"].astype(str).tolist() == combined["internalName"].astype(str).tolist()
