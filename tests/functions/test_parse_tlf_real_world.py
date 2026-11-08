"""Source-backed TLF snapshot gate exercising the real public processor (#228)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from ryan_library.processors.tuflow.base_processor import BaseProcessor


@pytest.fixture(scope="module")
def regression_snapshot() -> dict[str, dict[str, Any]]:
    snapshot_path = Path(__file__).parents[1] / "test_data" / "tlf_regression_snapshot.json"
    if not snapshot_path.is_file():
        pytest.fail(f"Pinned TLF snapshot is missing: {snapshot_path}")
    return json.loads(snapshot_path.read_text(encoding="utf-8"))


def _json_value(value: Any) -> Any:
    """Match the existing JSON snapshot representation without recomputing data."""
    if pd.isna(value):
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def test_parse_tlf_real_world(regression_snapshot: dict[str, dict[str, Any]]) -> None:
    log_dir = Path(__file__).parents[1] / "test_data" / "tuflow" / "TUFLOW_Example_Model_Dataset" / "log"
    tlf_files = sorted(path for path in log_dir.glob("*.tlf") if not path.name.endswith((".hpc.tlf", ".gpu.tlf")))
    assert tlf_files, "No pinned TUFLOW main logs found"
    observed = {path.name for path in tlf_files}
    assert observed == set(regression_snapshot), (
        f"Pinned TLF snapshot membership drift; added={sorted(observed - set(regression_snapshot))}, "
        f"missing={sorted(set(regression_snapshot) - observed)}. Investigate rather than silently skipping files."
    )

    for log_path in tlf_files:
        processor = BaseProcessor.from_file(log_path)
        processor.process()  # Production includes modern and accepted legacy completion semantics.
        expected = regression_snapshot[log_path.name]
        assert processor.processed, f"Supported log rejected: {log_path.name}"
        assert len(processor.df) == 1, f"Unexpected TLF row count: {log_path.name}"
        actual: dict[str, Any] = {str(key): _json_value(value) for key, value in processor.df.iloc[0].to_dict().items()}
        # WP01 #231 adds explicit diagnostics not present in the pinned legacy snapshot.
        # Every pre-existing expected value and all fixture inventory membership stay strict.
        missing = set(expected) - set(actual)
        assert not missing, f"Missing pinned TLF fields in {log_path.name}: {sorted(missing)}"

        accepted_diagnostics = {
            "TLF_1D_Negative_Depths",
            "TLF_HPC_HCN_Repeated_Timesteps",
            "TLF_HPC_NaN_Repeated_Timesteps",
            "TLF_HPC_NaN_Warnings_2550",
            "TLF_Warnings_Prior",
            "TLF_Warnings_During",
            "TLF_Checks_Prior",
            "TLF_Checks_During",
            "TLF_Quadtree_ZC_Cells",
            "TLF_Quadtree_ZU_Faces",
            "TLF_Quadtree_ZV_Faces",
            "TLF_Quadtree_ZH_Corners",
        }
        unexpected = set(actual) - set(expected) - accepted_diagnostics
        if log_path.name == "EG05_018.tlf":
            # WP01 #224 preserves the archived -72 h starting time.
            assert actual.get("Model_Start_Time") == -72.0
            unexpected.discard("Model_Start_Time")
        assert not unexpected, f"Unexpected TLF fields in {log_path.name}: {sorted(unexpected)}"

        for field, expected_value in expected.items():
            assert actual[field] == expected_value, (
                f"TLF snapshot value drift in {log_path.name} for {field!r}: "
                f"expected {expected_value!r}, got {actual[field]!r}"
            )
