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
    tlf_files = sorted(
        path for path in log_dir.glob("*.tlf") if not path.name.endswith((".hpc.tlf", ".gpu.tlf"))
    )
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
        assert processor.processed and len(processor.df) == 1, f"Supported log rejected: {log_path.name}"
        actual = {key: _json_value(value) for key, value in processor.df.iloc[0].to_dict().items()}
        assert actual == expected, f"Production TLF snapshot drift in {log_path.name}"
