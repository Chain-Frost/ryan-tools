"""Validate legacy table reporting and exclusive output creation."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from ryan_library.orchestrators.tuflow.tlf_missing_runs import orchestrate_missing_runs_check


def test_csv_summary_and_collision(tmp_path: Path) -> None:
    source = tmp_path / "tracking.csv"
    pd.DataFrame(
        {
            "trim_run_code": ["M"] * 10,
            "Duration": [60] * 10,
            "AEP": [1] * 10,
            "TP": [f"TP{n:02d}" for n in range(1, 11)],
        }
    ).to_csv(source, index=False)
    original_input = source.read_bytes()
    output = orchestrate_missing_runs_check(source)
    assert output == tmp_path / "tracking__missing_runs_summary.csv"
    original_output = output.read_bytes()
    assert b"trim_run_code" in original_output
    with pytest.raises(FileExistsError):
        orchestrate_missing_runs_check(source)
    assert output.read_bytes() == original_output
    assert source.read_bytes() == original_input


def test_excel_selection(tmp_path: Path) -> None:
    source = tmp_path / "tracking.xlsx"
    with (
        patch("ryan_library.orchestrators.tuflow.tlf_missing_runs.pd.read_excel", return_value=pd.DataFrame()) as read,
        patch(
            "ryan_library.orchestrators.tuflow.tlf_missing_runs.summarize_for_cli",
            return_value=("Summary", pd.DataFrame({"message": ["Summary"]})),
        ),
    ):
        output = orchestrate_missing_runs_check(source, sheet_name="Runs")
    read.assert_called_once_with(io=source, sheet_name="Runs")
    assert "Summary" in output.read_text()
