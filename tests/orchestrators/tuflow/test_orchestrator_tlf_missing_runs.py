"""Tests for tlf_missing_runs orchestrator."""

import pandas as pd
from pathlib import Path
from unittest.mock import patch

from ryan_library.orchestrators.tuflow.tlf_missing_runs import orchestrate_missing_runs_check


@patch("ryan_library.orchestrators.tuflow.tlf_missing_runs.pd.read_csv")
@patch("ryan_library.orchestrators.tuflow.tlf_missing_runs.summarize_for_cli")
@patch("ryan_library.orchestrators.tuflow.tlf_missing_runs.pd.DataFrame.to_csv")
def test_orchestrate_missing_runs_csv(mock_to_csv, mock_summarize, mock_read_csv):
    mock_summarize.return_value = ("Summary\nNext line", pd.DataFrame())
    out_path = orchestrate_missing_runs_check(Path("test.csv"))
    
    assert mock_read_csv.called
    assert mock_summarize.called
    assert out_path == Path("test__missing_runs_summary.csv")


@patch("ryan_library.orchestrators.tuflow.tlf_missing_runs.pd.read_excel")
@patch("ryan_library.orchestrators.tuflow.tlf_missing_runs.summarize_for_cli")
@patch("ryan_library.orchestrators.tuflow.tlf_missing_runs.pd.DataFrame.to_csv")
def test_orchestrate_missing_runs_excel(mock_to_csv, mock_summarize, mock_read_excel):
    mock_summarize.return_value = ("Summary\nNext line", pd.DataFrame())
    out_path = orchestrate_missing_runs_check(Path("test.xlsx"))
    
    assert mock_read_excel.called
    assert mock_summarize.called
    assert out_path == Path("test__missing_runs_summary.csv")
