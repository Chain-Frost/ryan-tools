"""Tests for ryan_library.scripts.tuflow.closure_durations."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from ryan_library.scripts.tuflow import closure_durations


def _mock_df() -> MagicMock:
    df = MagicMock(spec=pd.DataFrame)
    df.empty = False
    return df


@patch("ryan_library.scripts.tuflow.closure_durations._export_closure_duration_artifacts")
@patch("ryan_library.scripts.tuflow.closure_durations.summarise_results")
@patch("ryan_library.scripts.tuflow.closure_durations._calculate_threshold_durations")
@patch("ryan_library.scripts.tuflow.closure_durations._collect_po_data")
@patch("ryan_library.scripts.tuflow.closure_durations.bulk_read_and_merge_tuflow_csv")
@patch("ryan_library.scripts.tuflow.closure_durations.setup_logger")
def test_run_closure_durations_success(
    mock_logger,
    mock_bulk,
    mock_collect,
    mock_calc,
    mock_summary,
    mock_export,
) -> None:
    mock_logger.return_value.__enter__.return_value = "log_queue"

    collection = MagicMock()
    collection.processors = [MagicMock(data_type="PO", df=pd.DataFrame({"A": [1]}))]
    mock_bulk.return_value = collection

    result_df: MagicMock = _mock_df()
    summary_df: MagicMock = _mock_df()
    mock_collect.return_value = pd.DataFrame(
        data={
            "Type": ["Flow", "Flow"],
            "Time": [0.0, 1.0],
            "Value": [0.0, 2.0],
            "Location": ["L1", "L1"],
            "directory_path": ["path", "path"],
            "aep_text": ["1%", "1%"],
            "duration_text": ["1hr", "1hr"],
            "tp_text": ["12", "12"],
        }
    )
    mock_calc.return_value = result_df
    mock_summary.return_value = summary_df

    closure_durations.run_closure_durations(paths=[Path(".")], thresholds=[1.0], data_type="Flow")

    mock_bulk.assert_called_once()
    mock_collect.assert_called_once()
    mock_calc.assert_called_once()
    result_df.to_parquet.assert_called_once()
    mock_export.assert_called_once()


@patch("ryan_library.scripts.tuflow.closure_durations.setup_logger")
@patch("ryan_library.scripts.tuflow.closure_durations.bulk_read_and_merge_tuflow_csv")
def test_run_closure_durations_no_processors(mock_bulk, mock_logger) -> None:
    mock_logger.return_value.__enter__.return_value = "log_queue"
    collection = MagicMock()
    collection.processors = []
    mock_bulk.return_value = collection

    closure_durations.run_closure_durations(paths=[Path(".")], thresholds=[1.0], data_type="Flow")

    mock_bulk.assert_called_once()
