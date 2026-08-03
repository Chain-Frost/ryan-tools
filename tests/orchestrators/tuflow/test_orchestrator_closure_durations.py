"""Unit tests for ryan_library.orchestrators.tuflow.closure_durations."""

from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest

from ryan_library.orchestrators.tuflow.closure_durations import (
    run_closure_durations,
    _export_closure_duration_artifacts,
)


@pytest.fixture
def mock_pipeline():
    with (
        patch("ryan_library.orchestrators.tuflow.closure_durations.bulk_read_and_merge_tuflow_csv") as mock_bulk,
        patch("ryan_library.orchestrators.tuflow.closure_durations._collect_po_data") as mock_collect,
        patch("ryan_library.orchestrators.tuflow.closure_durations._calculate_threshold_durations") as mock_calc,
        patch("ryan_library.orchestrators.tuflow.closure_durations.summarise_results") as mock_summary,
        patch("ryan_library.orchestrators.tuflow.closure_durations._export_closure_duration_artifacts") as mock_export,
        patch("pandas.DataFrame.to_parquet") as mock_to_parquet,
        patch("pandas.DataFrame.to_csv") as mock_to_csv,
    ):
        yield {
            "bulk": mock_bulk,
            "collect": mock_collect,
            "calc": mock_calc,
            "summary": mock_summary,
            "export": mock_export,
            "parquet": mock_to_parquet,
            "csv": mock_to_csv,
        }


def test_run_closure_durations_no_processors(mock_pipeline) -> None:
    collection = MagicMock()
    collection.processors = []
    mock_pipeline["bulk"].return_value = collection

    run_closure_durations()

    mock_pipeline["bulk"].assert_called_once()
    mock_pipeline["collect"].assert_not_called()


def test_run_closure_durations_no_po_data(mock_pipeline) -> None:
    collection = MagicMock()
    collection.processors = ["dummy"]
    mock_pipeline["bulk"].return_value = collection

    mock_pipeline["collect"].return_value = pd.DataFrame()

    run_closure_durations()

    mock_pipeline["collect"].assert_called_once()
    mock_pipeline["calc"].assert_not_called()


def test_run_closure_durations_no_exceedance_data(mock_pipeline) -> None:
    collection = MagicMock()
    collection.processors = ["dummy"]
    mock_pipeline["bulk"].return_value = collection

    mock_pipeline["collect"].return_value = pd.DataFrame({"A": [1]})
    mock_pipeline["calc"].return_value = pd.DataFrame()

    run_closure_durations()

    mock_pipeline["calc"].assert_called_once()
    mock_pipeline["summary"].assert_not_called()


def test_run_closure_durations_success(mock_pipeline) -> None:
    collection = MagicMock()
    collection.processors = ["dummy"]
    mock_pipeline["bulk"].return_value = collection

    mock_pipeline["collect"].return_value = pd.DataFrame({"A": [1]})

    result_df = pd.DataFrame({"B": [2]})
    mock_pipeline["calc"].return_value = result_df

    # Needs the columns it tries to sort
    summary_df = pd.DataFrame({"Path": ["p1"], "Location": ["L1"], "ThresholdFlow": [10.0], "AEP": ["1% AEP"]})
    mock_pipeline["summary"].return_value = summary_df

    run_closure_durations(paths=[Path("test")], thresholds=[10.0], allowed_locations=("L1",))

    mock_pipeline["parquet"].assert_called_once()
    mock_pipeline["csv"].assert_called_once()
    mock_pipeline["summary"].assert_called_once()
    mock_pipeline["export"].assert_called_once()

    args, kwargs = mock_pipeline["export"].call_args
    # The AEP_sort_key column should have been created and then dropped
    assert "AEP_sort_key" not in kwargs["summary_df"].columns


@patch("ryan_library.orchestrators.tuflow.closure_durations.ExcelExporter")
def test_export_closure_duration_artifacts(mock_exporter) -> None:
    # Empty dataframes
    _export_closure_duration_artifacts(
        durations_df=pd.DataFrame(), summary_df=pd.DataFrame(), timestamp="20200101-1200", export_mode="excel"
    )
    mock_exporter.return_value.export_dataframes.assert_not_called()

    # With data
    df = pd.DataFrame({"A": [1]})
    _export_closure_duration_artifacts(durations_df=df, summary_df=df, timestamp="20200101-1200", export_mode="parquet")

    mock_exporter.return_value.export_dataframes.assert_called_once()
    args, kwargs = mock_exporter.return_value.export_dataframes.call_args
    assert kwargs["export_mode"] == "parquet"
    assert "20200101-1200_closure_durations" in kwargs["export_dict"]
