"""Smoke tests for TUFLOW orchestrators."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch

from ryan_library.orchestrators.tuflow.tuflow_logsummary_append import append_to_master_log_summary
from ryan_library.orchestrators.tuflow.peak_check_po_csvs import main_processing as peak_check_main
from ryan_library.orchestrators.tuflow.tuflow_timeseries_stability import main_processing as stability_main


def test_tuflow_logsummary_append_smoke(tmp_path: Path) -> None:
    wb_path = tmp_path / "master.xlsx"
    wb_path.touch()

    with (
        patch("ryan_library.orchestrators.tuflow.tuflow_logsummary_append.load_existing_log_summary_rows") as mock_load,
        patch("ryan_library.orchestrators.tuflow.tuflow_logsummary_append.discover_log_files") as mock_discover,
        patch("ryan_library.orchestrators.tuflow.tuflow_logsummary_append.filter_new_log_files") as mock_filter,
        patch("ryan_library.orchestrators.tuflow.tuflow_logsummary_append.process_log_files") as mock_process,
        patch("ryan_library.orchestrators.tuflow.tuflow_logsummary_append.build_log_summary_dataframe") as mock_build,
        patch(
            "ryan_library.orchestrators.tuflow.tuflow_logsummary_append.append_dataframe_to_workbook_table"
        ) as mock_append,
    ):

        mock_load.return_value = MagicMock()
        mock_discover.return_value = [tmp_path / "1.tlf"]
        mock_filter.return_value = [tmp_path / "1.tlf"]
        mock_result = MagicMock(status="OK")
        mock_process.return_value = [mock_result]
        mock_build.return_value = MagicMock()
        mock_build.return_value.empty = False

        append_to_master_log_summary(master_workbook_path=wb_path, console_log_level="DEBUG", use_live_dashboard=False)

        mock_load.assert_called_once()
        mock_discover.assert_called_once()
        mock_filter.assert_called_once()
        mock_process.assert_called_once()
        mock_build.assert_called_once()
        mock_append.assert_called_once()


def test_peak_check_po_csvs_smoke(tmp_path: Path) -> None:
    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()
    out_file = tmp_path / "out.xlsx"

    with (
        patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs._collect_files") as mock_collect,
        patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs.cf.ProcessPoolExecutor") as mock_pool,
        patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs.ExcelExporter") as mock_export,
    ):

        mock_collect.return_value = [csv_dir / "1_PO.csv"]

        # Setup mock for pool so it returns a dummy list of results
        mock_executor = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_executor
        mock_executor.map.return_value = [[{"Peak": 1}]]

        peak_check_main(paths_to_process=[csv_dir], output_dir=csv_dir, max_workers=1)

        mock_collect.assert_called_once()
        mock_pool.assert_called_once()
        mock_export.assert_called_once()


def test_tuflow_timeseries_stability_smoke(tmp_path: Path) -> None:
    csv_dir = tmp_path / "csvs"
    csv_dir.mkdir()
    out_file = tmp_path / "out.xlsx"

    with (
        patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.Path.rglob") as mock_rglob,
        patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.cf.ProcessPoolExecutor") as mock_pool,
        patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.ExcelExporter") as mock_export,
    ):

        mock_rglob.return_value = [csv_dir / "1_PO.csv"]

        mock_executor = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_executor
        mock_executor.map.return_value = [[{"Stability": "OK"}]]

        stability_main(paths_to_process=[csv_dir], result_types=("PO",), output_dir=csv_dir, max_workers=1)

        mock_rglob.assert_called()
        mock_pool.assert_called_once()
        mock_export.assert_called_once()
