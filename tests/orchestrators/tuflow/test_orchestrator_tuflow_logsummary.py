"""Unit tests for ryan_library.orchestrators.tuflow.tuflow_logsummary."""

from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd
import pytest

from ryan_library.orchestrators.tuflow.tuflow_logsummary import (
    process_log_file_for_dashboard,
    _process_log_file_dataframe,
    main_processing,
    discover_log_files,
    build_log_summary_dataframe,
    LogFileProcessingResult,
    _format_bytes,
)


@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary._process_log_file_dataframe")
def test_process_log_file_for_dashboard(mock_process, tmp_path: Path) -> None:
    # 1. OK case
    mock_process.return_value = pd.DataFrame({"A": [1]})
    res = process_log_file_for_dashboard(tmp_path / "1.tlf")
    assert res.status == "OK"
    assert "1 row" in res.detail

    # 2. SKIP case (empty df)
    mock_process.return_value = pd.DataFrame()
    res = process_log_file_for_dashboard(tmp_path / "1.tlf")
    assert res.status == "SKIP"

    # 3. FAIL case (Exception)
    mock_process.side_effect = Exception("test error")
    res = process_log_file_for_dashboard(tmp_path / "1.tlf")
    assert res.status == "FAIL"
    assert "test error" in res.detail


@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.read_log_file")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.search_for_completion")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.is_complete_tlf")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.process_top_lines")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.finalise_data")
def test_process_log_file_dataframe(
    mock_finalise, mock_process_top, mock_is_complete, mock_search, mock_read, tmp_path: Path
) -> None:
    log_path = tmp_path / "test.tlf"
    log_path.write_text("dummy")

    # Empty log file -> return empty df
    mock_read.return_value = []
    assert _process_log_file_dataframe(log_path).empty

    # Has lines, incomplete run
    mock_read.return_value = ["line"]
    mock_search.return_value = ({}, 0, None)
    mock_is_complete.return_value = False
    assert _process_log_file_dataframe(log_path).empty

    # Has lines, complete run, success = 4
    mock_is_complete.return_value = True
    mock_process_top.return_value = ({"Runcode": "test"}, 4, False, False, False)

    # Finalize returns valid df
    mock_finalise.return_value = pd.DataFrame({"A": [1]})
    res = _process_log_file_dataframe(log_path)
    assert not res.empty

    # Finalize fails (returns empty df)
    mock_finalise.return_value = pd.DataFrame()
    assert _process_log_file_dataframe(log_path).empty

    # Success != 4
    mock_process_top.return_value = ({"Runcode": "test"}, 3, False, False, False)
    assert _process_log_file_dataframe(log_path).empty


@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.discover_log_files")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.process_log_files")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.build_log_summary_dataframe")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.ExcelExporter")
def test_main_processing(mock_exporter, mock_build, mock_process, mock_discover, tmp_path: Path) -> None:
    mock_save = mock_exporter.return_value.save_to_excel
    # No files
    mock_discover.return_value = []
    main_processing()
    mock_process.assert_not_called()

    # With files, empty results
    mock_discover.return_value = [tmp_path / "1.tlf"]
    mock_process.return_value = [
        LogFileProcessingResult(logfile=tmp_path / "1.tlf", data_frame=pd.DataFrame(), status="SKIP", detail="")
    ]
    mock_build.return_value = pd.DataFrame()
    main_processing()
    mock_save.assert_not_called()

    # With files, valid results
    mock_build.return_value = pd.DataFrame({"A": [1]})
    main_processing()
    mock_save.assert_called_once()

    # Test error in save_to_excel
    mock_save.side_effect = Exception("save error")
    # Shouldn't crash, just log exception
    main_processing()


def test_discover_log_files(tmp_path: Path) -> None:
    (tmp_path / "valid.tlf").touch()
    (tmp_path / "valid.hpc.tlf").touch()  # Excluded

    res = discover_log_files(root_dir=tmp_path)
    assert len(res) == 1
    assert res[0].name == "valid.tlf"


def test_format_bytes() -> None:
    assert _format_bytes(500) == "500 B"
    assert _format_bytes(1500) == "1.5 KB"
    assert _format_bytes(1500 * 1024) == "1.5 MB"
    assert _format_bytes(1500 * 1024 * 1024) == "1.5 GB"


def test_process_log_file() -> None:
    from ryan_library.orchestrators.tuflow.tuflow_logsummary import process_log_file

    with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary._process_log_file_dataframe") as mock_df:
        mock_df.return_value = pd.DataFrame({"A": [1]})
        res = process_log_file(Path("test.tlf"))
        assert not res.empty


@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary.run_dashboard_workflow")
@patch("ryan_library.orchestrators.tuflow.tuflow_logsummary._log_processing_results")
def test_process_log_files(mock_log, mock_run, tmp_path: Path) -> None:
    from ryan_library.orchestrators.tuflow.tuflow_logsummary import process_log_files

    mock_run.return_value = []
    log_file = tmp_path / "1.tlf"
    log_file.touch()
    process_log_files(
        files=[log_file],
        root_dir=Path.cwd(),
        use_live_dashboard=False,
        live_refresh_per_second=1.0,
        live_max_rows=10,
        log_queue=None,
        console_log_level="DEBUG",
    )
    mock_run.assert_called_once()
    mock_log.assert_called_once()


def test_build_log_summary_dataframe() -> None:
    from ryan_library.orchestrators.tuflow.tuflow_logsummary import build_log_summary_dataframe, LogFileProcessingResult

    f1 = Path("1.tlf")
    f2 = Path("2.tlf")

    # Sorting by file_indexes and removing empty dataframes
    r1 = LogFileProcessingResult(f2, pd.DataFrame({"StartDate": [2]}), "OK", "")
    r2 = LogFileProcessingResult(f1, pd.DataFrame({"StartDate": [1]}), "OK", "")

    with patch("ryan_library.orchestrators.tuflow.tuflow_logsummary._reorder_log_summary_columns") as mock_reorder:
        mock_reorder.side_effect = lambda data_frame: data_frame

        df = build_log_summary_dataframe(files=[f1, f2], processing_results=[r1, r2])
        assert len(df) == 2


def test_dashboard_helpers() -> None:
    from ryan_library.orchestrators.tuflow.tuflow_logsummary import (
        _format_dashboard_label,
        _log_processing_results,
        _dashboard_status,
        _dashboard_detail,
        LogFileProcessingResult,
    )

    p = Path.cwd() / "test.tlf"
    assert _format_dashboard_label(logfile=p) == "test.tlf"

    # outside cwd
    p2 = Path("C:/outside.tlf")
    assert _format_dashboard_label(logfile=p2) == str(p2)

    r_ok = LogFileProcessingResult(p, pd.DataFrame(), "OK", "detail")
    r_skip = LogFileProcessingResult(p, pd.DataFrame(), "SKIP", "detail")
    r_fail = LogFileProcessingResult(p, pd.DataFrame(), "FAIL", "detail")

    _log_processing_results(processing_results=[r_ok, r_skip, r_fail])

    assert _dashboard_status(r_ok) == "OK"
    assert _dashboard_detail(r_ok) == "detail"
