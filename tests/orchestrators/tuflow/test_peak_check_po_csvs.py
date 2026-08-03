"""Unit tests for ryan_library.orchestrators.tuflow.peak_check_po_csvs."""

from pathlib import Path
from unittest.mock import patch, MagicMock

from ryan_library.orchestrators.tuflow.peak_check_po_csvs import (
    _collect_files,
    _analyze_peak_worker,
    main_processing,
    PeakCheckConfig,
)


def test_collect_files(tmp_path: Path) -> None:
    dir1 = tmp_path / "dir1"
    dir1.mkdir()

    file1 = dir1 / "test_PO.csv"
    file1.touch()

    file2 = dir1 / "other.csv"
    file2.touch()

    file3 = tmp_path / "not_a_dir"
    file3.touch()

    res = _collect_files(paths_to_process=[dir1, dir1, file3], csv_glob="*_PO.csv")

    # Should only return file1, no duplicates, skips file3
    assert res == [file1]


@patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs.flatten_peak_results")
@patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs.analyze_peak_csv")
def test_analyze_peak_worker(mock_analyze, mock_flatten) -> None:
    config = MagicMock(spec=PeakCheckConfig)
    mock_analyze.return_value = ["dummy_result"]
    mock_flatten.return_value = [{"col": "val"}]

    res = _analyze_peak_worker("test_PO.csv", config)

    mock_analyze.assert_called_once_with(path=Path("test_PO.csv"), config=config)
    mock_flatten.assert_called_once_with(results=["dummy_result"])
    assert res == [{"col": "val"}]


@patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs._collect_files")
def test_main_processing_no_files(mock_collect, tmp_path: Path) -> None:
    mock_collect.return_value = []

    # Should exit early, not raise error
    main_processing(paths_to_process=[tmp_path])
    mock_collect.assert_called_once()


@patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs.ExcelExporter")
@patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs.cf.ProcessPoolExecutor")
@patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs._collect_files")
def test_main_processing_no_rows(mock_collect, mock_pool, mock_export, tmp_path: Path) -> None:
    mock_collect.return_value = [Path("test_PO.csv")]

    mock_executor = MagicMock()
    mock_pool.return_value.__enter__.return_value = mock_executor
    mock_executor.map.return_value = [[]]  # worker returns empty list of rows

    # Should exit before exporting
    main_processing(paths_to_process=[tmp_path])

    mock_pool.assert_called_once()
    mock_export.return_value.save_to_excel.assert_not_called()


@patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs.ExcelExporter")
@patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs.cf.ProcessPoolExecutor")
@patch("ryan_library.orchestrators.tuflow.peak_check_po_csvs._collect_files")
def test_main_processing_with_rows(mock_collect, mock_pool, mock_export, tmp_path: Path) -> None:
    mock_collect.return_value = [Path("test_PO.csv")]

    mock_executor = MagicMock()
    mock_pool.return_value.__enter__.return_value = mock_executor
    # Return some dummy rows with columns that need reindexing
    mock_executor.map.return_value = [[{"run_code": "R1", "extra_col": 1}, {"run_code": "R2", "status": "OK"}]]

    main_processing(paths_to_process=[tmp_path], export_mode="parquet")

    mock_export.return_value.save_to_excel.assert_called_once()
    args, kwargs = mock_export.return_value.save_to_excel.call_args
    assert kwargs["export_mode"] == "parquet"

    df = kwargs["data_frame"]
    assert "run_code" in df.columns
    assert "extra_col" in df.columns

    # run_code should be first because it's in first_cols
    assert list(df.columns)[0] == "run_code"
