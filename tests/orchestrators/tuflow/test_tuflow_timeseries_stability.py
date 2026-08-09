"""Tests for ryan_library.orchestrators.tuflow.tuflow_timeseries_stability."""

import pytest
from pathlib import Path
import pandas as pd
from unittest.mock import patch, MagicMock

from ryan_library.orchestrators.tuflow.tuflow_timeseries_stability import (
    _analyze_stability_worker,
    main_processing,
    DEFAULT_RESULT_TYPES,
)
from ryan_library.functions.tuflow.po_timeseries_checks import (
    normalize_result_types,
    collect_timeseries_files,
)


class TestNormalizeResultTypes:
    def test_normalize_empty(self):
        assert normalize_result_types(None, ("PO", "Q"), DEFAULT_RESULT_TYPES) == DEFAULT_RESULT_TYPES
        assert normalize_result_types([], ("PO", "Q"), DEFAULT_RESULT_TYPES) == DEFAULT_RESULT_TYPES

    def test_normalize_valid(self):
        assert normalize_result_types(["po", "q"], ("PO", "Q"), DEFAULT_RESULT_TYPES) == ("PO", "Q")

    def test_normalize_invalid(self):
        assert normalize_result_types(["po", "invalid"], ("PO", "Q"), DEFAULT_RESULT_TYPES) == ("PO",)

    def test_normalize_all(self):
        assert normalize_result_types(["all"], ("PO", "Q"), DEFAULT_RESULT_TYPES) == ("PO", "Q")


class TestCollectFiles:
    def test_collect_files(self, tmp_path):
        d1 = tmp_path / "dir1"
        d1.mkdir()
        (d1 / "test_PO.csv").touch()
        (d1 / "test_1d_Q.csv").touch()

        # Test non-directory
        f1 = tmp_path / "file1.txt"
        f1.touch()

        globs = {"PO": "**/*_PO.csv", "Q": "**/*_1d_Q.csv"}
        files = collect_timeseries_files([d1, f1], ["PO", "Q"], globs)

        assert len(files) == 2
        types = {t for _, t in files}
        assert types == {"PO", "Q"}


class TestAnalyzeWorker:
    @patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.analyze_stability_q_csv")
    @patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.analyze_stability_csv")
    @patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.flatten_stability_results")
    def test_worker_routing(self, mock_flatten, mock_csv, mock_q, tmp_path):
        config = MagicMock()
        mock_flatten.return_value = [{"res": 1}]

        # Test Q route
        res = _analyze_stability_worker(str(tmp_path), "Q", config)
        assert mock_q.called
        assert not mock_csv.called
        assert res == [{"res": 1}]

        mock_q.reset_mock()
        mock_csv.reset_mock()

        # Test PO route
        res = _analyze_stability_worker(str(tmp_path), "PO", config)
        assert mock_csv.called
        assert not mock_q.called
        assert res == [{"res": 1}]


class TestMainProcessing:
    @patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.collect_timeseries_files")
    def test_main_no_files(self, mock_collect, tmp_path):
        mock_collect.return_value = []
        main_processing(paths_to_process=[tmp_path], result_types=["PO"])
        # Should return without exception

    @patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.collect_timeseries_files")
    @patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.ExcelExporter")
    def test_main_no_rows_returned(self, mock_exporter, mock_collect, tmp_path):
        mock_collect.return_value = [(tmp_path / "a.csv", "PO")]
        with patch(
            "ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.cf.ProcessPoolExecutor"
        ) as MockExecutor:
            mock_pool = MockExecutor.return_value.__enter__.return_value
            mock_pool.map.return_value = [[]]

            main_processing(paths_to_process=[tmp_path], result_types=["PO"])
            assert not mock_exporter.called

    @patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.collect_timeseries_files")
    @patch("ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.ExcelExporter")
    def test_main_success(self, mock_exporter, mock_collect, tmp_path):
        mock_collect.return_value = [(tmp_path / "a.csv", "PO")]

        with patch(
            "ryan_library.orchestrators.tuflow.tuflow_timeseries_stability.cf.ProcessPoolExecutor"
        ) as MockExecutor:
            mock_pool = MockExecutor.return_value.__enter__.return_value
            # return a list containing a single row dict
            mock_pool.map.return_value = [[{"run_code": "EXG", "status": "OK", "extra": "info"}]]

            main_processing(paths_to_process=[tmp_path], result_types=["PO"])

            assert mock_exporter.return_value.save_to_excel.called
            df = mock_exporter.return_value.save_to_excel.call_args[1]["data_frame"]
            assert list(df.columns) == ["run_code", "status", "extra"]
