"""Tests for ryan_library.orchestrators.tuflow.tuflow_culverts_mean."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock

from ryan_library.orchestrators.tuflow.tuflow_culverts_mean import (
    run_culvert_mean_report,
    run_culvert_median_report,
    _run_culvert_statistic_report,
    find_culvert_aep_dur_mean,
    find_culvert_aep_dur_median,
    find_culvert_aep_mean_max,
    find_culvert_aep_median_max,
    _find_culvert_aep_dur_statistic,
    _find_culvert_aep_statistic_max,
    _preferred_metric_column,
    _group_columns,
    _ordered_columns,
    _group_key_values,
)


@pytest.fixture
def sample_aggregated_df() -> pd.DataFrame:
    data = {
        "aep_text": ["1%AEP", "1%AEP", "1%AEP", "1%AEP", "1%AEP", "1%AEP"],
        "duration_text": ["10m", "10m", "10m", "20m", "20m", "20m"],
        "trim_runcode": ["EXG", "EXG", "EXG", "EXG", "EXG", "EXG"],
        "Chan ID": ["C1", "C1", "C1", "C1", "C1", "C1"],
        "Q": [1.0, 2.0, 3.0, 5.0, 4.0, 6.0],
        "V": [0.1, 0.2, 0.3, 0.5, 0.4, 0.6],
        "tp_text": ["01", "02", "03", "01", "02", "03"],
        "tp_numeric": [1, 2, 3, 1, 2, 3],
    }
    return pd.DataFrame(data)


class TestFindStatistics:
    def test_find_culvert_aep_dur_mean(self, sample_aggregated_df):
        res = find_culvert_aep_dur_mean(sample_aggregated_df)
        assert not res.empty
        # For 10m: Q mean is 2.0, V mean is 0.2
        # For 20m: Q mean is 5.0, V mean is 0.5
        assert len(res) == 2
        row_10 = res[res["duration_text"] == "10m"].iloc[0]
        assert row_10["mean_Q"] == 2.0
        assert row_10["adopted_Q"] == 2.0
        assert row_10["adopted_tp_text"] == "02"
        assert row_10["min_Q"] == 1.0
        assert row_10["max_Q"] == 3.0

    def test_find_culvert_aep_dur_median(self, sample_aggregated_df):
        res = find_culvert_aep_dur_median(sample_aggregated_df)
        assert not res.empty
        assert len(res) == 2
        row_10 = res[res["duration_text"] == "10m"].iloc[0]
        # median of [1, 2, 3] upper_middle is 2.0
        assert row_10["median_Q"] == 2.0
        assert row_10["adopted_Q"] == 2.0
        assert row_10["adopted_tp_text"] == "02"

    def test_find_culvert_aep_dur_empty_checks(self):
        # Empty df
        assert _find_culvert_aep_dur_statistic(pd.DataFrame(), "mean").empty

        # Missing grouping columns
        assert _find_culvert_aep_dur_statistic(pd.DataFrame({"Q": [1]}), "mean").empty

        # Missing numeric columns
        df_no_num = pd.DataFrame({"aep_text": ["1"], "duration_text": ["1"], "trim_runcode": ["E"], "Chan ID": ["C"]})
        assert _find_culvert_aep_dur_statistic(df_no_num, "mean").empty

    def test_find_culvert_aep_mean_max(self, sample_aggregated_df):
        mean_df = find_culvert_aep_dur_mean(sample_aggregated_df)
        max_df = find_culvert_aep_mean_max(mean_df)
        assert not max_df.empty
        assert len(max_df) == 1
        # Max should be 20m where mean Q is 5.0
        assert max_df.iloc[0]["duration_text"] == "20m"
        assert max_df.iloc[0]["mean_Q"] == 5.0

    def test_find_culvert_aep_max_empty_checks(self):
        assert _find_culvert_aep_statistic_max(pd.DataFrame(), "mean").empty

        # No metric columns
        df_no_metric = pd.DataFrame({"aep_text": ["1"], "Chan ID": ["C"], "trim_runcode": ["E"]})
        assert _find_culvert_aep_statistic_max(df_no_metric, "mean").empty

        # Missing group columns
        df_bad_group = pd.DataFrame({"mean_Q": [1]})
        assert _find_culvert_aep_statistic_max(df_bad_group, "mean").empty

        # Only NaN metrics
        df_nan = pd.DataFrame(
            {"aep_text": ["1"], "duration_text": ["1"], "Chan ID": ["C"], "trim_runcode": ["E"], "mean_Q": [pd.NA]}
        )
        assert _find_culvert_aep_statistic_max(df_nan, "mean").empty


class TestOrchestratorReport:
    @patch("ryan_library.orchestrators.tuflow.tuflow_culverts_mean.ExcelExporter")
    @patch("ryan_library.orchestrators.tuflow.tuflow_culverts_mean.bulk_read_and_merge_tuflow_csv")
    def test_run_report_success(self, mock_bulk, mock_exporter, tmp_path, sample_aggregated_df):
        mock_collection = MagicMock()
        mock_collection.processors = [1]
        mock_collection.combine_1d_maximums.return_value = sample_aggregated_df
        mock_bulk.return_value = mock_collection

        _run_culvert_statistic_report(
            statistic="mean",
            output_suffix="mean",
            script_directory=tmp_path,
            paths_to_process=[tmp_path],
            export_raw=True,
        )
        assert mock_exporter.return_value.export_dataframes.called

    @patch("ryan_library.orchestrators.tuflow.tuflow_culverts_mean.bulk_read_and_merge_tuflow_csv")
    def test_run_report_empty_processors(self, mock_bulk, tmp_path):
        mock_collection = MagicMock()
        mock_collection.processors = []
        mock_bulk.return_value = mock_collection

        _run_culvert_statistic_report(
            statistic="mean",
            output_suffix="mean",
            script_directory=tmp_path,
        )
        # Should exit early without raising

    @patch("ryan_library.orchestrators.tuflow.tuflow_culverts_mean.bulk_read_and_merge_tuflow_csv")
    def test_run_report_empty_dataframe(self, mock_bulk, tmp_path):
        mock_collection = MagicMock()
        mock_collection.processors = [1]
        mock_collection.combine_1d_maximums.return_value = pd.DataFrame()
        mock_bulk.return_value = mock_collection

        _run_culvert_statistic_report(
            statistic="mean",
            output_suffix="mean",
            script_directory=tmp_path,
        )

    @patch("ryan_library.orchestrators.tuflow.tuflow_culverts_mean.bulk_read_and_merge_tuflow_csv")
    def test_run_report_empty_locations(self, mock_bulk, tmp_path, sample_aggregated_df):
        mock_collection = MagicMock()
        mock_collection.processors = [1]
        mock_collection.filter_locations.return_value = frozenset()
        mock_collection.combine_1d_maximums.return_value = sample_aggregated_df
        mock_bulk.return_value = mock_collection

        _run_culvert_statistic_report(
            statistic="mean",
            output_suffix="mean",
            script_directory=tmp_path,
            locations_to_include=["non_existent"],
        )

    @patch("ryan_library.orchestrators.tuflow.tuflow_culverts_mean._find_culvert_aep_dur_statistic")
    @patch("ryan_library.orchestrators.tuflow.tuflow_culverts_mean.bulk_read_and_merge_tuflow_csv")
    def test_run_report_empty_dur_stat(self, mock_bulk, mock_find, tmp_path, sample_aggregated_df):
        mock_collection = MagicMock()
        mock_collection.processors = [1]
        mock_collection.combine_1d_maximums.return_value = sample_aggregated_df
        mock_bulk.return_value = mock_collection
        mock_find.return_value = pd.DataFrame()

        _run_culvert_statistic_report(
            statistic="mean",
            output_suffix="mean",
            script_directory=tmp_path,
        )


class TestHelpers:
    def test_preferred_metric_column(self):
        df = pd.DataFrame(columns=["mean_Other", "mean_Q", "mean_V"])
        assert _preferred_metric_column(df, "mean") == "mean_Q"

        df2 = pd.DataFrame(columns=["median_Other", "median_V"])
        assert _preferred_metric_column(df2, "median") == "median_V"

        df3 = pd.DataFrame(columns=["mean_Other"])
        assert _preferred_metric_column(df3, "mean") == "mean_Other"

    def test_group_key_values(self):
        assert _group_key_values(["A", "B"], ("1", "2")) == {"A": "1", "B": "2"}
        assert _group_key_values(["A"], "1") == {"A": "1"}
