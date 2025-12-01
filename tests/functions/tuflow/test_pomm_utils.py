"""Tests for ryan_library.functions.tuflow.pomm_utils."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
from pathlib import Path
from ryan_library.functions.tuflow import pomm_utils

class TestAggregation:
    @pytest.fixture
    def sample_df(self):
        return pd.DataFrame({
            "aep_text": ["1%", "1%", "1%", "2%"],
            "duration_text": ["1h", "1h", "2h", "1h"],
            "Location": ["Loc1", "Loc1", "Loc1", "Loc1"],
            "Type": ["Type1", "Type1", "Type1", "Type1"],
            "trim_runcode": ["Run1", "Run1", "Run1", "Run1"],
            "AbsMax": [10.0, 20.0, 15.0, 5.0],
            "tp_text": ["TP1", "TP2", "TP1", "TP1"]
        })

    def test_find_aep_dur_max(self, sample_df):
        """Test finding max per AEP/Duration group."""
        # Group: 1%/1h/Loc1/Type1/Run1 -> 2 rows (10, 20). Max is 20.
        # Group: 1%/2h/Loc1/Type1/Run1 -> 1 row (15). Max is 15.
        # Group: 2%/1h/Loc1/Type1/Run1 -> 1 row (5). Max is 5.
        
        res = pomm_utils.find_aep_dur_max(sample_df)
        assert len(res) == 3
        # Check 1% 1h group
        row1 = res[(res["aep_text"] == "1%") & (res["duration_text"] == "1h")]
        assert not row1.empty
        assert row1["AbsMax"].iloc[0] == 20.0
        assert row1["aep_dur_bin"].iloc[0] == 2  # Group size

    def test_find_aep_max(self, sample_df):
        """Test finding max per AEP group."""
        # First get aep_dur_max
        aep_dur_max = pomm_utils.find_aep_dur_max(sample_df)
        # 1% group: 20 (1h) and 15 (2h). Max is 20.
        # 2% group: 5 (1h). Max is 5.
        
        res = pomm_utils.find_aep_max(aep_dur_max)
        assert len(res) == 2
        row1 = res[res["aep_text"] == "1%"]
        assert not row1.empty
        assert row1["AbsMax"].iloc[0] == 20.0
        assert row1["aep_bin"].iloc[0] == 2 # 2 durations in 1% group

    @patch("ryan_library.functions.tuflow.pomm_utils.median_calc")
    def test_find_aep_dur_median(self, mock_median, sample_df):
        """Test finding median per AEP/Duration group."""
        # Mock median_calc return
        # It returns (stats_dict, thinned_df)
        # We only care about stats_dict
        mock_median.return_value = ({"median": 100.0, "mean_PeakFlow": 100.0}, None)
        
        res = pomm_utils.find_aep_dur_median(sample_df)
        assert not res.empty
        assert "MedianAbsMax" in res.columns
        assert res["MedianAbsMax"].iloc[0] == 100.0
        
        # Check if median_calc was called for each group
        # Groups: 1%/1h, 1%/2h, 2%/1h -> 3 calls
        assert mock_median.call_count == 3

    def test_find_aep_median_max(self, sample_df):
        """Test finding max of medians."""
        # We need a DataFrame that looks like output of find_aep_dur_median
        # Columns: aep_text, Location, Type, trim_runcode, MedianAbsMax
        median_df = pd.DataFrame({
            "aep_text": ["1%", "1%"],
            "Location": ["Loc1", "Loc1"],
            "Type": ["Type1", "Type1"],
            "trim_runcode": ["Run1", "Run1"],
            "MedianAbsMax": [50.0, 60.0],
            "duration_text": ["1h", "2h"]
        })
        
        res = pomm_utils.find_aep_median_max(median_df)
        assert len(res) == 1
        assert res["MedianAbsMax"].iloc[0] == 60.0
        assert res["aep_bin"].iloc[0] == 2

class TestHelpers:
    def test_ordered_columns(self):
        """Test column ordering logic."""
        df = pd.DataFrame(columns=["A", "B", "C", "D", "E"])
        groups = [["A", "C"], ["B"]]
        info = ["E"]
        # Expected: A, C, B, D (remaining), E
        res = pomm_utils._ordered_columns(df, groups, info)
        assert res == ["A", "C", "B", "D", "E"]

    def test_remove_columns_containing(self):
        """Test removing columns by substring."""
        df = pd.DataFrame({"Mean_A": [1], "Median_B": [2], "Other": [3]})
        res = pomm_utils._remove_columns_containing(df, ("mean",))
        assert "Mean_A" not in res.columns
        assert "Median_B" in res.columns
        assert "Other" in res.columns

class TestReporting:
    @patch("ryan_library.functions.tuflow.pomm_utils.ExcelExporter")
    def test_save_to_excel(self, mock_exporter, tmp_path):
        """Test saving to Excel delegates to ExcelExporter."""
        df = pd.DataFrame({"A": [1]})
        pomm_utils.save_to_excel(
            aep_dur_max=df,
            aep_max=df,
            aggregated_df=df,
            output_path=tmp_path / "out.xlsx"
        )
        assert mock_exporter.return_value.export_dataframes.called
        call_args = mock_exporter.return_value.export_dataframes.call_args
        assert call_args.kwargs["file_name"] == "out.xlsx"

class TestProcessing:
    @patch("ryan_library.functions.tuflow.pomm_utils.BaseProcessor")
    def test_process_file_success(self, mock_base_processor):
        """Test successful file processing."""
        mock_instance = MagicMock()
        mock_instance.validate_data.return_value = True
        mock_instance.processed = True
        mock_base_processor.from_file.return_value = mock_instance
        
        path = Path("test.csv")
        res = pomm_utils.process_file(path)
        
        assert res == mock_instance
        mock_instance.process.assert_called_once()
        mock_instance.validate_data.assert_called_once()

    @patch("ryan_library.functions.tuflow.pomm_utils.BaseProcessor")
    def test_process_file_with_filter(self, mock_base_processor):
        """Test file processing with location filter."""
        mock_instance = MagicMock()
        mock_base_processor.from_file.return_value = mock_instance
        
        path = Path("test.csv")
        filters = frozenset(["Loc1"])
        pomm_utils.process_file(path, location_filter=filters)
        
        mock_instance.filter_locations.assert_called_once_with(filters)

    @patch("ryan_library.functions.tuflow.pomm_utils.BaseProcessor")
    def test_process_file_failure(self, mock_base_processor):
        """Test exception handling in process_file."""
        mock_base_processor.from_file.side_effect = Exception("Test Error")
        
        with pytest.raises(Exception, match="Test Error"):
            pomm_utils.process_file(Path("test.csv"))

    @patch("ryan_library.functions.tuflow.pomm_utils.Pool")
    @patch("ryan_library.functions.tuflow.pomm_utils.calculate_pool_size")
    def test_process_files_in_parallel(self, mock_calc_pool, mock_pool):
        """Test parallel processing orchestration."""
        mock_calc_pool.return_value = 2
        mock_pool_instance = mock_pool.return_value.__enter__.return_value
        
        # Mock results from starmap
        mock_proc1 = MagicMock()
        mock_proc1.processed = True
        mock_proc1.df = pd.DataFrame({"A": [1]}) # Needs data to be added
        mock_proc2 = MagicMock()
        mock_proc2.processed = False # Should be skipped
        mock_pool_instance.starmap.return_value = [mock_proc1, mock_proc2]
        
        files = [Path("f1.csv"), Path("f2.csv")]
        log_queue = MagicMock()
        
        res = pomm_utils.process_files_in_parallel(files, log_queue)
        
        assert len(res.processors) == 1
        assert res.processors[0] == mock_proc1

    @patch("ryan_library.functions.tuflow.pomm_utils.collect_files")
    @patch("ryan_library.functions.tuflow.pomm_utils.process_files_in_parallel")
    def test_combine_processors_from_paths(self, mock_parallel, mock_collect):
        """Test combining processors from paths."""
        mock_collect.return_value = [Path("f1.csv")]
        mock_result_collection = MagicMock()
        mock_parallel.return_value = mock_result_collection
        
        res = pomm_utils.combine_processors_from_paths([Path("dir")])
        
        assert res == mock_result_collection
        mock_collect.assert_called_once()
        mock_parallel.assert_called_once()

    @patch("ryan_library.functions.tuflow.pomm_utils.collect_files")
    def test_combine_processors_no_files(self, mock_collect):
        """Test combine returns empty collection if no files found."""
        mock_collect.return_value = []
        res = pomm_utils.combine_processors_from_paths([Path("dir")])
        assert not res.processors

class TestMeanAggregation:
    @pytest.fixture
    def sample_median_df(self):
        return pd.DataFrame({
            "aep_text": ["1%", "1%"],
            "duration_text": ["1h", "2h"],
            "Location": ["Loc1", "Loc1"],
            "Type": ["Type1", "Type1"],
            "trim_runcode": ["Run1", "Run1"],
            "MedianAbsMax": [10.0, 20.0],
            "mean_PeakFlow": [12.0, 18.0],
            "mean_including_zeroes": [12.0, 18.0],
            "mean_excluding_zeroes": [12.0, 18.0],
            "mean_Duration": [1.0, 2.0],
            "mean_TP": ["TP1", "TP1"],
            "low": [5.0, 10.0],
            "high": [15.0, 25.0],
            "count": [10, 10],
            "count_bin": [10, 10],
            "mean_storm_is_median_storm": [True, False]
        })

    @patch("ryan_library.functions.tuflow.pomm_utils.find_aep_dur_median")
    def test_find_aep_dur_mean(self, mock_find_median, sample_median_df):
        """Test extracting mean stats."""
        mock_find_median.return_value = sample_median_df
        
        res = pomm_utils.find_aep_dur_mean(pd.DataFrame()) # Input ignored due to mock
        
        assert not res.empty
        assert "mean_PeakFlow" in res.columns
        # MedianAbsMax is still present at this stage, it's filtered out later by save_peak_report_mean
        assert "MedianAbsMax" in res.columns

    def test_find_aep_mean_max(self, sample_median_df):
        """Test finding max of means."""
        # 1% group: 12.0 (1h) and 18.0 (2h). Max mean is 18.0.
        
        res = pomm_utils.find_aep_mean_max(sample_median_df)
        
        assert len(res) == 1
        assert res["mean_PeakFlow"].iloc[0] == 18.0
        assert res["duration_text"].iloc[0] == "2h"

class TestReportWrappers:
    @patch("ryan_library.functions.tuflow.pomm_utils.save_to_excel")
    @patch("ryan_library.functions.tuflow.pomm_utils.find_aep_median_max")
    @patch("ryan_library.functions.tuflow.pomm_utils.find_aep_dur_median")
    def test_save_peak_report_median(self, mock_dur, mock_max, mock_save, tmp_path):
        """Test median report wrapper."""
        mock_dur.return_value = pd.DataFrame({"MedianAbsMax": [10]})
        mock_max.return_value = pd.DataFrame({"MedianAbsMax": [10]})
        
        pomm_utils.save_peak_report_median(
            aggregated_df=pd.DataFrame(),
            script_directory=tmp_path,
            timestamp="2025"
        )
        
        mock_save.assert_called_once()
        
    @patch("ryan_library.functions.tuflow.pomm_utils.save_to_excel")
    @patch("ryan_library.functions.tuflow.pomm_utils.find_aep_mean_max")
    @patch("ryan_library.functions.tuflow.pomm_utils.find_aep_dur_mean")
    def test_save_peak_report_mean(self, mock_dur, mock_max, mock_save, tmp_path):
        """Test mean report wrapper."""
        mock_dur.return_value = pd.DataFrame({"mean_PeakFlow": [10]})
        mock_max.return_value = pd.DataFrame({"mean_PeakFlow": [10]})
        
        pomm_utils.save_peak_report_mean(
            aggregated_df=pd.DataFrame(),
            script_directory=tmp_path,
            timestamp="2025"
        )
        
        mock_save.assert_called_once()


class TestRLLQmxNormalization:
    @patch("ryan_library.functions.tuflow.pomm_utils.combine_df_from_paths")
    def test_aggregated_from_paths_normalization(self, mock_combine):
        """Test normalization of RLLQmx columns."""
        # Simulate a combined DF with RLLQmx columns
        mock_combine.return_value = pd.DataFrame({
            "Chan ID": ["Loc1", "Loc2"],
            "Q": [10.0, 20.0],
            "Time": [1.0, 2.0],
            # "Type" is missing
        })
        
        res = pomm_utils.aggregated_from_paths([Path(".")])
        
        assert "Location" in res.columns
        assert res["Location"].tolist() == ["Loc1", "Loc2"]
        assert "AbsMax" in res.columns
        assert res["AbsMax"].tolist() == [10.0, 20.0]
        assert "Type" in res.columns
        assert res["Type"].tolist() == ["Q", "Q"]

    @patch("ryan_library.functions.tuflow.pomm_utils.combine_df_from_paths")
    def test_aggregated_from_paths_mixed(self, mock_combine):
        """Test normalization with mixed POMM and RLLQmx data."""
        # Simulate mixed DF
        mock_combine.return_value = pd.DataFrame({
            "Location": ["Loc1", None],
            "Chan ID": [None, "Loc2"],
            "AbsMax": [10.0, None],
            "Q": [None, 20.0],
            "Type": ["V", None]
        })
        
        res = pomm_utils.aggregated_from_paths([Path(".")])
        
        assert res["Location"].tolist() == ["Loc1", "Loc2"]
        assert res["AbsMax"].tolist() == [10.0, 20.0]
        assert res["Type"].tolist() == ["V", "Q"]
