"""Tests for ryan_library.functions.tuflow.pomm_utils."""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch
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
