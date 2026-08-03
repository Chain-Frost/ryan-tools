"""Additional coverage tests for pomm_utils."""

import pytest
import pandas as pd
from pandas import DataFrame
from unittest.mock import MagicMock, patch
from pathlib import Path

from ryan_library.functions.tuflow import pomm_utils
from ryan_library.classes.column_definitions import ColumnMetadataRegistry


class TestSelectInternalNames:
    def test_missing_columns(self):
        df = pd.DataFrame({"A": [1]})
        med, mean = pomm_utils._select_internal_names_for_group(df)
        assert pd.isna(med) and pd.isna(mean)

    def test_no_valid_absmax(self):
        df = pd.DataFrame({"internalName": ["A"], "AbsMax": [pd.NA]})
        med, mean = pomm_utils._select_internal_names_for_group(df)
        assert pd.isna(med) and pd.isna(mean)

    def test_select_names(self):
        # 3 values: 10, 20, 30. Mean=20, Median=20
        df = pd.DataFrame({"internalName": ["A", "B", "C"], "AbsMax": [10.0, 30.0, 20.0]})
        med, mean = pomm_utils._select_internal_names_for_group(df)
        # sorted AbsMax: 10 (A), 20 (C), 30 (B). Median pos is 3/2 = 1 -> index for C
        assert med == "C"
        # mean is 20, closest is C
        assert mean == "C"


class TestCombineDFFromPaths:
    @patch("ryan_library.functions.tuflow.pomm_utils.combine_processors_from_paths")
    def test_combine_df_empty(self, mock_combine):
        mock_pc = MagicMock()
        mock_pc.processors = []
        mock_combine.return_value = mock_pc

        df = pomm_utils.combine_df_from_paths([Path(".")])
        assert df.empty


class TestMetadataRows:
    def test_build_metadata_rows_no_directory(self):
        df = pd.DataFrame({"A": [1]})
        res = pomm_utils._build_metadata_rows("2023", True, df, df, df, "aep-dur", "aep")
        assert "Source directories" not in res

    def test_build_metadata_rows_with_directory(self):
        df = pd.DataFrame({"directory_path": ["/path/1", "/path/1", "/path/2", pd.NA]})
        res = pomm_utils._build_metadata_rows("2023", True, df, df, df, "aep-dur", "aep")
        assert "Source directories" in res
        # Should be sorted unique
        path1 = str(Path("/path/1"))
        path2 = str(Path("/path/2"))
        assert path1 in res["Source directories"]
        assert path2 in res["Source directories"]


class TestDataDictionary:
    def test_build_data_dictionary_no_columns(self):
        df = pd.DataFrame()
        registry = ColumnMetadataRegistry.default()
        res = pomm_utils._build_data_dictionary(registry, {"sheet1": df}, {"meta": "data"})
        assert not res.empty
        # Should have a "no columns" entry
        assert (res["column"] == "<no columns>").any()


class TestSavePeakReport:
    @patch("ryan_library.functions.tuflow.pomm_utils.save_to_excel")
    @patch("ryan_library.functions.tuflow.pomm_utils.find_aep_max")
    @patch("ryan_library.functions.tuflow.pomm_utils.find_aep_dur_max")
    def test_save_peak_report(self, mock_dur, mock_max, mock_save, tmp_path):
        df = pd.DataFrame({"A": [1]})
        mock_dur.return_value = df
        mock_max.return_value = df

        pomm_utils.save_peak_report(df, tmp_path, "2023")
        assert mock_save.called


class TestFindAepDurMedianEdgeCases:
    @patch("ryan_library.functions.tuflow.pomm_utils.median_calc")
    def test_find_median_with_internal_name_and_missing_dur(self, mock_median):
        # We need to simulate median_calc returning stats and test normalization
        mock_median.return_value = (
            {
                "median": 10.0,
                "median_duration": "1h",
                "mean_Duration": "1h",
                "median_TP": "1",  # Use simple string that might not be NA
                "mean_TP": "1",
            },
            None,
        )

        df = pd.DataFrame(
            {
                "aep_text": ["1%"],
                "duration_text": ["1h"],
                "Location": ["L1"],
                "Type": ["Q"],
                "trim_runcode": ["EXG"],
                "internalName": ["Int1"],
                "AbsMax": [10.0],
                "tp_text": ["001"],
            }
        )

        res = pomm_utils.find_aep_dur_median(df)
        assert "MedianAbsMax" in res.columns
        assert res["MedianAbsMax"].iloc[0] == 10.0
        assert "mean_storm_is_median_storm" in res.columns
        assert res["mean_storm_is_median_storm"].iloc[0] == True
        assert res["internalName"].iloc[0] == "Int1"


class TestFindAepMedianMaxEdgeCases:
    def test_mean_peakflow_missing(self):
        # mean_PeakFlow not in columns
        df = pd.DataFrame(
            {
                "aep_text": ["1%"],
                "Location": ["L1"],
                "Type": ["Q"],
                "trim_runcode": ["EXG"],
                "MedianAbsMax": [10.0],
                "duration_text": ["1h"],
            }
        )
        res = pomm_utils.find_aep_median_max(df)
        assert len(res) == 1
        assert res["MedianAbsMax"].iloc[0] == 10.0

    def test_mean_peakflow_present(self):
        df = pd.DataFrame(
            {
                "aep_text": ["1%"],
                "Location": ["L1"],
                "Type": ["Q"],
                "trim_runcode": ["EXG"],
                "MedianAbsMax": [10.0],
                "duration_text": ["1h"],
                "mean_PeakFlow": [15.0],
                "mean_including_zeroes": [15.0],
            }
        )
        res = pomm_utils.find_aep_median_max(df)
        assert "mean_PeakFlow" in res.columns
        assert res["mean_PeakFlow"].iloc[0] == 15.0


class TestFindAepMeanMaxEdgeCases:
    def test_mean_peakflow_missing(self):
        df = pd.DataFrame({"aep_text": ["1%"], "Location": ["L1"], "Type": ["Q"], "trim_runcode": ["EXG"]})
        res = pomm_utils.find_aep_mean_max(df)
        assert res.empty

    def test_mean_peakflow_no_valid(self):
        df = pd.DataFrame(
            {"aep_text": ["1%"], "Location": ["L1"], "Type": ["Q"], "trim_runcode": ["EXG"], "mean_PeakFlow": [pd.NA]}
        )
        res = pomm_utils.find_aep_mean_max(df)
        assert res.empty
