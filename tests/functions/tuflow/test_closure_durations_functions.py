"""Unit tests for ryan_library.functions.tuflow.closure_durations_functions."""

from unittest.mock import MagicMock, patch
import pandas as pd
from pandas import DataFrame

from ryan_library.functions.tuflow import closure_durations_functions as cdf
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection


def test_first_value() -> None:
    s = pd.Series([None, "", "  ", "hello", "world"])
    assert cdf.first_value(s) == "hello"

    s_empty = pd.Series([None, ""])
    assert cdf.first_value(s_empty) == ""


def test_timestep_from_series() -> None:
    s = pd.Series([0, 1.5, 3.0, "not_a_number"])
    assert cdf.timestep_from_series(s) == 1.5

    s_short = pd.Series([1.0])
    assert cdf.timestep_from_series(s_short) is None


def test_collect_po_data() -> None:
    mock_po_proc = MagicMock()
    mock_po_proc.data_type = "PO"
    mock_po_proc.df = pd.DataFrame({"A": [1]})

    mock_other_proc = MagicMock()
    mock_other_proc.data_type = "Q"
    mock_other_proc.df = pd.DataFrame({"B": [2]})

    collection = MagicMock(spec=ProcessorCollection)
    collection.processors = [mock_po_proc, mock_other_proc, mock_po_proc]

    df = cdf.collect_po_data(collection)
    assert len(df) == 2
    assert "A" in df.columns
    assert "B" not in df.columns


def test_collect_po_data_empty() -> None:
    collection = MagicMock(spec=ProcessorCollection)
    collection.processors = []
    assert cdf.collect_po_data(collection).empty


def test_calculate_threshold_durations() -> None:
    df = pd.DataFrame(
        {
            "Type": ["Flow", " Flow ", "V", "Flow"],
            "Time": [0, 1, 2, 3],
            "Value": [10, 20, 50, 30],
            "Location": ["Loc1", "Loc1", "Loc1", "Loc1"],
            "aep_text": ["1% AEP"] * 4,
            "duration_text": ["2hr"] * 4,
            "tp_text": ["TP1"] * 4,
            "directory_path": ["/path"] * 4,
            "trim_runcode": ["run1"] * 4,
        }
    )

    res = cdf.calculate_threshold_durations(df, thresholds=[15.0], measurement_type="flow")

    assert len(res) == 1
    row = res.iloc[0]
    assert row["Location"] == "Loc1"
    assert row["ThresholdFlow"] == 15.0
    assert row["Duration_Exceeding"] == 2.0
    assert row["AEP"] == "1% AEP"


def test_calculate_threshold_durations_empty() -> None:
    res = cdf.calculate_threshold_durations(pd.DataFrame(), [10], "Flow")
    assert res.empty


def test_calculate_threshold_durations_missing_location() -> None:
    df = pd.DataFrame({"Type": ["Flow"], "Time": [0], "Value": [10]})
    res = cdf.calculate_threshold_durations(df, [5], "Flow")
    assert res.empty


def test_calculate_threshold_durations_no_timestep() -> None:
    df = pd.DataFrame({"Type": ["Flow", "Flow"], "Time": [0, 0], "Value": [10, 20], "Location": ["Loc1", "Loc1"]})
    res = cdf.calculate_threshold_durations(df, [5], "Flow")
    assert res.empty


def test_summarise_results() -> None:
    """Test results summarisation."""
    df = pd.DataFrame(
        data={
            "out_path": ["p1", "p1"],
            "Location": ["L1", "L1"],
            "ThresholdFlow": [1.0, 1.0],
            "AEP": ["1%", "1%"],
            "Duration": ["1hr", "2hr"],
            "TP": ["tp1", "tp1"],
            "Duration_Exceeding": [10.0, 20.0],
            "trim_runcode": ["run1", "run1"],
        }
    )

    # Mock median_stats to return dummy dict
    with patch("ryan_library.functions.pandas.median_calc.median_stats") as mock_stats:
        mock_stats.return_value = (
            {
                "median": 15.0,
                "median_duration": "1.5hr",
                "median_TP": "tp1",
                "low": 10.0,
                "high": 20.0,
                "mean_including_zeroes": 15.0,
            },
            None,
        )

        result: DataFrame = cdf.summarise_results(df)

    assert not result.empty
    assert "Central_Value" in result.columns
    assert result.iloc[0]["Central_Value"] == 15.0
    assert result.iloc[0]["trim_runcode"] == "run1" if "trim_runcode" in result.columns else True
