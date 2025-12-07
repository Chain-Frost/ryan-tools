"""Unit tests for ryan_library.functions.tuflow.closure_durations_functions."""

from unittest.mock import patch
import pandas as pd
from pandas import DataFrame

from ryan_library.functions.tuflow.closure_durations_functions import summarise_results


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

        result: DataFrame = summarise_results(df)

    assert not result.empty
    assert "Central_Value" in result.columns
    assert result.iloc[0]["Central_Value"] == 15.0
