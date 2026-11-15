"""Regression for pandas-compatible POMM duration/TP comparison (#227)."""

from pandas import DataFrame

from ryan_library.functions.tuflow.pomm_utils import find_aep_dur_median


def test_actual_pomm_aggregation_maps_durations_on_supported_pandas() -> None:
    """Exercise the actual numeric aggregation, not a mocked median helper."""
    dataframe = DataFrame(
        {
            "aep_text": ["01p"] * 3,
            "duration_text": ["0060m"] * 3,
            "Location": ["L1"] * 3,
            "Type": ["Flow"] * 3,
            "trim_runcode": ["Model"] * 3,
            "internalName": [f"Model_01p_0060m_TP{tp:02d}" for tp in (1, 2, 3)],
            "tp_text": ["TP01", "TP02", "TP03"],
            "AbsMax": [1.0, 2.0, 3.0],
        }
    )

    result = find_aep_dur_median(aggregated_df=dataframe)

    assert len(result) == 1
    row = result.iloc[0]
    assert row["MedianAbsMax"] == 2.0
    assert row["mean_PeakFlow"] == 2.0
    assert row["median_TP"] == "TP02"
    assert row["mean_TP"] == "TP02"
    assert bool(row["mean_storm_is_median_storm"])
