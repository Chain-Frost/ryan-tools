import numpy as np
import pandas as pd

from ryan_library.functions.pandas.median_calc import (
    median_calc,
    summarise_duration_statistics,
)


def make_df(values: list[float], tps: list[str], dur: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        data={
            "val": values,
            "tp": tps,
            "dur": dur,
        }
    )


def test_median_stats_for_group_odd() -> None:
    df: pd.DataFrame = make_df([1, 2, 3], ["A", "B", "C"], ["5", "5", "5"])
    stats = summarise_duration_statistics(df, "val", "tp", "dur")
    assert stats.get("median") == 2
    assert stats.get("low") == 1
    assert stats.get("high") == 3
    assert stats.get("count_TP") == 3
    assert stats.get("median_duration") == "5"
    assert stats.get("median_TP") == "B"
    assert stats.get("mean_PeakFlow") == 2


def test_median_stats_for_group_even() -> None:
    df: pd.DataFrame = make_df([4, 1, 3, 2], ["A", "B", "C", "D"], ["1", "1", "1", "1"])
    stats = summarise_duration_statistics(df, "val", "tp", "dur")
    # sorted values [1,2,3,4], median index 2 -> value 3
    assert stats.get("median") == 3
    assert stats.get("low") == 1
    assert stats.get("high") == 4
    assert stats.get("count_TP") == 4
    assert stats.get("median_duration") == "1"
    assert stats.get("median_TP") == "C"
    assert stats.get("mean_PeakFlow") == 2


def test_median_stats_for_group_zeros() -> None:
    df: pd.DataFrame = make_df([0, 0, 0], ["A", "B", "C"], ["d", "d", "d"])
    stats = summarise_duration_statistics(df, "val", "tp", "dur")
    assert stats.get("median") == 0
    assert stats.get("low") == 0
    assert stats.get("high") == 0
    assert "mean_excluding_zeroes" in stats
    assert np.isnan(stats["mean_excluding_zeroes"])
    assert stats.get("mean_PeakFlow") == 0


def test_median_calc_multiple_groups() -> None:
    df: pd.DataFrame = pd.concat(
        [
            make_df([1, 2, 3], ["A", "B", "C"], ["5", "5", "5"]),
            make_df([5, 6], ["D", "E"], ["10", "10"]),
        ]
    )
    max_stats, stats_list = median_calc(df, "val", "tp", "dur")
    assert len(stats_list) == 2
    assert max_stats.get("median") == 6
    assert max_stats.get("median_duration") == "10"
    assert max_stats.get("count_TP_aep") == 5
    assert max_stats.get("count_duration") == 2


def test_median_calc_ties_choose_first() -> None:
    df: pd.DataFrame = pd.concat(
        [
            make_df([2, 2], ["A", "B"], ["5", "5"]),
            make_df([2, 2], ["C", "D"], ["10", "10"]),
        ]
    )
    max_stats, _ = median_calc(df, "val", "tp", "dur")
    # groupby sorts keys so '10' comes before '5'; tie keeps first group ('10')
    assert max_stats.get("median_duration") == "10"
    assert max_stats.get("median") == 2


def test_median_calc_single_group() -> None:
    df: pd.DataFrame = make_df([7, 9, 8], ["A", "B", "C"], ["1", "1", "1"])
    max_stats, stats_list = median_calc(df, "val", "tp", "dur")
    assert max_stats.get("median") == 8
    assert max_stats.get("count_TP_aep") == 3
    assert max_stats.get("count_duration") == 1
    assert len(stats_list) == 1
