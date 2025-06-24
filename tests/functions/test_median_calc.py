import pandas as pd
import numpy as np


from ryan_library.functions.pandas.median_calc import (
    _median_stats_for_group,
    median_calc,
)


def make_df(values, tps, dur) -> pd.DataFrame:
    return pd.DataFrame(
        data={
            "val": values,
            "tp": tps,
            "dur": dur,
        }
    )


def test_median_stats_for_group_odd() -> None:
    df: pd.DataFrame = make_df([1, 2, 3], ["A", "B", "C"], ["5", "5", "5"])
    stats: dict = _median_stats_for_group(df, "val", "tp", "dur")
    assert stats["median"] == 2
    assert stats["low"] == 1
    assert stats["high"] == 3
    assert stats["count"] == 3
    assert stats["Duration"] == "5"
    assert stats["Critical_TP"] == "B"


def test_median_stats_for_group_even() -> None:
    df: pd.DataFrame = make_df([4, 1, 3, 2], ["A", "B", "C", "D"], ["1", "1", "1", "1"])
    stats: dict = _median_stats_for_group(df, "val", "tp", "dur")
    # sorted values [1,2,3,4], median index 2 -> value 3
    assert stats["median"] == 3
    assert stats["low"] == 1
    assert stats["high"] == 4
    assert stats["count"] == 4
    assert stats["Duration"] == "1"
    assert stats["Critical_TP"] == "C"


def test_median_stats_for_group_zeros() -> None:
    df: pd.DataFrame = make_df([0, 0, 0], ["A", "B", "C"], ["d", "d", "d"])
    stats = _median_stats_for_group(df, "val", "tp", "dur")
    assert stats["median"] == 0
    assert stats["low"] == 0
    assert stats["high"] == 0
    assert np.isnan(stats["mean_excluding_zeroes"])


def test_median_calc_multiple_groups() -> None:
    df: pd.DataFrame = pd.concat(
        [
            make_df([1, 2, 3], ["A", "B", "C"], ["5", "5", "5"]),
            make_df([5, 6], ["D", "E"], ["10", "10"]),
        ]
    )
    max_stats, stats_list = median_calc(df, "val", "tp", "dur")
    assert len(stats_list) == 2
    assert max_stats["median"] == 6
    assert max_stats["Duration"] == "10"
    assert max_stats["count_bin"] == 5


def test_median_calc_ties_choose_first() -> None:
    df: pd.DataFrame = pd.concat(
        [
            make_df([2, 2], ["A", "B"], ["5", "5"]),
            make_df([2, 2], ["C", "D"], ["10", "10"]),
        ]
    )
    max_stats, _ = median_calc(df, "val", "tp", "dur")
    # groupby sorts keys so '10' comes before '5'; tie keeps first group ('10')
    assert max_stats["Duration"] == "10"
    assert max_stats["median"] == 2


def test_median_calc_single_group() -> None:
    df: pd.DataFrame = make_df([7, 9, 8], ["A", "B", "C"], ["1", "1", "1"])
    max_stats, stats_list = median_calc(df, "val", "tp", "dur")
    assert max_stats["median"] == 8
    assert max_stats["count_bin"] == 3
    assert len(stats_list) == 1
