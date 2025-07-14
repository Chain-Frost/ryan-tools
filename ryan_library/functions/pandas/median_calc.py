# ryan_library\functions\pandas\median_calc.py
"""Utilities for computing median statistics for grouped data."""

import pandas as pd
from pandas import DataFrame
from typing import Any


def _median_stats_for_group(durgrp: pd.DataFrame, stat_col: str, tp_col: str, dur_col: str) -> dict[str, Any]:
    """Return statistics for a single duration group."""

    ensemblestat: DataFrame = durgrp.sort_values(stat_col, ascending=True, na_position="first")
    r: int = len(ensemblestat.index)
    medianpos = int(r / 2)

    mean_including_zeroes = float(ensemblestat[stat_col].mean())
    mean_excluding_zeroes = float(ensemblestat[ensemblestat[stat_col] != 0][stat_col].mean())

    return {
        "mean_including_zeroes": mean_including_zeroes,
        "mean_excluding_zeroes": mean_excluding_zeroes,
        "Duration": ensemblestat[dur_col].iloc[medianpos],
        "Critical_TP": ensemblestat[tp_col].iloc[medianpos],
        "low": ensemblestat[stat_col].iloc[0],
        "high": ensemblestat[stat_col].iloc[-1],
        "count": r,
        "median": ensemblestat[stat_col].iloc[medianpos],
    }


def median_stats(
    thinned_df: pd.DataFrame, stat_col: str, tp_col: str, dur_col: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return median statistics for each duration group and the maximum median.
    The logic mirrors the ``stats`` function in ``TUFLOW_2023_max_med_from POMM_v9.py``.
    For each duration group the DataFrame is sorted by ``statcol``. The median
    value is selected, along with the associated temporal pattern. The group with
    the highest median is returned separately.

    Parameters
    ----------
    thinned_df:
        Data for a single AEP across multiple temporal patterns and durations.
    statcol:
        Column containing the numeric statistic to rank by (e.g. ``"AbsMax"``).
    tpcol:
        Column holding the temporal pattern identifier.
    durcol:
        Column holding the duration identifier.

    Returns
    -------
    tuple[dict[str, Any], list[dict[str, Any]]]
        A tuple containing the stats for the duration with the largest median and
        a list of stats for each duration group.
    """

    max_stats_dict: dict[str, Any] = {}
    bin_stats_list: list[dict[str, Any]] = []
    tracking_median: float = float("-inf")
    count_bin: int = 0

    for _, durgrp in thinned_df.groupby(by=dur_col):
        stats_dict: dict[str, Any] = _median_stats_for_group(
            durgrp=durgrp, stat_col=stat_col, tp_col=tp_col, dur_col=dur_col
        )

        if stats_dict["median"] > tracking_median:
            max_stats_dict = stats_dict.copy()
            tracking_median = stats_dict["median"]

        bin_stats_list.append(stats_dict)
        count_bin += stats_dict["count"]

    max_stats_dict["count_bin"] = count_bin
    # override low/high with the true min/max over all groups:
    if not thinned_df.empty:
        global_low = float(thinned_df[stat_col].min())
        global_high = float(thinned_df[stat_col].max())
        max_stats_dict["low"] = global_low
        max_stats_dict["high"] = global_high
    return max_stats_dict, bin_stats_list


def median_calc(
    thinned_df: pd.DataFrame, statcol: str, tpcol: str, durcol: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Compatibility wrapper for previous function name."""

    return median_stats(thinned_df=thinned_df, stat_col=statcol, tp_col=tpcol, dur_col=durcol)
