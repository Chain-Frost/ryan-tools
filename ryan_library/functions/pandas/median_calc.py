# ryan_library\functions\pandas\median_calc.py
"""Utilities for summarising grouped statistics for POMM reports."""

__lazy_modules__ = ["pandas"]

from collections.abc import Callable
from typing import TypedDict

import pandas as pd
from pandas import DataFrame


class MedianStatistics(TypedDict, total=False):
    """Known fields emitted by the median-statistics workflow."""

    mean_including_zeroes: float
    mean_excluding_zeroes: float
    median_duration: object
    median_TP: object
    mean_Duration: object
    mean_TP: object
    mean_PeakFlow: float
    low: float
    high: float
    count_TP: int
    median: float
    count_TP_aep: int
    count_duration: int


def upper_middle_position(row_count: int) -> int:
    """Return the upper-middle row position used by the TUFLOW median workflows."""

    return int(row_count / 2)


def upper_middle_row(group: pd.DataFrame, value_column: str) -> pd.Series:
    """Return the upper-middle row after sorting, preserving an actual simulation record."""

    sorted_group: DataFrame = group.sort_values(value_column, ascending=True, na_position="first")
    return sorted_group.iloc[upper_middle_position(row_count=len(sorted_group.index))]


def upper_middle_value(group: pd.DataFrame, value_column: str) -> object:
    """Return the upper-middle sorted value for a column."""

    if value_column not in group.columns:
        return pd.NA
    return upper_middle_row(group=group, value_column=value_column).get(value_column, pd.NA)


def summarise_duration_statistics(durgrp: pd.DataFrame, stat_col: str, tp_col: str, dur_col: str) -> MedianStatistics:
    """Return median and mean-adjacent statistics for a single duration group."""

    ensemblestat: DataFrame = durgrp.sort_values(stat_col, ascending=True, na_position="first")
    r: int = len(ensemblestat.index)
    medianpos = upper_middle_position(row_count=r)

    stat_series = ensemblestat[stat_col]
    mean_including_zeroes = float(stat_series.mean())
    mean_excluding_zeroes = float(ensemblestat[ensemblestat[stat_col] != 0][stat_col].mean())

    mean_duration: object = pd.NA
    mean_tp: object = pd.NA
    mean_peak_flow = float("nan")
    if stat_series.notna().any():
        closest_idx: int | str = (stat_series - mean_including_zeroes).abs().idxmin()
        mean_duration = ensemblestat.loc[closest_idx, dur_col]
        mean_tp = ensemblestat.loc[closest_idx, tp_col]
        mean_peak_flow = float(pd.to_numeric(ensemblestat.loc[closest_idx, stat_col], errors="coerce"))

    return {
        "mean_including_zeroes": mean_including_zeroes,
        "mean_excluding_zeroes": mean_excluding_zeroes,
        "median_duration": ensemblestat[dur_col].iloc[medianpos],
        "median_TP": ensemblestat[tp_col].iloc[medianpos],
        "mean_Duration": mean_duration,
        "mean_TP": mean_tp,
        "mean_PeakFlow": mean_peak_flow,
        "low": float(ensemblestat[stat_col].iloc[0]),
        "high": float(ensemblestat[stat_col].iloc[-1]),
        "count_TP": r,
        "median": float(ensemblestat[stat_col].iloc[medianpos]),
    }


def calculate_median_statistics(
    thinned_df: pd.DataFrame, stat_col: str, tp_col: str, dur_col: str
) -> tuple[MedianStatistics, list[MedianStatistics]]:
    """Return per-duration stats and the record with the largest median.
    The logic is based on the ``stats`` function in ``TUFLOW_2023_max_med_from POMM_v9.py``.
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

    max_stats_dict: MedianStatistics = {}
    bin_stats_list: list[MedianStatistics] = []
    tracking_median: float = float("-inf")
    count_TP_aep: int = 0
    duration_bins: int = 0

    for _, durgrp in thinned_df.groupby(by=dur_col):
        stats_dict: MedianStatistics = summarise_duration_statistics(
            durgrp=durgrp, stat_col=stat_col, tp_col=tp_col, dur_col=dur_col
        )

        median_value: float | None = stats_dict.get("median")
        if median_value is not None and median_value > tracking_median:
            max_stats_dict = stats_dict.copy()
            tracking_median = median_value

        bin_stats_list.append(stats_dict)
        count_TP_aep += stats_dict.get("count_TP", 0)
        duration_bins += 1

    max_stats_dict["count_TP_aep"] = count_TP_aep
    max_stats_dict["count_duration"] = duration_bins
    # override low/high with the true min/max over all groups:
    if not thinned_df.empty:
        global_low = float(thinned_df[stat_col].min())
        global_high = float(thinned_df[stat_col].max())
        max_stats_dict["low"] = global_low
        max_stats_dict["high"] = global_high
    return max_stats_dict, bin_stats_list


def median_calc(
    thinned_df: pd.DataFrame, statcol: str, tpcol: str, durcol: str
) -> tuple[MedianStatistics, list[MedianStatistics]]:
    """Compatibility wrapper retaining the legacy public function name."""

    return calculate_median_statistics(thinned_df=thinned_df, stat_col=statcol, tp_col=tpcol, dur_col=durcol)


# Backwards compatibility for older imports
median_stats: Callable[[pd.DataFrame, str, str, str], tuple[MedianStatistics, list[MedianStatistics]]] = (
    calculate_median_statistics
)
