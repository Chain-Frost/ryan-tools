"""Utilities for computing median statistics for grouped data."""

import pandas as pd

from typing import Any


def _median_stats_for_group(durgrp: pd.DataFrame, stat_col: str, tp_col: str, dur_col: str) -> dict[str, Any]:
    """Return statistics for a single duration group."""

    ensemblestat = durgrp.sort_values(stat_col, ascending=True, na_position="first")
    r = len(ensemblestat.index)
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
    """Return median statistics for each duration group and the maximum median."""

    max_stats_dict: dict[str, Any] = {}
    bin_stats_list: list[dict[str, Any]] = []
    tracking_median: float = float("-inf")
    count_bin: int = 0

    for _, durgrp in thinned_df.groupby(by=dur_col):
        stats_dict = _median_stats_for_group(durgrp=durgrp, stat_col=stat_col, tp_col=tp_col, dur_col=dur_col)

        if stats_dict["median"] > tracking_median:
            max_stats_dict = stats_dict.copy()
            tracking_median = stats_dict["median"]

        bin_stats_list.append(stats_dict)
        count_bin += stats_dict["count"]

    max_stats_dict["count_bin"] = count_bin
    return max_stats_dict, bin_stats_list


def median_calc(
    thinned_df: pd.DataFrame, statcol: str, tpcol: str, durcol: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Compatibility wrapper for previous function name."""

    return median_stats(thinned_df, statcol, tpcol, durcol)


__all__ = ["_median_stats_for_group", "median_stats", "median_calc"]
