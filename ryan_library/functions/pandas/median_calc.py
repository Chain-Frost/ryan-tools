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
    count_unusable_TP: int
    count_unusable_TP_aep: int
    count_duration_unusable: int
    tp_population_status: str
    median_internalName: object
    mean_internalName: object
    median_file: object
    mean_file: object


def upper_middle_position(row_count: int) -> int:
    """Return the upper-middle position for the valid members of a TUFLOW ensemble."""
    return row_count // 2


def finite_sorted_rows(group: DataFrame, value_column: str) -> DataFrame:
    """Return finite observations in stable statistic/TP/source order.

    Non-numeric, missing and infinite values do not represent usable hydraulic
    observations. Zero is a valid observation. Secondary keys make all ties
    deterministic across input permutations without changing untied ranks.
    """
    if value_column not in group.columns:
        return group.iloc[:0].copy()

    values = pd.to_numeric(arg=group[value_column], errors="coerce")
    finite = values.notna() & values.abs().ne(float("inf"))
    if not bool(finite.any()):
        return group.iloc[:0].copy()

    valid = group.loc[finite].copy()
    valid[value_column] = values.loc[finite]
    sort_columns = [value_column]
    temporary_columns: list[str] = []
    for candidate in ("tp_numeric", "tp_text", "internalName", "file"):
        if candidate not in valid.columns or candidate == value_column:
            continue
        temporary = f"_finite_sort_{len(temporary_columns)}"
        if candidate == "tp_numeric":
            valid[temporary] = pd.to_numeric(arg=valid[candidate], errors="coerce").fillna(float("inf"))
        else:
            valid[temporary] = valid[candidate].astype("string").fillna("")
        temporary_columns.append(temporary)
        sort_columns.append(temporary)

    return valid.sort_values(by=sort_columns, kind="stable").drop(columns=temporary_columns)


def upper_middle_row(group: DataFrame, value_column: str) -> pd.Series:
    """Choose the upper-middle actual finite observation, including stable ties."""
    ordered = finite_sorted_rows(group=group, value_column=value_column)
    if ordered.empty:
        msg = f"No finite values exist for {value_column!r}."
        raise ValueError(msg)
    return ordered.iloc[upper_middle_position(row_count=len(ordered))]


def upper_middle_value(group: DataFrame, value_column: str) -> object:
    """Return the upper-middle finite value, or NA without a finite population."""
    ordered = finite_sorted_rows(group=group, value_column=value_column)
    if ordered.empty:
        return pd.NA
    return ordered[value_column].iloc[upper_middle_position(row_count=len(ordered))]


def summarise_duration_statistics(durgrp: DataFrame, stat_col: str, tp_col: str, dur_col: str) -> MedianStatistics:
    """Calculate finite-member TP statistics and coherent selected-event metadata."""
    observed_count = len(durgrp)
    ensemblestat = finite_sorted_rows(group=durgrp, value_column=stat_col)
    contributing_count = len(ensemblestat)
    unusable_count = observed_count - contributing_count
    status = "UNUSABLE" if contributing_count == 0 else ("PARTIAL" if unusable_count else "OK")

    if ensemblestat.empty:
        return {
            "mean_including_zeroes": float("nan"),
            "mean_excluding_zeroes": float("nan"),
            "mean_Duration": pd.NA,
            "mean_TP": pd.NA,
            "mean_PeakFlow": float("nan"),
            "median_duration": pd.NA,
            "median_TP": pd.NA,
            "median": float("nan"),
            "low": float("nan"),
            "high": float("nan"),
            "count_TP": 0,
            "count_unusable_TP": unusable_count,
            "tp_population_status": status,
        }

    # Sort by statistic first so an equal-distance mean tie adopts the lower
    # observed flow; then the stable TP/source keys determine any exact tie.
    stat_series = ensemblestat[stat_col]
    mean_including_zeroes = float(stat_series.mean())
    mean_excluding_zeroes = float(stat_series[stat_series != 0].mean())
    mean_position = int((stat_series - mean_including_zeroes).abs().to_numpy().argmin())
    mean_row = ensemblestat.iloc[mean_position]
    median_row = ensemblestat.iloc[upper_middle_position(row_count=contributing_count)]

    stats: MedianStatistics = {
        "mean_including_zeroes": mean_including_zeroes,
        "mean_excluding_zeroes": mean_excluding_zeroes,
        "mean_Duration": mean_row[dur_col],
        "mean_TP": mean_row[tp_col],
        "mean_PeakFlow": float(mean_row[stat_col]),
        "median_duration": median_row[dur_col],
        "median_TP": median_row[tp_col],
        "median": float(median_row[stat_col]),
        "low": float(stat_series.iloc[0]),
        "high": float(stat_series.iloc[-1]),
        "count_TP": contributing_count,
        "count_unusable_TP": unusable_count,
        "tp_population_status": status,
    }
    if "internalName" in ensemblestat.columns:
        stats["median_internalName"] = median_row["internalName"]
        stats["mean_internalName"] = mean_row["internalName"]
    if "file" in ensemblestat.columns:
        stats["median_file"] = median_row["file"]
        stats["mean_file"] = mean_row["file"]
    return stats


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

    Returns:
    -------
    tuple[dict[str, Any], list[dict[str, Any]]]
        A tuple containing the stats for the duration with the largest median and
        a list of stats for each duration group.
    """
    max_stats_dict: MedianStatistics = {}
    bin_stats_list: list[MedianStatistics] = []
    tracking_median: float = float("-inf")
    count_TP_aep = 0
    unusable_TP_aep = 0
    duration_bins = 0
    unusable_bins = 0

    for _, durgrp in thinned_df.groupby(by=dur_col):
        stats_dict: MedianStatistics = summarise_duration_statistics(
            durgrp=durgrp, stat_col=stat_col, tp_col=tp_col, dur_col=dur_col
        )

        median_value: float | None = stats_dict.get("median")
        if median_value is not None and median_value > tracking_median:
            max_stats_dict = stats_dict.copy()
            tracking_median = median_value
        elif not max_stats_dict:
            # Retain the first unusable bin's metadata if every bin lacks
            # finite observations. A later valid bin still supersedes it.
            max_stats_dict = stats_dict.copy()

        bin_stats_list.append(stats_dict)
        count_TP_aep += stats_dict.get("count_TP", 0)
        unusable_TP_aep += stats_dict.get("count_unusable_TP", 0)
        if stats_dict.get("count_TP", 0):
            duration_bins += 1
        else:
            unusable_bins += 1

    max_stats_dict["count_TP_aep"] = count_TP_aep
    max_stats_dict["count_unusable_TP_aep"] = unusable_TP_aep
    max_stats_dict["count_duration"] = duration_bins
    max_stats_dict["count_duration_unusable"] = unusable_bins
    # Global ranges use the same finite TP population as every duration bin.
    all_valid = finite_sorted_rows(group=thinned_df, value_column=stat_col)
    if not all_valid.empty:
        max_stats_dict["low"] = float(all_valid[stat_col].iloc[0])
        max_stats_dict["high"] = float(all_valid[stat_col].iloc[-1])
    elif "median" not in max_stats_dict:
        max_stats_dict["median"] = float("nan")
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
