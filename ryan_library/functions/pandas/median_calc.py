from typing import Any

import pandas as pd


def _median_stats_for_group(
    durgrp: pd.DataFrame, statcol: str, tpcol: str, durcol: str
) -> dict[str, Any]:
    """Return statistics for a single duration group."""

    ensemblestat: pd.DataFrame = durgrp.sort_values(
        statcol, ascending=True, na_position="first"
    )
    r: int = len(ensemblestat.index)
    medianpos: int = int(r / 2)

    mean_including_zeroes: float = float(ensemblestat[statcol].mean())
    mean_excluding_zeroes: float = float(
        ensemblestat[ensemblestat[statcol] != 0][statcol].mean()
    )

    return {
        "mean_including_zeroes": mean_including_zeroes,
        "mean_excluding_zeroes": mean_excluding_zeroes,
        "Duration": ensemblestat[durcol].iloc[medianpos],
        "Critical_TP": ensemblestat[tpcol].iloc[medianpos],
        "low": ensemblestat[statcol].iloc[0],
        "high": ensemblestat[statcol].iloc[-1],
        "count": r,
        "median": ensemblestat[statcol].iloc[medianpos],
    }


def median_calc(
    thinned_df: pd.DataFrame, statcol: str, tpcol: str, durcol: str
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

    for _, durgrp in thinned_df.groupby(by=durcol):
        stats_dict: dict[str, Any] = _median_stats_for_group(
            durgrp=durgrp, statcol=statcol, tpcol=tpcol, durcol=durcol
        )

        if stats_dict["median"] > tracking_median:
            max_stats_dict = stats_dict.copy()
            tracking_median = stats_dict["median"]

        bin_stats_list.append(stats_dict)
        count_bin += stats_dict["count"]

    max_stats_dict["count_bin"] = count_bin
    return max_stats_dict, bin_stats_list
