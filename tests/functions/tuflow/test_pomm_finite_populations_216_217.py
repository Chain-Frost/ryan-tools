"""Independent POMM finite-population, count and event provenance regressions."""

from itertools import permutations
from pathlib import Path

import pandas as pd
import pytest
from pandas import DataFrame

from ryan_library.functions.tuflow.pomm_utils import (
    find_aep_dur_max,
    find_aep_dur_mean,
    find_aep_dur_median,
    find_aep_max,
    find_aep_mean_max,
    find_aep_median_max,
    save_peak_report_mean,
    save_peak_report_median,
)


def _events(values: list[float], duration: str = "0060m") -> DataFrame:
    return DataFrame(
        {
            "aep_text": ["01p"] * len(values),
            "duration_text": [duration] * len(values),
            "Location": ["Outlet"] * len(values),
            "Type": ["Flow"] * len(values),
            "trim_runcode": ["Scenario"] * len(values),
            "tp_text": [f"TP{index:02d}" for index in range(1, len(values) + 1)],
            "tp_numeric": list(range(1, len(values) + 1)),
            "AbsMax": values,
            "internalName": [f"Scenario_01p_{duration}_TP{index:02d}" for index in range(1, len(values) + 1)],
            "file": [f"Scenario_01p_{duration}_TP{index:02d}_POMM.csv" for index in range(1, len(values) + 1)],
        }
    )


def test_upper_middle_uses_sixth_of_ten_finite_members() -> None:
    group = _events([float(index) for index in range(1, 11)])
    row = find_aep_dur_median(group).iloc[0]
    assert row["MedianAbsMax"] == 6
    assert row["median_TP"] == "TP06"
    assert row["count_TP"] == 10
    assert row["count_unusable_TP"] == 0
    assert row["tp_population_status"] == "OK"


def test_nearest_mean_tie_provenance_is_from_one_event_in_every_permutation() -> None:
    # TP01=4, TP02=1, TP03=3, TP04=2; mean 2.5.
    # Lower adjacent value 2, TP04 wins the tie; median is value 3, TP03.
    source = _events([4, 1, 3, 2])
    for ordering in permutations(range(4)):
        group = source.iloc[list(ordering)].reset_index(drop=True)
        median_row = find_aep_dur_median(group).iloc[0]
        mean_row = find_aep_dur_mean(group).iloc[0]
        assert median_row["MedianAbsMax"] == 3
        assert median_row["median_TP"] == "TP03"
        assert median_row["internalName"] == "Scenario_01p_0060m_TP03"
        assert median_row["file"] == "Scenario_01p_0060m_TP03_POMM.csv"
        assert median_row["mean_TP"] == "TP04"
        assert median_row["mean_PeakFlow"] == 2
        assert median_row["mean_internalName"] == "Scenario_01p_0060m_TP04"
        assert median_row["mean_file"] == "Scenario_01p_0060m_TP04_POMM.csv"
        assert mean_row["internalName"] == median_row["mean_internalName"]
        assert mean_row["file"] == median_row["mean_file"]


@pytest.mark.parametrize(
    ("values", "expected_median", "expected_count", "expected_status"),
    [
        ([2.0, float("nan"), float("nan")], 2.0, 1, "PARTIAL"),
        ([1.0, 2.0, float("nan")], 2.0, 2, "PARTIAL"),
        ([2.0, float("inf"), 4.0], 4.0, 2, "PARTIAL"),
        ([0.0, 0.0, 0.0], 0.0, 3, "OK"),
    ],
)
def test_missing_infinite_and_valid_zero_members(
    values: list[float], expected_median: float, expected_count: int, expected_status: str
) -> None:
    group = _events(values)
    median = find_aep_dur_median(group).iloc[0]
    assert median["MedianAbsMax"] == expected_median
    assert median["count_TP"] == expected_count
    assert median["count_unusable_TP"] == len(values) - expected_count
    assert median["tp_population_status"] == expected_status
    maximum = find_aep_dur_max(group).iloc[0]
    assert maximum["count_TP"] == expected_count
    assert maximum["count_unusable_TP"] == len(values) - expected_count


def test_all_invalid_bin_never_governs_or_claims_complete_population() -> None:
    group = _events([float("nan"), float("inf"), float("-inf")])
    median = find_aep_dur_median(group).iloc[0]
    assert pd.isna(median["MedianAbsMax"])
    assert median["count_TP"] == 0
    assert median["count_unusable_TP"] == 3
    assert median["tp_population_status"] == "UNUSABLE"
    assert find_aep_median_max(find_aep_dur_median(group)).empty
    assert find_aep_mean_max(find_aep_dur_mean(group)).empty
    assert find_aep_dur_max(group).empty


def test_unequal_duration_tp_counts_remain_event_counts_in_final_tables() -> None:
    group = pd.concat([_events([1, 3, 5], "0060m"), _events([4, 6], "0120m")], ignore_index=True)
    duration_max = find_aep_dur_max(group)
    assert len(duration_max) == 2
    assert set(duration_max["count_TP"].tolist()) == {2, 3}
    assert set(duration_max["count_TP_aep"].tolist()) == {5}
    assert set(duration_max["count_duration"].tolist()) == {2}
    final_max = find_aep_max(duration_max).iloc[0]
    assert final_max["count_TP_aep"] == 5
    assert final_max["count_duration"] == 2

    duration_median = find_aep_dur_median(group)
    assert set(duration_median["count_TP_aep"].tolist()) == {5}
    assert set(duration_median["count_duration"].tolist()) == {2}
    final_median = find_aep_median_max(duration_median).iloc[0]
    assert final_median["MedianAbsMax"] == 6
    assert final_median["count_TP_aep"] == 5
    assert final_median["count_duration"] == 2

    final_mean = find_aep_mean_max(find_aep_dur_mean(group)).iloc[0]
    assert final_mean["mean_PeakFlow"] == 4
    assert final_mean["count_TP_aep"] == 5
    assert final_mean["count_duration"] == 2


def test_workbooks_preserve_counts_selected_run_and_dictionary(tmp_path: Path) -> None:
    group = pd.concat([_events([1, 3, 5], "0060m"), _events([4, 6], "0120m")], ignore_index=True)
    save_peak_report_median(group, script_directory=tmp_path, timestamp="wp03")
    save_peak_report_mean(group, script_directory=tmp_path, timestamp="wp03")

    for suffix, sheet, statistic in (
        ("_med_peaks.xlsx", "aep-max", "MedianAbsMax"),
        ("_mean_peaks.xlsx", "aep-mean-max", "mean_PeakFlow"),
    ):
        workbook_path = tmp_path / f"wp03{suffix}"
        assert workbook_path.is_file()
        with pd.ExcelFile(workbook_path) as workbook:
            assert {"POMM", sheet, "data-dictionary"}.issubset(workbook.sheet_names)
            reported = workbook.parse(sheet)
            dictionary = workbook.parse("data-dictionary")
        assert len(reported) == 1
        assert reported.iloc[0]["count_TP_aep"] == 5
        assert reported.iloc[0]["count_duration"] == 2
        assert reported.iloc[0][statistic] == (6 if statistic == "MedianAbsMax" else 4)
        assert pd.notna(reported.iloc[0]["internalName"])
        assert {"count_TP_aep", "count_duration", "internalName"}.issubset(set(dictionary["column"]))
