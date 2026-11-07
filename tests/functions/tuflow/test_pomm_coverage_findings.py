"""Coverage findings must describe all sampled bins without changing the selected peak."""

from pathlib import Path

import pandas as pd
import pytest
from pandas import DataFrame

from ryan_library.functions.tuflow.pomm_utils import find_aep_mean_max, save_peak_report_mean


def duration_frame(durations: list[str], counts: list[int], peaks: list[float]) -> DataFrame:
    return DataFrame(
        {
            "aep_text": "01.0p",
            "Location": "PO1",
            "Type": "Q",
            "trim_runcode": "EXG",
            "duration_text": durations,
            "mean_Duration": durations,
            "count_TP": counts,
            "mean_PeakFlow": peaks,
        }
    )


def test_complete_interior_peak_has_no_findings() -> None:
    result = find_aep_mean_max(duration_frame(["0060m", "0090m", "00120m"], [10, 10, 10], [1, 3, 2]))
    row = result.iloc[0]
    assert row["mean_PeakFlow"] == 3
    assert row["min_duration_tested_minutes"] == 60
    assert row["max_duration_tested_minutes"] == 120
    assert row["findings_status"] == "OK"


@pytest.mark.parametrize(("peaks", "bound"), [([3, 2, 1], "minimum"), ([1, 2, 3], "maximum")])
def test_boundary_warning(peaks: list[float], bound: str) -> None:
    row = find_aep_mean_max(duration_frame(["60m", "90m", "120m"], [10] * 3, peaks)).iloc[0]
    assert bound in row["peak_duration_warning"]


def test_all_bins_checked_and_gaps_reported() -> None:
    row = find_aep_mean_max(duration_frame(["30m", "60m", "120m"], [9, 10, 11], [1, 3, 2])).iloc[0]
    assert row["missing_durations_minutes"] == "45, 90"
    assert "30m: 9" in row["tp_bin_warning"]
    assert "120m: 11" in row["tp_bin_warning"]
    assert row["peak_duration_warning"] == ""


def test_single_duration_and_separate_groups() -> None:
    one = duration_frame(["120m"], [10], [3])
    two = duration_frame(["60m", "90m", "120m"], [10] * 3, [1, 3, 2])
    two["Location"] = "PO2"
    rows = find_aep_mean_max(pd.concat([one, two], ignore_index=True)).set_index("Location")
    assert "minimum and maximum" in str(rows.loc["PO1", "peak_duration_warning"])
    assert rows.loc["PO1", "missing_durations_minutes"] == ""
    assert rows.loc["PO2", "findings_status"] == "OK"


def test_invalid_duration_and_missing_count_are_not_silent() -> None:
    frame = duration_frame(["unknown", "60m", "120m"], [10] * 3, [1, 3, 2]).drop(columns="count_TP")
    row = find_aep_mean_max(frame).iloc[0]
    assert "Unrecognised" in row["duration_coverage_warning"]
    assert "unknown" in row["tp_bin_warning"]


def test_custom_schedule_and_expected_count() -> None:
    frame = duration_frame(["30m", "60m", "120m"], [5] * 3, [1, 3, 2])
    row = find_aep_mean_max(frame, standard_durations=(30, 60, 120), expected_tp_count=5).iloc[0]
    assert row["findings_status"] == "OK"
    with pytest.raises(ValueError, match="expected_tp_count"):
        find_aep_mean_max(frame, expected_tp_count=0)
    with pytest.raises(ValueError, match="standard_durations"):
        find_aep_mean_max(frame, standard_durations=())


def test_findings_survive_excel_export(tmp_path: Path) -> None:
    records: list[dict[str, object]] = []
    for duration, count, value in [(30, 9, 1), (60, 10, 3), (120, 10, 2)]:
        for tp in range(count):
            records.append(
                {
                    "aep_text": "01.0p",
                    "Location": "PO1",
                    "Type": "Q",
                    "trim_runcode": "EXG",
                    "duration_text": f"{duration:05d}m",
                    "tp_text": f"TP{tp + 1:02d}",
                    "AbsMax": value,
                }
            )
    save_peak_report_mean(DataFrame(records), tmp_path, "coverage", include_pomm=False)
    # pandas exposes incompletely typed optional Excel backends in this overload.
    result = pd.read_excel(  # pyright: ignore[reportUnknownMemberType]
        tmp_path / "coverage_mean_peaks.xlsx", sheet_name="aep-mean-max"
    )
    assert result.loc[0, "min_duration_tested_minutes"] == 30
    assert result.loc[0, "max_duration_tested_minutes"] == 120
    assert result.loc[0, "missing_durations_minutes"] == "45, 90"
    assert "00030m: 9" in str(result.loc[0, "tp_bin_warning"])
    assert result.loc[0, "findings_status"] == "WARNING"
