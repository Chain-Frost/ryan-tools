"""Finite-discharge TP and adopted-event regressions for culvert statistics (#216)."""

from itertools import permutations
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest
from pandas import DataFrame

from ryan_library.orchestrators.tuflow import tuflow_culverts_mean


def _culvert_events(values: list[float], duration: str = "0060m") -> DataFrame:
    return DataFrame(
        {
            "aep_text": ["01p"] * len(values),
            "duration_text": [duration] * len(values),
            "trim_runcode": ["EXG"] * len(values),
            "Chan ID": ["C1"] * len(values),
            "Q": values,
            "V": [float(index) / 10 for index in range(1, len(values) + 1)],
            "tp_text": [f"TP{index:02d}" for index in range(1, len(values) + 1)],
            "tp_numeric": list(range(1, len(values) + 1)),
            "internalName": [f"EXG_01p_{duration}_TP{index:02d}" for index in range(1, len(values) + 1)],
            "Height": [3.0] * len(values),
        }
    )


@pytest.mark.parametrize(
    ("values", "expected_mean", "expected_median", "mean_adopted", "median_adopted", "status"),
    [
        ([2.0, float("nan"), float("nan")], 2.0, 2.0, "TP01", "TP01", "PARTIAL"),
        ([1.0, 2.0, float("nan")], 1.5, 2.0, "TP01", "TP02", "PARTIAL"),
        ([2.0, float("inf"), 4.0], 3.0, 4.0, "TP01", "TP03", "PARTIAL"),
        ([0.0, 0.0, 0.0], 0.0, 0.0, "TP01", "TP02", "OK"),
    ],
)
def test_finite_q_only_and_valid_zero(
    values: list[float],
    expected_mean: float,
    expected_median: float,
    mean_adopted: str,
    median_adopted: str,
    status: str,
) -> None:
    group = _culvert_events(values)
    expected_count = sum(pd.notna(v) and abs(v) != float("inf") for v in values)
    mean = tuflow_culverts_mean.find_culvert_aep_dur_mean(group).iloc[0]
    median = tuflow_culverts_mean.find_culvert_aep_dur_median(group).iloc[0]
    for row in (mean, median):
        assert row["count"] == expected_count
        assert row["count_total"] == len(values)
        assert row["count_unusable"] == len(values) - expected_count
        assert row["tp_population_status"] == status

    assert mean["mean_Q"] == expected_mean
    assert median["median_Q"] == expected_median
    assert mean["adopted_tp_text"] == mean_adopted
    assert median["adopted_tp_text"] == median_adopted
    assert mean["adopted_Q"] == group.loc[group["tp_text"] == mean_adopted, "Q"].iloc[0]
    assert median["adopted_Q"] == group.loc[group["tp_text"] == median_adopted, "Q"].iloc[0]


def test_all_invalid_and_geometry_only_cannot_govern_peak() -> None:
    invalid = _culvert_events([float("nan"), float("inf"), float("-inf")])
    for statistic, metric in (("mean", "mean_Q"), ("median", "median_Q")):
        result = tuflow_culverts_mean._find_culvert_aep_dur_statistic(invalid, statistic)
        assert len(result) == 1
        assert result.iloc[0]["count"] == 0
        assert result.iloc[0]["count_unusable"] == 3
        assert result.iloc[0]["tp_population_status"] == "UNUSABLE"
        assert metric not in result or result[metric].isna().all()
        assert tuflow_culverts_mean._find_culvert_aep_statistic_max(result, statistic).empty

    geometry_only = invalid.drop(columns=["Q"])
    assert tuflow_culverts_mean.find_culvert_aep_dur_mean(geometry_only).empty
    assert tuflow_culverts_mean.find_culvert_aep_dur_median(geometry_only).empty
    assert tuflow_culverts_mean._preferred_metric_column(DataFrame({"mean_Height": [3.0]}), "mean") is None


def test_ties_use_stable_event_identity_not_input_order() -> None:
    source = _culvert_events([1.0, 1.0, 1.0])
    for ordering in permutations(range(3)):
        group = source.iloc[list(ordering)].reset_index(drop=True)
        mean = tuflow_culverts_mean.find_culvert_aep_dur_mean(group).iloc[0]
        median = tuflow_culverts_mean.find_culvert_aep_dur_median(group).iloc[0]
        assert mean["adopted_tp_text"] == "TP01"
        assert median["adopted_tp_text"] == "TP02"
        assert mean["adopted_Q"] == 1
        assert median["adopted_Q"] == 1


def test_real_mean_export_reports_partial_count_and_governing_duration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    values = pd.concat(
        [_culvert_events([1.0, float("nan")], "0060m"), _culvert_events([2.0, 3.0], "0120m")],
        ignore_index=True,
    )
    collection = MagicMock()
    collection.processors = [1]
    collection.combine_1d_maximums.return_value = values

    def combine_fixture(**_options: object) -> MagicMock:
        return collection

    monkeypatch.setattr(tuflow_culverts_mean, "bulk_read_and_merge_tuflow_csv", combine_fixture)
    tuflow_culverts_mean.run_culvert_mean_report(
        script_directory=tmp_path,
        paths_to_process=[tmp_path],
        export_raw=True,
    )

    matches = list(tmp_path.glob("*_culvert_mean_peaks.xlsx"))
    assert len(matches) == 1
    with pd.ExcelFile(matches[0]) as workbook:
        assert {"aep-dur-mean", "aep-mean-max", "culvert-maximums", "data-dictionary"}.issubset(
            workbook.sheet_names
        )
        per_duration = workbook.parse("aep-dur-mean")
        peak = workbook.parse("aep-mean-max")
        dictionary = workbook.parse("data-dictionary")
    assert per_duration["count"].tolist() == [1, 2]
    assert per_duration["count_unusable"].tolist() == [1, 0]
    assert per_duration["tp_population_status"].tolist() == ["PARTIAL", "OK"]
    assert peak.iloc[0]["duration_text"] == "0120m"
    assert peak.iloc[0]["mean_Q"] == 2.5
    assert {"count", "count_total", "count_unusable", "tp_population_status"}.issubset(set(dictionary["column"]))
