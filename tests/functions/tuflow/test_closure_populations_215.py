# pyright: reportPrivateUsage=false
"""Regressions for dry closure events, unusable values and scenario identity (#215)."""

from itertools import permutations
from pathlib import Path

import pandas as pd
import pytest

from ryan_library.functions.tuflow.closure_durations_functions import (
    calculate_threshold_durations,
    summarise_results,
)


def _po_event(
    run: str,
    tp: str,
    values: list[float],
    *,
    duration: str = "0060m",
    location: str = "Road",
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "directory_path": ["output"] * len(values),
            "trim_runcode": [run] * len(values),
            "Location": [location] * len(values),
            "aep_text": ["01p"] * len(values),
            "duration_text": [duration] * len(values),
            "tp_text": [tp] * len(values),
            "Type": ["Flow"] * len(values),
            "Time": list(range(len(values))),
            "Value": values,
        }
    )


def _analyse(frames: list[pd.DataFrame], thresholds: list[float]) -> tuple[pd.DataFrame, pd.DataFrame]:
    observations = calculate_threshold_durations(pd.concat(frames, ignore_index=True), thresholds, "Flow")
    return observations, summarise_results(observations)


def test_zero_zero_one_hours_median_and_multiple_thresholds() -> None:
    raw, summary = _analyse(
        [
            _po_event("Existing", "TP01", [0, 0, 0]),
            _po_event("Existing", "TP02", [0, 0, 0]),
            _po_event("Existing", "TP03", [0, 10, 0]),
        ],
        [5.0, 15.0],
    )
    assert len(raw) == 6
    assert raw.loc[raw["ThresholdFlow"].eq(5.0), "Duration_Exceeding"].tolist() == [0.0, 0.0, 1.0]
    assert raw.loc[raw["ThresholdFlow"].eq(15.0), "Duration_Exceeding"].tolist() == [0.0, 0.0, 0.0]
    assert raw["Event_Status"].eq("OK").all()
    assert len(summary) == 2
    assert summary["Central_Value"].tolist() == [0.0, 0.0]
    assert summary["Average_Value"].tolist() == pytest.approx([1 / 3, 0.0])
    assert summary["count_TP_aep"].tolist() == [3, 3]
    assert summary["count_unusable_TP_aep"].tolist() == [0, 0]
    assert summary["tp_population_status"].tolist() == ["OK", "OK"]


def test_all_dry_events_report_zero_not_skipped() -> None:
    raw, summary = _analyse([_po_event("Existing", tp, [-3, 0, 0]) for tp in ("TP01", "TP02")], [5.0])
    assert raw["Duration_Exceeding"].tolist() == [0.0, 0.0]
    assert summary.iloc[0]["Central_Value"] == 0.0
    assert summary.iloc[0]["Average_Value"] == 0.0
    assert summary.iloc[0]["count_TP_aep"] == 2
    assert summary.iloc[0]["count_duration"] == 1


def test_scenarios_are_not_pooled_and_provenance_is_exportable() -> None:
    frames = [
        _po_event(run, f"TP{tp:02d}", [10.0] * samples)
        for run, samples in (("Existing", 2), ("Proposed", 10))
        for tp in range(1, 4)
    ]
    raw, summary = _analyse(frames, [5.0])
    assert len(raw) == 6
    by_run = summary.set_index("trim_runcode")
    assert set(by_run.index) == {"Existing", "Proposed"}
    assert by_run.loc["Existing", "Central_Value"] == 2.0
    assert by_run.loc["Proposed", "Central_Value"] == 10.0
    assert by_run.loc["Existing", "Average_Value"] == 2.0
    assert by_run.loc["Proposed", "Average_Value"] == 10.0
    assert by_run["count_TP_aep"].to_dict() == {"Existing": 3, "Proposed": 3}


@pytest.mark.parametrize("invalid", [float("nan"), float("inf"), float("-inf"), -99999.0])
def test_unusable_values_are_unknown_not_dry(invalid: float) -> None:
    raw, summary = _analyse(
        [_po_event("Existing", "TP01", [0, 0, 0]), _po_event("Existing", "TP02", [0, invalid, 0])],
        [5.0],
    )
    assert raw["Event_Status"].tolist() == ["OK", "UNUSABLE"]
    assert raw.loc[raw["TP"].eq("TP01"), "Duration_Exceeding"].iloc[0] == 0.0
    assert pd.isna(raw.loc[raw["TP"].eq("TP02"), "Duration_Exceeding"].iloc[0])
    row = summary.iloc[0]
    assert row["Central_Value"] == 0
    assert row["count_TP_aep"] == 1
    assert row["count_unusable_TP_aep"] == 1
    assert row["tp_population_status"] == "PARTIAL"


def test_all_invalid_events_do_not_claim_zero() -> None:
    raw, summary = _analyse(
        [_po_event("Existing", "TP01", [0, float("nan"), 0]), _po_event("Existing", "TP02", [0, float("inf"), 0])],
        [5.0],
    )
    assert raw["Duration_Exceeding"].isna().all()
    assert pd.isna(summary.iloc[0]["Central_Value"])
    assert summary.iloc[0]["count_TP_aep"] == 0
    assert summary.iloc[0]["count_unusable_TP_aep"] == 2
    assert summary.iloc[0]["tp_population_status"] == "UNUSABLE"


def test_missing_timestep_is_unknown_not_zero() -> None:
    bad = _po_event("Existing", "TP09", [0, 0, 0])
    bad["Time"] = 0
    raw, summary = _analyse([_po_event("Existing", "TP01", [0, 0, 0]), bad], [5.0])
    assert raw["Event_Status"].tolist() == ["OK", "UNUSABLE"]
    assert summary.iloc[0]["count_TP_aep"] == 1
    assert summary.iloc[0]["count_unusable_TP_aep"] == 1


def test_only_observed_events_count_in_incomplete_ensemble() -> None:
    raw, summary = _analyse([_po_event("Existing", "TP03", [0, 9, 0])], [5.0])
    assert len(raw) == 1
    assert summary.iloc[0]["count_TP_aep"] == 1
    assert summary.iloc[0]["count_unusable_TP_aep"] == 0
    # Do not invent missing TP01/TP02 without an expected-run manifest.
    assert summary.iloc[0]["tp_population_status"] == "OK"


def test_equal_median_rows_select_stable_tp_across_permutations() -> None:
    frames = [_po_event("Existing", tp, [0, 0, 0]) for tp in ("TP03", "TP01", "TP02")]
    for order in permutations(frames):
        _, summary = _analyse(list(order), [5.0])
        assert summary.iloc[0]["Critical_Tp"] == "TP02"


def test_partial_events_across_durations_are_counted_once_per_threshold() -> None:
    frames = [
        _po_event("Existing", "TP01", [0, 9, 0], duration="0060m"),
        _po_event("Existing", "TP02", [0, float("nan"), 0], duration="0060m"),
        _po_event("Existing", "TP01", [0, 10, 0], duration="0120m"),
    ]
    raw, summary = _analyse(frames, [5.0, 15.0])
    assert len(raw) == 6
    assert summary["count_TP_aep"].tolist() == [2, 2]
    assert summary["count_unusable_TP_aep"].tolist() == [1, 1]
    assert summary["count_duration"].tolist() == [2, 2]
    assert summary["tp_population_status"].tolist() == ["PARTIAL", "PARTIAL"]
    assert summary.loc[summary["ThresholdFlow"].eq(15.0), "Central_Value"].iloc[0] == 0.0


def test_missing_or_reserved_time_invalidates_event() -> None:
    for bad_time in (float("nan"), float("inf"), -99999.0):
        event = _po_event("Existing", "TP01", [0, 0, 0])
        event["Time"] = event["Time"].astype(float)
        event.loc[1, "Time"] = bad_time
        raw, summary = _analyse([event], [5.0])
        assert raw["Event_Status"].tolist() == ["UNUSABLE"]
        assert pd.isna(summary.iloc[0]["Central_Value"])
        assert summary.iloc[0]["count_unusable_TP_aep"] == 1


def test_export_readback_keeps_zeroes_scenarios_and_counts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """The existing protected exporter must publish true raw/report values."""
    pytest.importorskip("pyarrow")
    from contextlib import closing

    from openpyxl import load_workbook

    from ryan_library.orchestrators.tuflow.closure_durations import _export_closure_duration_artifacts

    monkeypatch.chdir(tmp_path)
    raw, summary = _analyse(
        [
            _po_event("Existing", "TP01", [0, 0, 0]),
            _po_event("Existing", "TP02", [0, 10, 0]),
            _po_event("Proposed", "TP01", [0, 10, 0]),
            _po_event("Proposed", "TP02", [0, 10, 0]),
        ],
        [5.0],
    )
    _export_closure_duration_artifacts(
        durations_df=raw, summary_df=summary, timestamp="20261108-1800", export_mode="both"
    )
    csv = pd.read_csv(tmp_path / "20261108-1800_durex.csv")
    parquet = pd.read_parquet(tmp_path / "20261108-1800_durex.parquet.gzip")
    for exported in (csv, parquet):
        assert sorted(exported.loc[exported["trim_runcode"].eq("Existing"), "Duration_Exceeding"].tolist()) == [
            0.0,
            1.0,
        ]
        assert exported["Event_Status"].eq("OK").all()
    workbooks = list(tmp_path.glob("*closure_durations.xlsx"))
    assert len(workbooks) == 1
    with closing(load_workbook(workbooks[0], read_only=True, data_only=True)) as wb:
        values = list(wb["Summary"].values)
    headers = values[0]
    run_idx = headers.index("trim_runcode")
    median_idx = headers.index("Central_Value")
    valid_idx = headers.index("count_TP_aep")
    assert {row[run_idx]: (row[median_idx], row[valid_idx]) for row in values[1:]} == {
        "Existing": (1.0, 2),
        "Proposed": (1.0, 2),
    }
