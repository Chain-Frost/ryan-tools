"""Behavioral tests for the maintained RORB closure-duration orchestrator."""

# pyright: reportPrivateUsage=false

from pathlib import Path

import pandas as pd

from ryan_library.orchestrators.rorb.closure_durations import (
    _default_thresholds,
    _process_hydrographs,
    _summarise_results,
    _worker_count,
)


def test_default_thresholds_are_unique_and_ascending() -> None:
    thresholds = _default_thresholds()

    assert thresholds == sorted(set(thresholds))
    assert thresholds[:3] == [1.0, 2.0, 3.0]
    assert thresholds[-1] == 2090.0


def test_worker_count_respects_explicit_limit() -> None:
    assert _worker_count(job_count=2, pool_size=20) == 2
    assert _worker_count(job_count=20, pool_size=2) == 2


def test_process_hydrographs_can_run_serially(tmp_path: Path) -> None:
    hydrograph_path = tmp_path / "hydrograph.csv"
    hydrograph_path.write_text(
        "Header\nHeader\nInc,Time (hrs),Calculated hydrograph:  L1\n0,0,0\n1,1,10\n",
        encoding="utf-8",
    )
    batch = pd.DataFrame(
        {
            "AEP": ["1"],
            "Duration": [12.0],
            "TPat": [1],
            "csv": [str(hydrograph_path)],
            "Path": [str(tmp_path / "batch.out")],
        }
    )

    result = _process_hydrographs(batch, [5.0, 50.0], pool_size=1)

    assert result["Duration_Exceeding"].tolist() == [1.0, 0.0]


def test_summary_uses_upper_middle_critical_duration_and_includes_zeroes() -> None:
    result = pd.DataFrame(
        {
            "AEP": ["1"] * 8,
            "Duration": [12] * 4 + [24] * 4,
            "TP": [1, 2, 3, 4] * 2,
            "Location": ["L1"] * 8,
            "ThresholdFlow": [50.0] * 8,
            "Duration_Exceeding": [0.0, 2.0, 4.0, 6.0, 0.0, 1.0, 9.0, 10.0],
            "out_path": ["batch.out"] * 8,
        }
    )

    summary = _summarise_results(result)

    row = summary.iloc[0]
    assert row["Central_Value"] == 9.0
    assert row["Critical_Duration"] == 24
    assert row["Critical_Tp"] == 3
    assert row["Low_Value"] == 0.0
    assert row["High_Value"] == 10.0
    assert row["Average_Value"] == 5.0


def test_summary_uses_the_middle_record_for_an_odd_pattern_count() -> None:
    result = pd.DataFrame(
        {
            "AEP": ["1"] * 5,
            "Duration": [12] * 5,
            "TP": [1, 2, 3, 4, 5],
            "Location": ["L1"] * 5,
            "ThresholdFlow": [50.0] * 5,
            "Duration_Exceeding": [0.0, 1.0, 2.0, 3.0, 4.0],
            "out_path": ["batch.out"] * 5,
        }
    )

    row = _summarise_results(result).iloc[0]

    assert row["Central_Value"] == 2.0
    assert row["Critical_Tp"] == 3
