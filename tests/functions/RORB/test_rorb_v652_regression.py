"""Regression coverage for the maintained RORB 6.52 readers."""

from pathlib import Path

import pandas as pd
import pytest

from ryan_library.functions.RORB.read_rorb_files import (
    analyze_hydrograph,
    parse_batch_output,
    read_hydrograph_csv,
)

RORB_FIXTURES = Path(__file__).parents[2] / "test_data" / "RORB"


def _write_hydrograph(path: Path, *, times: str = "0.0,0.0\n1.0,10.0\n2.0,0.0") -> None:
    path.write_text(
        "RORBWin CSV Output File\n"
        "Refer output file for complete listing of inputs and outputs\n"
        "Inc,Time (hrs),Calculated hydrograph:  Loc1\n"
        + "\n".join(f"{index},{row}" for index, row in enumerate(times.splitlines()))
        + "\n",
        encoding="utf-8",
    )


def test_read_hydrograph_drops_increment_column(tmp_path: Path) -> None:
    hydrograph_path = tmp_path / "hydrograph.csv"
    _write_hydrograph(hydrograph_path)

    result = read_hydrograph_csv(hydrograph_path)

    assert result.columns.tolist() == ["Time (hrs)", "Calculated hydrograph:  Loc1"]


def test_analyze_hydrograph_retains_zeroes_without_treating_time_as_a_location(tmp_path: Path) -> None:
    hydrograph_path = tmp_path / "hydrograph.csv"
    _write_hydrograph(hydrograph_path)

    result = analyze_hydrograph(
        aep="0.2EY",
        duration="12",
        tp=1,
        csv_path=hydrograph_path,
        out_path=tmp_path / "batch.out",
        thresholds=[5.0, 50.0],
    )

    assert result["Location"].unique().tolist() == ["Loc1"]
    assert result["ThresholdFlow"].tolist() == [5.0, 50.0]
    assert result["Duration_Exceeding"].tolist() == [1.0, 0.0]


def test_analyze_hydrograph_rejects_non_uniform_time_steps(tmp_path: Path) -> None:
    hydrograph_path = tmp_path / "hydrograph.csv"
    _write_hydrograph(hydrograph_path, times="0.0,0.0\n1.0,10.0\n2.5,0.0")

    result = analyze_hydrograph(
        aep="1",
        duration="12",
        tp=1,
        csv_path=hydrograph_path,
        out_path=tmp_path / "batch.out",
        thresholds=[5.0],
    )

    assert result.empty


def test_parse_batch_output_handles_ey_aep_and_minutes(tmp_path: Path) -> None:
    batch_path = tmp_path / "example_batch.out"
    batch_path.write_text(
        "Peak  Description\n"
        "  01  Calculated hydrograph:\n"
        "\n"
        " Run        Duration   Unit   AEP   TPat  Rain(mm)  ARF  PbDepth  Peak0001\n"
        "  1         90 minute  0.2EY  3     40.20     0.93 0.30     55.3\n"
        "Run,    Representative hydrograph\n",
        encoding="utf-8",
    )

    result = parse_batch_output(batch_path)

    assert len(result.index) == 1
    assert result.loc[0, "Duration"] == pytest.approx(1.5)
    assert result.loc[0, "AEP"] == "0.2EY"
    assert result.loc[0, "TPat"] == 3
    assert Path(str(result.loc[0, "csv"])).name == "example_ aep0p2EY_du90minutetp3.csv"


def test_supplied_rorb_v652_example_parses_and_analyzes() -> None:
    batch_path = RORB_FIXTURES / "12hr-areal-example" / "Fig6_7_batch.out"

    batch = parse_batch_output(batch_path)

    assert len(batch.index) == 80
    assert {"0.5EY", "0.2EY"}.issubset(set(batch["AEP"].astype(str)))
    assert all(Path(path).is_file() for path in batch["csv"].astype(str))
    first_run = batch.iloc[0]
    assert [float(str(first_run[name])) for name in ("IL", "CL", "kc", "m", "ROC")] == [
        12.0,
        1.9,
        10.0,
        0.8,
        1.0,
    ]

    example_row = batch[(batch["AEP"] == "0.2EY") & (batch["TPat"] == 1)].iloc[0]
    result = analyze_hydrograph(
        aep=str(example_row["AEP"]),
        duration=str(example_row["Duration"]),
        tp=int(float(str(example_row["TPat"]))),
        csv_path=Path(str(example_row["csv"])),
        out_path=batch_path,
        thresholds=[50.0, 100.0, 1000.0],
    )

    assert result["Location"].tolist() == ["Calculated hydrograph"] * 3
    assert result["Duration_Exceeding"].tolist() == [9.0, 2.5, 0.0]
    assert pd.api.types.is_float_dtype(result["Duration_Exceeding"])
