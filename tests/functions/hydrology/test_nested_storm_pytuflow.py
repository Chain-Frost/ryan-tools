"""Read actual nested-storm exports using the repository-pinned PyTUFLOW.

The accepted wheel is installed by hosted CI. These parser checks are not
native RF timing, rainfall-loss or hydraulic solver acceptance.
"""

from __future__ import annotations

import csv
from importlib import import_module
from math import fsum, isnan
from pathlib import Path
from typing import Any, cast

import pytest

from ryan_library.functions.hydrology.nested_storm_tuflow import RainKind, write_tuflow_bundle
from ryan_library.functions.hydrology.nested_storms import (
    DepthAnchor,
    EventLosses,
    RainfallCurve,
    StormResult,
    generate_storm,
    write_tuflow_csv,
)
from ryan_library.functions.hydrology.rainfall_loss_screening import LossSpec


def _storms() -> dict[str, StormResult]:
    curve = RainfallCurve("1%", "synthetic", (DepthAnchor(5, 4), DepthAnchor(10, 6), DepthAnchor(20, 8)))
    future = RainfallCurve(
        "1%",
        "synthetic",
        (DepthAnchor(5, 4, 0.8, 1.25), DepthAnchor(10, 6, 0.8, 1.25), DepthAnchor(20, 8, 0.8, 1.25)),
        scenario="2050_SSP2",
    )
    return {
        "AEP1_20_FL": generate_storm(
            curve, 20, timestep_min=5, loading="FL", losses=EventLosses(LossSpec(2, continuing_loss_mm_per_hour=3))
        ),
        "AEP1_20_ML_2050": generate_storm(
            future,
            20,
            timestep_min=5,
            loading="ML",
            arf_override=1,
            losses=EventLosses(LossSpec(3, continuing_loss_mm_per_hour=1), "complete", preburst_increments_mm=(5, 0)),
        ),
        "AEP1_20_BL": generate_storm(
            curve, 20, timestep_min=10, loading="BL", losses=EventLosses(LossSpec(4, continuing_loss_mm_per_hour=2))
        ),
    }


def _series(storm: StormResult, kind: RainKind) -> tuple[float, ...]:
    selected = storm.gross_mm if kind == "gross" else storm.excess_mm
    assert selected is not None
    return (0.0, *selected, 0.0)


def _check_parsed_series(
    frame: Any, column: str, expected: tuple[float, ...], timestep: int, *, total_rows: int
) -> None:
    assert frame.index.name == "Time"
    assert list(frame.columns) == [column]
    times = cast("list[float]", frame.index.tolist())
    values = cast("list[float]", frame[column].tolist())
    assert len(times) == len(values) == total_rows
    assert times == pytest.approx([index * timestep / 60 for index in range(total_rows)], abs=1e-12, rel=1e-12)
    actual = values[: len(expected)]
    assert actual == pytest.approx(expected, abs=1e-12, rel=1e-12)
    assert actual[0] == actual[-1] == 0
    assert fsum(actual) == pytest.approx(fsum(expected), abs=1e-12, rel=1e-12)
    assert all(isnan(value) for value in values[len(expected) :])


@pytest.mark.parametrize("kind", ["gross", "excess"])
def test_pytuflow_reads_generated_bundle_and_event_files(tmp_path: Path, kind: RainKind) -> None:
    pytuflow = pytest.importorskip("pytuflow")
    batch = import_module("pytuflow.batch_validation")
    validation = import_module("pytuflow.validation")
    storms = _storms()
    folder = tmp_path / kind
    write_tuflow_bundle(folder, storms, boundary_name="RF", kind=kind)
    # Only the minimal test TCF is authored here; never modify generated input
    # files or reconstruct event substitutions by hand.
    tcf = folder / "model.tcf"
    tcf.write_text("BC Event File == Event_File.tef\nBC Database == bc_dbase.csv\n", encoding="utf-8")
    model = pytuflow.TCF(tcf)
    flags = [{"e1": name} for name in storms]
    options = validation.ValidationOptions(check_bc_csv_headers=True, check_raster_metadata=False, check_crs=False)
    result = batch.validate_batch_inputs(model, flags, options=options)
    assert result.passed, result.issues
    assert len(result.dependencies.runs) == len(storms)
    for name, storm in storms.items():
        ctx = model.context({"e1": name})
        frame = ctx.bc_dbase().value("RF")
        expected = _series(storm, kind)
        _check_parsed_series(frame, "Rainfall", expected, storm.timestep_min, total_rows=len(expected))
    unknown = batch.validate_batch_inputs(model, [{"e1": "not_defined"}], options=options)
    assert not unknown.passed


@pytest.mark.parametrize("kind", ["gross", "excess"])
def test_pytuflow_reads_workbook_style_combined_csv(tmp_path: Path, kind: RainKind) -> None:
    pytuflow = pytest.importorskip("pytuflow")
    curve = RainfallCurve("1%", "synthetic", (DepthAnchor(5, 4), DepthAnchor(10, 6), DepthAnchor(20, 8)))
    losses = EventLosses(LossSpec(1, continuing_loss_mm_per_hour=2))
    storms = {
        "LONG_FL": generate_storm(curve, 20, timestep_min=5, loading="FL", losses=losses),
        "SHORT_BL": generate_storm(curve, 10, timestep_min=5, loading="BL", losses=losses),
    }
    rain_path = tmp_path / "rain.csv"
    write_tuflow_csv(rain_path, storms, kind=kind)
    database_path = tmp_path / "bc_dbase.csv"
    with database_path.open("x", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["Name", "Source", "Column 1", "Column 2"])
        for name in storms:
            writer.writerow([name, rain_path.name, "Time", name])
    database = pytuflow.BCDatabase(database_path).context()
    total_rows = max(len(storm.gross_mm) + 2 for storm in storms.values())
    for name, storm in storms.items():
        _check_parsed_series(database.value(name), name, _series(storm, kind), storm.timestep_min, total_rows=total_rows)
