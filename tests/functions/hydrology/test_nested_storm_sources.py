"""Offline saved-input adapter and event bundle tests."""

import csv
import json
from pathlib import Path
from typing import Any

import pytest

from ryan_library.functions.hydrology.nested_storm_sources import (
    apply_arr_loss_factors,
    arr_preburst_ratio,
    constant_preburst_increments,
    curve_from_arr_response,
    read_arr_response,
    regional_storm_losses,
    selected_preburst_ratio,
)
from ryan_library.functions.hydrology.nested_storm_tuflow import write_tuflow_bundle
from ryan_library.functions.hydrology.nested_storms import EventLosses, generate_storm
from ryan_library.functions.hydrology.rainfall_loss_screening import LossSpec


class _Response:
    def ifd_table(self, year: int) -> dict[str, Any]:
        scale = 1 if year == 1990 else 2
        return {
            "index": [5, 60, 120],
            "columns": [1, 2],
            "data": [[5 * scale, 4], [20 * scale, 10], [30 * scale, 15]],
        }

    def layer(self, name: str, required: bool = False) -> dict[str, Any] | None:
        if name == "Preburst50":
            return {"index": [60], "columns": [1, 2], "data": [[0.1, 0.2]]}
        if required:
            msg = f"Missing {name}"
            raise ValueError(msg)
        return None

    def climate_change_rainfall_factors(self, baseline_year: int, ssp: str) -> tuple[list[float], list[float]]:
        assert (baseline_year, ssp) == (2050, "SSP2")
        return [1, 2], [1.2, 1.5]

    def climate_change_loss_factors(self, baseline_year: int, ssp: str) -> tuple[float, float]:
        assert (baseline_year, ssp) == (2050, "SSP2")
        return 0.8, 0.5


def test_arr_source_without_ensemble_patterns() -> None:
    response = _Response()
    current = curve_from_arr_response(response, "1%", source="site")
    future = curve_from_arr_response(response, "1%", source="site", climate=(2050, "SSP2"), arf_override=1)
    assert current.anchors[1].point_depth_mm == 40
    assert future.anchors[1].point_depth_mm == 20
    assert future.anchors[0].rainfall_factor == 1.2
    assert future.anchors[-1].rainfall_factor == pytest.approx(1.5)
    assert future.scenario == "2050_SSP2"
    assert generate_storm(future, 120, timestep_min=5).burst_total_mm == pytest.approx(45)
    with pytest.raises(ValueError, match="source coverage"):
        generate_storm(future, 120)
    with pytest.raises(ValueError, match="Missing or ambiguous"):
        curve_from_arr_response(response, "1.1%", source="site")


def test_arr_preburst_and_loss_factors_once() -> None:
    response = _Response()
    curve = curve_from_arr_response(response, "1%", source="site", climate=(2050, "SSP2"))
    ratio = arr_preburst_ratio(response, "1%", 60, percentile="recommended")
    baseline = EventLosses(LossSpec(10, continuing_loss_mm_per_hour=4), "storm_reduce", preburst_ratio=ratio)
    losses = apply_arr_loss_factors(response, baseline, (2050, "SSP2"))
    storm = generate_storm(curve, 60, timestep_min=5, losses=losses)
    assert storm.preburst_mm == pytest.approx(2.4)
    assert storm.applied_losses is not None
    assert storm.applied_losses.initial_loss_mm == pytest.approx(5.6)
    assert storm.applied_losses.continuing_loss_mm_per_hour == 2
    with pytest.raises(ValueError, match="Do not combine"):
        apply_arr_loss_factors(response, losses, (2050, "SSP2"))
    with pytest.raises(KeyError):
        arr_preburst_ratio(response, "1%", 120, percentile="recommended")


def test_tuflow_bundle(tmp_path: Path) -> None:
    curve = curve_from_arr_response(_Response(), "1%", source="site")
    first = generate_storm(curve, 60, timestep_min=5, losses=EventLosses(LossSpec(2, continuing_loss_mm_per_hour=3)))
    second = generate_storm(curve, 120, timestep_min=10, losses=EventLosses(LossSpec(5, continuing_loss_mm_per_hour=1)))
    storms = {"AEP1_60_FL": first, "AEP1_120_ML": second}
    folder = tmp_path / "gross"
    write_tuflow_bundle(folder, storms, boundary_name="RF", kind="gross")
    with (folder / "bc_dbase.csv").open(encoding="utf-8", newline="") as stream:
        rows = list(csv.reader(stream))
    assert rows[1] == ["RF", r"rf_inflow\rain_~NSTORM~.csv", "Time", "Rainfall"]
    for name, storm in storms.items():
        assert f"BC Event Source == ~NSTORM~ | {name}" in (folder / "Event_File.tef").read_text(encoding="utf-8")
        with (folder / "rf_inflow" / f"rain_{name}.csv").open(encoding="utf-8", newline="") as stream:
            rain_rows = list(csv.reader(stream))
        assert sum(float(row[1]) for row in rain_rows[1:]) == pytest.approx(storm.burst_total_mm)
        assert float(rain_rows[-1][0]) == pytest.approx((storm.duration_min + storm.timestep_min) / 60)
    trd = (folder / "rainfall_losses.trd").read_text(encoding="utf-8")
    assert "Set Variable IL_RF == 2.0" in trd or "Set Variable IL_RF == 2\n" in trd
    assert "Set Variable IL_RF == 5.0" in trd or "Set Variable IL_RF == 5\n" in trd
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["native_qualified"] is False
    with pytest.raises(FileExistsError):
        write_tuflow_bundle(folder, storms, boundary_name="RF", kind="gross")
    excess = tmp_path / "excess"
    write_tuflow_bundle(excess, storms, boundary_name="RF", kind="excess")
    assert "Set Variable IL_RF == 0.0" in (excess / "rainfall_losses.trd").read_text(encoding="utf-8")
    with pytest.raises(ValueError, match="Unsafe"):
        write_tuflow_bundle(tmp_path / "unsafe", {"../bad": first}, boundary_name="RF", kind="gross")
    assert not (tmp_path / "unsafe").exists()


class _RegionalResponse(_Response):
    def __init__(self, layers: dict[str, Any]) -> None:
        self.layers = layers

    def layer(self, name: str, required: bool = False) -> dict[str, Any] | None:
        if name in self.layers:
            return self.layers[name]
        return super().layer(name, required)


def test_regional_losses_are_full_storm_values() -> None:
    response = _RegionalResponse(
        {"StormLossesNonNSW": {"Storm Initial Losses (mm)": 30, "Storm Continuing Losses (mm/h)": 4}}
    )
    spec, source = regional_storm_losses(response, "1%")
    assert spec == LossSpec(30, continuing_loss_mm_per_hour=4)
    assert source == "ARR StormLossesNonNSW"
    ratio, layer = selected_preburst_ratio(response, "1%", 60, percentile="recommended")
    assert (ratio, layer) == (0.1, "Preburst50")
    curve = curve_from_arr_response(response, "1%", source="site", climate=(2050, "SSP2"))
    selected = EventLosses(spec, "storm_reduce", preburst_ratio=ratio, source=source)
    storm = generate_storm(curve, 60, timestep_min=5, losses=apply_arr_loss_factors(response, selected, (2050, "SSP2")))
    assert storm.applied_losses is not None
    assert storm.applied_losses.initial_loss_mm == pytest.approx(21.6)
    assert storm.applied_losses.continuing_loss_mm_per_hour == 2
    assert storm.loss_selection is not None
    assert storm.loss_selection.source == source
    assert storm.loss_selection.climate_factors_source == "ARR ClimateChange 2050_SSP2"


def test_regional_nsw_exact_row_or_explicit_user_loss() -> None:
    response = _RegionalResponse(
        {
            "NewStormLosses": {
                "losses": [
                    {"AEP": "1%", "Storm Initial Loss (mm)": 45, "Continuing Loss (mm/h)": 2},
                    {"AEP": "2%", "Storm Initial Loss (mm)": 35, "Continuing Loss (mm/h)": 3},
                ]
            },
            "StormLosses": {"Storm Initial Losses (mm)": 12, "Storm Continuing Losses (mm/h)": 1},
        }
    )
    assert regional_storm_losses(response, "2%") == (
        LossSpec(35, continuing_loss_mm_per_hour=3),
        "ARR NewStormLosses (2%)",
    )
    with pytest.raises(ValueError, match="No unique"):
        regional_storm_losses(response, "5%")
    with pytest.raises(ValueError, match="No regional"):
        regional_storm_losses(_Response(), "1%")


def test_constant_preburst_explicit_duration_and_mass() -> None:
    curve = curve_from_arr_response(_Response(), "1%", source="site", climate=(2050, "SSP2"))
    prefix = constant_preburst_increments(curve, 60, timestep_min=5, preburst_duration_min=15, ratio=0.1)
    assert prefix == pytest.approx((0.8, 0.8, 0.8))
    storm = generate_storm(
        curve,
        60,
        timestep_min=5,
        losses=EventLosses(LossSpec(1, continuing_loss_mm_per_hour=0), "complete", preburst_increments_mm=prefix),
    )
    assert storm.preburst_mm == pytest.approx(2.4)
    assert storm.burst_total_mm == pytest.approx(24)
    assert storm.excess_mm is not None
    assert sum(storm.excess_mm) == pytest.approx(25.4)
    with pytest.raises(ValueError, match="multiple"):
        constant_preburst_increments(curve, 60, timestep_min=5, preburst_duration_min=13, ratio=0.1)


def test_saved_response_uses_actual_pytuflow_reader(tmp_path: Path) -> None:
    pytest.importorskip("pytuflow.arr.api_client")
    path = tmp_path / "arr.json"
    path.write_text(
        json.dumps(
            {
                "layers": {
                    "RecIFD": {
                        "Default Current (2030) Baseline": {
                            "index": [5, 60],
                            "columns": [1],
                            "data": [[5], [20]],
                        }
                    },
                    "StormLossesNonNSW": {"Storm Initial Losses (mm)": 12, "Storm Continuing Losses (mm/h)": 3},
                    "Preburst50": {"index": [60], "columns": [1], "data": [[0.1]]},
                }
            }
        ),
        encoding="utf-8",
    )
    response = read_arr_response(path)
    curve = curve_from_arr_response(response, "1%", source=str(path), arf_override=1)
    spec, source = regional_storm_losses(response, "1%")
    ratio, layer = selected_preburst_ratio(response, "1%", 60, percentile="recommended")
    storm = generate_storm(
        curve,
        60,
        timestep_min=5,
        losses=EventLosses(spec, "storm_reduce", preburst_ratio=ratio, source=source, preburst_source=layer),
    )
    assert storm.burst_total_mm == pytest.approx(20)
    assert storm.applied_losses == LossSpec(10, continuing_loss_mm_per_hour=3)
