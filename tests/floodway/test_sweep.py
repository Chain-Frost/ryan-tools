"""Tests for floodway interior discharge-sweep orchestration."""

import pytest

import ryan_library.orchestrators.floodway.sweep as sweep_module
from ryan_library.classes.culvert import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    CulvertMaterialName,
    RoadwayCrestPointDefinition,
    RoadwayProfileDefinition,
    RoadwaySurfaceName,
    Scenario,
)
from ryan_library.classes.floodway import (
    FloodwayFormation,
    FloodwayFormationZone,
    FloodwayZone,
)
from ryan_library.orchestrators.floodway import (
    assess_floodway_discharge_sweep,
    find_roadway_overtopping_onset,
    find_roadway_submergence_onset,
)


def _crossing() -> CrossingDefinition:
    return CrossingDefinition(
        name="Sweep floodway",
        groups=(
            CulvertGroupDefinition(
                name="Relief pipe",
                barrel=CircularBarrelDefinition(
                    diameter_mm=600.0,
                    length=30.0,
                    inlet_invert=9.4,
                    outlet_invert=9.2,
                    roughness=0.013,
                    material=CulvertMaterialName.CONCRETE_PIPE,
                ),
            ),
        ),
        roadway=RoadwayProfileDefinition(
            points=(
                RoadwayCrestPointDefinition(station=0.0, elevation=10.30),
                RoadwayCrestPointDefinition(station=10.0, elevation=10.10),
                RoadwayCrestPointDefinition(station=20.0, elevation=10.25),
            ),
            discharge_coefficient=1.7,
            surface=RoadwaySurfaceName.PAVED,
        ),
    )


def _formation() -> FloodwayFormation:
    return FloodwayFormation(
        name="Sweep formation",
        crest_flow_length=9.0,
        zones=(
            FloodwayFormationZone(zone=FloodwayZone.PAVEMENT, slope=0.03, roughness=0.015),
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_SHOULDER, elevation=10.0),
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_BATTER, slope=1.0 / 3.0, roughness=0.04),
        ),
    )


def _scenario() -> Scenario:
    return Scenario(
        name="Maximum event",
        discharge=8.0,
        tailwater=9.5,
        aep_percent=2.0,
        source="Synthetic sweep regression",
    )


def test_discharge_sweep_detects_overtopping_and_retains_non_aep_interior_states() -> None:
    crossing = _crossing()
    base = _scenario()

    onset = find_roadway_overtopping_onset(crossing, base)
    assessments = assess_floodway_discharge_sweep(crossing, base, _formation(), points=5)

    assert onset is not None
    assert 0.0 < onset < base.discharge
    assert len(assessments) >= 5
    assert all(item.hydraulics.aep_percent is None for item in assessments)
    assert all("discharge sweep" in (item.hydraulics.source or "").lower() for item in assessments)
    assert any(item.hydraulics.roadway_discharge > 0.0 for item in assessments)


def test_discharge_sweep_honours_explicit_minimum_bound() -> None:
    assessments = assess_floodway_discharge_sweep(
        _crossing(),
        _scenario(),
        _formation(),
        points=4,
        minimum_discharge=6.0,
        maximum_discharge=8.0,
    )

    assert assessments
    for assessment in assessments:
        total_discharge = assessment.hydraulics.total_discharge
        assert total_discharge is not None
        assert 6.0 <= total_discharge <= 8.0


def test_submergence_search_scans_interior_when_upper_endpoint_is_free(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_overtopping_onset(
        _crossing: CrossingDefinition,
        _scenario: Scenario,
        *,
        maximum_discharge: float | None = None,
        discharge_tolerance: float = 1e-6,
        roadway_discharge_tolerance: float = 1e-9,
    ) -> float | None:
        del maximum_discharge, discharge_tolerance, roadway_discharge_tolerance
        return 1.0

    def fake_solve(_crossing: CrossingDefinition, scenario: Scenario) -> Scenario:
        return scenario

    def fake_submerged(scenario: Scenario) -> bool:
        return 2.0 <= scenario.discharge <= 4.0

    monkeypatch.setattr(sweep_module, "find_roadway_overtopping_onset", fake_overtopping_onset)
    monkeypatch.setattr(sweep_module, "solve_crossing_scenario", fake_solve)
    monkeypatch.setattr(sweep_module, "_roadway_submerged", fake_submerged)

    onset = find_roadway_submergence_onset(
        _crossing(),
        _scenario(),
        maximum_discharge=8.0,
        discharge_tolerance=1e-6,
    )

    assert onset == pytest.approx(2.0, abs=1e-5)
