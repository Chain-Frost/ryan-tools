"""Focused integration tests for the culvert-roadway to floodway boundary."""

import pytest
from culvert_solver import CrossingHydraulicResult, RoadwayProfileWeir, calculate_roadway_overtopping

from ryan_library.classes.culvert import RoadwayCrestPointDefinition, RoadwayProfileDefinition, RoadwaySurfaceName, ScenarioResult
from ryan_library.classes.floodway import RoadwaySegmentState
from ryan_library.functions.culvert import build_solver_roadway
from ryan_library.functions.floodway import build_floodway_scenario_hydraulics


def test_profile_roadway_definition_builds_public_solver_profile() -> None:
    definition = RoadwayProfileDefinition(
        points=(
            RoadwayCrestPointDefinition(station=0.0, elevation=10.2),
            RoadwayCrestPointDefinition(station=10.0, elevation=10.0),
            RoadwayCrestPointDefinition(station=20.0, elevation=10.3),
        ),
        discharge_coefficient=1.7,
        surface=RoadwaySurfaceName.PAVED,
    )

    roadway = build_solver_roadway(definition)

    assert isinstance(roadway, RoadwayProfileWeir)
    assert roadway.crest_length == pytest.approx(20.0)
    assert roadway.minimum_crest_elevation == pytest.approx(10.0)


def test_floodway_adapter_uses_public_segment_unit_discharge_and_lengths() -> None:
    definition = RoadwayProfileDefinition(
        points=(
            RoadwayCrestPointDefinition(station=0.0, elevation=10.0),
            RoadwayCrestPointDefinition(station=12.0, elevation=10.2),
        ),
        discharge_coefficient=1.7,
        surface=RoadwaySurfaceName.PAVED,
    )
    roadway = build_solver_roadway(definition)
    roadway_result = calculate_roadway_overtopping(
        roadway,
        headwater_elevation=10.8,
        tailwater_elevation=9.5,
    )
    hydraulic_result = CrossingHydraulicResult(
        headwater_elevation=10.8,
        total_discharge=roadway_result.discharge,
        tailwater_elevation=9.5,
        group_results=(),
        roadway_result=roadway_result,
    )
    scenario = ScenarioResult(
        crossing_name="Floodway",
        scenario_name="1% AEP",
        hydraulic_result=hydraulic_result,
        aep_percent=1.0,
        source="Synthetic regression case",
    )

    adapted = build_floodway_scenario_hydraulics(scenario)

    assert adapted.roadway_discharge == pytest.approx(roadway_result.discharge)
    assert len(adapted.segments) == len(roadway_result.segment_results)
    first = adapted.segments[0]
    source = roadway_result.segment_results[0]
    assert first.unit_discharge == pytest.approx(source.unit_discharge)
    assert first.physical_interval_length == pytest.approx(source.physical_interval_length)
    assert first.effective_length == pytest.approx(source.effective_length)
    assert first.flow_state is RoadwaySegmentState.FREE_UNSUBMERGED


def test_floodway_adapter_preserves_supported_submergence_state() -> None:
    definition = RoadwayProfileDefinition(
        points=(
            RoadwayCrestPointDefinition(station=0.0, elevation=10.0),
            RoadwayCrestPointDefinition(station=10.0, elevation=10.0),
        ),
        discharge_coefficient=1.7,
        surface=RoadwaySurfaceName.PAVED,
    )
    roadway = build_solver_roadway(definition)
    roadway_result = calculate_roadway_overtopping(
        roadway,
        headwater_elevation=11.0,
        tailwater_elevation=10.8,
    )
    hydraulic_result = CrossingHydraulicResult(
        headwater_elevation=11.0,
        total_discharge=roadway_result.discharge,
        tailwater_elevation=10.8,
        group_results=(),
        roadway_result=roadway_result,
    )
    scenario = ScenarioResult(
        crossing_name="Floodway",
        scenario_name="Submerged",
        hydraulic_result=hydraulic_result,
    )

    adapted = build_floodway_scenario_hydraulics(scenario)

    assert adapted.segments
    assert all(segment.flow_state is RoadwaySegmentState.SUPPORTED_SUBMERGED for segment in adapted.segments)
    assert all(segment.submergence_ratio is not None for segment in adapted.segments)
    assert all(segment.submergence_factor is not None for segment in adapted.segments)
