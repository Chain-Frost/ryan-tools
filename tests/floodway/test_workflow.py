"""End-to-end tests across the culvert workflow and floodway adapter boundary."""

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
from ryan_library.functions.floodway import build_floodway_scenario_hydraulics
from ryan_library.orchestrators.culvert.solve import solve_crossing_scenario


def test_profile_roadway_crossing_solve_flows_into_floodway_segment_state() -> None:
    crossing = CrossingDefinition(
        name="Profile floodway",
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
    scenario = Scenario(
        name="Overtopping event",
        discharge=8.0,
        tailwater=9.5,
        aep_percent=2.0,
        source="Synthetic integration regression",
    )

    solved = solve_crossing_scenario(crossing, scenario)
    floodway = build_floodway_scenario_hydraulics(solved)

    assert solved.hydraulic_result.roadway_result is not None
    assert floodway.roadway_discharge > 0.0
    assert floodway.segments
    assert sum(segment.discharge for segment in floodway.segments) == solved.hydraulic_result.roadway_discharge
    assert any(segment.unit_discharge > 0.0 for segment in floodway.segments)
    assert all(segment.physical_interval_length > 0.0 for segment in floodway.segments)
