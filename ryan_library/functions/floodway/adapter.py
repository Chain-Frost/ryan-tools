"""Adapt authoritative culvert-solver roadway results into floodway workflow state."""

from ...classes.culvert.results import ScenarioResult
from ...classes.floodway import FloodwayScenarioHydraulics, RoadwaySegmentHydraulicState, RoadwaySegmentState


def build_floodway_scenario_hydraulics(result: ScenarioResult) -> FloodwayScenarioHydraulics:
    """Build a floodway-facing hydraulic snapshot from one solved culvert scenario.

    The adapter intentionally consumes only public ``culvert_solver`` result
    attributes retained by ``ScenarioResult``. It does not reconstruct roadway
    discharge from crest geometry or numerical integration weights.
    """
    roadway_result = result.hydraulic_result.roadway_result
    if roadway_result is None:
        segments: tuple[RoadwaySegmentHydraulicState, ...] = ()
    else:
        segments = tuple(
            RoadwaySegmentHydraulicState(
                source_interval_index=segment.source_interval_index,
                interval_start_station=segment.interval_start_station,
                interval_end_station=segment.interval_end_station,
                integration_station=segment.integration_station,
                physical_interval_length=segment.physical_interval_length,
                effective_length=segment.effective_length,
                crest_elevation=segment.crest_elevation,
                upstream_head=segment.upstream_head,
                downstream_head=segment.downstream_head,
                discharge=segment.discharge,
                unit_discharge=segment.unit_discharge,
                flow_state=RoadwaySegmentState(segment.flow_state.value),
                submergence_ratio=segment.submergence_ratio,
                submergence_factor=segment.submergence_factor,
            )
            for segment in roadway_result.segment_results
        )

    return FloodwayScenarioHydraulics(
        scenario_name=result.scenario_name,
        aep_percent=result.aep_percent,
        headwater_elevation=result.hydraulic_result.headwater_elevation,
        tailwater_elevation=result.hydraulic_result.tailwater_elevation,
        roadway_discharge=result.hydraulic_result.roadway_discharge,
        segments=segments,
        source=result.source,
        notes=result.notes,
    )
