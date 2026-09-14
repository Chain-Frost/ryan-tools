"""Coordinate local A-F floodway assessment from authoritative crossing results."""

from collections.abc import Sequence

from ...classes.culvert import ScenarioResult
from ...classes.floodway import (
    FloodwayApplicabilityStatus,
    FloodwayEventEnvelope,
    FloodwayFormation,
    FloodwayScenarioAssessment,
    FloodwayScenarioHydraulics,
    FloodwayZone,
    FloodwayZoneAssessment,
    GoverningFloodwayDemand,
    RoadwaySegmentState,
)
from ...functions.floodway import (
    build_floodway_event_envelope,
    build_floodway_scenario_hydraulics,
    build_zone_demand,
    calculate_mrwa_surface_velocity,
)

_DIRECT_MRWA_SURFACE_ZONES = {
    FloodwayZone.DOWNSTREAM_BATTER,
    FloodwayZone.PAVEMENT,
}

_SPECIALIST_ZONE_MESSAGES: dict[FloodwayZone, str] = {
    FloodwayZone.DOWNSTREAM_TOE: "Toe scour/impingement is not reduced to an unsupported scalar method.",
    FloodwayZone.DOWNSTREAM_SHOULDER: "Shoulder pressure/uplift requires a supported pressure method or specialist review.",
    FloodwayZone.UPSTREAM_BATTER: "No general upstream-batter capacity method is adopted in this increment.",
    FloodwayZone.FOUNDATION: "Foundation uplift, seepage and piping require specialist/geotechnical assessment.",
}


def assess_floodway_hydraulics(
    hydraulics: FloodwayScenarioHydraulics,
    formation: FloodwayFormation,
) -> FloodwayScenarioAssessment:
    """Assess represented A-F zones at each roadway integration station.

    Roadway ``unit_discharge`` is consumed directly from the public
    ``ryan-culverts`` segment state. ``effective_length`` and physical source
    interval length are intentionally not used to reconstruct local discharge.

    The first increment evaluates the MRWA Equation 4/6 surface-velocity path
    only for downstream batter (B) and pavement (D). Without a verified Figure
    4.6 ``K`` and the complete event/regime procedure, those results remain
    explicitly ``SOURCE_DATA_REQUIRED`` diagnostics. Other A-F zones fail closed
    to specialist review rather than receiving invented force/capacity methods.
    """
    assessments: list[FloodwayZoneAssessment] = []

    for segment in hydraulics.segments:
        for formation_zone in formation.zones:
            zone = formation_zone.zone
            if segment.flow_state is RoadwaySegmentState.INACTIVE:
                assessments.append(
                    FloodwayZoneAssessment(
                        scenario_name=hydraulics.scenario_name,
                        aep_percent=hydraulics.aep_percent,
                        source_interval_index=segment.source_interval_index,
                        integration_station=segment.integration_station,
                        flow_state=segment.flow_state,
                        zone=zone,
                        applicability=FloodwayApplicabilityStatus.NOT_APPLICABLE,
                        message="No roadway overtopping flow is active at this integration station.",
                    )
                )
                continue

            if zone not in _DIRECT_MRWA_SURFACE_ZONES:
                assessments.append(
                    FloodwayZoneAssessment(
                        scenario_name=hydraulics.scenario_name,
                        aep_percent=hydraulics.aep_percent,
                        source_interval_index=segment.source_interval_index,
                        integration_station=segment.integration_station,
                        flow_state=segment.flow_state,
                        zone=zone,
                        applicability=FloodwayApplicabilityStatus.SPECIALIST_REVIEW_REQUIRED,
                        message=_SPECIALIST_ZONE_MESSAGES.get(
                            zone,
                            "No supported scalar floodway method is adopted for this zone.",
                        ),
                    )
                )
                continue

            if formation_zone.slope is None or formation_zone.roughness is None:
                assessments.append(
                    FloodwayZoneAssessment(
                        scenario_name=hydraulics.scenario_name,
                        aep_percent=hydraulics.aep_percent,
                        source_interval_index=segment.source_interval_index,
                        integration_station=segment.integration_station,
                        flow_state=segment.flow_state,
                        zone=zone,
                        applicability=FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED,
                        message="MRWA surface-velocity calculation requires zone slope and Manning roughness.",
                    )
                )
                continue

            velocity_result = calculate_mrwa_surface_velocity(
                zone=zone,
                unit_discharge=segment.unit_discharge,
                slope=formation_zone.slope,
                roughness=formation_zone.roughness,
            )
            demand = build_zone_demand(velocity_result)
            assessments.append(
                FloodwayZoneAssessment(
                    scenario_name=hydraulics.scenario_name,
                    aep_percent=hydraulics.aep_percent,
                    source_interval_index=segment.source_interval_index,
                    integration_station=segment.integration_station,
                    flow_state=segment.flow_state,
                    zone=zone,
                    applicability=velocity_result.applicability,
                    velocity_result=velocity_result,
                    demand=demand,
                    message=(
                        "Equation 4/6 diagnostic only until the source-backed Figure 4.6/event-regime path supplies "
                        "the limiting-velocity check."
                    ),
                )
            )

    return FloodwayScenarioAssessment(
        hydraulics=hydraulics,
        zone_assessments=tuple(assessments),
    )


def assess_floodway_scenario(
    result: ScenarioResult,
    formation: FloodwayFormation,
) -> FloodwayScenarioAssessment:
    """Adapt one authoritative crossing scenario and assess its floodway formation."""
    return assess_floodway_hydraulics(build_floodway_scenario_hydraulics(result), formation)


def build_floodway_envelope_from_assessments(
    assessments: Sequence[FloodwayScenarioAssessment],
) -> FloodwayEventEnvelope:
    """Build independent zone/metric governors from supported local demand states."""
    candidates = tuple(
        GoverningFloodwayDemand(
            scenario_name=zone_assessment.scenario_name,
            aep_percent=zone_assessment.aep_percent,
            demand=zone_assessment.demand,
            source_interval_index=zone_assessment.source_interval_index,
            integration_station=zone_assessment.integration_station,
        )
        for scenario_assessment in assessments
        for zone_assessment in scenario_assessment.zone_assessments
        if zone_assessment.demand is not None
    )
    return build_floodway_event_envelope(candidates)
