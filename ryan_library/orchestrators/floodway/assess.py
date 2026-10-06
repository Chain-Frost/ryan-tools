"""Coordinate local A-F floodway assessment from authoritative crossing results."""

from collections.abc import Sequence

from ...classes.culvert import CrossingDefinition, Scenario, ScenarioResult
from ...classes.floodway import (
    FloodwayApplicabilityStatus,
    FloodwayEventEnvelope,
    FloodwayFormation,
    FloodwayFormationZone,
    FloodwayScenarioAssessment,
    FloodwayScenarioHydraulics,
    FloodwayZone,
    FloodwayZoneAssessment,
    GoverningFloodwayDemand,
    Hec23OvertoppingRiprapResult,
    RoadwaySegmentHydraulicState,
    RoadwaySegmentState,
)
from ...functions.floodway import (
    build_floodway_event_envelope,
    build_floodway_scenario_hydraulics,
    build_submerged_pavement_demand,
    build_zone_demand,
    calculate_mrwa_submerged_pavement_velocity,
    calculate_mrwa_surface_velocity,
    evaluate_hec23_overtopping_riprap,
    mrwa_appendix_submergence_reached,
    mrwa_simplified_free_flow_applicable,
    mrwa_transition_submergence_ratio,
    select_mrwa_rock_slope_protection,
)

from ..culvert.solve import solve_crossing_scenario


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


def _with_2d_verification(
    formation: FloodwayFormation,
    applicability: FloodwayApplicabilityStatus,
    message: str,
) -> tuple[FloodwayApplicabilityStatus, str]:
    """Escalate otherwise-supported 1D results when project geometry needs 2D review."""
    reason = formation.two_d_verification_reason
    if not reason or applicability not in {
        FloodwayApplicabilityStatus.SUPPORTED,
        FloodwayApplicabilityStatus.LEGACY_REPRODUCTION,
    }:
        return applicability, message
    note = f"2D hydraulic verification is recommended: {reason}"
    return (
        FloodwayApplicabilityStatus.TWO_D_VERIFICATION_RECOMMENDED,
        f"{message} {note}".strip(),
    )


def _mrwa_depth_ratio_context(segment: RoadwaySegmentHydraulicState) -> tuple[float | None, str]:
    """Return D/H and a source-specific threshold note without changing solver state."""
    if segment.upstream_head <= 0.0:
        return None, "No positive upstream head is available for an MRWA D/H check."

    ratio = segment.downstream_head / segment.upstream_head
    if mrwa_simplified_free_flow_applicable(ratio):
        return (
            ratio,
            f"D/H={ratio:.3f} is within the Section 4.4.3 simplified free-flow range D/H < 0.76.",
        )
    if not mrwa_appendix_submergence_reached(ratio):
        return (
            ratio,
            f"D/H={ratio:.3f} is above the Section 4.4.3 0.76 limit but below the Appendix C/D "
            "legacy point of submergence at 0.8; the source thresholds are retained separately.",
        )
    return (
        ratio,
        f"D/H={ratio:.3f} reaches or exceeds the Appendix C/D legacy point of submergence at 0.8.",
    )


def _hec23_protection_result(
    *,
    segment: RoadwaySegmentHydraulicState,
    formation_zone: FloodwayFormationZone,
) -> Hec23OvertoppingRiprapResult | None:
    """Evaluate configured HEC-23 DG5 protection independently of MRWA velocity applicability."""
    design = formation_zone.hec23_riprap
    if (
        formation_zone.zone is not FloodwayZone.DOWNSTREAM_BATTER
        or design is None
        or formation_zone.slope is None
        or segment.unit_discharge <= 0.0
    ):
        return None
    return evaluate_hec23_overtopping_riprap(
        unit_discharge=segment.unit_discharge,
        slope=formation_zone.slope,
        uniformity_coefficient=design.uniformity_coefficient,
        porosity=design.porosity,
        specific_gravity=design.specific_gravity,
        angle_of_repose_degrees=design.angle_of_repose_degrees,
        selected_d50_m=design.selected_d50_m,
    )


def _mrwa_velocity_limit_inputs(
    *,
    hydraulics: FloodwayScenarioHydraulics,
    segment: RoadwaySegmentHydraulicState,
    formation: FloodwayFormation,
    zone: FloodwayZone,
) -> tuple[float | None, FloodwayApplicabilityStatus | None, str]:
    """Resolve MRWA Equation 7 ``delta_p`` and free-flow regime applicability."""
    head = segment.upstream_head
    if head <= 0.0:
        return None, FloodwayApplicabilityStatus.NOT_APPLICABLE, "No positive upstream head is available."

    depth_ratio, threshold_note = _mrwa_depth_ratio_context(segment)
    if (
        segment.flow_state is RoadwaySegmentState.FREE_UNSUBMERGED
        and depth_ratio is not None
        and mrwa_appendix_submergence_reached(depth_ratio)
    ):
        return (
            None,
            FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED,
            threshold_note
            + " The authoritative roadway solver still reports free flow, so the MRWA legacy state is not overridden "
            "and no compliance-grade Equation 7 limit is claimed.",
        )

    shoulder = formation.get_zone(FloodwayZone.DOWNSTREAM_SHOULDER)
    shoulder_elevation = None if shoulder is None else shoulder.elevation

    if zone is FloodwayZone.PAVEMENT:
        if shoulder_elevation is None:
            return (
                None,
                FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED,
                "MRWA pavement Equation 7 requires downstream-shoulder elevation.",
            )
        delta_p = segment.crest_elevation - shoulder_elevation
        if delta_p < 0.0:
            return (
                None,
                FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED,
                "Downstream-shoulder elevation is above the local roadway crest.",
            )
        return (
            delta_p,
            None,
            "MRWA pavement velocity uses the Equation 4/7 limiting-velocity path. " + threshold_note,
        )

    if hydraulics.tailwater_elevation <= segment.crest_elevation:
        return (
            segment.crest_elevation - hydraulics.tailwater_elevation,
            None,
            "Low-tailwater plunging flow uses delta_p from the roadway crest to tailwater. " + threshold_note,
        )

    if formation.crest_flow_length is None:
        return (
            None,
            FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED,
            "MRWA batter regime classification requires crest_flow_length for Figure 4.5 H/l.",
        )

    head_to_length = head / formation.crest_flow_length
    try:
        transition_ratio = mrwa_transition_submergence_ratio(head_to_length)
    except ValueError:
        return (
            None,
            FloodwayApplicabilityStatus.OUTSIDE_SOURCE_RANGE,
            "Local H/l is outside the digitised MRWA Figure 4.5 source domain; no extrapolation is permitted.",
        )

    depth_ratio = segment.downstream_head / head
    if depth_ratio > transition_ratio:
        return (
            None,
            FloodwayApplicabilityStatus.NOT_APPLICABLE,
            "MRWA Figure 4.5 classifies this free-flow state as surface flow rather than plunging batter flow.",
        )

    if shoulder_elevation is None:
        return (
            None,
            FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED,
            "MRWA plunging-flow batter Equation 7 requires downstream-shoulder elevation when tailwater is above crest.",
        )

    delta_p = segment.crest_elevation - shoulder_elevation
    if delta_p < 0.0:
        return (
            None,
            FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED,
            "Downstream-shoulder elevation is above the local roadway crest.",
        )
    return (
        delta_p,
        None,
        "MRWA Figure 4.5 classifies this state as plunging flow; Equation 4/7 applies. " + threshold_note,
    )


def assess_floodway_hydraulics(
    hydraulics: FloodwayScenarioHydraulics,
    formation: FloodwayFormation,
) -> FloodwayScenarioAssessment:
    """Assess represented A-F zones at each roadway integration station.

    Roadway ``unit_discharge`` is consumed directly from the public
    ``ryan-culverts`` segment state. ``effective_length`` and physical source
    interval length are intentionally not used to reconstruct local discharge.

    The MRWA Equation 4/6/7 path is evaluated for free-flow pavement and
    downstream-batter states where the source geometry is available. Figure 4.5
    is used to distinguish plunging from surface flow when tailwater is above the
    crest. Unsupported A-F mechanisms continue to fail closed.
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

            protection_result = _hec23_protection_result(
                segment=segment,
                formation_zone=formation_zone,
            )

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

            if segment.flow_state is RoadwaySegmentState.SUPPORTED_SUBMERGED:
                if zone is FloodwayZone.PAVEMENT and segment.downstream_head > 0.0:
                    depth_ratio, threshold_note = _mrwa_depth_ratio_context(segment)
                    applicability = (
                        FloodwayApplicabilityStatus.LEGACY_REPRODUCTION
                        if depth_ratio is not None and mrwa_appendix_submergence_reached(depth_ratio)
                        else FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED
                    )
                    velocity_result = calculate_mrwa_submerged_pavement_velocity(
                        unit_discharge=segment.unit_discharge,
                        downstream_depth=segment.downstream_head,
                        applicability=applicability,
                    )
                    demand = build_submerged_pavement_demand(velocity_result)
                    assessment_message = (
                        "MRWA submerged pavement velocity uses the guide's approximate V ~= q/D relation. "
                        + threshold_note
                        + " Roadway free/submerged state remains authoritative to ryan-culverts."
                    )
                    assessment_applicability, assessment_message = _with_2d_verification(
                        formation,
                        velocity_result.applicability,
                        assessment_message,
                    )
                    assessments.append(
                        FloodwayZoneAssessment(
                            scenario_name=hydraulics.scenario_name,
                            aep_percent=hydraulics.aep_percent,
                            source_interval_index=segment.source_interval_index,
                            integration_station=segment.integration_station,
                            flow_state=segment.flow_state,
                            zone=zone,
                            applicability=assessment_applicability,
                            velocity_result=velocity_result,
                            demand=demand,
                            message=assessment_message,
                        )
                    )
                else:
                    assessments.append(
                        FloodwayZoneAssessment(
                            scenario_name=hydraulics.scenario_name,
                            aep_percent=hydraulics.aep_percent,
                            source_interval_index=segment.source_interval_index,
                            integration_station=segment.integration_station,
                            flow_state=segment.flow_state,
                            zone=zone,
                            applicability=FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED,
                            protection_result=protection_result,
                            message=(
                                "No supported MRWA submerged downstream-batter velocity method is adopted in this "
                                "increment; enhanced protection evidence remains separately reportable when configured."
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
                        protection_result=protection_result,
                        message="MRWA surface-velocity calculation requires zone slope and Manning roughness.",
                    )
                )
                continue

            delta_p, limiting_status, message = _mrwa_velocity_limit_inputs(
                hydraulics=hydraulics,
                segment=segment,
                formation=formation,
                zone=zone,
            )
            if limiting_status in {
                FloodwayApplicabilityStatus.NOT_APPLICABLE,
                FloodwayApplicabilityStatus.OUTSIDE_SOURCE_RANGE,
            }:
                assessments.append(
                    FloodwayZoneAssessment(
                        scenario_name=hydraulics.scenario_name,
                        aep_percent=hydraulics.aep_percent,
                        source_interval_index=segment.source_interval_index,
                        integration_station=segment.integration_station,
                        flow_state=segment.flow_state,
                        zone=zone,
                        applicability=limiting_status,
                        protection_result=protection_result,
                        message=message,
                    )
                )
                continue

            velocity_result = calculate_mrwa_surface_velocity(
                zone=zone,
                unit_discharge=segment.unit_discharge,
                slope=formation_zone.slope,
                roughness=formation_zone.roughness,
                total_head=segment.upstream_head if delta_p is not None else None,
                delta_p=delta_p,
            )
            demand = build_zone_demand(velocity_result)
            method_applicability = limiting_status or velocity_result.applicability
            mrwa_protection_result = (
                select_mrwa_rock_slope_protection(demand.velocity)
                if zone is FloodwayZone.DOWNSTREAM_BATTER
                and method_applicability is FloodwayApplicabilityStatus.LEGACY_REPRODUCTION
                else None
            )
            applicability, message = _with_2d_verification(
                formation,
                method_applicability,
                message,
            )
            assessments.append(
                FloodwayZoneAssessment(
                    scenario_name=hydraulics.scenario_name,
                    aep_percent=hydraulics.aep_percent,
                    source_interval_index=segment.source_interval_index,
                    integration_station=segment.integration_station,
                    flow_state=segment.flow_state,
                    zone=zone,
                    applicability=applicability,
                    velocity_result=velocity_result,
                    demand=demand,
                    mrwa_protection_result=mrwa_protection_result,
                    protection_result=protection_result,
                    message=message,
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
            total_discharge=scenario_assessment.hydraulics.total_discharge,
            roadway_discharge=scenario_assessment.hydraulics.roadway_discharge,
            headwater_elevation=scenario_assessment.hydraulics.headwater_elevation,
            tailwater_elevation=scenario_assessment.hydraulics.tailwater_elevation,
            flow_state=zone_assessment.flow_state,
            assessment_applicability=zone_assessment.applicability,
            assessment_message=zone_assessment.message,
        )
        for scenario_assessment in assessments
        for zone_assessment in scenario_assessment.zone_assessments
        if zone_assessment.demand is not None
    )
    return build_floodway_event_envelope(candidates)



def assess_floodway_crossing(
    crossing: CrossingDefinition,
    scenarios: Sequence[Scenario],
    formation: FloodwayFormation,
) -> tuple[FloodwayScenarioAssessment, ...]:
    """Solve and assess one crossing across the supplied hydraulic scenarios."""
    return tuple(
        assess_floodway_scenario(solve_crossing_scenario(crossing, scenario), formation)
        for scenario in scenarios
    )
