"""Tests for floodway A-F assessment orchestration."""

from ryan_library.classes.floodway import (
    FloodwayApplicabilityStatus,
    FloodwayFormation,
    FloodwayFormationZone,
    FloodwayScenarioHydraulics,
    FloodwayZone,
    Hec23RiprapDesignInput,
    RoadwaySegmentHydraulicState,
    RoadwaySegmentState,
)
from ryan_library.orchestrators.floodway import (
    assess_floodway_hydraulics,
    build_floodway_envelope_from_assessments,
)


def _segment(
    *,
    q: float,
    station: float,
    state: RoadwaySegmentState,
    upstream_head: float | None = None,
    downstream_head: float = 0.0,
) -> RoadwaySegmentHydraulicState:
    head = (0.8 if q > 0.0 else 0.0) if upstream_head is None else upstream_head
    return RoadwaySegmentHydraulicState(
        source_interval_index=1,
        interval_start_station=10.0,
        interval_end_station=20.0,
        integration_station=station,
        physical_interval_length=10.0,
        effective_length=2.5,
        crest_elevation=100.0,
        upstream_head=head,
        downstream_head=downstream_head,
        discharge=q * 2.5,
        unit_discharge=q,
        flow_state=state,
    )


def _formation() -> FloodwayFormation:
    return FloodwayFormation(
        name="Test floodway",
        zones=(
            FloodwayFormationZone(zone=FloodwayZone.PAVEMENT, slope=0.02, roughness=0.016),
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_SHOULDER),
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_BATTER, slope=0.20, roughness=0.05),
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_TOE),
            FloodwayFormationZone(zone=FloodwayZone.FOUNDATION),
        ),
    )


def test_surface_zones_use_upstream_local_q_without_reconstructing_from_lengths() -> None:
    hydraulics = FloodwayScenarioHydraulics(
        scenario_name="2% AEP",
        aep_percent=2.0,
        headwater_elevation=100.8,
        tailwater_elevation=99.5,
        roadway_discharge=25.0,
        segments=(
            _segment(q=2.3, station=12.5, state=RoadwaySegmentState.FREE_UNSUBMERGED),
        ),
    )

    assessment = assess_floodway_hydraulics(hydraulics, _formation())
    pavement = next(item for item in assessment.zone_assessments if item.zone is FloodwayZone.PAVEMENT)
    batter = next(item for item in assessment.zone_assessments if item.zone is FloodwayZone.DOWNSTREAM_BATTER)

    assert pavement.demand is not None
    assert batter.demand is not None
    assert pavement.demand.unit_discharge == 2.3
    assert batter.demand.unit_discharge == 2.3
    assert pavement.applicability is FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED
    assert batter.applicability is FloodwayApplicabilityStatus.LEGACY_REPRODUCTION
    assert batter.velocity_result is not None
    assert batter.velocity_result.coefficient_k is not None


def test_unsupported_zones_fail_closed_to_specialist_review() -> None:
    hydraulics = FloodwayScenarioHydraulics(
        scenario_name="1% AEP",
        aep_percent=1.0,
        headwater_elevation=101.0,
        tailwater_elevation=99.5,
        roadway_discharge=25.0,
        segments=(
            _segment(q=2.5, station=15.0, state=RoadwaySegmentState.FREE_UNSUBMERGED),
        ),
    )

    assessment = assess_floodway_hydraulics(hydraulics, _formation())

    for zone in (
        FloodwayZone.DOWNSTREAM_TOE,
        FloodwayZone.DOWNSTREAM_SHOULDER,
        FloodwayZone.FOUNDATION,
    ):
        item = next(candidate for candidate in assessment.zone_assessments if candidate.zone is zone)
        assert item.demand is None
        assert item.applicability is FloodwayApplicabilityStatus.SPECIALIST_REVIEW_REQUIRED


def test_inactive_segment_is_not_assigned_artificial_demands() -> None:
    hydraulics = FloodwayScenarioHydraulics(
        scenario_name="Minor",
        aep_percent=20.0,
        headwater_elevation=100.0,
        tailwater_elevation=99.5,
        roadway_discharge=0.0,
        segments=(
            _segment(q=0.0, station=12.5, state=RoadwaySegmentState.INACTIVE),
        ),
    )

    assessment = assess_floodway_hydraulics(hydraulics, _formation())

    assert assessment.zone_assessments
    assert all(item.demand is None for item in assessment.zone_assessments)
    assert all(item.applicability is FloodwayApplicabilityStatus.NOT_APPLICABLE for item in assessment.zone_assessments)


def test_envelope_retains_governing_integration_station() -> None:
    lower = assess_floodway_hydraulics(
        FloodwayScenarioHydraulics(
            scenario_name="5% AEP",
            aep_percent=5.0,
            headwater_elevation=100.7,
            tailwater_elevation=99.5,
            roadway_discharge=20.0,
            segments=(
                _segment(q=1.5, station=12.5, state=RoadwaySegmentState.FREE_UNSUBMERGED),
            ),
        ),
        _formation(),
    )
    higher = assess_floodway_hydraulics(
        FloodwayScenarioHydraulics(
            scenario_name="2% AEP",
            aep_percent=2.0,
            headwater_elevation=100.9,
            tailwater_elevation=99.5,
            roadway_discharge=30.0,
            segments=(
                _segment(q=2.5, station=17.5, state=RoadwaySegmentState.FREE_UNSUBMERGED),
            ),
        ),
        _formation(),
    )

    envelope = build_floodway_envelope_from_assessments((lower, higher))
    pavement_velocity = next(
        governor
        for governor in envelope.governors
        if governor.zone is FloodwayZone.PAVEMENT and governor.metric.value == "velocity"
    )

    assert pavement_velocity.scenario_name == "2% AEP"
    assert pavement_velocity.source_interval_index == 1
    assert pavement_velocity.integration_station == 17.5

def test_complete_formation_applies_figure_4_5_and_4_6_to_plunging_flow() -> None:
    formation = FloodwayFormation(
        name="Complete floodway",
        crest_flow_length=9.0,
        zones=(
            FloodwayFormationZone(zone=FloodwayZone.PAVEMENT, slope=0.03, roughness=0.015),
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_SHOULDER, elevation=99.85),
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_BATTER, slope=1.0 / 3.0, roughness=0.04),
        ),
    )
    hydraulics = FloodwayScenarioHydraulics(
        scenario_name="Transition-side event",
        aep_percent=2.0,
        headwater_elevation=100.9,
        tailwater_elevation=100.5,
        roadway_discharge=40.0,
        segments=(
            _segment(
                q=1.443,
                station=12.5,
                state=RoadwaySegmentState.FREE_UNSUBMERGED,
                upstream_head=0.90,
                downstream_head=0.50,
            ),
        ),
    )

    assessment = assess_floodway_hydraulics(hydraulics, formation)
    pavement = next(item for item in assessment.zone_assessments if item.zone is FloodwayZone.PAVEMENT)
    batter = next(item for item in assessment.zone_assessments if item.zone is FloodwayZone.DOWNSTREAM_BATTER)

    assert pavement.applicability is FloodwayApplicabilityStatus.LEGACY_REPRODUCTION
    assert batter.applicability is FloodwayApplicabilityStatus.LEGACY_REPRODUCTION
    assert pavement.velocity_result is not None
    assert batter.velocity_result is not None
    assert pavement.velocity_result.coefficient_k == batter.velocity_result.coefficient_k
    assert "plunging flow" in batter.message


def test_figure_4_5_surface_flow_does_not_invent_batter_demand() -> None:
    formation = FloodwayFormation(
        name="Surface-flow floodway",
        crest_flow_length=9.0,
        zones=(
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_SHOULDER, elevation=99.85),
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_BATTER, slope=1.0 / 3.0, roughness=0.04),
        ),
    )
    hydraulics = FloodwayScenarioHydraulics(
        scenario_name="Surface-flow event",
        aep_percent=1.0,
        headwater_elevation=100.9,
        tailwater_elevation=100.7,
        roadway_discharge=50.0,
        segments=(
            _segment(
                q=1.5,
                station=12.5,
                state=RoadwaySegmentState.FREE_UNSUBMERGED,
                upstream_head=0.90,
                downstream_head=0.70,
            ),
        ),
    )

    assessment = assess_floodway_hydraulics(hydraulics, formation)
    batter = next(item for item in assessment.zone_assessments if item.zone is FloodwayZone.DOWNSTREAM_BATTER)

    assert batter.demand is None
    assert batter.applicability is FloodwayApplicabilityStatus.NOT_APPLICABLE
    assert "surface flow" in batter.message

def test_downstream_batter_can_carry_separate_hec23_protection_result() -> None:
    formation = FloodwayFormation(
        name="Protected floodway",
        zones=(
            FloodwayFormationZone(
                zone=FloodwayZone.DOWNSTREAM_BATTER,
                slope=0.20,
                roughness=0.05,
                hec23_riprap=Hec23RiprapDesignInput(
                    selected_d50_m=0.15,
                    uniformity_coefficient=2.1,
                    porosity=0.45,
                ),
            ),
        ),
    )
    hydraulics = FloodwayScenarioHydraulics(
        scenario_name="HEC-23 design event",
        aep_percent=2.0,
        headwater_elevation=100.8,
        tailwater_elevation=99.5,
        roadway_discharge=10.0,
        segments=(
            _segment(
                q=0.186,
                station=12.5,
                state=RoadwaySegmentState.FREE_UNSUBMERGED,
                upstream_head=0.8,
            ),
        ),
    )

    assessment = assess_floodway_hydraulics(hydraulics, formation)
    batter = next(item for item in assessment.zone_assessments if item.zone is FloodwayZone.DOWNSTREAM_BATTER)

    assert batter.protection_result is not None
    assert batter.protection_result.source_id == "FHWA-HEC23-V2-DG5-EQ5.1-5.3"
    assert batter.protection_result.is_sufficient
    assert batter.protection_result.minimum_d50_m < batter.protection_result.selected_d50_m
    assert batter.demand is not None
    assert batter.demand.layer.value == "mrwa_compliance"
    assert batter.protection_result.layer.value == "enhanced_assessment"



def test_supported_submerged_state_uses_q_over_d_for_pavement_only() -> None:
    formation = FloodwayFormation(
        name="Submerged floodway",
        zones=(
            FloodwayFormationZone(zone=FloodwayZone.PAVEMENT, slope=0.03, roughness=0.015),
            FloodwayFormationZone(zone=FloodwayZone.DOWNSTREAM_BATTER, slope=1.0 / 3.0, roughness=0.04),
        ),
    )
    hydraulics = FloodwayScenarioHydraulics(
        scenario_name="Submerged event",
        aep_percent=1.0,
        headwater_elevation=101.0,
        tailwater_elevation=100.6,
        roadway_discharge=30.0,
        segments=(
            _segment(
                q=1.2,
                station=12.5,
                state=RoadwaySegmentState.SUPPORTED_SUBMERGED,
                upstream_head=1.0,
                downstream_head=0.4,
            ),
        ),
    )

    assessment = assess_floodway_hydraulics(hydraulics, formation)
    pavement = next(item for item in assessment.zone_assessments if item.zone is FloodwayZone.PAVEMENT)
    batter = next(item for item in assessment.zone_assessments if item.zone is FloodwayZone.DOWNSTREAM_BATTER)

    assert pavement.demand is not None
    assert pavement.demand.velocity == 3.0
    assert pavement.demand.depth_m == 0.4
    assert pavement.applicability is FloodwayApplicabilityStatus.LEGACY_REPRODUCTION
    assert "q/D" in pavement.message

    assert batter.demand is None
    assert batter.applicability is FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED
