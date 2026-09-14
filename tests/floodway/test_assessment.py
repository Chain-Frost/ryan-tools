"""Tests for floodway A-F assessment orchestration."""

from ryan_library.classes.floodway import (
    FloodwayApplicabilityStatus,
    FloodwayFormation,
    FloodwayFormationZone,
    FloodwayScenarioHydraulics,
    FloodwayZone,
    RoadwaySegmentHydraulicState,
    RoadwaySegmentState,
)
from ryan_library.orchestrators.floodway import (
    assess_floodway_hydraulics,
    build_floodway_envelope_from_assessments,
)


def _segment(*, q: float, station: float, state: RoadwaySegmentState) -> RoadwaySegmentHydraulicState:
    return RoadwaySegmentHydraulicState(
        source_interval_index=1,
        interval_start_station=10.0,
        interval_end_station=20.0,
        integration_station=station,
        physical_interval_length=10.0,
        effective_length=2.5,
        crest_elevation=100.0,
        upstream_head=0.8 if q > 0.0 else 0.0,
        downstream_head=0.0,
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
    assert batter.applicability is FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED


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
