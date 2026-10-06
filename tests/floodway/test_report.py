"""Tests for reviewable floodway Markdown reporting."""

from ryan_library.classes.floodway import (
    FloodwayApplicabilityStatus,
    FloodwayAssessmentLayer,
    FloodwayFormation,
    FloodwayFormationZone,
    FloodwayScenarioHydraulics,
    FloodwayZone,
    FloodwayZoneDemand,
    GoverningFloodwayDemand,
    Hec23RiprapDesignInput,
    RoadwaySegmentHydraulicState,
    RoadwaySegmentState,
)
from ryan_library.functions.floodway import build_floodway_event_envelope
from ryan_library.orchestrators.floodway import (
    assess_floodway_hydraulics,
    render_floodway_envelope_markdown,
    render_floodway_scenario_markdown,
)


def test_markdown_report_keeps_demand_measures_separate() -> None:
    envelope = build_floodway_event_envelope(
        (
            GoverningFloodwayDemand(
                scenario_name="1% AEP",
                aep_percent=1.0,
                total_discharge=150.0,
                roadway_discharge=20.0,
                headwater_elevation=101.2,
                tailwater_elevation=100.1,
                flow_state=RoadwaySegmentState.FREE_UNSUBMERGED,
                assessment_applicability=FloodwayApplicabilityStatus.TWO_D_VERIFICATION_RECOMMENDED,
                assessment_message="Skew requires 2D verification.",
                integration_station=15.0,
                demand=FloodwayZoneDemand(
                    zone=FloodwayZone.DOWNSTREAM_BATTER,
                    unit_discharge=2.0,
                    velocity=3.5,
                    dynamic_pressure_pa=6125.0,
                    momentum_flux_per_width_npm=7000.0,
                    applicability=FloodwayApplicabilityStatus.LEGACY_REPRODUCTION,
                    layer=FloodwayAssessmentLayer.MRWA_COMPLIANCE,
                    source_id="MRWA-FLOODWAY-DESIGN-GUIDE-2006-EQ4-7",
                ),
            ),
        )
    )

    report = render_floodway_envelope_markdown(envelope)

    assert "1% AEP" in report
    assert "MRWA-FLOODWAY-DESIGN-GUIDE-2006-EQ4-7" in report
    assert "dynamic_pressure" in report
    assert "momentum_flux" in report
    assert "150.000" in report
    assert "20.000" in report
    assert "free_unsubmerged" in report
    assert "15.000" in report
    assert "two_d_verification_recommended" in report
    assert "Skew requires 2D verification." in report
    assert "does not combine them into a generic floodway force" in report
    assert "MRWA-FLOODWAYS-V3-2023-06-12" in report
    assert "2% AEP" in report


def test_markdown_report_handles_empty_envelope() -> None:
    report = render_floodway_envelope_markdown(build_floodway_event_envelope(()))

    assert "No active floodway demand states were supplied." in report


def test_scenario_report_keeps_hec23_enhanced_check_separate_from_mrwa() -> None:
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
        scenario_name="2% AEP",
        aep_percent=2.0,
        headwater_elevation=100.8,
        tailwater_elevation=99.5,
        roadway_discharge=0.5,
        segments=(
            RoadwaySegmentHydraulicState(
                source_interval_index=0,
                interval_start_station=0.0,
                interval_end_station=10.0,
                integration_station=5.0,
                physical_interval_length=10.0,
                effective_length=2.5,
                crest_elevation=100.0,
                upstream_head=0.8,
                downstream_head=0.0,
                discharge=0.465,
                unit_discharge=0.186,
                flow_state=RoadwaySegmentState.FREE_UNSUBMERGED,
            ),
        ),
    )

    assessment = assess_floodway_hydraulics(hydraulics, formation)
    report = render_floodway_scenario_markdown(assessment)

    assert "MRWA / hydraulic demand assessment" in report
    assert "MRWA legacy rock slope protection" in report
    assert "MRWA-FLOODWAY-DESIGN-GUIDE-2006-TABLE-5.1" in report
    assert "Enhanced protection assessment" in report
    assert "FHWA-HEC23-V2-DG5-EQ5.1-5.3" in report
    assert "not relabelled as MRWA compliance" in report
    assert "| B |" in report
    assert "| yes |" in report
