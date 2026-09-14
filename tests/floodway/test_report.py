"""Tests for reviewable floodway Markdown reporting."""

from ryan_library.classes.floodway import (
    FloodwayApplicabilityStatus,
    FloodwayAssessmentLayer,
    FloodwayZone,
    FloodwayZoneDemand,
    GoverningFloodwayDemand,
)
from ryan_library.functions.floodway import build_floodway_event_envelope
from ryan_library.orchestrators.floodway import render_floodway_envelope_markdown


def test_markdown_report_keeps_demand_measures_separate() -> None:
    envelope = build_floodway_event_envelope(
        (
            GoverningFloodwayDemand(
                scenario_name="1% AEP",
                aep_percent=1.0,
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
    assert "does not combine them into a generic floodway force" in report


def test_markdown_report_handles_empty_envelope() -> None:
    report = render_floodway_envelope_markdown(build_floodway_event_envelope(()))

    assert "No active floodway demand states were supplied." in report
