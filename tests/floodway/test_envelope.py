"""Regression tests for floodway event-envelope governing-state selection."""

from ryan_library.classes.floodway import (
    FloodwayApplicabilityStatus,
    FloodwayAssessmentLayer,
    FloodwayZone,
    FloodwayZoneDemand,
    GoverningFloodwayDemand,
)
from ryan_library.classes.floodway.envelope import FloodwayEnvelopeMetric
from ryan_library.functions.floodway.envelope import build_floodway_event_envelope


def _candidate(
    *,
    scenario: str,
    aep: float,
    zone: FloodwayZone,
    unit_discharge: float,
    velocity: float,
) -> GoverningFloodwayDemand:
    density = 1000.0
    return GoverningFloodwayDemand(
        scenario_name=scenario,
        aep_percent=aep,
        demand=FloodwayZoneDemand(
            zone=zone,
            unit_discharge=unit_discharge,
            velocity=velocity,
            dynamic_pressure_pa=0.5 * density * velocity * velocity,
            momentum_flux_per_width_npm=density * unit_discharge * velocity,
            applicability=FloodwayApplicabilityStatus.SUPPORTED,
            layer=FloodwayAssessmentLayer.DIAGNOSTIC,
            source_id="synthetic-envelope-test",
        ),
    )


def test_event_envelope_retains_interior_velocity_maximum() -> None:
    candidates = (
        _candidate(
            scenario="20% AEP",
            aep=20.0,
            zone=FloodwayZone.DOWNSTREAM_BATTER,
            unit_discharge=1.0,
            velocity=2.0,
        ),
        _candidate(
            scenario="5% AEP",
            aep=5.0,
            zone=FloodwayZone.DOWNSTREAM_BATTER,
            unit_discharge=2.0,
            velocity=4.0,
        ),
        _candidate(
            scenario="1% AEP",
            aep=1.0,
            zone=FloodwayZone.DOWNSTREAM_BATTER,
            unit_discharge=4.0,
            velocity=3.0,
        ),
    )

    envelope = build_floodway_event_envelope(candidates)

    governor = envelope.governor(
        FloodwayZone.DOWNSTREAM_BATTER,
        FloodwayEnvelopeMetric.VELOCITY,
    )
    assert governor is not None
    assert governor.scenario_name == "5% AEP"
    assert governor.demand.velocity == 4.0


def test_event_envelope_allows_different_metrics_and_zones_to_govern_different_events() -> None:
    candidates = (
        _candidate(
            scenario="Event A",
            aep=10.0,
            zone=FloodwayZone.PAVEMENT,
            unit_discharge=1.0,
            velocity=5.0,
        ),
        _candidate(
            scenario="Event B",
            aep=2.0,
            zone=FloodwayZone.PAVEMENT,
            unit_discharge=4.0,
            velocity=4.0,
        ),
        _candidate(
            scenario="Event C",
            aep=1.0,
            zone=FloodwayZone.DOWNSTREAM_BATTER,
            unit_discharge=3.0,
            velocity=3.5,
        ),
    )

    envelope = build_floodway_event_envelope(candidates)

    pavement_velocity = envelope.governor(
        FloodwayZone.PAVEMENT,
        FloodwayEnvelopeMetric.VELOCITY,
    )
    pavement_momentum = envelope.governor(
        FloodwayZone.PAVEMENT,
        FloodwayEnvelopeMetric.MOMENTUM_FLUX,
    )
    batter_velocity = envelope.governor(
        FloodwayZone.DOWNSTREAM_BATTER,
        FloodwayEnvelopeMetric.VELOCITY,
    )

    assert pavement_velocity is not None
    assert pavement_velocity.scenario_name == "Event A"
    assert pavement_momentum is not None
    assert pavement_momentum.scenario_name == "Event B"
    assert batter_velocity is not None
    assert batter_velocity.scenario_name == "Event C"


def test_event_envelope_accepts_no_active_demands() -> None:
    envelope = build_floodway_event_envelope(())

    assert envelope.candidates == ()
    assert envelope.governors == ()
