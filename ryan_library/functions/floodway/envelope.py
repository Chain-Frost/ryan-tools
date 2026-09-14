"""Event-envelope selection for independent floodway hydraulic-demand measures."""

from collections import defaultdict
from collections.abc import Sequence

from ...classes.floodway.envelope import (
    FloodwayEnvelopeGovernor,
    FloodwayEnvelopeMetric,
    FloodwayEventEnvelope,
)
from ...classes.floodway.models import FloodwayZone
from ...classes.floodway.results import GoverningFloodwayDemand


def _metric_value(candidate: GoverningFloodwayDemand, metric: FloodwayEnvelopeMetric) -> float:
    demand = candidate.demand
    if metric is FloodwayEnvelopeMetric.VELOCITY:
        return demand.velocity
    if metric is FloodwayEnvelopeMetric.DYNAMIC_PRESSURE:
        return demand.dynamic_pressure_pa
    return demand.momentum_flux_per_width_npm


def build_floodway_event_envelope(
    candidates: Sequence[GoverningFloodwayDemand],
) -> FloodwayEventEnvelope:
    """Select independent governing events for each represented floodway zone.

    Every supplied candidate is retained in the returned envelope. For each zone,
    velocity, dynamic pressure and momentum flux are governed independently so a
    lower-discharge event can remain controlling where the hydraulics demand it.
    Equal metric values retain the earliest supplied candidate deterministically.
    """
    retained = tuple(candidates)
    grouped: dict[FloodwayZone, list[GoverningFloodwayDemand]] = defaultdict(list)
    for candidate in retained:
        grouped[candidate.demand.zone].append(candidate)

    governors: list[FloodwayEnvelopeGovernor] = []
    for zone, zone_candidates in grouped.items():
        for metric in FloodwayEnvelopeMetric:
            governing = max(
                zone_candidates,
                key=lambda candidate, metric=metric: _metric_value(candidate, metric),
            )
            governors.append(
                FloodwayEnvelopeGovernor(
                    zone=zone,
                    metric=metric,
                    scenario_name=governing.scenario_name,
                    aep_percent=governing.aep_percent,
                    demand=governing.demand,
                    source_interval_index=governing.source_interval_index,
                    integration_station=governing.integration_station,
                )
            )

    return FloodwayEventEnvelope(candidates=retained, governors=tuple(governors))
