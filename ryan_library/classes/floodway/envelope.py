"""Typed event-envelope results for floodway design assessment."""

from dataclasses import dataclass
from enum import StrEnum
from math import isfinite

from .models import FloodwayZone
from .results import FloodwayZoneDemand, GoverningFloodwayDemand


class FloodwayEnvelopeMetric(StrEnum):
    """Independent hydraulic-demand metrics retained by the event envelope."""

    VELOCITY = "velocity"
    DYNAMIC_PRESSURE = "dynamic_pressure"
    MOMENTUM_FLUX = "momentum_flux"


@dataclass(frozen=True, slots=True)
class FloodwayEnvelopeGovernor:
    """One governing event/state for one zone and one independent demand metric."""

    zone: FloodwayZone
    metric: FloodwayEnvelopeMetric
    scenario_name: str
    aep_percent: float | None
    demand: FloodwayZoneDemand
    source_interval_index: int = 0
    integration_station: float = 0.0

    def __post_init__(self) -> None:
        name = self.scenario_name.strip()
        if not name:
            msg = "scenario_name must be nonempty text."
            raise ValueError(msg)
        if self.demand.zone is not self.zone:
            msg = "Governor zone must match demand.zone."
            raise ValueError(msg)
        if self.source_interval_index < 0:
            msg = "source_interval_index must be non-negative."
            raise ValueError(msg)
        station = float(self.integration_station)
        if not isfinite(station):
            msg = "integration_station must be finite."
            raise ValueError(msg)
        object.__setattr__(self, "scenario_name", name)
        object.__setattr__(self, "integration_station", station)


@dataclass(frozen=True, slots=True)
class FloodwayEventEnvelope:
    """All supplied demand states plus independent governing results by zone/metric.

    The envelope deliberately retains velocity, dynamic pressure and momentum
    flux as separate limit-state demand measures. It does not collapse them into
    a generic floodway force.
    """

    candidates: tuple[GoverningFloodwayDemand, ...]
    governors: tuple[FloodwayEnvelopeGovernor, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "candidates", tuple(self.candidates))
        object.__setattr__(self, "governors", tuple(self.governors))
        keys = [(item.zone, item.metric) for item in self.governors]
        if len(keys) != len(set(keys)):
            msg = "Only one governor may be retained for each zone/metric pair."
            raise ValueError(msg)

    def governor(
        self,
        zone: FloodwayZone,
        metric: FloodwayEnvelopeMetric,
    ) -> FloodwayEnvelopeGovernor | None:
        """Return the governing result for one zone/metric pair when available."""
        return next(
            (
                item
                for item in self.governors
                if item.zone is zone and item.metric is metric
            ),
            None,
        )
