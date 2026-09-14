"""Typed result models for floodway hydraulic-demand and envelope assessment."""

from dataclasses import dataclass
from math import isfinite

from .models import (
    FloodwayApplicabilityStatus,
    FloodwayAssessmentLayer,
    FloodwayScenarioHydraulics,
    FloodwayZone,
    RoadwaySegmentState,
)


def _finite(value: float, name: str) -> float:
    result = float(value)
    if not isfinite(result):
        msg = f"{name} must be finite."
        raise ValueError(msg)
    return result


def _nonnegative(value: float, name: str) -> float:
    result = _finite(value, name)
    if result < 0.0:
        msg = f"{name} must be non-negative."
        raise ValueError(msg)
    return result


def _aep(value: float | None) -> float | None:
    if value is None:
        return None
    result = _nonnegative(value, "aep_percent")
    if result > 100.0:
        msg = "aep_percent must not exceed 100."
        raise ValueError(msg)
    return result


@dataclass(frozen=True, slots=True)
class MrwaSurfaceVelocityResult:
    """Traceable MRWA 2006 surface-velocity calculation for one local state."""

    zone: FloodwayZone
    unit_discharge: float
    slope: float
    roughness: float
    steady_state_velocity: float
    specific_energy: float
    maximum_attainable_velocity: float | None
    adopted_velocity: float
    coefficient_k: float | None
    total_head: float | None
    applicability: FloodwayApplicabilityStatus
    layer: FloodwayAssessmentLayer = FloodwayAssessmentLayer.MRWA_COMPLIANCE
    source_id: str = "MRWA-FLOODWAY-DESIGN-GUIDE-2006-EQ4-7"

    def __post_init__(self) -> None:
        for name in (
            "unit_discharge",
            "slope",
            "roughness",
            "steady_state_velocity",
            "specific_energy",
            "adopted_velocity",
        ):
            value = getattr(self, name)
            object.__setattr__(self, name, _nonnegative(value, name))
        if self.roughness <= 0.0:
            msg = "roughness must be strictly positive."
            raise ValueError(msg)
        if self.maximum_attainable_velocity is not None:
            object.__setattr__(
                self,
                "maximum_attainable_velocity",
                _nonnegative(self.maximum_attainable_velocity, "maximum_attainable_velocity"),
            )
        if self.coefficient_k is not None:
            object.__setattr__(self, "coefficient_k", _nonnegative(self.coefficient_k, "coefficient_k"))
        if self.total_head is not None:
            object.__setattr__(self, "total_head", _nonnegative(self.total_head, "total_head"))


@dataclass(frozen=True, slots=True)
class FloodwayZoneDemand:
    """Physical hydraulic demand retained separately from design capacity/protection."""

    zone: FloodwayZone
    unit_discharge: float
    velocity: float
    dynamic_pressure_pa: float
    momentum_flux_per_width_npm: float
    specific_energy_m: float | None = None
    depth_m: float | None = None
    froude_number: float | None = None
    applicability: FloodwayApplicabilityStatus = FloodwayApplicabilityStatus.SUPPORTED
    layer: FloodwayAssessmentLayer = FloodwayAssessmentLayer.DIAGNOSTIC
    source_id: str = ""

    def __post_init__(self) -> None:
        for name in ("unit_discharge", "velocity", "dynamic_pressure_pa", "momentum_flux_per_width_npm"):
            object.__setattr__(self, name, _nonnegative(getattr(self, name), name))
        if self.specific_energy_m is not None:
            object.__setattr__(self, "specific_energy_m", _nonnegative(self.specific_energy_m, "specific_energy_m"))
        if self.depth_m is not None:
            object.__setattr__(self, "depth_m", _nonnegative(self.depth_m, "depth_m"))
        if self.froude_number is not None:
            object.__setattr__(self, "froude_number", _nonnegative(self.froude_number, "froude_number"))
        object.__setattr__(self, "source_id", self.source_id.strip())


@dataclass(frozen=True, slots=True)
class GoverningFloodwayDemand:
    """Governing local zone demand selected from an event/scenario envelope."""

    scenario_name: str
    aep_percent: float | None
    source_interval_index: int
    integration_station: float
    demand: FloodwayZoneDemand

    def __post_init__(self) -> None:
        name = self.scenario_name.strip()
        if not name:
            msg = "scenario_name must be nonempty text."
            raise ValueError(msg)
        if self.source_interval_index < 0:
            msg = "source_interval_index must be non-negative."
            raise ValueError(msg)
        object.__setattr__(self, "scenario_name", name)
        object.__setattr__(self, "aep_percent", _aep(self.aep_percent))
        object.__setattr__(self, "integration_station", _finite(self.integration_station, "integration_station"))


@dataclass(frozen=True, slots=True)
class FloodwayZoneAssessment:
    """Assessment of one A-F zone at one roadway integration location and event."""

    scenario_name: str
    aep_percent: float | None
    source_interval_index: int
    integration_station: float
    flow_state: RoadwaySegmentState
    zone: FloodwayZone
    applicability: FloodwayApplicabilityStatus
    velocity_result: MrwaSurfaceVelocityResult | None = None
    demand: FloodwayZoneDemand | None = None
    message: str = ""

    def __post_init__(self) -> None:
        name = self.scenario_name.strip()
        if not name:
            msg = "scenario_name must be nonempty text."
            raise ValueError(msg)
        if self.source_interval_index < 0:
            msg = "source_interval_index must be non-negative."
            raise ValueError(msg)
        if (self.velocity_result is None) != (self.demand is None):
            msg = "velocity_result and demand must either both be present or both be absent."
            raise ValueError(msg)
        object.__setattr__(self, "scenario_name", name)
        object.__setattr__(self, "aep_percent", _aep(self.aep_percent))
        object.__setattr__(self, "integration_station", _finite(self.integration_station, "integration_station"))
        object.__setattr__(self, "message", self.message.strip())


@dataclass(frozen=True, slots=True)
class FloodwayScenarioAssessment:
    """All zone assessments derived from one solved crossing scenario."""

    hydraulics: FloodwayScenarioHydraulics
    zone_assessments: tuple[FloodwayZoneAssessment, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "zone_assessments", tuple(self.zone_assessments))


@dataclass(frozen=True, slots=True)
class FloodwayEnvelopeResult:
    """Scenario envelope with independent governing states for unlike demand measures."""

    scenarios: tuple[FloodwayScenarioAssessment, ...]
    governing_velocity: GoverningFloodwayDemand | None
    governing_dynamic_pressure: GoverningFloodwayDemand | None
    governing_momentum_flux: GoverningFloodwayDemand | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "scenarios", tuple(self.scenarios))
