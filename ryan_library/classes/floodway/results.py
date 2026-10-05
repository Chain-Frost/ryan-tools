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
from .protection import Hec23OvertoppingRiprapResult


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
class MrwaSubmergedPavementVelocityResult:
    """MRWA 2006 submerged-pavement velocity approximation ``V ~= q / D``."""

    unit_discharge: float
    downstream_depth: float
    velocity: float
    applicability: FloodwayApplicabilityStatus = FloodwayApplicabilityStatus.LEGACY_REPRODUCTION
    layer: FloodwayAssessmentLayer = FloodwayAssessmentLayer.MRWA_COMPLIANCE
    source_id: str = "MRWA-FLOODWAY-DESIGN-GUIDE-2006-SUBMERGED-Q-OVER-D"

    def __post_init__(self) -> None:
        object.__setattr__(self, "unit_discharge", _nonnegative(self.unit_discharge, "unit_discharge"))
        depth = _nonnegative(self.downstream_depth, "downstream_depth")
        if depth <= 0.0:
            msg = "downstream_depth must be strictly positive."
            raise ValueError(msg)
        object.__setattr__(self, "downstream_depth", depth)
        object.__setattr__(self, "velocity", _nonnegative(self.velocity, "velocity"))


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
    """Candidate local zone demand for event-envelope governing-state selection."""

    scenario_name: str
    aep_percent: float | None
    demand: FloodwayZoneDemand
    source_interval_index: int = 0
    integration_station: float = 0.0
    total_discharge: float | None = None
    roadway_discharge: float | None = None
    headwater_elevation: float | None = None
    tailwater_elevation: float | None = None
    flow_state: RoadwaySegmentState | None = None

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
        if self.total_discharge is not None:
            object.__setattr__(self, "total_discharge", _nonnegative(self.total_discharge, "total_discharge"))
        if self.roadway_discharge is not None:
            object.__setattr__(self, "roadway_discharge", _nonnegative(self.roadway_discharge, "roadway_discharge"))
        if self.headwater_elevation is not None:
            object.__setattr__(
                self,
                "headwater_elevation",
                _finite(self.headwater_elevation, "headwater_elevation"),
            )
        if self.tailwater_elevation is not None:
            object.__setattr__(
                self,
                "tailwater_elevation",
                _finite(self.tailwater_elevation, "tailwater_elevation"),
            )


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
    velocity_result: MrwaSurfaceVelocityResult | MrwaSubmergedPavementVelocityResult | None = None
    demand: FloodwayZoneDemand | None = None
    protection_result: Hec23OvertoppingRiprapResult | None = None
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
