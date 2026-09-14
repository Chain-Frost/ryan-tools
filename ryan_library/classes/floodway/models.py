"""Typed floodway formation, hydraulic-state, and assessment models."""

from dataclasses import dataclass
from enum import StrEnum
from math import isfinite


class FloodwayZone(StrEnum):
    """Main Roads WA floodway failure/design zones from the 2006 guide."""

    DOWNSTREAM_TOE = "A"
    DOWNSTREAM_BATTER = "B"
    DOWNSTREAM_SHOULDER = "C"
    PAVEMENT = "D"
    UPSTREAM_BATTER = "E"
    FOUNDATION = "F"


class FloodwayAssessmentLayer(StrEnum):
    """Keep compliance, enhanced checks, and diagnostics explicitly separate."""

    MRWA_COMPLIANCE = "mrwa_compliance"
    ENHANCED_ASSESSMENT = "enhanced_assessment"
    DIAGNOSTIC = "diagnostic"


class FloodwayApplicabilityStatus(StrEnum):
    """Method-level applicability status for floodway calculations."""

    SUPPORTED = "supported"
    LEGACY_REPRODUCTION = "legacy_reproduction"
    NOT_APPLICABLE = "not_applicable"
    SOURCE_DATA_REQUIRED = "source_data_required"
    OUTSIDE_SOURCE_RANGE = "outside_source_range"
    SPECIALIST_REVIEW_REQUIRED = "specialist_review_required"
    TWO_D_VERIFICATION_RECOMMENDED = "two_d_verification_recommended"


class RoadwaySegmentState(StrEnum):
    """Downstream-consumable roadway state mirrored from ``culvert_solver``."""

    INACTIVE = "inactive"
    FREE_UNSUBMERGED = "free_unsubmerged"
    SUPPORTED_SUBMERGED = "supported_submerged"


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


def _positive(value: float, name: str) -> float:
    result = _finite(value, name)
    if result <= 0.0:
        msg = f"{name} must be strictly positive."
        raise ValueError(msg)
    return result


@dataclass(frozen=True, slots=True)
class FloodwayFormationZone:
    """One A-F formation zone with only the geometry/material data known for that zone."""

    zone: FloodwayZone
    slope: float | None = None
    roughness: float | None = None
    elevation: float | None = None
    label: str = ""

    def __post_init__(self) -> None:
        if self.slope is not None:
            object.__setattr__(self, "slope", _nonnegative(self.slope, "slope"))
        if self.roughness is not None:
            object.__setattr__(self, "roughness", _positive(self.roughness, "roughness"))
        if self.elevation is not None:
            object.__setattr__(self, "elevation", _finite(self.elevation, "elevation"))
        object.__setattr__(self, "label", self.label.strip())


@dataclass(frozen=True, slots=True)
class FloodwayFormation:
    """Cross-road formation description keyed by the Main Roads A-F zones."""

    name: str
    zones: tuple[FloodwayFormationZone, ...]

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            msg = "name must be nonempty text."
            raise ValueError(msg)
        zones = tuple(self.zones)
        if not zones:
            msg = "zones must contain at least one floodway formation zone."
            raise ValueError(msg)
        identifiers = [zone.zone for zone in zones]
        if len(identifiers) != len(set(identifiers)):
            msg = "Each floodway A-F zone may appear at most once."
            raise ValueError(msg)
        object.__setattr__(self, "name", name)
        object.__setattr__(self, "zones", zones)

    def get_zone(self, zone: FloodwayZone) -> FloodwayFormationZone | None:
        """Return the requested A-F zone when it is represented in this formation."""
        return next((item for item in self.zones if item.zone is zone), None)


@dataclass(frozen=True, slots=True)
class RoadwaySegmentHydraulicState:
    """Stable floodway-facing snapshot of one public roadway integration result."""

    source_interval_index: int
    interval_start_station: float
    interval_end_station: float
    integration_station: float
    physical_interval_length: float
    effective_length: float
    crest_elevation: float
    upstream_head: float
    downstream_head: float
    discharge: float
    unit_discharge: float
    flow_state: RoadwaySegmentState
    submergence_ratio: float | None = None
    submergence_factor: float | None = None

    def __post_init__(self) -> None:
        if self.source_interval_index < 0:
            msg = "source_interval_index must be non-negative."
            raise ValueError(msg)
        start = _finite(self.interval_start_station, "interval_start_station")
        end = _finite(self.interval_end_station, "interval_end_station")
        if end <= start:
            msg = "interval_end_station must be greater than interval_start_station."
            raise ValueError(msg)
        object.__setattr__(self, "interval_start_station", start)
        object.__setattr__(self, "interval_end_station", end)
        object.__setattr__(self, "integration_station", _finite(self.integration_station, "integration_station"))
        object.__setattr__(self, "physical_interval_length", _positive(self.physical_interval_length, "physical_interval_length"))
        object.__setattr__(self, "effective_length", _positive(self.effective_length, "effective_length"))
        object.__setattr__(self, "crest_elevation", _finite(self.crest_elevation, "crest_elevation"))
        object.__setattr__(self, "upstream_head", _nonnegative(self.upstream_head, "upstream_head"))
        object.__setattr__(self, "downstream_head", _nonnegative(self.downstream_head, "downstream_head"))
        object.__setattr__(self, "discharge", _nonnegative(self.discharge, "discharge"))
        object.__setattr__(self, "unit_discharge", _nonnegative(self.unit_discharge, "unit_discharge"))
        if self.submergence_ratio is not None:
            object.__setattr__(self, "submergence_ratio", _nonnegative(self.submergence_ratio, "submergence_ratio"))
        if self.submergence_factor is not None:
            object.__setattr__(self, "submergence_factor", _nonnegative(self.submergence_factor, "submergence_factor"))


@dataclass(frozen=True, slots=True)
class FloodwayScenarioHydraulics:
    """Floodway-facing hydraulic snapshot for one solved crossing scenario."""

    scenario_name: str
    aep_percent: float | None
    headwater_elevation: float
    tailwater_elevation: float
    roadway_discharge: float
    segments: tuple[RoadwaySegmentHydraulicState, ...]
    source: str | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        name = self.scenario_name.strip()
        if not name:
            msg = "scenario_name must be nonempty text."
            raise ValueError(msg)
        if self.aep_percent is not None:
            aep = _positive(self.aep_percent, "aep_percent")
            if aep > 100.0:
                msg = "aep_percent must not exceed 100."
                raise ValueError(msg)
            object.__setattr__(self, "aep_percent", aep)
        object.__setattr__(self, "scenario_name", name)
        object.__setattr__(self, "headwater_elevation", _finite(self.headwater_elevation, "headwater_elevation"))
        object.__setattr__(self, "tailwater_elevation", _finite(self.tailwater_elevation, "tailwater_elevation"))
        object.__setattr__(self, "roadway_discharge", _nonnegative(self.roadway_discharge, "roadway_discharge"))
        object.__setattr__(self, "segments", tuple(self.segments))
        source = None if self.source is None else self.source.strip()
        object.__setattr__(self, "source", source or None)
        object.__setattr__(self, "notes", self.notes.strip())
