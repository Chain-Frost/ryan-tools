"""Typed protection results for sourced floodway assessment methods."""

from dataclasses import dataclass
from math import isfinite

from .models import FloodwayApplicabilityStatus, FloodwayAssessmentLayer


def _nonnegative(value: float, name: str) -> float:
    result = float(value)
    if not isfinite(result) or result < 0.0:
        msg = f"{name} must be finite and non-negative."
        raise ValueError(msg)
    return result


@dataclass(frozen=True, slots=True)
class Hec23OvertoppingRiprapResult:
    """HEC-23 DG5 overtopping-riprap sizing and layer-capacity evidence.

    ``minimum_d50_m`` is the theoretical Equation 5.2 requirement. The separately
    supplied ``selected_d50_m`` is the adopted gradation median size used by the
    subsequent interstitial-flow and layer-capacity checks.
    """

    unit_discharge: float
    slope: float
    minimum_d50_m: float
    selected_d50_m: float
    interstitial_velocity_ms: float
    average_interstitial_velocity_ms: float
    all_flow_interstitial_depth_m: float
    minimum_two_d50_thickness_m: float
    meets_minimum_d50: bool
    allowable_surface_depth_m: float | None
    surface_unit_discharge_m2s: float
    required_interstitial_unit_discharge_m2s: float
    required_interstitial_thickness_m: float
    two_d50_sufficient: bool
    four_d50_sufficient: bool
    recommended_thickness_m: float | None
    requires_larger_gradation: bool
    applicability: FloodwayApplicabilityStatus = FloodwayApplicabilityStatus.SUPPORTED
    layer: FloodwayAssessmentLayer = FloodwayAssessmentLayer.ENHANCED_ASSESSMENT
    source_id: str = "FHWA-HEC23-V2-DG5-EQ5.1-5.3"

    def __post_init__(self) -> None:
        for name in (
            "unit_discharge",
            "slope",
            "minimum_d50_m",
            "selected_d50_m",
            "interstitial_velocity_ms",
            "average_interstitial_velocity_ms",
            "all_flow_interstitial_depth_m",
            "minimum_two_d50_thickness_m",
            "surface_unit_discharge_m2s",
            "required_interstitial_unit_discharge_m2s",
            "required_interstitial_thickness_m",
        ):
            object.__setattr__(self, name, _nonnegative(getattr(self, name), name))
        if self.allowable_surface_depth_m is not None:
            object.__setattr__(
                self,
                "allowable_surface_depth_m",
                _nonnegative(self.allowable_surface_depth_m, "allowable_surface_depth_m"),
            )
        if self.recommended_thickness_m is not None:
            object.__setattr__(
                self,
                "recommended_thickness_m",
                _nonnegative(self.recommended_thickness_m, "recommended_thickness_m"),
            )
        if self.requires_larger_gradation and self.recommended_thickness_m is not None:
            msg = "recommended_thickness_m must be None when a larger gradation is required."
            raise ValueError(msg)

    @property
    def d50_m(self) -> float:
        """Return the selected gradation median size used for capacity checks."""
        return self.selected_d50_m

    @property
    def is_sufficient(self) -> bool:
        """Return whether the selected gradation and required layer checks are satisfied."""
        return self.meets_minimum_d50 and not self.requires_larger_gradation



@dataclass(frozen=True, slots=True)
class MrwaRockSlopeProtectionResult:
    """MRWA 2006 Table 5.1 dumped-rock slope-protection selection."""

    velocity_ms: float
    rock_class: str
    section_thickness_m: float | None
    nominal_class_tonnes: float | None = None
    no_rock_required: bool = False
    requires_special_design: bool = False
    applicability: FloodwayApplicabilityStatus = FloodwayApplicabilityStatus.LEGACY_REPRODUCTION
    layer: FloodwayAssessmentLayer = FloodwayAssessmentLayer.MRWA_COMPLIANCE
    source_id: str = "MRWA-FLOODWAY-DESIGN-GUIDE-2006-TABLE-5.1"

    def __post_init__(self) -> None:
        object.__setattr__(self, "velocity_ms", _nonnegative(self.velocity_ms, "velocity_ms"))
        rock_class = self.rock_class.strip()
        if not rock_class:
            msg = "rock_class must be nonempty text."
            raise ValueError(msg)
        object.__setattr__(self, "rock_class", rock_class)
        if self.section_thickness_m is not None:
            object.__setattr__(
                self,
                "section_thickness_m",
                _nonnegative(self.section_thickness_m, "section_thickness_m"),
            )
        if self.nominal_class_tonnes is not None:
            object.__setattr__(
                self,
                "nominal_class_tonnes",
                _nonnegative(self.nominal_class_tonnes, "nominal_class_tonnes"),
            )
        if self.no_rock_required and self.requires_special_design:
            msg = "A protection result cannot require both no rock and special design."
            raise ValueError(msg)
