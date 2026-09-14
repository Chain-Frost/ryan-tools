"""Compose sourced hydraulic primitives into floodway design-assessment results."""

from collections.abc import Sequence

from ...classes.floodway import (
    FloodwayApplicabilityStatus,
    FloodwayAssessmentLayer,
    FloodwayZone,
    FloodwayZoneDemand,
    GoverningFloodwayDemand,
    MrwaSurfaceVelocityResult,
)
from .hydraulics import (
    dynamic_pressure,
    governing_velocity,
    momentum_flux_per_width,
    mrwa_maximum_attainable_velocity,
    mrwa_specific_energy,
    mrwa_steady_state_velocity,
)


def calculate_mrwa_surface_velocity(
    *,
    zone: FloodwayZone,
    unit_discharge: float,
    slope: float,
    roughness: float,
    total_head: float | None = None,
    coefficient_k: float | None = None,
) -> MrwaSurfaceVelocityResult:
    """Evaluate the source-defined MRWA steady/max-velocity path for one zone.

    Equation 4 and Equation 6 can be evaluated from ``q``, ``S`` and ``n``.
    Equation 7 additionally requires a source-backed Figure 4.6 ``K`` value and
    total head. Until both are supplied, the result is explicitly marked
    ``SOURCE_DATA_REQUIRED`` and the steady-state velocity is retained only as
    an intermediate diagnostic rather than silently presented as the fully
    checked governing velocity.
    """
    if (total_head is None) != (coefficient_k is None):
        msg = "total_head and coefficient_k must be supplied together."
        raise ValueError(msg)

    steady = mrwa_steady_state_velocity(unit_discharge, slope, roughness)
    energy = mrwa_specific_energy(unit_discharge, steady)

    if total_head is None or coefficient_k is None:
        maximum = None
        adopted = steady
        applicability = FloodwayApplicabilityStatus.SOURCE_DATA_REQUIRED
    else:
        maximum = mrwa_maximum_attainable_velocity(total_head, coefficient_k)
        adopted = governing_velocity(steady, maximum)
        applicability = FloodwayApplicabilityStatus.LEGACY_REPRODUCTION

    return MrwaSurfaceVelocityResult(
        zone=zone,
        unit_discharge=unit_discharge,
        slope=slope,
        roughness=roughness,
        steady_state_velocity=steady,
        specific_energy=energy,
        maximum_attainable_velocity=maximum,
        adopted_velocity=adopted,
        coefficient_k=coefficient_k,
        total_head=total_head,
        applicability=applicability,
    )


def build_zone_demand(
    velocity_result: MrwaSurfaceVelocityResult,
) -> FloodwayZoneDemand:
    """Convert a supported/intermediate velocity result into distinct physical demands."""
    velocity = velocity_result.adopted_velocity
    return FloodwayZoneDemand(
        zone=velocity_result.zone,
        unit_discharge=velocity_result.unit_discharge,
        velocity=velocity,
        dynamic_pressure_pa=dynamic_pressure(velocity),
        momentum_flux_per_width_npm=momentum_flux_per_width(velocity_result.unit_discharge, velocity),
        specific_energy_m=velocity_result.specific_energy,
        applicability=velocity_result.applicability,
        layer=(
            FloodwayAssessmentLayer.MRWA_COMPLIANCE
            if velocity_result.applicability is FloodwayApplicabilityStatus.LEGACY_REPRODUCTION
            else FloodwayAssessmentLayer.DIAGNOSTIC
        ),
        source_id=velocity_result.source_id,
    )


def select_governing_velocity(candidates: Sequence[GoverningFloodwayDemand]) -> GoverningFloodwayDemand:
    """Return the candidate with the maximum velocity demand."""
    if not candidates:
        msg = "candidates must contain at least one demand."
        raise ValueError(msg)
    return max(candidates, key=lambda candidate: candidate.demand.velocity)


def select_governing_dynamic_pressure(candidates: Sequence[GoverningFloodwayDemand]) -> GoverningFloodwayDemand:
    """Return the candidate with the maximum dynamic-pressure demand."""
    if not candidates:
        msg = "candidates must contain at least one demand."
        raise ValueError(msg)
    return max(candidates, key=lambda candidate: candidate.demand.dynamic_pressure_pa)


def select_governing_momentum_flux(candidates: Sequence[GoverningFloodwayDemand]) -> GoverningFloodwayDemand:
    """Return the candidate with the maximum momentum-flux-per-width demand."""
    if not candidates:
        msg = "candidates must contain at least one demand."
        raise ValueError(msg)
    return max(candidates, key=lambda candidate: candidate.demand.momentum_flux_per_width_npm)
