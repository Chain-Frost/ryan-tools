"""Reusable calculations and adapters for floodway design workflows."""

from .adapter import build_floodway_scenario_hydraulics
from .assessment import (
    build_zone_demand,
    calculate_mrwa_surface_velocity,
    select_governing_dynamic_pressure,
    select_governing_momentum_flux,
    select_governing_velocity,
)
from .hydraulics import (
    GRAVITATIONAL_ACCELERATION,
    STANDARD_WATER_DENSITY,
    dynamic_pressure,
    froude_number_rectangular,
    governing_velocity,
    momentum_flux_per_width,
    mrwa_maximum_attainable_velocity,
    mrwa_specific_energy,
    mrwa_steady_state_velocity,
    rectangular_critical_depth,
)

__all__: list[str] = [
    "GRAVITATIONAL_ACCELERATION",
    "STANDARD_WATER_DENSITY",
    "build_floodway_scenario_hydraulics",
    "build_zone_demand",
    "calculate_mrwa_surface_velocity",
    "dynamic_pressure",
    "froude_number_rectangular",
    "governing_velocity",
    "momentum_flux_per_width",
    "mrwa_maximum_attainable_velocity",
    "mrwa_specific_energy",
    "mrwa_steady_state_velocity",
    "rectangular_critical_depth",
    "select_governing_dynamic_pressure",
    "select_governing_momentum_flux",
    "select_governing_velocity",
]
