"""Reusable calculations and adapters for floodway design workflows."""

from .adapter import build_floodway_scenario_hydraulics
from .assessment import (
    build_zone_demand,
    calculate_mrwa_surface_velocity,
    select_governing_dynamic_pressure,
    select_governing_momentum_flux,
    select_governing_velocity,
)
from .envelope import build_floodway_event_envelope
from .export import (
    export_floodway_envelope_json,
    export_floodway_governors_csv,
    floodway_envelope_record,
    floodway_governor_record,
    floodway_zone_demand_record,
)
from .hec23 import (
    HEC23_SI_KU,
    HEC23_SI_MANNING_STRICKLER,
    evaluate_hec23_overtopping_riprap,
    hec23_allowable_surface_depth,
    hec23_interstitial_velocity,
    hec23_manning_roughness,
    hec23_overtopping_riprap_d50,
    hec23_surface_unit_discharge,
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
    "HEC23_SI_KU",
    "HEC23_SI_MANNING_STRICKLER",
    "STANDARD_WATER_DENSITY",
    "build_floodway_event_envelope",
    "build_floodway_scenario_hydraulics",
    "build_zone_demand",
    "calculate_mrwa_surface_velocity",
    "dynamic_pressure",
    "evaluate_hec23_overtopping_riprap",
    "export_floodway_envelope_json",
    "export_floodway_governors_csv",
    "floodway_envelope_record",
    "floodway_governor_record",
    "floodway_zone_demand_record",
    "froude_number_rectangular",
    "governing_velocity",
    "hec23_allowable_surface_depth",
    "hec23_interstitial_velocity",
    "hec23_manning_roughness",
    "hec23_overtopping_riprap_d50",
    "hec23_surface_unit_discharge",
    "momentum_flux_per_width",
    "mrwa_maximum_attainable_velocity",
    "mrwa_specific_energy",
    "mrwa_steady_state_velocity",
    "rectangular_critical_depth",
    "select_governing_dynamic_pressure",
    "select_governing_momentum_flux",
    "select_governing_velocity",
]
