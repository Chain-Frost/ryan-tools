"""Machine-readable exports for floodway assessment results."""

import csv
import json
from pathlib import Path

from ...classes.floodway import FloodwayEnvelopeGovernor, FloodwayEventEnvelope, FloodwayZoneDemand


def floodway_zone_demand_record(demand: FloodwayZoneDemand) -> dict[str, object]:
    """Serialize one zone demand without recalculating any engineering quantity."""
    return {
        "zone": demand.zone.value,
        "unit_discharge_m2s": demand.unit_discharge,
        "velocity_ms": demand.velocity,
        "dynamic_pressure_pa": demand.dynamic_pressure_pa,
        "momentum_flux_per_width_npm": demand.momentum_flux_per_width_npm,
        "specific_energy_m": demand.specific_energy_m,
        "depth_m": demand.depth_m,
        "froude_number": demand.froude_number,
        "applicability": demand.applicability.value,
        "assessment_layer": demand.layer.value,
        "source_id": demand.source_id,
    }


def floodway_governor_record(governor: FloodwayEnvelopeGovernor) -> dict[str, object]:
    """Serialize one governing zone/metric event with its retained demand evidence."""
    return {
        "zone": governor.zone.value,
        "metric": governor.metric.value,
        "scenario": governor.scenario_name,
        "aep_percent": governor.aep_percent,
        "demand": floodway_zone_demand_record(governor.demand),
    }


def floodway_envelope_record(envelope: FloodwayEventEnvelope) -> dict[str, object]:
    """Serialize the complete event envelope including all candidate states."""
    return {
        "candidates": [
            {
                "scenario": candidate.scenario_name,
                "aep_percent": candidate.aep_percent,
                "demand": floodway_zone_demand_record(candidate.demand),
            }
            for candidate in envelope.candidates
        ],
        "governors": [floodway_governor_record(governor) for governor in envelope.governors],
    }


def export_floodway_envelope_json(envelope: FloodwayEventEnvelope, path: Path) -> Path:
    """Write the complete floodway event envelope to deterministic JSON."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(floodway_envelope_record(envelope), indent=2), encoding="utf-8")
    return target


def export_floodway_governors_csv(envelope: FloodwayEventEnvelope, path: Path) -> Path:
    """Write one compact summary row per governing zone/metric combination."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "zone",
        "metric",
        "scenario",
        "aep_percent",
        "unit_discharge_m2s",
        "velocity_ms",
        "dynamic_pressure_pa",
        "momentum_flux_per_width_npm",
        "specific_energy_m",
        "depth_m",
        "froude_number",
        "applicability",
        "assessment_layer",
        "source_id",
    ]
    with target.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for governor in envelope.governors:
            demand = floodway_zone_demand_record(governor.demand)
            writer.writerow(
                {
                    "zone": governor.zone.value,
                    "metric": governor.metric.value,
                    "scenario": governor.scenario_name,
                    "aep_percent": governor.aep_percent,
                    **{key: demand[key] for key in fieldnames[4:]},
                }
            )
    return target
