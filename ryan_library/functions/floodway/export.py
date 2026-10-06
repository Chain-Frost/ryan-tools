"""Machine-readable exports for floodway assessment results."""

import csv
import json
from collections.abc import Sequence
from pathlib import Path

from ...classes.floodway import (
    FloodwayEnvelopeGovernor,
    FloodwayEventEnvelope,
    FloodwayScenarioAssessment,
    FloodwayZoneAssessment,
    FloodwayZoneDemand,
    Hec23OvertoppingRiprapResult,
    MrwaRockSlopeProtectionResult,
    MrwaSubmergedPavementVelocityResult,
    MrwaSurfaceVelocityResult,
    RoadwaySegmentState,
)
from .mrwa_protection import (
    MRWA_CURRENT_FLOODWAY_GUIDANCE,
    MRWA_CURRENT_FLOODWAY_GUIDANCE_SOURCE_ID,
)


def floodway_zone_demand_record(demand: FloodwayZoneDemand) -> dict[str, object]:
    """Serialize one zone demand without recalculating any engineering quantity."""
    return {
        "zone": demand.zone.value,
        "unit_discharge_m2s": demand.unit_discharge,
        "velocity_ms": demand.velocity,
        "dynamic_pressure_pa": demand.dynamic_pressure_pa,
        "momentum_flux_per_width_npm": demand.momentum_flux_per_width_npm,
        "specific_energy_m": demand.specific_energy_m,
        "velocity_head_m": demand.velocity_head_m,
        "depth_m": demand.depth_m,
        "froude_number": demand.froude_number,
        "applicability": demand.applicability.value,
        "assessment_layer": demand.layer.value,
        "source_id": demand.source_id,
    }


def _hydraulic_evidence_record(
    *,
    total_discharge: float | None,
    roadway_discharge: float | None,
    headwater_elevation: float | None,
    tailwater_elevation: float | None,
    flow_state: RoadwaySegmentState | None,
) -> dict[str, object]:
    return {
        "total_discharge_m3s": total_discharge,
        "roadway_discharge_m3s": roadway_discharge,
        "headwater_elevation_m": headwater_elevation,
        "tailwater_elevation_m": tailwater_elevation,
        "flow_state": None if flow_state is None else flow_state.value,
    }


def floodway_velocity_result_record(
    result: MrwaSurfaceVelocityResult | MrwaSubmergedPavementVelocityResult,
) -> dict[str, object]:
    """Serialize sourced MRWA velocity evidence without recalculation."""
    if isinstance(result, MrwaSurfaceVelocityResult):
        return {
            "method": "mrwa_surface_velocity",
            "zone": result.zone.value,
            "unit_discharge_m2s": result.unit_discharge,
            "slope": result.slope,
            "roughness_manning_n": result.roughness,
            "steady_state_velocity_ms": result.steady_state_velocity,
            "specific_energy_m": result.specific_energy,
            "maximum_attainable_velocity_ms": result.maximum_attainable_velocity,
            "adopted_velocity_ms": result.adopted_velocity,
            "figure_4_6_k": result.coefficient_k,
            "total_head_m": result.total_head,
            "applicability": result.applicability.value,
            "assessment_layer": result.layer.value,
            "source_id": result.source_id,
        }
    return {
        "method": "mrwa_submerged_pavement_q_over_d",
        "unit_discharge_m2s": result.unit_discharge,
        "downstream_depth_m": result.downstream_depth,
        "velocity_ms": result.velocity,
        "applicability": result.applicability.value,
        "assessment_layer": result.layer.value,
        "source_id": result.source_id,
    }


def mrwa_rock_protection_record(result: MrwaRockSlopeProtectionResult) -> dict[str, object]:
    """Serialize MRWA 2006 Table 5.1 rock slope-protection evidence."""
    return {
        "velocity_ms": result.velocity_ms,
        "rock_class": result.rock_class,
        "section_thickness_m": result.section_thickness_m,
        "nominal_class_tonnes": result.nominal_class_tonnes,
        "no_rock_required": result.no_rock_required,
        "requires_special_design": result.requires_special_design,
        "applicability": result.applicability.value,
        "assessment_layer": result.layer.value,
        "source_id": result.source_id,
    }


def hec23_protection_record(result: Hec23OvertoppingRiprapResult) -> dict[str, object]:
    """Serialize HEC-23 DG5 protection evidence."""
    return {
        "unit_discharge_m2s": result.unit_discharge,
        "slope": result.slope,
        "minimum_d50_m": result.minimum_d50_m,
        "selected_d50_m": result.selected_d50_m,
        "interstitial_velocity_ms": result.interstitial_velocity_ms,
        "average_interstitial_velocity_ms": result.average_interstitial_velocity_ms,
        "all_flow_interstitial_depth_m": result.all_flow_interstitial_depth_m,
        "minimum_two_d50_thickness_m": result.minimum_two_d50_thickness_m,
        "meets_minimum_d50": result.meets_minimum_d50,
        "allowable_surface_depth_m": result.allowable_surface_depth_m,
        "surface_unit_discharge_m2s": result.surface_unit_discharge_m2s,
        "required_interstitial_unit_discharge_m2s": result.required_interstitial_unit_discharge_m2s,
        "required_interstitial_thickness_m": result.required_interstitial_thickness_m,
        "two_d50_sufficient": result.two_d50_sufficient,
        "four_d50_sufficient": result.four_d50_sufficient,
        "recommended_thickness_m": result.recommended_thickness_m,
        "requires_larger_gradation": result.requires_larger_gradation,
        "is_sufficient": result.is_sufficient,
        "applicability": result.applicability.value,
        "assessment_layer": result.layer.value,
        "source_id": result.source_id,
    }


def floodway_zone_assessment_record(assessment: FloodwayZoneAssessment) -> dict[str, object]:
    """Serialize one local A-F assessment with method evidence and warnings."""
    return {
        "scenario": assessment.scenario_name,
        "aep_percent": assessment.aep_percent,
        "source_interval_index": assessment.source_interval_index,
        "integration_station_m": assessment.integration_station,
        "flow_state": assessment.flow_state.value,
        "zone": assessment.zone.value,
        "applicability": assessment.applicability.value,
        "message": assessment.message,
        "velocity_result": (
            None if assessment.velocity_result is None else floodway_velocity_result_record(assessment.velocity_result)
        ),
        "demand": None if assessment.demand is None else floodway_zone_demand_record(assessment.demand),
        "mrwa_protection_result": (
            None
            if assessment.mrwa_protection_result is None
            else mrwa_rock_protection_record(assessment.mrwa_protection_result)
        ),
        "protection_result": (
            None if assessment.protection_result is None else hec23_protection_record(assessment.protection_result)
        ),
    }


def floodway_scenario_record(assessment: FloodwayScenarioAssessment) -> dict[str, object]:
    """Serialize one complete floodway scenario assessment."""
    hydraulics = assessment.hydraulics
    return {
        "scenario": hydraulics.scenario_name,
        "aep_percent": hydraulics.aep_percent,
        "source": hydraulics.source,
        "notes": hydraulics.notes,
        "total_discharge_m3s": hydraulics.total_discharge,
        "roadway_discharge_m3s": hydraulics.roadway_discharge,
        "headwater_elevation_m": hydraulics.headwater_elevation,
        "tailwater_elevation_m": hydraulics.tailwater_elevation,
        "zone_assessments": [floodway_zone_assessment_record(item) for item in assessment.zone_assessments],
    }


def floodway_governor_record(governor: FloodwayEnvelopeGovernor) -> dict[str, object]:
    """Serialize one governing zone/metric event with its retained hydraulic evidence."""
    return {
        "zone": governor.zone.value,
        "metric": governor.metric.value,
        "scenario": governor.scenario_name,
        "aep_percent": governor.aep_percent,
        "source_interval_index": governor.source_interval_index,
        "integration_station_m": governor.integration_station,
        "assessment_applicability": (
            None if governor.assessment_applicability is None else governor.assessment_applicability.value
        ),
        "assessment_message": governor.assessment_message,
        **_hydraulic_evidence_record(
            total_discharge=governor.total_discharge,
            roadway_discharge=governor.roadway_discharge,
            headwater_elevation=governor.headwater_elevation,
            tailwater_elevation=governor.tailwater_elevation,
            flow_state=governor.flow_state,
        ),
        "demand": floodway_zone_demand_record(governor.demand),
    }


def floodway_envelope_record(envelope: FloodwayEventEnvelope) -> dict[str, object]:
    """Serialize the complete event envelope including all candidate states."""
    return {
        "current_mrwa_guidance": {
            "source_id": MRWA_CURRENT_FLOODWAY_GUIDANCE_SOURCE_ID,
            "requirements": [
                {"id": identifier, "requirement": requirement}
                for identifier, requirement in MRWA_CURRENT_FLOODWAY_GUIDANCE
            ],
        },
        "candidates": [
            {
                "scenario": candidate.scenario_name,
                "aep_percent": candidate.aep_percent,
                "source_interval_index": candidate.source_interval_index,
                "integration_station_m": candidate.integration_station,
                "assessment_applicability": (
                    None if candidate.assessment_applicability is None else candidate.assessment_applicability.value
                ),
                "assessment_message": candidate.assessment_message,
                **_hydraulic_evidence_record(
                    total_discharge=candidate.total_discharge,
                    roadway_discharge=candidate.roadway_discharge,
                    headwater_elevation=candidate.headwater_elevation,
                    tailwater_elevation=candidate.tailwater_elevation,
                    flow_state=candidate.flow_state,
                ),
                "demand": floodway_zone_demand_record(candidate.demand),
            }
            for candidate in envelope.candidates
        ],
        "governors": [floodway_governor_record(governor) for governor in envelope.governors],
    }


def export_floodway_scenarios_json(
    assessments: Sequence[FloodwayScenarioAssessment],
    path: Path,
) -> Path:
    """Write complete scenario assessments, including protection evidence, to JSON."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps([floodway_scenario_record(item) for item in assessments], indent=2),
        encoding="utf-8",
    )
    return target


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
        "source_interval_index",
        "integration_station_m",
        "total_discharge_m3s",
        "roadway_discharge_m3s",
        "headwater_elevation_m",
        "tailwater_elevation_m",
        "flow_state",
        "assessment_applicability",
        "assessment_message",
        "unit_discharge_m2s",
        "velocity_ms",
        "dynamic_pressure_pa",
        "momentum_flux_per_width_npm",
        "specific_energy_m",
        "velocity_head_m",
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
            evidence = _hydraulic_evidence_record(
                total_discharge=governor.total_discharge,
                roadway_discharge=governor.roadway_discharge,
                headwater_elevation=governor.headwater_elevation,
                tailwater_elevation=governor.tailwater_elevation,
                flow_state=governor.flow_state,
            )
            writer.writerow(
                {
                    "zone": governor.zone.value,
                    "metric": governor.metric.value,
                    "scenario": governor.scenario_name,
                    "aep_percent": governor.aep_percent,
                    "source_interval_index": governor.source_interval_index,
                    "integration_station_m": governor.integration_station,
                    **evidence,
                    "assessment_applicability": (
                        "" if governor.assessment_applicability is None else governor.assessment_applicability.value
                    ),
                    "assessment_message": governor.assessment_message,
                    **{key: demand[key] for key in fieldnames[13:]},
                }
            )
    return target
