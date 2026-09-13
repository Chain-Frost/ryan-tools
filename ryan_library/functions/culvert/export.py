"""Machine-readable exports for culvert workflow results."""

import csv
import json
from collections.abc import Iterable
from pathlib import Path

from culvert_solver import (
    ConvergenceRecord,
    EntranceLossSelection,
    ExitLossSelection,
    HeadLossComponents,
    InletCoefficientSelection,
    SourceReference,
    TailwaterResolution,
)

from ...classes.culvert.criteria import DesignCriteria
from ...classes.culvert.crossing import (
    CircularBarrelDefinition,
    CrossingDefinition,
    RectangularBarrelDefinition,
)
from ...classes.culvert.results import CandidateAssessment, CrossingRatingResult, DesignResult, ScenarioResult


def _source_record(source: SourceReference | None) -> dict[str, object] | None:
    if source is None:
        return None
    return {
        "source_id": source.source_id,
        "publication": source.publication,
        "edition": source.edition,
        "locator": source.locator,
        "url": source.url,
        "applicability": source.applicability,
        "notes": source.notes,
    }


def _tailwater_record(resolution: TailwaterResolution | None) -> dict[str, object] | None:
    if resolution is None:
        return None
    return {
        "method": resolution.method.value,
        "elevation_m": resolution.elevation,
        "discharge_m3s": resolution.discharge,
        "channel_invert_elevation_m": resolution.channel_invert_elevation,
        "depth_m": resolution.depth,
        "roughness": resolution.roughness,
        "friction_slope": resolution.friction_slope,
        "interpolation": None if resolution.interpolation is None else resolution.interpolation.value,
        "method_source": _source_record(resolution.method_source),
        "rating_curve_source": _source_record(resolution.rating_curve_source),
    }


def _convergence_record(record: ConvergenceRecord | None) -> dict[str, object] | None:
    if record is None:
        return None
    result = record.result
    return {
        "calculation": record.calculation.value,
        "root": result.root,
        "residual": result.residual,
        "bracket": list(result.bracket),
        "iterations": result.iterations,
    }


def _head_loss_record(losses: HeadLossComponents | None) -> dict[str, object] | None:
    if losses is None:
        return None
    return {
        "entrance_m": losses.entrance,
        "friction_m": losses.friction,
        "exit_m": losses.exit,
        "total_m": losses.total,
    }


def _inlet_selection_record(selection: InletCoefficientSelection | None) -> dict[str, object] | None:
    if selection is None:
        return None
    coefficients = selection.coefficients
    return {
        "name": coefficients.name,
        "chart": coefficients.chart,
        "scale": coefficients.scale,
        "form": coefficients.form.value,
        "k": coefficients.k,
        "m": coefficients.m,
        "c": coefficients.c,
        "y": coefficients.y,
        "slope_correction": coefficients.slope_correction,
        "shape": coefficients.shape.value,
        "basis": selection.basis.value,
        "source": _source_record(selection.source),
    }


def _entrance_loss_selection_record(selection: EntranceLossSelection | None) -> dict[str, object] | None:
    if selection is None:
        return None
    return {
        "ke": selection.ke,
        "name": selection.name,
        "basis": selection.basis.value,
        "shape": selection.shape.value,
        "source": _source_record(selection.source),
    }


def _exit_loss_selection_record(selection: ExitLossSelection | None) -> dict[str, object] | None:
    if selection is None:
        return None
    return {
        "ko": selection.ko,
        "name": selection.name,
        "basis": selection.basis.value,
        "source": _source_record(selection.source),
    }


def crossing_definition_record(crossing: CrossingDefinition) -> dict[str, object]:
    """Serialize one workflow-owned crossing definition for design auditability."""
    groups: list[dict[str, object]] = []
    for group in crossing.groups:
        barrel = group.barrel
        barrel_record: dict[str, object] = {
            "length_m": barrel.length,
            "inlet_invert_m": barrel.inlet_invert,
            "outlet_invert_m": barrel.outlet_invert,
            "roughness": barrel.roughness,
            "material": barrel.material.value,
            "label": barrel.label,
        }
        if isinstance(barrel, RectangularBarrelDefinition):
            barrel_record.update(shape="rectangular", span_mm=barrel.span_mm, rise_mm=barrel.rise_mm)
        elif isinstance(barrel, CircularBarrelDefinition):  # pyright: ignore[reportUnnecessaryIsInstance]
            barrel_record.update(shape="circular", diameter_mm=barrel.diameter_mm)
        groups.append({"name": group.name, "quantity": group.quantity, "barrel": barrel_record})
    roadway = crossing.roadway
    return {
        "name": crossing.name,
        "groups": groups,
        "roadway": (
            None
            if roadway is None
            else {
                "crest_elevation_m": roadway.crest_elevation,
                "crest_length_m": roadway.crest_length,
                "discharge_coefficient": roadway.discharge_coefficient,
                "label": roadway.label,
            }
        ),
    }


def design_criteria_record(criteria: DesignCriteria) -> dict[str, object]:
    """Serialize the exact constraints used for a design search."""
    return {
        "maximum_headwater_elevation_m": criteria.maximum_headwater_elevation,
        "maximum_headwater_depth_m": criteria.maximum_headwater_depth,
        "maximum_headwater_ratio": criteria.maximum_headwater_ratio,
        "minimum_freeboard_m": criteria.minimum_freeboard,
        "maximum_outlet_velocity_ms": criteria.maximum_outlet_velocity,
        "maximum_roadway_discharge_m3s": criteria.maximum_roadway_discharge,
        "maximum_barrel_count": criteria.maximum_barrel_count,
        "maximum_total_structure_width_m": criteria.maximum_total_structure_width,
        "require_resolved_result": criteria.require_resolved_result,
    }


def scenario_result_record(result: ScenarioResult) -> dict[str, object]:
    """Serialize one scenario result with summary fields and detailed solver evidence."""
    hydraulic = result.hydraulic_result
    warning_records = [
        {
            "code": warning.code.value,
            "message": warning.message,
            "result_status": warning.result_status.value,
        }
        for group_result in hydraulic.group_results
        for warning in group_result.barrel_result.warnings
    ]
    applicability_records = [
        {
            "code": notice.code.value,
            "message": notice.message,
            "source": _source_record(notice.source),
        }
        for notice in hydraulic.applicability_notices
    ]
    group_records: list[dict[str, object]] = []
    for index, group_result in enumerate(hydraulic.group_results, start=1):
        barrel = group_result.barrel_result
        group_records.append(
            {
                "group_index": index,
                "quantity": group_result.group.quantity,
                "total_discharge_m3s": group_result.total_discharge,
                "barrel_discharge_m3s": group_result.barrel_discharge,
                "headwater_elevation_m": barrel.headwater_elevation,
                "headwater_depth_m": barrel.headwater_depth,
                "tailwater_elevation_m": barrel.tailwater_elevation,
                "tailwater_depth_m": barrel.tailwater_depth,
                "control_type": barrel.control_type.value,
                "flow_regime": barrel.regime.value,
                "outlet_velocity_ms": barrel.velocity_outlet,
                "outlet_depth_m": barrel.outlet_depth,
                "critical_depth_m": barrel.critical_depth,
                "normal_depth_m": barrel.normal_depth,
                "profile_curve": None if barrel.profile_curve is None else barrel.profile_curve.value,
                "outlet_sequent_depth_m": barrel.outlet_sequent_depth,
                "hydraulic_jump_station_m": barrel.hydraulic_jump_station,
                "hydraulic_jump_swept_out": barrel.hydraulic_jump_swept_out,
                "full_flow_length_m": barrel.full_flow_length,
                "adopted_roughness_manning_n": barrel.adopted_roughness,
                "roughness_selection_basis": (
                    None if barrel.roughness_selection_basis is None else barrel.roughness_selection_basis.value
                ),
                "roughness_source": _source_record(barrel.roughness_source),
                "inlet_coefficient_selection": _inlet_selection_record(barrel.inlet_coefficient_selection),
                "entrance_loss_selection": _entrance_loss_selection_record(barrel.entrance_loss_selection),
                "exit_loss_selection": _exit_loss_selection_record(barrel.exit_loss_selection),
                "outlet_control_losses": _head_loss_record(barrel.outlet_control_losses),
                "full_flow_losses": _head_loss_record(barrel.full_flow_losses),
                "convergence": [_convergence_record(record) for record in barrel.convergence],
                "discharge_convergence": _convergence_record(group_result.discharge_convergence),
                "tailwater_resolution": _tailwater_record(group_result.tailwater_resolution),
            }
        )
    return {
        "alternative": result.alternative_name,
        "crossing": result.crossing_name,
        "scenario": result.scenario_name,
        "aep_percent": result.aep_percent,
        "scenario_source": result.source,
        "scenario_notes": result.notes,
        "target_headwater_elevation_m": result.target_headwater_elevation,
        "target_headwater_residual_m": result.target_headwater_residual,
        "tailwater_override_elevation_m": result.tailwater_override_elevation,
        "tailwater_was_event_override": result.tailwater_was_event_override,
        "discharge_m3s": hydraulic.total_discharge,
        "headwater_elevation_m": hydraulic.headwater_elevation,
        "tailwater_elevation_m": hydraulic.tailwater_elevation,
        "culvert_discharge_m3s": hydraulic.culvert_discharge,
        "roadway_discharge_m3s": hydraulic.roadway_discharge,
        "maximum_outlet_velocity_ms": result.maximum_outlet_velocity,
        "status": hydraulic.status.value,
        "warning_codes": list(result.warning_codes),
        "warnings": warning_records,
        "applicability_codes": list(result.applicability_codes),
        "applicability_notices": applicability_records,
        "headwater_convergence": _convergence_record(hydraulic.headwater_convergence),
        "tailwater_resolution": _tailwater_record(hydraulic.tailwater_resolution),
        "group_results": group_records,
    }


def candidate_assessment_record(assessment: CandidateAssessment) -> dict[str, object]:
    """Flatten one candidate assessment while retaining governing rejection reasons."""
    return {
        "candidate": assessment.candidate.name,
        "crossing_definition": crossing_definition_record(assessment.candidate.crossing),
        "passed": assessment.passed,
        "worst_status": assessment.worst_status.value,
        "failure_reasons": list(assessment.failure_reasons),
        "failures": [
            {
                "code": failure.code.value,
                "scenario": failure.scenario_name,
                "message": failure.message,
            }
            for failure in assessment.failures
        ],
        "scenario_results": [scenario_result_record(result) for result in assessment.scenario_results],
    }


def export_scenario_results_json(results: Iterable[ScenarioResult], path: Path) -> Path:
    """Write scenario results as detailed JSON and return the output path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = [scenario_result_record(result) for result in results]
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target


def export_scenario_results_csv(results: Iterable[ScenarioResult], path: Path) -> Path:
    """Write one stable summary row per scenario result as CSV."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "alternative",
        "crossing",
        "scenario",
        "aep_percent",
        "scenario_source",
        "scenario_notes",
        "target_headwater_elevation_m",
        "target_headwater_residual_m",
        "tailwater_override_elevation_m",
        "tailwater_was_event_override",
        "discharge_m3s",
        "headwater_elevation_m",
        "tailwater_elevation_m",
        "culvert_discharge_m3s",
        "roadway_discharge_m3s",
        "maximum_outlet_velocity_ms",
        "status",
        "warning_codes",
        "applicability_codes",
    ]
    with target.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            row = scenario_result_record(result)
            row["warning_codes"] = ";".join(result.warning_codes)
            row["applicability_codes"] = ";".join(result.applicability_codes)
            writer.writerow({name: row[name] for name in fieldnames})
    return target


def export_design_result_json(
    result: DesignResult,
    path: Path,
    *,
    criteria: DesignCriteria | None = None,
) -> Path:
    """Write ranked candidate assessments as JSON."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "recommended_candidate": None if result.recommended is None else result.recommended.candidate.name,
        "design_criteria": None if criteria is None else design_criteria_record(criteria),
        "assessments": [candidate_assessment_record(assessment) for assessment in result.assessments],
    }
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target


def rating_point_record(result: CrossingRatingResult, index: int) -> dict[str, object]:
    """Serialize one complete rating point with warning and tailwater evidence."""
    point = result.rating_curve.points[index]
    return {
        "crossing": result.crossing_name,
        "discharge_m3s": point.discharge,
        "headwater_elevation_m": point.headwater_elevation,
        "headwater_depth_m": point.headwater_depth,
        "tailwater_elevation_m": point.tailwater_elevation,
        "tailwater_depth_m": point.tailwater_depth,
        "outlet_velocity_ms": point.outlet_velocity,
        "control_type": point.control_type.value,
        "flow_regime": point.regime.value,
        "status": point.status.value,
        "warnings": [{"code": warning.code.value, "message": warning.message} for warning in point.warnings],
        "tailwater_resolution": _tailwater_record(point.tailwater_resolution),
    }


def export_crossing_rating_json(result: CrossingRatingResult, path: Path) -> Path:
    """Write a complete crossing rating curve as JSON."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = [rating_point_record(result, index) for index in range(len(result.rating_curve.points))]
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target


def export_crossing_rating_csv(result: CrossingRatingResult, path: Path) -> Path:
    """Write stable scalar rating-curve fields and warning codes as CSV."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "crossing",
        "discharge_m3s",
        "headwater_elevation_m",
        "headwater_depth_m",
        "tailwater_elevation_m",
        "tailwater_depth_m",
        "outlet_velocity_ms",
        "control_type",
        "flow_regime",
        "status",
        "warning_codes",
        "tailwater_method",
    ]
    with target.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for point in result.rating_curve.points:
            writer.writerow(
                {
                    "crossing": result.crossing_name,
                    "discharge_m3s": point.discharge,
                    "headwater_elevation_m": point.headwater_elevation,
                    "headwater_depth_m": point.headwater_depth,
                    "tailwater_elevation_m": point.tailwater_elevation,
                    "tailwater_depth_m": point.tailwater_depth,
                    "outlet_velocity_ms": point.outlet_velocity,
                    "control_type": point.control_type.value,
                    "flow_regime": point.regime.value,
                    "status": point.status.value,
                    "warning_codes": ";".join(warning.code.value for warning in point.warnings),
                    "tailwater_method": (
                        None if point.tailwater_resolution is None else point.tailwater_resolution.method.value
                    ),
                }
            )
    return target
