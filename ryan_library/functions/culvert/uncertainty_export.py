"""Auditable CSV and JSON exports for application-level hydraulic studies."""

import csv
import json
from dataclasses import asdict
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from culvert_solver import (
    HydraulicUncertaintyParameter,
    ManningChannelTailwater,
    TailwaterCondition,
    TailwaterInput,
    TailwaterRatingCurve,
)
from culvert_solver import (
    __version__ as solver_version,
)

from ...classes.culvert.project import CulvertProject
from ...classes.culvert.uncertainty_results import StudyEvaluation, StudySummary, UncertaintyStudyResult
from .config import uncertainty_study_record
from .export import crossing_definition_record, design_criteria_record, scenario_result_record


def _tailwater_input_record(tailwater: TailwaterInput) -> dict[str, object]:
    if isinstance(tailwater, (float, int)):
        return {"type": "fixed", "elevation_m": float(tailwater)}
    if isinstance(tailwater, (TailwaterCondition, ManningChannelTailwater, TailwaterRatingCurve)):
        return {"type": type(tailwater).__name__, "definition": asdict(tailwater)}
    return {
        "type": f"{type(tailwater).__module__}.{type(tailwater).__qualname__}",
        "definition_available": False,
        "notes": "External Python boundary; retain its configuration separately. Resolved evidence is in each result.",
    }


def _project_input_record(project: CulvertProject) -> dict[str, object]:
    return {
        "name": project.name,
        "source": project.source,
        "notes": project.notes,
        "crossings": [crossing_definition_record(crossing) for crossing in project.crossings],
        "alternatives": [
            {
                "name": alternative.name,
                "source": alternative.source,
                "notes": alternative.notes,
                "crossing": crossing_definition_record(alternative.crossing),
            }
            for alternative in project.alternatives
        ],
        "scenarios": [
            {
                "name": scenario.name,
                "discharge_m3s": scenario.discharge,
                "tailwater": _tailwater_input_record(scenario.tailwater),
                "aep_percent": scenario.aep_percent,
                "source": scenario.source,
                "notes": scenario.notes,
                "target_headwater_elevation_m": scenario.target_headwater_elevation,
                "tailwater_override_elevation_m": scenario.tailwater_override_elevation,
            }
            for scenario in project.scenarios
        ],
        "design_criteria": None if project.design_criteria is None else design_criteria_record(project.design_criteria),
        "uncertainty_studies": [uncertainty_study_record(study) for study in project.uncertainty_studies],
    }


def study_evaluation_record(evaluation: StudyEvaluation) -> dict[str, object]:
    """Serialize one outcome, retaining sample provenance even when no result exists."""
    scenario = evaluation.scenario
    alternative = evaluation.alternative
    return {
        "sample_id": evaluation.sample.sample_id,
        "crossing": evaluation.crossing.name,
        "crossing_source": evaluation.crossing.source,
        "crossing_notes": evaluation.crossing.notes,
        "crossing_definition": crossing_definition_record(evaluation.crossing),
        "scenario": scenario.name,
        "aep_percent": scenario.aep_percent,
        "scenario_source": scenario.source,
        "scenario_notes": scenario.notes,
        "base_discharge_m3s": scenario.discharge,
        "target_headwater_elevation_m": scenario.target_headwater_elevation,
        "base_event_tailwater_override_elevation_m": scenario.tailwater_override_elevation,
        "base_tailwater": _tailwater_input_record(scenario.tailwater),
        "tailwater_was_sample_override": any(
            parameter.parameter is HydraulicUncertaintyParameter.TAILWATER_ELEVATION
            for parameter in evaluation.sample.parameters
        ),
        "alternative": evaluation.alternative_name,
        "alternative_source": None if alternative is None else alternative.source,
        "alternative_notes": None if alternative is None else alternative.notes,
        "parameters": [asdict(parameter) for parameter in evaluation.sample.parameters],
        "status": "failed" if evaluation.status is None else evaluation.status.value,
        "failure": None if evaluation.failure is None else asdict(evaluation.failure),
        "result": None if evaluation.result is None else scenario_result_record(evaluation.result),
    }


def study_summary_record(summary: StudySummary) -> dict[str, object]:
    """Serialize eligible counts, all outcome counts and per-metric governing sample IDs."""
    return {
        "crossing": summary.crossing_name,
        "scenario": summary.scenario_name,
        "alternative": summary.alternative_name,
        "evaluation_count": summary.evaluation_count,
        "eligible_count": summary.eligible_count,
        "excluded_count": summary.evaluation_count - summary.eligible_count,
        "status_counts": dict(summary.status_counts),
        "warning_codes": list(summary.warning_codes),
        "applicability_codes": list(summary.applicability_codes),
        "metrics": [asdict(metric) for metric in summary.metrics],
    }


def uncertainty_result_record(result: UncertaintyStudyResult) -> dict[str, object]:
    """Serialize complete input policy, outcome matrix and conditional statistics."""
    try:
        package_version: str | None = version("ryan_functions")
    except PackageNotFoundError:
        package_version = None
    return {
        "schema_version": 1,
        "installed_distribution_version": package_version,
        "solver_version": solver_version,
        "project": _project_input_record(result.project),
        "study": uncertainty_study_record(result.study),
        "sampling_policy": {
            "bounded": "inclusive Cartesian grid; sample_count points per parameter; midpoint when count=1",
            "monte_carlo": "independent uniform parameter streams; upstream sampler with seed + parameter index",
            "application": "absolute SI values; roughness and entrance loss apply to every group",
        },
        "aggregation_policy": {
            "accepted_statuses": [status.value for status in result.study.aggregation_statuses],
            "percentile_method": "linear interpolation at (n-1)*p/100",
            "population": "conditional on accepted hydraulic status and finite metric values",
            "interpretation": "descriptive sample statistics; bounded sweeps imply no probability distribution",
        },
        "evaluations": [study_evaluation_record(evaluation) for evaluation in result.evaluations],
        "summaries": [study_summary_record(summary) for summary in result.summaries],
    }


def export_uncertainty_json(result: UncertaintyStudyResult, path: Path) -> Path:
    """Write a complete study including detailed hydraulic warnings and solver evidence."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(uncertainty_result_record(result), indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return target


def export_uncertainty_csv(result: UncertaintyStudyResult, path: Path) -> Path:
    """Write one row per evaluation, including failed and excluded outcomes."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fields: tuple[str, ...] = (
        "study",
        "sample_id",
        "crossing",
        "scenario",
        "alternative",
        "aep_percent",
        "status",
        "eligible",
        "parameters_json",
        "discharge_m3s",
        "headwater_elevation_m",
        "tailwater_elevation_m",
        "maximum_outlet_velocity_ms",
        "culvert_discharge_m3s",
        "roadway_discharge_m3s",
        "group_discharges_json",
        "warnings_json",
        "applicability_notices_json",
        "failure_json",
    )
    with target.open("w", newline="", encoding="utf-8") as stream:
        writer: csv.DictWriter[str] = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for evaluation in result.evaluations:
            record = study_evaluation_record(evaluation)
            hydraulic = None if evaluation.result is None else evaluation.result.hydraulic_result
            detail = {} if evaluation.result is None else scenario_result_record(evaluation.result)
            row = {
                key: record[key]
                for key in ("sample_id", "crossing", "scenario", "alternative", "aep_percent", "status")
            }
            row.update(
                {
                    key: detail.get(key)
                    for key in (
                        "discharge_m3s",
                        "headwater_elevation_m",
                        "tailwater_elevation_m",
                        "maximum_outlet_velocity_ms",
                        "culvert_discharge_m3s",
                        "roadway_discharge_m3s",
                    )
                }
            )
            row.update(
                study=result.study.name,
                eligible=evaluation.status in result.study.aggregation_statuses,
                parameters_json=json.dumps(record["parameters"], allow_nan=False),
                group_discharges_json=json.dumps(
                    [] if hydraulic is None else [group.total_discharge for group in hydraulic.group_results]
                ),
                warnings_json=json.dumps(detail.get("warnings", []), allow_nan=False),
                applicability_notices_json=json.dumps(detail.get("applicability_notices", []), allow_nan=False),
                failure_json=json.dumps(record["failure"], allow_nan=False),
            )
            writer.writerow(row)
    return target


def export_uncertainty_summary_csv(result: UncertaintyStudyResult, path: Path) -> Path:
    """Write a row per metric and selected scenario, with denominators and percentiles."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as stream:
        fields: tuple[str, ...] = (
            "study",
            "crossing",
            "scenario",
            "alternative",
            "evaluation_count",
            "eligible_count",
            "status_counts_json",
            "metric",
            "unit",
            "count",
            "minimum",
            "maximum",
            "mean",
            "percentiles_json",
            "maximum_sample_id",
        )
        writer: csv.DictWriter[str] = csv.DictWriter(
            stream,
            fieldnames=fields,
        )
        writer.writeheader()
        for summary in result.summaries:
            for metric in summary.metrics:
                row = asdict(metric)
                row["percentiles_json"] = json.dumps(row.pop("percentiles"), allow_nan=False)
                row.update(
                    study=result.study.name,
                    crossing=summary.crossing_name,
                    scenario=summary.scenario_name,
                    alternative=summary.alternative_name,
                    evaluation_count=summary.evaluation_count,
                    eligible_count=summary.eligible_count,
                    status_counts_json=json.dumps(dict(summary.status_counts)),
                )
                writer.writerow(row)
    return target
