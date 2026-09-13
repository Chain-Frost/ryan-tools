"""Machine-readable exports for culvert workflow results."""

import csv
import json
from collections.abc import Iterable
from pathlib import Path

from ...classes.culvert.results import CandidateAssessment, DesignResult, ScenarioResult


def scenario_result_record(result: ScenarioResult) -> dict[str, object]:
    """Flatten one scenario result into stable summary fields."""
    hydraulic = result.hydraulic_result
    return {
        "alternative": result.alternative_name,
        "crossing": result.crossing_name,
        "scenario": result.scenario_name,
        "discharge_m3s": hydraulic.total_discharge,
        "headwater_elevation_m": hydraulic.headwater_elevation,
        "tailwater_elevation_m": hydraulic.tailwater_elevation,
        "culvert_discharge_m3s": hydraulic.culvert_discharge,
        "roadway_discharge_m3s": hydraulic.roadway_discharge,
        "maximum_outlet_velocity_ms": result.maximum_outlet_velocity,
        "status": hydraulic.status.value,
        "warning_codes": list(result.warning_codes),
        "applicability_codes": list(result.applicability_codes),
    }


def candidate_assessment_record(assessment: CandidateAssessment) -> dict[str, object]:
    """Flatten one candidate assessment while retaining governing rejection reasons."""
    return {
        "candidate": assessment.candidate.name,
        "passed": assessment.passed,
        "worst_status": assessment.worst_status.value,
        "failure_reasons": list(assessment.failure_reasons),
        "scenario_results": [scenario_result_record(result) for result in assessment.scenario_results],
    }


def export_scenario_results_json(results: Iterable[ScenarioResult], path: Path) -> Path:
    """Write scenario summaries as JSON and return the output path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = [scenario_result_record(result) for result in results]
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target


def export_scenario_results_csv(results: Iterable[ScenarioResult], path: Path) -> Path:
    """Write one summary row per scenario result as CSV."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "alternative",
        "crossing",
        "scenario",
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
            writer.writerow(row)
    return target


def export_design_result_json(result: DesignResult, path: Path) -> Path:
    """Write ranked candidate assessments as JSON."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "recommended_candidate": None if result.recommended is None else result.recommended.candidate.name,
        "assessments": [candidate_assessment_record(assessment) for assessment in result.assessments],
    }
    target.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return target
