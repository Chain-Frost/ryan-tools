"""Human-readable summaries for culvert workflow results."""

from collections.abc import Sequence

from ...classes.culvert.results import DesignResult, ScenarioResult


def _table_text(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ")


def render_scenario_markdown(results: Sequence[ScenarioResult]) -> str:
    """Render scenario summaries as a compact Markdown table."""
    lines = [
        "| Alternative | Crossing | Scenario | Q (m3/s) | HW (m) | TW (m) | Vout max (m/s) | Status | Notices |",
        "| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for result in results:
        hydraulic = result.hydraulic_result
        lines.append(
            "| "
            + " | ".join(
                (
                    _table_text(result.alternative_name or "—"),
                    _table_text(result.crossing_name),
                    _table_text(result.scenario_name),
                    f"{hydraulic.total_discharge:.3f}",
                    f"{hydraulic.headwater_elevation:.3f}",
                    f"{hydraulic.tailwater_elevation:.3f}",
                    f"{result.maximum_outlet_velocity:.3f}",
                    hydraulic.status.value,
                    _table_text(", ".join((*result.warning_codes, *result.applicability_codes)) or "—"),
                )
            )
            + " |"
        )
    return "\n".join(lines) + "\n"


def render_design_markdown(result: DesignResult) -> str:
    """Render ranked design assessments and governing failure reasons."""
    lines = [
        "| Rank | Candidate | Result | Governing reason |",
        "| ---: | --- | --- | --- |",
    ]
    for index, assessment in enumerate(result.assessments, start=1):
        reason = "—" if assessment.passed else _table_text("; ".join(assessment.failure_reasons))
        lines.append(
            f"| {index} | {_table_text(assessment.candidate.name)} | "
            f"{'PASS' if assessment.passed else 'REJECT'} | {reason} |"
        )
    return "\n".join(lines) + "\n"
