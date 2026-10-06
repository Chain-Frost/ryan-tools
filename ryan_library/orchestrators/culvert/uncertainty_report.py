"""Concise review summaries for completed uncertainty studies."""

from ...classes.culvert.uncertainty_results import UncertaintyStudyResult


def _cell(value: str) -> str:
    return value.replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def _number(value: float | None) -> str:
    return "unavailable" if value is None else f"{value:.6g}"


def render_uncertainty_markdown(result: UncertaintyStudyResult) -> str:
    """Show denominators and excluded statuses alongside conditional output envelopes."""
    study = result.study
    lines = [
        f"# Uncertainty study: {_cell(study.name)}",
        "",
        f"Project: {_cell(result.project.name)}. Mode: `{study.sampling_mode.value}`. Seed: `{study.seed}`.",
        "",
        "Statistics use only these hydraulic statuses: "
        + ", ".join(f"`{status.value}`" for status in study.aggregation_statuses)
        + ".",
        "Percentiles describe eligible samples using linear interpolation; they are not confidence limits.",
        "Bounded sweeps are sensitivity grids without an assumed probability distribution.",
        "Sampled discharge is an imposed flow, not a capacity calculation.",
        "",
        "| Crossing | Scenario | Alternative | Eligible / total | Outcome counts | Warnings / applicability |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for summary in result.summaries:
        counts = ", ".join(f"{status}: {count}" for status, count in summary.status_counts)
        notices = ", ".join((*summary.warning_codes, *summary.applicability_codes)) or "none"
        lines.append(
            f"| {_cell(summary.crossing_name)} | {_cell(summary.scenario_name)} | {_cell(summary.alternative_name or 'Base')} "
            f"| {summary.eligible_count} / {summary.evaluation_count} | {counts} | {_cell(notices)} |"
        )
    lines.extend(
        (
            "",
            "| Crossing / scenario / alternative | Metric | Count | Min | Max | Mean | Percentiles | Governing sample |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        )
    )
    for summary in result.summaries:
        identity = _cell(f"{summary.crossing_name} / {summary.scenario_name} / {summary.alternative_name or 'Base'}")
        for metric in summary.metrics:
            percentiles = (
                ", ".join(f"P{percentile:g}={value:.6g}" for percentile, value in metric.percentiles) or "unavailable"
            )
            lines.append(
                f"| {identity} | {metric.metric} ({metric.unit}) | {metric.count} | {_number(metric.minimum)} "
                f"| {_number(metric.maximum)} | {_number(metric.mean)} | {percentiles} | {_cell(metric.maximum_sample_id or 'unavailable')} |"
            )
    failures = [evaluation for evaluation in result.evaluations if evaluation.failure is not None]
    if failures:
        lines.extend(("", "Failed evaluations remain in CSV/JSON. Failure categories:", ""))
        messages = dict.fromkeys(
            f"{evaluation.failure.kind.value}: {evaluation.failure.message}"
            for evaluation in failures
            if evaluation.failure is not None
        )
        lines.extend(f"- {_cell(message)}" for message in messages)
    lines.extend(
        (
            "",
            "Full parameter sources, hydraulic warnings, applicability notices and solver evidence are retained in JSON.",
            "",
        )
    )
    return "\n".join(lines)
