"""Conditional output envelopes and empirical percentiles for hydraulic studies."""

from collections import Counter
from collections.abc import Sequence
from math import floor, isfinite
from statistics import fmean

from culvert_solver import HydraulicResultStatus

from ...classes.culvert.uncertainty import UncertaintyStudy
from ...classes.culvert.uncertainty_results import StudyEvaluation, StudyMetricSummary, StudySummary

_METRIC_UNITS = {
    "headwater_elevation_m": "m",
    "maximum_headwater_depth_m": "m",
    "maximum_outlet_velocity_ms": "m/s",
    "total_discharge_m3s": "m3/s",
    "culvert_discharge_m3s": "m3/s",
    "roadway_discharge_m3s": "m3/s",
}


def _metrics(evaluation: StudyEvaluation) -> dict[str, float]:
    if evaluation.result is None:
        return {}
    result = evaluation.result
    hydraulic = result.hydraulic_result
    values = {
        "headwater_elevation_m": hydraulic.headwater_elevation,
        "maximum_headwater_depth_m": max(group.barrel_result.headwater_depth for group in hydraulic.group_results),
        "maximum_outlet_velocity_ms": result.maximum_outlet_velocity,
        "total_discharge_m3s": hydraulic.total_discharge,
        "culvert_discharge_m3s": hydraulic.culvert_discharge,
        "roadway_discharge_m3s": hydraulic.roadway_discharge,
    }
    values.update(
        (f"group_{index}_discharge_m3s", group.total_discharge)
        for index, group in enumerate(hydraulic.group_results, start=1)
    )
    return values


def _percentile(values: list[float], percentile: float) -> float:
    position = (len(values) - 1) * percentile / 100.0
    lower = floor(position)
    upper = min(lower + 1, len(values) - 1)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def _metric_summary(
    metric: str, evaluations: Sequence[StudyEvaluation], percentiles: tuple[float, ...]
) -> StudyMetricSummary:
    pairs = [
        (values[metric], evaluation.sample.sample_id)
        for evaluation in evaluations
        if metric in (values := _metrics(evaluation))
    ]
    finite_pairs = [(value, sample_id) for value, sample_id in pairs if isfinite(value)]
    values = sorted(value for value, _ in finite_pairs)
    return StudyMetricSummary(
        metric=metric,
        unit=_METRIC_UNITS.get(metric, "m3/s"),
        count=len(values),
        minimum=min(values) if values else None,
        maximum=max(values) if values else None,
        mean=fmean(values) if values else None,
        percentiles=tuple((percentile, _percentile(values, percentile)) for percentile in percentiles)
        if values
        else (),
        maximum_sample_id=max(finite_pairs, key=lambda pair: pair[0])[1] if values else None,
    )


def aggregate_study_evaluations(
    evaluations: Sequence[StudyEvaluation], study: UncertaintyStudy
) -> tuple[StudySummary, ...]:
    """Keep every outcome in counts; statistics use only explicitly accepted statuses.

    Percentiles use linear interpolation at (n-1)*p/100. They describe the eligible
    sample population, including equal weighting of bounded grid points. They are
    neither confidence limits nor a hydraulic capacity determination.
    """
    grouped: dict[tuple[str, str, str | None], list[StudyEvaluation]] = {}
    for evaluation in evaluations:
        key = (evaluation.crossing.name, evaluation.scenario.name, evaluation.alternative_name)
        grouped.setdefault(key, []).append(evaluation)
    summaries: list[StudySummary] = []
    for (crossing, scenario, alternative), group in grouped.items():
        counts = Counter("failed" if item.status is None else item.status.value for item in group)
        eligible = [item for item in group if item.status in study.aggregation_statuses]
        metric_names = (
            *_METRIC_UNITS,
            *(f"group_{index}_discharge_m3s" for index in range(1, len(group[0].crossing.groups) + 1)),
        )
        summaries.append(
            StudySummary(
                crossing_name=crossing,
                scenario_name=scenario,
                alternative_name=alternative,
                evaluation_count=len(group),
                eligible_count=len(eligible),
                status_counts=tuple(
                    (status, counts[status]) for status in (*[value.value for value in HydraulicResultStatus], "failed")
                ),
                warning_codes=tuple(
                    dict.fromkeys(
                        code for item in group if item.result is not None for code in item.result.warning_codes
                    )
                ),
                applicability_codes=tuple(
                    dict.fromkeys(
                        code for item in group if item.result is not None for code in item.result.applicability_codes
                    )
                ),
                metrics=tuple(_metric_summary(metric, eligible, study.percentiles) for metric in metric_names),
            )
        )
    return tuple(summaries)
