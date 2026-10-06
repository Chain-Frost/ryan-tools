"""Bounded execution of project uncertainty studies through authoritative hydraulics."""

from collections.abc import Callable, Sequence

from ...classes.culvert.alternative import Alternative
from ...classes.culvert.crossing import CrossingDefinition
from ...classes.culvert.project import CulvertProject
from ...classes.culvert.uncertainty import UncertaintySamplingMode, UncertaintyStudy
from ...classes.culvert.uncertainty_results import UncertaintyStudyResult
from ...functions.culvert.uncertainty import generate_study_samples
from ...functions.culvert.uncertainty_evaluation import evaluate_crossing_sample
from ...functions.culvert.uncertainty_statistics import aggregate_study_evaluations


def _select[T](items: Sequence[T], names: tuple[str, ...], get_name: Callable[[T], str]) -> tuple[T, ...]:
    available = {get_name(item): item for item in items}
    if missing := set(names) - available.keys():
        msg = f"Unknown study selection(s): {', '.join(sorted(missing))}. Available: {', '.join(available)}"
        raise ValueError(msg)
    return tuple(available[name] for name in names) if names else tuple(items)


def select_uncertainty_targets(
    project: CulvertProject,
    study: UncertaintyStudy,
) -> tuple[tuple[CrossingDefinition, Alternative | None], ...]:
    """Resolve the exact crossing/alternative targets selected by a study."""
    crossings = _select(project.crossings, study.crossing_names, lambda item: item.name)
    alternatives = _select(project.alternatives, study.alternative_names, lambda item: item.name)
    targets: list[tuple[CrossingDefinition, Alternative | None]] = []
    if study.include_base_crossings:
        targets.extend((crossing, None) for crossing in crossings)
    targets.extend((alternative.crossing, alternative) for alternative in alternatives)
    if not targets:
        msg = "Study selection contains no crossings or alternatives to evaluate."
        raise ValueError(msg)
    return tuple(targets)


def run_uncertainty_study(project: CulvertProject, study: UncertaintyStudy) -> UncertaintyStudyResult:
    """Evaluate the selected matrix, retaining every expected hydraulic outcome.

    Empty name selectors mean all available members. Base crossings can be disabled
    explicitly for alternative-only studies. The full Cartesian evaluation count is
    checked before allocating samples or invoking hydraulics.
    """
    scenarios = _select(project.scenarios, study.scenario_names, lambda item: item.name)
    targets = select_uncertainty_targets(project, study)
    sample_count = (
        study.sample_count ** len(study.parameters)
        if study.sampling_mode is UncertaintySamplingMode.BOUNDED_SWEEP
        else study.sample_count
    )
    evaluation_count = sample_count * len(targets) * len(scenarios)
    if evaluation_count > study.maximum_evaluations:
        msg = (
            f"Study requires {evaluation_count} evaluations, exceeding maximum_evaluations={study.maximum_evaluations}."
        )
        raise ValueError(msg)
    samples = generate_study_samples(study)
    evaluations = tuple(
        evaluate_crossing_sample(crossing, scenario, sample, alternative=alternative)
        for crossing, alternative in targets
        for scenario in scenarios
        for sample in samples
    )
    return UncertaintyStudyResult(
        project=project,
        study=study,
        evaluations=evaluations,
        summaries=aggregate_study_evaluations(evaluations, study),
    )
