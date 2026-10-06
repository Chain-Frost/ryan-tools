"""Generate source-traceable discharge sweeps for floodway governing-state assessment."""

from collections.abc import Callable
from dataclasses import replace

from ...classes.culvert import CrossingDefinition, Scenario, ScenarioResult
from ...classes.floodway import FloodwayFormation, FloodwayScenarioAssessment
from ..culvert.solve import solve_crossing_scenario
from .assess import assess_floodway_scenario

_DEFAULT_ACTIVITY_TOLERANCE = 1e-9
_DEFAULT_BISECTION_ITERATIONS = 60
_DEFAULT_TRANSITION_SCAN_INTERVALS = 128


def _scenario_at_discharge(base_scenario: Scenario, discharge: float, *, name: str, note: str) -> Scenario:
    """Return a peak-flow scenario at a new discharge while retaining its downstream boundary."""
    source = base_scenario.source
    sweep_source = (
        f"{source}; floodway discharge sweep from {base_scenario.name}"
        if source
        else f"Floodway discharge sweep from {base_scenario.name}"
    )
    return replace(
        base_scenario,
        name=name,
        discharge=discharge,
        aep_percent=None,
        source=sweep_source,
        notes=note,
        target_headwater_elevation=None,
    )


def _roadway_active(result: ScenarioResult, *, tolerance: float) -> bool:
    return result.hydraulic_result.roadway_discharge > tolerance


def _roadway_submerged(result: ScenarioResult) -> bool:
    roadway_result = result.hydraulic_result.roadway_result
    if roadway_result is None:
        return False
    return any(segment.flow_state.value == "supported_submerged" for segment in roadway_result.segment_results)


def _validate_threshold_search(
    maximum_discharge: float,
    tolerance: float,
    minimum_discharge: float | None,
    scan_intervals: int | None,
) -> None:
    """Validate discharge bounds and optional interior scan resolution."""
    if maximum_discharge <= 0.0:
        msg = "maximum_discharge must be strictly positive."
        raise ValueError(msg)
    if tolerance <= 0.0:
        msg = "tolerance must be strictly positive."
        raise ValueError(msg)
    if minimum_discharge is not None and not 0.0 < minimum_discharge <= maximum_discharge:
        msg = "minimum_discharge must be strictly positive and no greater than maximum_discharge."
        raise ValueError(msg)
    if scan_intervals is not None and scan_intervals < 1:
        msg = "scan_intervals must be at least 1 when supplied."
        raise ValueError(msg)


def _find_first_condition_discharge(
    crossing: CrossingDefinition,
    base_scenario: Scenario,
    *,
    maximum_discharge: float,
    condition: Callable[[ScenarioResult], bool],
    tolerance: float,
    minimum_discharge: float | None = None,
    scan_intervals: int | None = None,
) -> float | None:
    _validate_threshold_search(maximum_discharge, tolerance, minimum_discharge, scan_intervals)

    lower = minimum_discharge if minimum_discharge is not None else max(min(maximum_discharge * 1e-6, 1e-4), 1e-9)
    lower_result = solve_crossing_scenario(
        crossing,
        _scenario_at_discharge(
            base_scenario,
            lower,
            name=f"{base_scenario.name} threshold lower",
            note="Internal floodway threshold search point.",
        ),
    )
    if condition(lower_result):
        return lower

    if scan_intervals is None:
        upper = maximum_discharge
        upper_result = solve_crossing_scenario(
            crossing,
            _scenario_at_discharge(
                base_scenario,
                upper,
                name=f"{base_scenario.name} threshold upper",
                note="Internal floodway threshold search point.",
            ),
        )
        if not condition(upper_result):
            return None
    else:
        previous = lower
        upper = lower
        for index in range(1, scan_intervals + 1):
            fraction = index / scan_intervals
            probe = lower + (maximum_discharge - lower) * fraction
            probe_result = solve_crossing_scenario(
                crossing,
                _scenario_at_discharge(
                    base_scenario,
                    probe,
                    name=f"{base_scenario.name} threshold scan",
                    note="Internal floodway threshold scan point.",
                ),
            )
            if condition(probe_result):
                lower = previous
                upper = probe
                break
            previous = probe
        else:
            return None

    for _ in range(_DEFAULT_BISECTION_ITERATIONS):
        midpoint = 0.5 * (lower + upper)
        result = solve_crossing_scenario(
            crossing,
            _scenario_at_discharge(
                base_scenario,
                midpoint,
                name=f"{base_scenario.name} threshold search",
                note="Internal floodway threshold search point.",
            ),
        )
        if condition(result):
            upper = midpoint
        else:
            lower = midpoint
        if upper - lower <= tolerance:
            break
    return upper


def find_roadway_overtopping_onset(
    crossing: CrossingDefinition,
    base_scenario: Scenario,
    *,
    maximum_discharge: float | None = None,
    discharge_tolerance: float = 1e-6,
    roadway_discharge_tolerance: float = _DEFAULT_ACTIVITY_TOLERANCE,
) -> float | None:
    """Return the first discharge with active roadway overtopping, if bracketed."""
    upper = base_scenario.discharge if maximum_discharge is None else maximum_discharge
    return _find_first_condition_discharge(
        crossing,
        base_scenario,
        maximum_discharge=upper,
        condition=lambda result: _roadway_active(result, tolerance=roadway_discharge_tolerance),
        tolerance=discharge_tolerance,
    )


def find_roadway_submergence_onset(
    crossing: CrossingDefinition,
    base_scenario: Scenario,
    *,
    maximum_discharge: float | None = None,
    discharge_tolerance: float = 1e-6,
) -> float | None:
    """Return the first discharge with a solver-supported submerged roadway segment, if bracketed."""
    upper = base_scenario.discharge if maximum_discharge is None else maximum_discharge
    overtopping_onset = find_roadway_overtopping_onset(
        crossing,
        base_scenario,
        maximum_discharge=upper,
        discharge_tolerance=discharge_tolerance,
    )
    if overtopping_onset is None:
        return None
    return _find_first_condition_discharge(
        crossing,
        base_scenario,
        maximum_discharge=upper,
        minimum_discharge=overtopping_onset,
        scan_intervals=_DEFAULT_TRANSITION_SCAN_INTERVALS,
        condition=_roadway_submerged,
        tolerance=discharge_tolerance,
    )


def _sample_discharges(start: float, stop: float, points: int) -> tuple[float, ...]:
    if points < 2:
        msg = "points must be at least 2."
        raise ValueError(msg)
    if not 0.0 < start <= stop:
        msg = "Discharge sweep requires 0 < start <= stop."
        raise ValueError(msg)
    if start == stop:
        return (start,)
    step = (stop - start) / (points - 1)
    return tuple(start + step * index for index in range(points))


def assess_floodway_discharge_sweep(
    crossing: CrossingDefinition,
    base_scenario: Scenario,
    formation: FloodwayFormation,
    *,
    points: int = 21,
    minimum_discharge: float | None = None,
    maximum_discharge: float | None = None,
    include_transition_searches: bool = True,
) -> tuple[FloodwayScenarioAssessment, ...]:
    """Assess a configurable interior discharge sweep using one scenario boundary condition.

    This is a peak-hydraulic state sweep, not hydrograph processing. Intermediate
    points have no AEP assigned and retain explicit provenance that the downstream
    boundary condition comes from the selected base scenario.
    """
    upper = base_scenario.discharge if maximum_discharge is None else maximum_discharge
    if upper <= 0.0:
        msg = "maximum_discharge must be strictly positive."
        raise ValueError(msg)

    onset = (
        find_roadway_overtopping_onset(
            crossing,
            base_scenario,
            maximum_discharge=upper,
        )
        if include_transition_searches
        else None
    )
    submergence = (
        find_roadway_submergence_onset(
            crossing,
            base_scenario,
            maximum_discharge=upper,
        )
        if include_transition_searches
        else None
    )

    if minimum_discharge is not None:
        start = minimum_discharge
    elif onset is not None:
        start = onset
    else:
        start = max(upper * 0.01, 1e-9)
    if not 0.0 < start <= upper:
        msg = "minimum_discharge must be strictly positive and no greater than maximum_discharge."
        raise ValueError(msg)

    discharges = set(_sample_discharges(start, upper, points))
    if onset is not None and start <= onset <= upper:
        discharges.add(onset)
        shallow = min(upper, onset + max(upper * 1e-4, 1e-6))
        if start <= shallow <= upper:
            discharges.add(shallow)
    if submergence is not None and start <= submergence <= upper:
        discharges.add(submergence)

    note = (
        f"Interior floodway discharge sweep using the downstream boundary from {base_scenario.name!r}. "
        "No AEP is assigned to interpolated discharge states; this is not hydrograph or duration analysis."
    )
    assessments: list[FloodwayScenarioAssessment] = []
    for index, discharge in enumerate(sorted(discharges), start=1):
        scenario = _scenario_at_discharge(
            base_scenario,
            discharge,
            name=f"{base_scenario.name} sweep {index:02d} Q={discharge:.6g}",
            note=note,
        )
        result = solve_crossing_scenario(crossing, scenario)
        assessments.append(assess_floodway_scenario(result, formation))
    return tuple(assessments)
