"""Convert imported hydraulic event targets into ordinary solve scenarios."""

from collections.abc import Sequence

from culvert_solver import TailwaterInput, solve_crossing_discharge_for_headwater

from ...classes.culvert.crossing import CrossingDefinition
from ...classes.culvert.event import EventDefinition
from ...classes.culvert.scenario import Scenario
from ...functions.culvert.adapter import build_solver_crossing


def materialize_event_scenarios(
    events: Sequence[EventDefinition],
    crossing: CrossingDefinition,
    *,
    default_tailwater: TailwaterInput,
) -> tuple[Scenario, ...]:
    """Resolve target-headwater rows and return normal forward-solve scenarios."""
    solver_crossing = build_solver_crossing(crossing)
    scenarios: list[Scenario] = []
    for event in events:
        tailwater: TailwaterInput = (
            default_tailwater if event.tailwater_elevation_m is None else event.tailwater_elevation_m
        )
        target = event.target_headwater_elevation_m
        discharge = (
            event.discharge_m3s
            if target is None
            else solve_crossing_discharge_for_headwater(
                crossing=solver_crossing,
                headwater_elevation=target,
                tailwater=tailwater,
            )
        )
        if discharge is None:
            msg = "Event target did not provide or resolve a discharge."
            raise RuntimeError(msg)
        scenarios.append(
            Scenario(
                name=event.scenario_name,
                discharge=discharge,
                tailwater=tailwater,
                aep_percent=event.aep_percent,
                source=event.source,
                notes=event.notes,
                target_headwater_elevation=target,
            )
        )
    return tuple(scenarios)
