"""Expose the authoritative crossing rating-curve capability."""

from collections.abc import Sequence

from culvert_solver import TailwaterInput, generate_crossing_rating_curve

from ...classes.culvert.crossing import CrossingDefinition
from ...classes.culvert.results import CrossingRatingResult
from ...functions.culvert.adapter import build_solver_crossing


def generate_crossing_rating(
    crossing: CrossingDefinition,
    discharges: Sequence[float],
    tailwater: TailwaterInput,
) -> CrossingRatingResult:
    """Generate a named crossing rating curve without reimplementing hydraulics."""
    rating_curve = generate_crossing_rating_curve(
        crossing=build_solver_crossing(crossing),
        discharges=discharges,
        tailwater=tailwater,
    )
    return CrossingRatingResult(crossing_name=crossing.name, rating_curve=rating_curve)
