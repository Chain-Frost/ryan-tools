"""Generate explicit size/quantity candidates without performing hydraulics."""

from dataclasses import replace

from ...classes.culvert.candidate import DesignCandidate
from ...classes.culvert.crossing import (
    CircularBarrelDefinition,
    CrossingDefinition,
    CulvertGroupDefinition,
    RectangularBarrelDefinition,
)


def _single_group(crossing: CrossingDefinition) -> CulvertGroupDefinition:
    if len(crossing.groups) != 1:
        raise ValueError("Automatic candidate generation currently requires a single-group crossing template.")
    return crossing.groups[0]


def generate_rectangular_candidates(
    template: CrossingDefinition,
    *,
    spans_mm: tuple[float, ...],
    rises_mm: tuple[float, ...],
    quantities: tuple[int, ...],
) -> tuple[DesignCandidate, ...]:
    """Generate the Cartesian product of explicit box sizes and quantities."""
    group = _single_group(template)
    if not isinstance(group.barrel, RectangularBarrelDefinition):
        raise ValueError("template must contain one rectangular barrel group.")
    if not spans_mm or not rises_mm or not quantities:
        raise ValueError("spans_mm, rises_mm, and quantities must all contain at least one value.")

    candidates: list[DesignCandidate] = []
    for span_mm in spans_mm:
        for rise_mm in rises_mm:
            for quantity in quantities:
                barrel = replace(group.barrel, span_mm=span_mm, rise_mm=rise_mm)
                candidate_group = replace(group, barrel=barrel, quantity=quantity)
                crossing = replace(template, groups=(candidate_group,))
                name = f"{quantity}x_{span_mm:g}x{rise_mm:g}_mm"
                candidates.append(DesignCandidate(name=name, crossing=crossing))
    return tuple(candidates)


def generate_circular_candidates(
    template: CrossingDefinition,
    *,
    diameters_mm: tuple[float, ...],
    quantities: tuple[int, ...],
) -> tuple[DesignCandidate, ...]:
    """Generate the Cartesian product of explicit pipe diameters and quantities."""
    group = _single_group(template)
    if not isinstance(group.barrel, CircularBarrelDefinition):
        raise ValueError("template must contain one circular barrel group.")
    if not diameters_mm or not quantities:
        raise ValueError("diameters_mm and quantities must both contain at least one value.")

    candidates: list[DesignCandidate] = []
    for diameter_mm in diameters_mm:
        for quantity in quantities:
            barrel = replace(group.barrel, diameter_mm=diameter_mm)
            candidate_group = replace(group, barrel=barrel, quantity=quantity)
            crossing = replace(template, groups=(candidate_group,))
            name = f"{quantity}x_{diameter_mm:g}_mm"
            candidates.append(DesignCandidate(name=name, crossing=crossing))
    return tuple(candidates)
