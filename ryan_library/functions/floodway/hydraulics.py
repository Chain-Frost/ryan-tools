"""Reusable sourced floodway hydraulic-demand calculations."""

from math import isfinite, sqrt

GRAVITATIONAL_ACCELERATION = 9.80665
STANDARD_WATER_DENSITY = 1000.0


def _finite(value: float, name: str) -> float:
    result = float(value)
    if not isfinite(result):
        msg = f"{name} must be finite."
        raise ValueError(msg)
    return result


def _positive(value: float, name: str) -> float:
    result = _finite(value, name)
    if result <= 0.0:
        msg = f"{name} must be strictly positive."
        raise ValueError(msg)
    return result


def _nonnegative(value: float, name: str) -> float:
    result = _finite(value, name)
    if result < 0.0:
        msg = f"{name} must be non-negative."
        raise ValueError(msg)
    return result


def mrwa_steady_state_velocity(unit_discharge: float, slope: float, roughness: float) -> float:
    """Return MRWA 2006 Equation 4 steady-state velocity in metres per second.

    The adopted form is ``[(1/n) * q**(2/3) * S**(1/2)]**(3/5)`` where
    ``q`` is unit discharge in m2/s, ``S`` is slope in m/m, and ``n`` is
    Manning roughness. A zero unit discharge or zero slope returns zero.
    """
    q = _nonnegative(unit_discharge, "unit_discharge")
    s = _nonnegative(slope, "slope")
    n = _positive(roughness, "roughness")
    if q == 0.0 or s == 0.0:
        return 0.0
    return ((1.0 / n) * q ** (2.0 / 3.0) * sqrt(s)) ** (3.0 / 5.0)


def mrwa_specific_energy(unit_discharge: float, velocity: float, *, gravity: float = GRAVITATIONAL_ACCELERATION) -> float:
    """Return MRWA 2006 Equation 6 specific-energy quantity in metres."""
    q = _nonnegative(unit_discharge, "unit_discharge")
    v = _nonnegative(velocity, "velocity")
    g = _positive(gravity, "gravity")
    if q > 0.0 and v == 0.0:
        msg = "velocity must be positive when unit_discharge is positive."
        raise ValueError(msg)
    if v == 0.0:
        return 0.0
    return v * v / (2.0 * g) + q / v


def mrwa_maximum_attainable_velocity(total_head: float, coefficient_k: float) -> float:
    """Return MRWA 2006 Equation 7 maximum attainable velocity in m/s."""
    head = _nonnegative(total_head, "total_head")
    k = _nonnegative(coefficient_k, "coefficient_k")
    return k * sqrt(head)


def governing_velocity(steady_state_velocity: float, maximum_attainable_velocity: float) -> float:
    """Return the lower physically available velocity for the MRWA event check."""
    steady = _nonnegative(steady_state_velocity, "steady_state_velocity")
    maximum = _nonnegative(maximum_attainable_velocity, "maximum_attainable_velocity")
    return min(steady, maximum)


def rectangular_critical_depth(unit_discharge: float, *, gravity: float = GRAVITATIONAL_ACCELERATION) -> float:
    """Return critical depth for rectangular unit discharge in metres.

    This is a diagnostic primitive only. Calling workflows remain responsible for
    deciding whether critical-depth interpretation is physically applicable to the
    roadway/floodway state being assessed.
    """
    q = _nonnegative(unit_discharge, "unit_discharge")
    g = _positive(gravity, "gravity")
    if q == 0.0:
        return 0.0
    return (q * q / g) ** (1.0 / 3.0)


def froude_number_rectangular(
    velocity: float,
    depth: float,
    *,
    gravity: float = GRAVITATIONAL_ACCELERATION,
) -> float:
    """Return rectangular-channel Froude number for a supported local state."""
    v = _nonnegative(velocity, "velocity")
    y = _positive(depth, "depth")
    g = _positive(gravity, "gravity")
    return v / sqrt(g * y)


def dynamic_pressure(velocity: float, *, density: float = STANDARD_WATER_DENSITY) -> float:
    """Return ``0.5 rho V^2`` dynamic pressure in pascals."""
    v = _nonnegative(velocity, "velocity")
    rho = _positive(density, "density")
    return 0.5 * rho * v * v


def momentum_flux_per_width(
    unit_discharge: float,
    velocity: float,
    *,
    density: float = STANDARD_WATER_DENSITY,
) -> float:
    """Return ``rho q V`` momentum flux per unit width in N/m."""
    q = _nonnegative(unit_discharge, "unit_discharge")
    v = _nonnegative(velocity, "velocity")
    rho = _positive(density, "density")
    return rho * q * v
