"""Reusable sourced floodway hydraulic-demand calculations."""

from math import isfinite, sqrt

GRAVITATIONAL_ACCELERATION = 9.80665
STANDARD_WATER_DENSITY = 1000.0
MRWA_FREE_FLOW_COEFFICIENT = 1.69
MRWA_FIGURE_4_6_MAX_DELTA_P_OVER_HEAD = 1.8

# Visual digitisation of MRWA Floodway Design Guide (2006), Figure 4.5.
# The source domain is deliberately retained and extrapolation is prohibited.
# Appendix D independently supports approximately 0.60 at H/l = 0.10 and
# 0.67-0.68 at H/l ~= 0.16-0.17.
MRWA_FIGURE_4_5_TRANSITION_POINTS: tuple[tuple[float, float], ...] = (
    (0.015, 0.00),
    (0.020, 0.16),
    (0.040, 0.35),
    (0.060, 0.48),
    (0.080, 0.56),
    (0.100, 0.60),
    (0.120, 0.64),
    (0.140, 0.66),
    (0.160, 0.67),
    (0.180, 0.685),
    (0.200, 0.69),
)


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


def mrwa_transition_submergence_ratio(head_to_flow_length: float) -> float:
    """Return Figure 4.5 ``(D/H)_trans`` by bounded linear interpolation.

    The ordinate data are visually digitised from Figure 4.5 of the 2006 MRWA
    Floodway Design Guide. Values outside the published graph domain are rejected
    rather than extrapolated.
    """
    ratio = _nonnegative(head_to_flow_length, "head_to_flow_length")
    lower_bound = MRWA_FIGURE_4_5_TRANSITION_POINTS[0][0]
    upper_bound = MRWA_FIGURE_4_5_TRANSITION_POINTS[-1][0]
    if not lower_bound <= ratio <= upper_bound:
        msg = (
            "head_to_flow_length is outside the digitised MRWA Figure 4.5 domain "
            f"[{lower_bound}, {upper_bound}]."
        )
        raise ValueError(msg)

    for (x0, y0), (x1, y1) in zip(
        MRWA_FIGURE_4_5_TRANSITION_POINTS,
        MRWA_FIGURE_4_5_TRANSITION_POINTS[1:],
        strict=False,
    ):
        if ratio <= x1:
            fraction = (ratio - x0) / (x1 - x0)
            return y0 + fraction * (y1 - y0)

    return MRWA_FIGURE_4_5_TRANSITION_POINTS[-1][1]


def mrwa_figure_4_6_k(
    delta_p_over_head: float,
    *,
    gravity: float = GRAVITATIONAL_ACCELERATION,
    free_flow_coefficient: float = MRWA_FREE_FLOW_COEFFICIENT,
) -> float:
    """Reconstruct MRWA Figure 4.6 ``K`` without graph digitisation.

    Figure 4.6 can be reproduced from the guide's own Equation 3 and Equation 6.
    Substitute ``q = C H**1.5`` and ``V = K sqrt(H)`` into the no-loss energy
    condition ``H + delta_p = V**2/(2g) + q/V``. The required Figure 4.6 value
    is the larger positive (supercritical/high-velocity) root.

    The published Figure 4.6 domain, ``0 <= delta_p/H <= 1.8``, is enforced.
    """
    ratio = _nonnegative(delta_p_over_head, "delta_p_over_head")
    if ratio > MRWA_FIGURE_4_6_MAX_DELTA_P_OVER_HEAD:
        msg = (
            "delta_p_over_head is outside the MRWA Figure 4.6 domain "
            f"[0.0, {MRWA_FIGURE_4_6_MAX_DELTA_P_OVER_HEAD}]."
        )
        raise ValueError(msg)

    g = _positive(gravity, "gravity")
    coefficient = _positive(free_flow_coefficient, "free_flow_coefficient")

    def residual(k_value: float) -> float:
        return k_value * k_value / (2.0 * g) + coefficient / k_value - (1.0 + ratio)

    # The residual is minimum at d(residual)/dK = 0. The Figure 4.6 branch is
    # the higher-velocity root to the right of this minimum.
    lower = (g * coefficient) ** (1.0 / 3.0)
    upper = sqrt(2.0 * g * (1.0 + ratio))
    if residual(lower) > 0.0 or residual(upper) < 0.0:
        msg = "MRWA Figure 4.6 reconstruction does not have a bounded high-velocity root."
        raise ValueError(msg)

    for _ in range(80):
        midpoint = 0.5 * (lower + upper)
        if residual(midpoint) > 0.0:
            upper = midpoint
        else:
            lower = midpoint
    return 0.5 * (lower + upper)


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
