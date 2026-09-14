"""FHWA HEC-23 Volume II DG5 overtopping-riprap calculations in SI units."""

from math import atan, cos, isfinite, radians, sin, sqrt, tan

from ...classes.floodway.protection import Hec23OvertoppingRiprapResult

GRAVITY = 9.80665
HEC23_SI_KU = 0.55
HEC23_SI_MANNING_STRICKLER = 0.0414


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


def hec23_overtopping_riprap_d50(
    *,
    unit_discharge: float,
    slope: float,
    uniformity_coefficient: float,
    specific_gravity: float,
    angle_of_repose_degrees: float,
    ku: float = HEC23_SI_KU,
) -> float:
    """Return HEC-23 DG5 Equation 5.2 median rock size ``d50`` in metres."""
    q = _positive(unit_discharge, "unit_discharge")
    s = _positive(slope, "slope")
    cu = _positive(uniformity_coefficient, "uniformity_coefficient")
    sg = _positive(specific_gravity, "specific_gravity")
    if sg <= 1.0:
        msg = "specific_gravity must be greater than 1.0."
        raise ValueError(msg)
    phi = radians(_positive(angle_of_repose_degrees, "angle_of_repose_degrees"))
    k = _positive(ku, "ku")
    alpha = atan(s)

    denominator_1 = sg * cos(alpha) - 1.0
    denominator_2 = cos(alpha) * tan(phi) - sin(alpha)
    if denominator_1 <= 0.0 or denominator_2 <= 0.0:
        msg = "Slope, specific gravity, and angle of repose produce an unsupported HEC-23 stability denominator."
        raise ValueError(msg)

    slope_stability = sin(alpha) / (denominator_1 * denominator_2)
    return (k * q**0.52 / (cu**0.25 * s**0.75)) * slope_stability**1.11


def hec23_interstitial_velocity(
    *,
    d50_m: float,
    slope: float,
    uniformity_coefficient: float,
    gravity: float = GRAVITY,
) -> float:
    """Return HEC-23 DG5 Equation 5.1 interstitial velocity in m/s."""
    d50 = _positive(d50_m, "d50_m")
    s = _positive(slope, "slope")
    cu = _positive(uniformity_coefficient, "uniformity_coefficient")
    g = _positive(gravity, "gravity")
    return 2.48 * sqrt(g * d50) * s**0.58 / cu**2.22


def hec23_allowable_surface_depth(
    *,
    d50_m: float,
    slope: float,
    specific_gravity: float,
    angle_of_repose_degrees: float,
) -> float:
    """Return HEC-23 DG5 Equation 5.3 allowable surface-flow depth in metres.

    Equation 5.3 is the mild-slope branch and is only used for slopes less than
    1V:4H (``S < 0.25``). Steeper slopes are designed for fully interstitial flow.
    """
    d50 = _positive(d50_m, "d50_m")
    s = _positive(slope, "slope")
    if s >= 0.25:
        msg = "HEC-23 Equation 5.3 applies only where slope is less than 0.25."
        raise ValueError(msg)
    sg = _positive(specific_gravity, "specific_gravity")
    if sg <= 1.0:
        msg = "specific_gravity must be greater than 1.0."
        raise ValueError(msg)
    phi = radians(_positive(angle_of_repose_degrees, "angle_of_repose_degrees"))
    return 0.06 * (sg - 1.0) * d50 * tan(phi) / (0.97 * s)


def hec23_manning_roughness(d50_m: float) -> float:
    """Return the SI Manning-Strickler roughness used in the DG5 worked example."""
    d50 = _positive(d50_m, "d50_m")
    return HEC23_SI_MANNING_STRICKLER * d50 ** (1.0 / 6.0)


def hec23_surface_unit_discharge(*, depth_m: float, slope: float, roughness: float) -> float:
    """Return unit discharge over riprap using the SI Manning relationship in DG5."""
    depth = _positive(depth_m, "depth_m")
    s = _positive(slope, "slope")
    n = _positive(roughness, "roughness")
    return depth ** (5.0 / 3.0) * sqrt(s) / n


def evaluate_hec23_overtopping_riprap(
    *,
    unit_discharge: float,
    slope: float,
    uniformity_coefficient: float,
    porosity: float,
    specific_gravity: float,
    angle_of_repose_degrees: float,
) -> Hec23OvertoppingRiprapResult:
    """Evaluate the HEC-23 DG5 riprap sizing/layer-capacity procedure.

    The function sizes the theoretical ``d50`` with Equation 5.2 and evaluates
    interstitial-flow capacity. It deliberately does not select an FHWA standard
    riprap gradation class; where 4 ``d50`` cannot carry the required interstitial
    flow, ``requires_larger_gradation`` is returned instead of inventing a class.
    """
    q = _positive(unit_discharge, "unit_discharge")
    s = _positive(slope, "slope")
    eta = _positive(porosity, "porosity")
    if eta >= 1.0:
        msg = "porosity must be less than 1.0."
        raise ValueError(msg)

    d50 = hec23_overtopping_riprap_d50(
        unit_discharge=q,
        slope=s,
        uniformity_coefficient=uniformity_coefficient,
        specific_gravity=specific_gravity,
        angle_of_repose_degrees=angle_of_repose_degrees,
    )
    interstitial_velocity = hec23_interstitial_velocity(
        d50_m=d50,
        slope=s,
        uniformity_coefficient=uniformity_coefficient,
    )
    average_velocity = eta * interstitial_velocity
    all_flow_depth = q / average_velocity
    minimum_thickness = 2.0 * d50

    if all_flow_depth <= minimum_thickness:
        allowable_surface_depth = None
        surface_discharge = 0.0
        required_interstitial_discharge = q
        required_thickness = all_flow_depth
    elif s < 0.25:
        allowable_surface_depth = hec23_allowable_surface_depth(
            d50_m=d50,
            slope=s,
            specific_gravity=specific_gravity,
            angle_of_repose_degrees=angle_of_repose_degrees,
        )
        roughness = hec23_manning_roughness(d50)
        surface_discharge = min(
            q,
            hec23_surface_unit_discharge(
                depth_m=allowable_surface_depth,
                slope=s,
                roughness=roughness,
            ),
        )
        required_interstitial_discharge = max(0.0, q - surface_discharge)
        required_thickness = required_interstitial_discharge / average_velocity
    else:
        allowable_surface_depth = None
        surface_discharge = 0.0
        required_interstitial_discharge = q
        required_thickness = all_flow_depth

    two_d50_capacity = minimum_thickness * average_velocity
    four_d50_capacity = 4.0 * d50 * average_velocity
    two_d50_sufficient = two_d50_capacity >= required_interstitial_discharge
    four_d50_sufficient = four_d50_capacity >= required_interstitial_discharge

    return Hec23OvertoppingRiprapResult(
        unit_discharge=q,
        slope=s,
        d50_m=d50,
        interstitial_velocity_ms=interstitial_velocity,
        average_interstitial_velocity_ms=average_velocity,
        all_flow_interstitial_depth_m=all_flow_depth,
        minimum_two_d50_thickness_m=minimum_thickness,
        allowable_surface_depth_m=allowable_surface_depth,
        surface_unit_discharge_m2s=surface_discharge,
        required_interstitial_unit_discharge_m2s=required_interstitial_discharge,
        required_interstitial_thickness_m=required_thickness,
        two_d50_sufficient=two_d50_sufficient,
        four_d50_sufficient=four_d50_sufficient,
        requires_larger_gradation=not four_d50_sufficient,
    )
