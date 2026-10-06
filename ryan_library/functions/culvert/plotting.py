"""Plots derived exclusively from already-computed culvert workflow results."""

# pyright: reportUnknownMemberType=false

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from ...classes.culvert.results import CrossingRatingResult, ScenarioResult

if TYPE_CHECKING:
    from matplotlib.figure import Figure


def _notice_text(result: ScenarioResult) -> str:
    codes = (*result.warning_codes, *result.applicability_codes)
    return "No hydraulic warnings" if not codes else "Warnings/notices: " + ", ".join(codes)


def plot_longitudinal_profile(
    result: ScenarioResult,
    *,
    group_index: int = 0,
) -> Figure:
    """Plot solver-computed invert, crown, water surface and energy grade."""
    from matplotlib import pyplot as plt

    try:
        barrel_result = result.hydraulic_result.group_results[group_index].barrel_result
    except IndexError as exc:
        msg = f"group_index {group_index} is outside the computed group results."
        raise ValueError(msg) from exc
    profile = barrel_result.profile
    if profile is None or not profile.points:
        msg = "The selected computed result does not contain a longitudinal profile."
        raise ValueError(msg)

    stations = [point.station for point in profile.points]
    figure, axis = plt.subplots()
    axis.plot(stations, [point.invert_elevation for point in profile.points], label="Invert")
    axis.plot(stations, [point.crown_elevation for point in profile.points], label="Crown")
    axis.plot(stations, [point.water_surface_elevation for point in profile.points], label="Water surface")
    axis.plot(stations, [point.energy_grade_elevation for point in profile.points], label="Energy grade")
    axis.axhline(result.hydraulic_result.headwater_elevation, linestyle="--", label="Headwater")
    axis.axhline(result.hydraulic_result.tailwater_elevation, linestyle=":", label="Tailwater")
    if barrel_result.hydraulic_jump_station is not None:
        axis.axvline(barrel_result.hydraulic_jump_station, color="tab:red", linestyle="--", label="Hydraulic jump")
    axis.set(
        title=f"{result.crossing_name} — {result.scenario_name}",
        xlabel="Station from inlet (m)",
        ylabel="Elevation (m)",
    )
    axis.grid(visible=True, alpha=0.25)
    axis.legend()
    figure.text(0.01, 0.01, _notice_text(result), fontsize="small")
    figure.tight_layout(rect=(0, 0.04, 1, 1))
    return figure


def plot_rating_curve(result: CrossingRatingResult) -> Figure:
    """Plot computed headwater and outlet velocity against discharge."""
    from matplotlib import pyplot as plt

    points = result.rating_curve.points
    if not points:
        msg = "The computed rating result contains no points."
        raise ValueError(msg)
    discharges = [point.discharge for point in points]
    figure, headwater_axis = plt.subplots()
    velocity_axis = headwater_axis.twinx()
    headwater_axis.plot(
        discharges,
        [point.headwater_elevation for point in points],
        color="tab:blue",
        label="Headwater",
    )
    velocity_axis.plot(
        discharges,
        [point.outlet_velocity for point in points],
        color="tab:orange",
        label="Outlet velocity",
    )
    headwater_axis.set(
        title=f"{result.crossing_name} rating curve",
        xlabel="Discharge (m³/s)",
        ylabel="Headwater elevation (m)",
    )
    velocity_axis.set_ylabel("Outlet velocity (m/s)")
    headwater_axis.grid(visible=True, alpha=0.25)
    lines = [*headwater_axis.lines, *velocity_axis.lines]
    headwater_axis.legend(lines, [str(line.get_label()) for line in lines])
    warning_codes = tuple(dict.fromkeys(warning.code.value for point in points for warning in point.warnings))
    notice = "No hydraulic warnings" if not warning_codes else "Warnings: " + ", ".join(warning_codes)
    figure.text(0.01, 0.01, notice, fontsize="small")
    figure.tight_layout(rect=(0, 0.04, 1, 1))
    return figure


def save_figure(figure: Figure, path: Path) -> Path:
    """Save a plot to a caller-selected path and return that path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(target)
    return target
