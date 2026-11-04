"""Generate stage-discharge plots and supporting selection summaries.

The script:
1. Selects the _PO.csv duration with the greatest water level for each reference row and case.
2. Smooths flow, water level, and depth with a centered moving average.
3. Aligns flow and water-level peaks by shifting the flow timestamps and interpolating flow onto the water-level clock.
4. Builds the greater-water-level envelope from the rising and falling limbs.
5. Finds the maximum flow across matching _POMM.csv durations for every AEP and case.
6. Draws case-specific AEP peak-flow callouts, track and formation levels, and an aligned secondary depth scale.

By default, separate Base and Sensitivity curves are generated. Case names are matched against the
final component before ``_PO.csv`` or ``_POMM.csv`` in each result filename.

Dependencies: numpy, pandas, matplotlib
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

import matplotlib as mpl

mpl.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

SCRIPT_VERSION = "2026-10-29.8"
DEFAULT_CASES = ("Base", "Sensitivity")
DEFAULT_MOVING_AVERAGE_MINUTES = 10.0
DEFAULT_TRACK_LEVEL = 347.75
DEFAULT_FORMATION_LEVEL = 346.65

DEFAULT_RESULTS_ROOT = Path(
    r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO"
    # r"\TUFLOW_MLGD\results\v06\FG01"
    r"\TUFLOW_MLGD\results\v12\FG02"
)
DEFAULT_OUTPUT_DIR = Path(
    r"P:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO"
    r"\4 ENGINEERING\11 HYDROLOGY\Stage Discharge Curve"
)
DEFAULT_REFERENCE = DEFAULT_OUTPUT_DIR / "Fortescue river reference.csv"


@dataclass(frozen=True)
class ReferenceRow:
    identifier: str
    flow_line: str
    water_level_point: str


@dataclass(frozen=True)
class PeakFlow:
    aep_folder: str
    aep_label: str
    value: float
    source: Path


@dataclass(frozen=True)
class CurveSelection:
    source: Path
    maximum_water_level: float
    time_at_maximum: float
    flow_at_maximum: float
    flow_time_offset_hours: float
    moving_average_samples: int
    paired_data_points: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=f"%(prog)s {SCRIPT_VERSION}")
    parser.add_argument(
        "--results-root",
        type=Path,
        default=DEFAULT_RESULTS_ROOT,
        help="Results folder containing AEP subfolders (default: configured project path).",
    )
    parser.add_argument(
        "--curve-aep",
        default="01.0p",
        help="AEP folder whose _PO.csv files define the stage-discharge curve (default: 01.0p).",
    )
    parser.add_argument(
        "--reference",
        type=Path,
        default=DEFAULT_REFERENCE,
        help="CSV containing ID, Flow Line, and Water Level Point columns.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Folder for plots and summary CSVs.",
    )
    parser.add_argument(
        "--cases",
        nargs="+",
        default=list(DEFAULT_CASES),
        help="Result cases to plot separately (default: Base Sensitivity).",
    )
    parser.add_argument(
        "--moving-average-minutes",
        type=float,
        default=DEFAULT_MOVING_AVERAGE_MINUTES,
        help="Centered moving-average window in minutes; use 0 to disable (default: 10).",
    )
    parser.add_argument(
        "--track-level",
        type=float,
        default=DEFAULT_TRACK_LEVEL,
        help="Track level to plot in mAHD (default: 347.75).",
    )
    parser.add_argument(
        "--formation-level",
        type=float,
        default=DEFAULT_FORMATION_LEVEL,
        help="Formation level to plot in mAHD (default: 346.65).",
    )
    return parser.parse_args()


def read_reference(path: Path) -> list[ReferenceRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    required = {"ID", "Flow Line", "Water Level Point"}
    if not rows or not required.issubset(rows[0]):
        msg = f"{path} is empty or lacks these columns: {sorted(required)}"
        raise RuntimeError(msg)

    references = [
        ReferenceRow(
            identifier=row["ID"].strip(),
            flow_line=row["Flow Line"].strip(),
            water_level_point=row["Water Level Point"].strip(),
        )
        for row in rows
    ]
    if len({row.identifier for row in references}) != len(references):
        msg = "Reference IDs must be unique"
        raise RuntimeError(msg)
    return references


def read_two_row_header(path: Path) -> tuple[list[str], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        result_types = next(reader)
        locations = next(reader)
    if len(result_types) != len(locations):
        msg = f"Header width mismatch in {path}"
        raise RuntimeError(msg)
    return result_types, locations


def build_column_lookup(result_types: list[str], locations: list[str]) -> dict[tuple[str, str], list[int]]:
    lookup: dict[tuple[str, str], list[int]] = {}
    for index, (result_type, location) in enumerate(zip(result_types, locations, strict=True)):
        lookup.setdefault((result_type.strip(), location.strip()), []).append(index)
    return lookup


def require_column(lookup: dict[tuple[str, str], list[int]], result_type: str, location: str, path: Path) -> int:
    matches = lookup.get((result_type, location), [])
    if len(matches) != 1:
        msg = f"{path.name}: expected one {result_type!r}/{location!r} column, found {len(matches)}"
        raise RuntimeError(msg)
    return matches[0]


def calculate_flow_time_offset(time: np.ndarray, flow: np.ndarray, water_level: np.ndarray) -> float:
    """Return the offset added to flow timestamps so the flow and water-level peaks coincide."""
    flow_peak_time = float(time[int(np.argmax(flow))])
    water_level_peak_time = float(time[int(np.argmax(water_level))])
    return water_level_peak_time - flow_peak_time


def shift_flow_to_water_level_time(
    time: np.ndarray,
    flow: np.ndarray,
    flow_time_offset_hours: float,
) -> np.ndarray:
    """Interpolate shifted flow values onto the unchanged water-level timestamps."""
    if len(time) < 2 or np.any(np.diff(time) <= 0.0):
        msg = "Time values must be strictly increasing to align the flow series"
        raise RuntimeError(msg)
    shifted_flow_time = time + flow_time_offset_hours
    return np.interp(time, shifted_flow_time, flow, left=np.nan, right=np.nan)


def moving_average_sample_count(time: np.ndarray, window_minutes: float) -> int:
    """Return an odd sample count spanning approximately the requested duration."""
    if window_minutes <= 0.0 or len(time) < 2:
        return 1
    time_step_hours = float(np.median(np.diff(time)))
    if time_step_hours <= 0.0:
        msg = "Time values must be strictly increasing to smooth the time series"
        raise RuntimeError(msg)
    radius = max(1, round((window_minutes / 60.0) / (2.0 * time_step_hours)))
    return 2 * radius + 1


def centered_moving_average(values: np.ndarray, window_samples: int) -> np.ndarray:
    """Smooth values without shifting their timestamps."""
    if window_samples <= 1:
        return values.copy()
    return pd.Series(values).rolling(window=window_samples, center=True, min_periods=1).mean().to_numpy(dtype=float)


def select_curve_files(
    curve_folder: Path,
    references: list[ReferenceRow],
    case: str,
    moving_average_minutes: float,
) -> dict[str, CurveSelection]:
    expected_suffix = f"_{case}_PO.csv".casefold()
    po_files = sorted(path for path in curve_folder.glob("*_PO.csv") if path.name.casefold().endswith(expected_suffix))
    if not po_files:
        msg = f"No {case!r} *_PO.csv files found in {curve_folder}"
        raise RuntimeError(msg)

    best: dict[str, CurveSelection] = {}
    for po_file in po_files:
        result_types, locations = read_two_row_header(po_file)
        lookup = build_column_lookup(result_types, locations)
        indices = {1}
        columns: dict[str, tuple[int, int]] = {}
        for reference in references:
            flow_index = require_column(lookup, "Flow", reference.flow_line, po_file)
            wl_index = require_column(lookup, "Water Level", reference.water_level_point, po_file)
            columns[reference.identifier] = (flow_index, wl_index)
            indices.update((flow_index, wl_index))

        frame = pd.read_csv(po_file, skiprows=2, header=None, usecols=sorted(indices), low_memory=False)
        time = pd.to_numeric(frame[1], errors="coerce").to_numpy(dtype=float)

        for reference in references:
            flow_index, wl_index = columns[reference.identifier]
            flow = pd.to_numeric(frame[flow_index], errors="coerce").to_numpy(dtype=float)
            water_level = pd.to_numeric(frame[wl_index], errors="coerce").to_numpy(dtype=float)
            valid = np.isfinite(time) & np.isfinite(flow) & np.isfinite(water_level)
            if not np.any(valid):
                msg = f"{po_file.name}: no paired data for {reference.identifier}"
                raise RuntimeError(msg)

            valid_time = time[valid]
            valid_flow = flow[valid]
            valid_wl = water_level[valid]
            moving_average_samples = moving_average_sample_count(valid_time, moving_average_minutes)
            valid_flow = centered_moving_average(valid_flow, moving_average_samples)
            valid_wl = centered_moving_average(valid_wl, moving_average_samples)
            flow_time_offset_hours = calculate_flow_time_offset(valid_time, valid_flow, valid_wl)
            aligned_flow = shift_flow_to_water_level_time(valid_time, valid_flow, flow_time_offset_hours)
            aligned = np.isfinite(aligned_flow)
            maximum_index = int(np.argmax(valid_wl))
            selection = CurveSelection(
                source=po_file,
                maximum_water_level=float(valid_wl[maximum_index]),
                time_at_maximum=float(valid_time[maximum_index]),
                flow_at_maximum=float(aligned_flow[maximum_index]),
                flow_time_offset_hours=flow_time_offset_hours,
                moving_average_samples=moving_average_samples,
                paired_data_points=int(np.count_nonzero(aligned)),
            )
            previous = best.get(reference.identifier)
            if previous is None or selection.maximum_water_level > previous.maximum_water_level:
                best[reference.identifier] = selection

    return best


def aep_sort_key(path: Path) -> float:
    return float(path.name.removesuffix("p"))


def aep_label(folder_name: str) -> str:
    return f"{float(folder_name.removesuffix('p')):g}%"


def select_peak_flows(
    results_root: Path,
    references: list[ReferenceRow],
    case: str,
) -> dict[str, list[PeakFlow]]:
    expected_suffix = f"_{case}_POMM.csv".casefold()
    files_by_folder = {
        folder: sorted(path for path in folder.glob("*_POMM.csv") if path.name.casefold().endswith(expected_suffix))
        for folder in results_root.iterdir()
        if folder.is_dir()
    }
    files_by_folder = {folder: files for folder, files in files_by_folder.items() if files}
    aep_folders = sorted(files_by_folder, key=aep_sort_key)
    if not aep_folders:
        msg = f"No AEP folders containing {case!r} *_POMM.csv files found in {results_root}"
        raise RuntimeError(msg)

    selected: dict[str, list[PeakFlow]] = {reference.identifier: [] for reference in references}
    for folder in aep_folders:
        pomm_files = files_by_folder[folder]
        best: dict[str, tuple[float, Path]] = {
            reference.identifier: (float("-inf"), pomm_files[0]) for reference in references
        }

        for pomm_file in pomm_files:
            with pomm_file.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                result_types = next(reader)
                locations = next(reader)
                maximum_row = next(reader)
            if len(maximum_row) < 2 or "Maximum" not in maximum_row[1]:
                msg = f"Maximum row not found in {pomm_file}"
                raise RuntimeError(msg)
            lookup = build_column_lookup(result_types, locations)

            for reference in references:
                index = require_column(lookup, "Flow", reference.flow_line, pomm_file)
                value = float(maximum_row[index])
                if value > best[reference.identifier][0]:
                    best[reference.identifier] = (value, pomm_file)

        for reference in references:
            value, source = best[reference.identifier]
            selected[reference.identifier].append(
                PeakFlow(
                    aep_folder=folder.name,
                    aep_label=aep_label(folder.name),
                    value=value,
                    source=source,
                )
            )

    return selected


def branch_by_flow(flow: np.ndarray, water_level: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    order = np.argsort(flow)
    sorted_flow = flow[order]
    sorted_wl = water_level[order]
    unique_flow, inverse = np.unique(sorted_flow, return_inverse=True)
    maximum_wl = np.full(len(unique_flow), -np.inf)
    np.maximum.at(maximum_wl, inverse, sorted_wl)
    return unique_flow, maximum_wl


def load_curve_data(
    source: Path,
    reference: ReferenceRow,
    flow_time_offset_hours: float,
    moving_average_minutes: float,
) -> tuple[np.ndarray, np.ndarray, float, dict[str, float | int]]:
    result_types, locations = read_two_row_header(source)
    lookup = build_column_lookup(result_types, locations)
    flow_index = require_column(lookup, "Flow", reference.flow_line, source)
    wl_index = require_column(lookup, "Water Level", reference.water_level_point, source)
    depth_index = require_column(lookup, "Depth", reference.water_level_point, source)

    frame = pd.read_csv(
        source,
        skiprows=2,
        header=None,
        usecols=sorted({1, flow_index, wl_index, depth_index}),
        low_memory=False,
    )
    time = pd.to_numeric(frame[1], errors="coerce").to_numpy(dtype=float)
    flow = pd.to_numeric(frame[flow_index], errors="coerce").to_numpy(dtype=float)
    water_level = pd.to_numeric(frame[wl_index], errors="coerce").to_numpy(dtype=float)
    depth = pd.to_numeric(frame[depth_index], errors="coerce").to_numpy(dtype=float)
    valid = np.isfinite(time) & np.isfinite(flow) & np.isfinite(water_level) & np.isfinite(depth)
    time, flow, water_level, depth = time[valid], flow[valid], water_level[valid], depth[valid]
    if not len(flow):
        msg = f"No valid flow/water-level/depth rows in {source}"
        raise RuntimeError(msg)
    has_zero_origin = bool(np.any(np.isclose(flow, 0.0) & np.isclose(depth, 0.0)))

    moving_average_samples = moving_average_sample_count(time, moving_average_minutes)
    flow = centered_moving_average(flow, moving_average_samples)
    water_level = centered_moving_average(water_level, moving_average_samples)
    depth = centered_moving_average(depth, moving_average_samples)

    flow = shift_flow_to_water_level_time(time, flow, flow_time_offset_hours)
    aligned = np.isfinite(flow)
    flow, water_level, depth = flow[aligned], water_level[aligned], depth[aligned]

    point_datum_values = water_level - depth
    point_datum = float(np.median(point_datum_values))
    datum_spread = float(np.max(point_datum_values) - np.min(point_datum_values))

    peak_index = int(np.argmax(flow))
    rise_flow, rise_wl = branch_by_flow(flow[: peak_index + 1], water_level[: peak_index + 1])
    fall_flow, fall_wl = branch_by_flow(flow[peak_index:], water_level[peak_index:])
    grid = np.unique(np.concatenate((rise_flow, fall_flow)))
    rise_available = (grid >= rise_flow.min()) & (grid <= rise_flow.max())
    fall_available = (grid >= fall_flow.min()) & (grid <= fall_flow.max())
    rise_wl_grid = np.full(len(grid), np.nan)
    fall_wl_grid = np.full(len(grid), np.nan)
    rise_wl_grid[rise_available] = np.interp(grid[rise_available], rise_flow, rise_wl)
    fall_wl_grid[fall_available] = np.interp(grid[fall_available], fall_flow, fall_wl)

    both_available = rise_available & fall_available
    use_falling = fall_available & (~rise_available | (fall_wl_grid > rise_wl_grid))
    envelope_wl = np.where(use_falling, fall_wl_grid, rise_wl_grid)
    difference = fall_wl_grid[both_available] - rise_wl_grid[both_available]

    if has_zero_origin:
        positive_flow = grid > 0.0
        grid = np.concatenate((np.array([0.0]), grid[positive_flow]))
        envelope_wl = np.concatenate((np.array([point_datum]), envelope_wl[positive_flow]))
        use_falling = np.concatenate((np.array([False]), use_falling[positive_flow]))

    comparison: dict[str, float | int] = {
        "envelope_points": len(grid),
        "falling_points": int(np.count_nonzero(use_falling)),
        "rising_points": int(np.count_nonzero(~use_falling)),
        "maximum_falling_excess": max(0.0, float(np.max(difference))) if len(difference) else 0.0,
        "maximum_rising_excess": max(0.0, float(np.max(-difference))) if len(difference) else 0.0,
        "datum_spread": datum_spread,
    }
    return grid, envelope_wl, point_datum, comparison


def plot_curve(
    output_path: Path,
    reference: ReferenceRow,
    flow: np.ndarray,
    water_level: np.ndarray,
    point_datum: float,
    peak_flows: list[PeakFlow],
    track_level: float,
    formation_level: float,
) -> None:
    # Matplotlib leaves plotting **kwargs untyped; ignore only that upstream member diagnostic.
    fig, ax = plt.subplots(figsize=(9.0, 6.0))
    fig.subplots_adjust(left=0.10, right=0.90, bottom=0.12, top=0.80)
    depth_axis = ax.twinx()  # pyright: ignore[reportUnknownMemberType]
    ax.plot(  # pyright: ignore[reportUnknownMemberType]
        flow,
        water_level,
        color="#005A9C",
        linewidth=1.4,
        zorder=3,
    )
    ax.axhline(track_level, color="#F57C00", linestyle="--", linewidth=1.4, zorder=2)  # pyright: ignore[reportUnknownMemberType]
    ax.axhline(formation_level, color="#6A1B9A", linestyle="-.", linewidth=1.4, zorder=2)  # pyright: ignore[reportUnknownMemberType]
    ax.text(  # pyright: ignore[reportUnknownMemberType]
        0.99,
        track_level,
        f"Track level: {track_level:.2f} mAHD",
        transform=ax.get_yaxis_transform(),
        color="#E65100",
        fontsize=8,
        ha="right",
        va="bottom",
    )
    ax.text(  # pyright: ignore[reportUnknownMemberType]
        0.99,
        formation_level,
        f"Formation level: {formation_level:.2f} mAHD",
        transform=ax.get_yaxis_transform(),
        color="#6A1B9A",
        fontsize=8,
        ha="right",
        va="bottom",
    )

    for peak in sorted(peak_flows, key=lambda item: float(item.aep_label.removesuffix("%")), reverse=True):
        ax.axvline(peak.value, color="#C62828", linestyle="--", linewidth=1.15, alpha=0.9, zorder=1)  # pyright: ignore[reportUnknownMemberType]
        ax.annotate(  # pyright: ignore[reportUnknownMemberType]
            f"{peak.aep_label} AEP\n{peak.value:.1f} m$^3$/s",
            xy=(peak.value, 1.010),
            xycoords=("data", "axes fraction"),
            color="#B71C1C",
            fontsize=7.2,
            ha="center",
            va="bottom",
            linespacing=1.05,
            bbox={
                "boxstyle": "round,pad=0.25",
                "facecolor": "white",
                "edgecolor": "#C62828",
                "linewidth": 0.9,
            },
            annotation_clip=False,
            zorder=5,
        )

    ax.set_title(reference.identifier, fontsize=14, fontweight="bold", y=1.08)  # pyright: ignore[reportUnknownMemberType]
    ax.set_xlabel("Flow rate (m$^3$/s)")  # pyright: ignore[reportUnknownMemberType]
    ax.set_ylabel("Water level (mAHD)")  # pyright: ignore[reportUnknownMemberType]
    depth_axis.set_ylabel("Depth (m)")  # pyright: ignore[reportUnknownMemberType]
    ax.grid(True, which="major", color="#D9D9D9", linewidth=0.8)  # pyright: ignore[reportUnknownMemberType]
    ax.grid(True, which="minor", color="#EEEEEE", linewidth=0.5)  # pyright: ignore[reportUnknownMemberType]
    ax.minorticks_on()
    depth_axis.grid(False)  # pyright: ignore[reportUnknownMemberType]
    all_x = np.concatenate((flow, np.array([peak.value for peak in peak_flows])))
    x_max = float(np.max(all_x))
    ax.set_xlim(0.0, x_max * 1.05)

    wl_max = max(float(np.max(water_level)), track_level, formation_level)
    wl_span = max(wl_max - point_datum, 0.1)
    primary_min = point_datum
    primary_max = wl_max + 0.10 * wl_span
    ax.set_ylim(primary_min, primary_max)

    depth_axis.set_ylim(0.0, primary_max - point_datum)

    fig.savefig(output_path, dpi=200, facecolor="white")  # pyright: ignore[reportUnknownMemberType]
    plt.close(fig)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        msg = f"No rows available for {path.name}"
        raise RuntimeError(msg)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def validate_plot_settings(
    cases: list[str], moving_average_minutes: float, track_level: float, formation_level: float
) -> None:
    """Validate case names, smoothing duration and plotted levels."""
    if not all(cases):
        msg = "Case names must not be empty"
        raise RuntimeError(msg)
    if len({case.casefold() for case in cases}) != len(cases):
        msg = "Case names must be unique (case-insensitive)"
        raise RuntimeError(msg)
    if not np.isfinite(moving_average_minutes) or moving_average_minutes < 0.0:
        msg = "Moving-average minutes must be a finite value greater than or equal to zero"
        raise RuntimeError(msg)
    if not np.isfinite(track_level) or not np.isfinite(formation_level):
        msg = "Track and formation levels must be finite"
        raise RuntimeError(msg)


def main() -> None:
    args = parse_args()
    print(f"{Path(__file__).name} {SCRIPT_VERSION}")
    results_root = args.results_root.absolute()
    reference_path = args.reference.absolute()
    output_dir = args.output_dir.absolute()
    curve_folder = results_root / args.curve_aep
    cases = [case.strip() for case in args.cases]
    moving_average_minutes = args.moving_average_minutes
    track_level = args.track_level
    formation_level = args.formation_level

    validate_plot_settings(cases, moving_average_minutes, track_level, formation_level)

    if not results_root.is_dir():
        msg = f"Results root does not exist: {results_root}"
        raise RuntimeError(msg)
    if not curve_folder.is_dir():
        msg = f"Curve AEP folder does not exist: {curve_folder}"
        raise RuntimeError(msg)
    if not reference_path.is_file():
        msg = f"Reference CSV does not exist: {reference_path}"
        raise RuntimeError(msg)
    output_dir.mkdir(parents=True, exist_ok=True)

    references = read_reference(reference_path)

    selection_rows: list[dict[str, object]] = []
    peak_rows: list[dict[str, object]] = []
    comparison_rows: list[dict[str, object]] = []

    for case in cases:
        curve_selections = select_curve_files(curve_folder, references, case, moving_average_minutes)
        peak_flows = select_peak_flows(results_root, references, case)

        for reference in references:
            selection = curve_selections[reference.identifier]
            flow, water_level, datum, comparison = load_curve_data(
                selection.source,
                reference,
                selection.flow_time_offset_hours,
                moving_average_minutes,
            )
            output_name = f"{reference.identifier}_{case}_Stage_Discharge_Curve.png"
            plot_curve(
                output_dir / output_name,
                reference,
                flow,
                water_level,
                datum,
                peak_flows[reference.identifier],
                track_level,
                formation_level,
            )

            selection_rows.append(
                {
                    "ID": reference.identifier,
                    "Flow Line": reference.flow_line,
                    "Water Level Point": reference.water_level_point,
                    "Case": case,
                    "Selected PO File": selection.source.name,
                    "Maximum Water Level (m AHD)": f"{selection.maximum_water_level:.4f}",
                    "Time at Maximum (hours)": f"{selection.time_at_maximum:.6f}",
                    "Flow at Maximum WL (m3/s)": f"{selection.flow_at_maximum:.4f}",
                    "Flow Time Offset Applied (minutes)": f"{selection.flow_time_offset_hours * 60.0:.3f}",
                    "Moving Average Window (minutes)": f"{moving_average_minutes:.3f}",
                    "Moving Average Samples": selection.moving_average_samples,
                    "Paired Data Points": selection.paired_data_points,
                    "Plot File": output_name,
                }
            )
            for peak in peak_flows[reference.identifier]:
                peak_rows.append(
                    {
                        "ID": reference.identifier,
                        "Flow Line": reference.flow_line,
                        "Case": case,
                        "AEP": peak.aep_label,
                        "Peak Flow (m3/s)": f"{peak.value:.4f}",
                        "Selected POMM File": peak.source.name,
                    }
                )
            comparison_rows.append(
                {
                    "ID": reference.identifier,
                    "Flow Line": reference.flow_line,
                    "Water Level Point": reference.water_level_point,
                    "Case": case,
                    "Envelope Points": comparison["envelope_points"],
                    "Falling Limb Selected Points": comparison["falling_points"],
                    "Rising Limb Selected Points": comparison["rising_points"],
                    "Maximum Falling-Limb Excess (m)": f"{comparison['maximum_falling_excess']:.4f}",
                    "Maximum Rising-Limb Excess (m)": f"{comparison['maximum_rising_excess']:.4f}",
                    "Depth Datum (mAHD)": f"{datum:.4f}",
                    "Depth Datum Spread (m)": f"{comparison['datum_spread']:.6f}",
                }
            )

    write_csv(output_dir / "Stage_Discharge_Curve_Selection_Summary.csv", selection_rows)
    write_csv(output_dir / "Stage_Discharge_AEP_Peak_Flows.csv", peak_rows)
    write_csv(output_dir / "Stage_Discharge_Limb_Comparison.csv", comparison_rows)

    print(f"Generated {len(references) * len(cases)} stage-discharge plots in {output_dir}")


if __name__ == "__main__":
    main()
