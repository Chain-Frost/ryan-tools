"""Generate FG01 stage-discharge plots and supporting selection summaries.

The script:
1. Selects the _PO.csv duration with the greatest water level for each reference row.
2. Builds the greater-water-level envelope from the rising and falling limbs.
3. Finds the maximum flow across all _POMM.csv durations for every AEP.
4. Draws AEP peak-flow callouts and an aligned secondary depth scale.

Dependencies: numpy, pandas, matplotlib
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


DEFAULT_RESULTS_ROOT = Path(
    r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO"
    r"\TUFLOW_MLGD\results\v06\FG01"
)
DEFAULT_OUTPUT_DIR = Path(
    r"P:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO"
    r"\4 ENGINEERING\11 HYDROLOGY\Stage Discharge Curve"
)
DEFAULT_REFERENCE = DEFAULT_OUTPUT_DIR / "Bridge PO reference.csv"


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
    paired_data_points: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--results-root",
        type=Path,
        default=DEFAULT_RESULTS_ROOT,
        help="FG01 results folder containing AEP subfolders (default: configured project path).",
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
    return parser.parse_args()


def read_reference(path: Path) -> list[ReferenceRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    required = {"ID", "Flow Line", "Water Level Point"}
    if not rows or not required.issubset(rows[0]):
        message = f"{path} is empty or lacks these columns: {sorted(required)}"
        raise RuntimeError(message)

    references = [
        ReferenceRow(
            identifier=row["ID"].strip(),
            flow_line=row["Flow Line"].strip(),
            water_level_point=row["Water Level Point"].strip(),
        )
        for row in rows
    ]
    if len({row.identifier for row in references}) != len(references):
        message = "Reference IDs must be unique"
        raise RuntimeError(message)
    return references


def read_two_row_header(path: Path) -> tuple[list[str], list[str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        result_types = next(reader)
        locations = next(reader)
    if len(result_types) != len(locations):
        message = f"Header width mismatch in {path}"
        raise RuntimeError(message)
    return result_types, locations


def build_column_lookup(result_types: list[str], locations: list[str]) -> dict[tuple[str, str], list[int]]:
    lookup: dict[tuple[str, str], list[int]] = {}
    for index, (result_type, location) in enumerate(zip(result_types, locations, strict=True)):
        lookup.setdefault((result_type.strip(), location.strip()), []).append(index)
    return lookup


def require_column(lookup: dict[tuple[str, str], list[int]], result_type: str, location: str, path: Path) -> int:
    matches = lookup.get((result_type, location), [])
    if len(matches) != 1:
        message = f"{path.name}: expected one {result_type!r}/{location!r} column, found {len(matches)}"
        raise RuntimeError(message)
    return matches[0]


def select_curve_files(
    curve_folder: Path,
    references: list[ReferenceRow],
) -> dict[str, CurveSelection]:
    po_files = sorted(curve_folder.glob("*_PO.csv"))
    if not po_files:
        message = f"No *_PO.csv files found in {curve_folder}"
        raise RuntimeError(message)

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
                message = f"{po_file.name}: no paired data for {reference.identifier}"
                raise RuntimeError(message)

            valid_time = time[valid]
            valid_flow = flow[valid]
            valid_wl = water_level[valid]
            maximum_index = int(np.argmax(valid_wl))
            selection = CurveSelection(
                source=po_file,
                maximum_water_level=float(valid_wl[maximum_index]),
                time_at_maximum=float(valid_time[maximum_index]),
                flow_at_maximum=float(valid_flow[maximum_index]),
                paired_data_points=len(valid_wl),
            )
            previous = best.get(reference.identifier)
            if previous is None or selection.maximum_water_level > previous.maximum_water_level:
                best[reference.identifier] = selection

    return best


def aep_sort_key(path: Path) -> float:
    return float(path.name.removesuffix("p"))


def aep_label(folder_name: str) -> str:
    return f"{float(folder_name.removesuffix('p')):g}%"


def select_peak_flows(results_root: Path, references: list[ReferenceRow]) -> dict[str, list[PeakFlow]]:
    aep_folders = sorted(
        (folder for folder in results_root.iterdir() if folder.is_dir() and list(folder.glob("*_POMM.csv"))),
        key=aep_sort_key,
    )
    if not aep_folders:
        message = f"No AEP folders containing *_POMM.csv files found in {results_root}"
        raise RuntimeError(message)

    selected: dict[str, list[PeakFlow]] = {reference.identifier: [] for reference in references}
    for folder in aep_folders:
        pomm_files = sorted(folder.glob("*_POMM.csv"))
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
                message = f"Maximum row not found in {pomm_file}"
                raise RuntimeError(message)
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
    flow, water_level, depth = flow[valid], water_level[valid], depth[valid]
    if not len(flow):
        message = f"No valid flow/water-level/depth rows in {source}"
        raise RuntimeError(message)

    point_datum_values = water_level - depth
    point_datum = float(np.median(point_datum_values))
    datum_spread = float(np.max(point_datum_values) - np.min(point_datum_values))

    peak_index = int(np.argmax(flow))
    rise_flow, rise_wl = branch_by_flow(flow[: peak_index + 1], water_level[: peak_index + 1])
    fall_flow, fall_wl = branch_by_flow(flow[peak_index:], water_level[peak_index:])
    overlap_min = max(float(rise_flow.min()), float(fall_flow.min()))
    overlap_max = min(float(rise_flow.max()), float(fall_flow.max()))
    grid = np.unique(
        np.concatenate(
            (
                rise_flow[(rise_flow >= overlap_min) & (rise_flow <= overlap_max)],
                fall_flow[(fall_flow >= overlap_min) & (fall_flow <= overlap_max)],
                np.array([overlap_min, overlap_max]),
            )
        )
    )
    rise_wl_grid = np.interp(grid, rise_flow, rise_wl)
    fall_wl_grid = np.interp(grid, fall_flow, fall_wl)
    use_falling = fall_wl_grid > rise_wl_grid
    envelope_wl = np.where(use_falling, fall_wl_grid, rise_wl_grid)
    difference = fall_wl_grid - rise_wl_grid

    comparison: dict[str, float | int] = {
        "envelope_points": len(grid),
        "falling_points": int(np.count_nonzero(use_falling)),
        "rising_points": int(np.count_nonzero(~use_falling)),
        "maximum_falling_excess": max(0.0, float(np.max(difference))),
        "maximum_rising_excess": max(0.0, float(np.max(-difference))),
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
) -> None:
    plt.switch_backend("Agg")
    fig, ax = plt.subplots(figsize=(9.0, 6.0))
    fig.subplots_adjust(left=0.10, right=0.90, bottom=0.12, top=0.86)
    depth_axis = ax.twinx()
    ax.plot(flow, water_level, color="#005A9C", linewidth=1.4, zorder=3)

    for peak in sorted(peak_flows, key=lambda item: float(item.aep_label.removesuffix("%")), reverse=True):
        ax.axvline(peak.value, color="#C62828", linestyle="--", linewidth=1.15, alpha=0.9, zorder=1)
        ax.annotate(
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

    ax.set_title(reference.identifier, fontsize=14, fontweight="bold", y=1.095)
    ax.set_xlabel("Flow rate (m$^3$/s)")
    ax.set_ylabel("Water level (mAHD)")
    depth_axis.set_ylabel("Depth (m)")
    ax.grid(True, which="major", color="#D9D9D9", linewidth=0.8)
    ax.grid(True, which="minor", color="#EEEEEE", linewidth=0.5)
    ax.minorticks_on()
    depth_axis.grid(False)

    all_x = np.concatenate((flow, np.array([peak.value for peak in peak_flows])))
    x_min, x_max = float(np.min(all_x)), float(np.max(all_x))
    x_span = max(x_max - x_min, 1.0)
    ax.set_xlim(x_min - 0.035 * x_span, x_max + 0.05 * x_span)

    wl_min, wl_max = float(np.min(water_level)), float(np.max(water_level))
    wl_span = max(wl_max - wl_min, 0.1)
    primary_min = wl_min - 0.035 * wl_span
    primary_max = wl_max + 0.10 * wl_span
    ax.set_ylim(primary_min, primary_max)

    fig.canvas.draw()
    primary_ticks = np.array(
        [tick for tick in ax.get_yticks() if primary_min - 1e-9 <= tick <= primary_max + 1e-9],
        dtype=float,
    )
    depth_axis.set_ylim(primary_min - point_datum, primary_max - point_datum)
    depth_axis.set_yticks(primary_ticks - point_datum)
    depth_axis.set_yticklabels([f"{tick - point_datum:.1f}" for tick in primary_ticks])

    fig.savefig(output_path, dpi=200, facecolor="white")
    plt.close(fig)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        message = f"No rows available for {path.name}"
        raise RuntimeError(message)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    results_root = args.results_root.absolute()
    reference_path = args.reference.absolute()
    output_dir = args.output_dir.absolute()
    curve_folder = results_root / args.curve_aep

    if not results_root.is_dir():
        message = f"Results root does not exist: {results_root}"
        raise RuntimeError(message)
    if not curve_folder.is_dir():
        message = f"Curve AEP folder does not exist: {curve_folder}"
        raise RuntimeError(message)
    if not reference_path.is_file():
        message = f"Reference CSV does not exist: {reference_path}"
        raise RuntimeError(message)
    output_dir.mkdir(parents=True, exist_ok=True)

    references = read_reference(reference_path)
    curve_selections = select_curve_files(curve_folder, references)
    peak_flows = select_peak_flows(results_root, references)

    selection_rows: list[dict[str, object]] = []
    peak_rows: list[dict[str, object]] = []
    comparison_rows: list[dict[str, object]] = []

    for reference in references:
        selection = curve_selections[reference.identifier]
        flow, water_level, datum, comparison = load_curve_data(selection.source, reference)
        output_name = f"{reference.identifier}_Stage_Discharge_Curve.png"
        plot_curve(
            output_dir / output_name,
            reference,
            flow,
            water_level,
            datum,
            peak_flows[reference.identifier],
        )

        selection_rows.append(
            {
                "ID": reference.identifier,
                "Flow Line": reference.flow_line,
                "Water Level Point": reference.water_level_point,
                "Selected PO File": selection.source.name,
                "Maximum Water Level (m AHD)": f"{selection.maximum_water_level:.4f}",
                "Time at Maximum (hours)": f"{selection.time_at_maximum:.6f}",
                "Flow at Maximum WL (m3/s)": f"{selection.flow_at_maximum:.4f}",
                "Paired Data Points": selection.paired_data_points,
                "Plot File": output_name,
            }
        )
        for peak in peak_flows[reference.identifier]:
            peak_rows.append(
                {
                    "ID": reference.identifier,
                    "Flow Line": reference.flow_line,
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

    print(f"Generated {len(references)} stage-discharge plots in {output_dir}")


if __name__ == "__main__":
    main()
