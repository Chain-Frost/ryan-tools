"""Generate FG01 stage-discharge plots and supporting selection summaries.

The script:
1. Selects the _PO.csv duration with the greatest water level for each reference row.
2. Builds the greater-water-level envelope over the *union* of measured limb
   flows, without extrapolating unobserved portions of either limb.
3. Finds finite maximum flows across _POMM.csv durations for every AEP.
4. Draws AEP peak-flow callouts and an aligned secondary depth scale.

Inputs are read-only. Results are staged and published as one dedicated report
directory. Existing reports require --overwrite and must contain this script's
marker; arbitrary directories are never overwritten.

PowerShell example (edit paths for the project):
    py -3.14 generate_stage_discharge_curves.py --results-root "C:\\Data\\TUFLOW\\results\\FG01" `
        --reference "C:\\Data\\References\\Bridge_PO_reference.csv" `
        --output-dir "C:\\Data\\Reports\\Stage_Discharge"

Dependencies: numpy, pandas, matplotlib
"""

from __future__ import annotations

import argparse
import csv
import math
import shutil
import sys
import tempfile
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Generic examples only; use CLI arguments or edit for a particular project.
SCRIPT_VERSION = "2026-10-29.1"
DEFAULT_RESULTS_ROOT = Path(r"C:\Data\TUFLOW\results\FG01")
DEFAULT_REFERENCE = Path(r"C:\Data\References\Bridge_PO_reference.csv")
# A dedicated output directory; this script does not write into arbitrary existing folders.
DEFAULT_OUTPUT_DIR = Path(r"C:\Data\Reports\Stage_Discharge")
_REPORT_MARKER = ".stage_discharge_report"
_SUMMARY_FILES = (
    "Stage_Discharge_Curve_Selection_Summary.csv",
    "Stage_Discharge_AEP_Peak_Flows.csv",
    "Stage_Discharge_Limb_Comparison.csv",
)
_WINDOWS_RESERVED = {
    "CON",
    "PRN",
    "AUX",
    "NUL",
    *(f"COM{index}" for index in range(1, 10)),
    *(f"LPT{index}" for index in range(1, 10)),
}


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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
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
        help="Dedicated output folder (created only after successful analysis).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Validate inputs and calculations without writing files.")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing report folder only when created by this script.",
    )
    return parser.parse_args(argv)


def _validate_reference_id(identifier: str) -> str:
    """Keep Windows output names unambiguous and within the report directory."""
    name = identifier.strip()
    if (
        not name
        or name in {".", ".."}
        or any(char in name for char in '\\\\/:*?"<>|')
        or any(ord(char) < 32 for char in name)
        or name.endswith((".", " "))
        or name.split(".", 1)[0].upper() in _WINDOWS_RESERVED
    ):
        message = f"Unsafe reference ID for an output filename: {identifier!r}"
        raise ValueError(message)
    return name


def read_reference(path: Path) -> list[ReferenceRow]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    required = {"ID", "Flow Line", "Water Level Point"}
    if not rows or not required.issubset(rows[0]):
        message = f"{path} is empty or lacks these columns: {sorted(required)}"
        raise RuntimeError(message)

    references = [
        ReferenceRow(
            identifier=_validate_reference_id(row["ID"]),
            flow_line=row["Flow Line"].strip(),
            water_level_point=row["Water Level Point"].strip(),
        )
        for row in rows
    ]
    if any(not row.flow_line or not row.water_level_point for row in references):
        message = "Reference rows must have non-empty Flow Line and Water Level Point values"
        raise ValueError(message)
    identifiers = [row.identifier.casefold() for row in references]
    if len(set(identifiers)) != len(identifiers):
        message = "Reference IDs must be unique ignoring Windows filename case"
        raise ValueError(message)
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


def find_column(
    lookup: dict[tuple[str, str], list[int]], result_type: str, location: str, path: Path
) -> int | None:
    """Return a unique column, or None if this result file lacks the location."""
    matches = lookup.get((result_type, location), [])
    if len(matches) > 1:
        message = f"{path.name}: duplicate {result_type!r}/{location!r} columns"
        raise RuntimeError(message)
    return matches[0] if matches else None


def require_column(lookup: dict[tuple[str, str], list[int]], result_type: str, location: str, path: Path) -> int:
    matches = lookup.get((result_type, location), [])
    if len(matches) != 1:
        message = f"{path.name}: expected one {result_type!r}/{location!r} column, found {len(matches)}"
        raise RuntimeError(message)
    return matches[0]


def select_curve_files(curve_folder: Path, references: list[ReferenceRow]) -> dict[str, CurveSelection]:
    """Choose the highest water-level hydrograph independently per reference."""
    po_files = sorted(curve_folder.glob("*_PO.csv"))
    if not po_files:
        message = f"No *_PO.csv files found in {curve_folder}"
        raise RuntimeError(message)

    best: dict[str, CurveSelection] = {}
    for po_file in po_files:
        result_types, locations = read_two_row_header(po_file)
        lookup = build_column_lookup(result_types, locations)
        if len(result_types) < 2:
            message = f"{po_file.name}: missing time column in two-row header"
            raise RuntimeError(message)
        indices = {1}
        columns: dict[str, tuple[int, int]] = {}
        for reference in references:
            flow_index = find_column(lookup, "Flow", reference.flow_line, po_file)
            wl_index = find_column(lookup, "Water Level", reference.water_level_point, po_file)
            if flow_index is None or wl_index is None:
                continue
            columns[reference.identifier] = (flow_index, wl_index)
            indices.update((flow_index, wl_index))
        if not columns:
            continue

        frame = pd.read_csv(po_file, skiprows=2, header=None, usecols=sorted(indices), low_memory=False)
        time = pd.to_numeric(frame[1], errors="coerce").to_numpy(dtype=float)
        for reference in references:
            pair = columns.get(reference.identifier)
            if pair is None:
                continue
            flow = pd.to_numeric(frame[pair[0]], errors="coerce").to_numpy(dtype=float)
            water_level = pd.to_numeric(frame[pair[1]], errors="coerce").to_numpy(dtype=float)
            valid = np.isfinite(time) & np.isfinite(flow) & np.isfinite(water_level)
            if not np.any(valid):
                continue
            indices_valid = np.flatnonzero(valid)
            maximum_index = int(indices_valid[np.argmax(water_level[valid])])
            selection = CurveSelection(
                source=po_file,
                maximum_water_level=float(water_level[maximum_index]),
                time_at_maximum=float(time[maximum_index]),
                flow_at_maximum=float(flow[maximum_index]),
                paired_data_points=len(indices_valid),
            )
            previous = best.get(reference.identifier)
            if previous is None or selection.maximum_water_level > previous.maximum_water_level:
                best[reference.identifier] = selection

    missing = [ref.identifier for ref in references if ref.identifier not in best]
    if missing:
        message = f"No valid PO hydrograph found for reference(s): {', '.join(missing)}"
        raise RuntimeError(message)
    return best


def aep_sort_key(path: Path) -> float:
    return float(path.name.removesuffix("p"))


def aep_label(folder_name: str) -> str:
    return f"{float(folder_name.removesuffix('p')):g}%"


def select_peak_flows(results_root: Path, references: list[ReferenceRow]) -> dict[str, list[PeakFlow]]:
    """Find finite maximum flow values separately for each reference and AEP."""
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
        best: dict[str, tuple[float, Path] | None] = {reference.identifier: None for reference in references}

        for pomm_file in pomm_files:
            with pomm_file.open("r", encoding="utf-8-sig", newline="") as handle:
                reader = csv.reader(handle)
                try:
                    result_types = next(reader)
                    locations = next(reader)
                    maximum_row = next(reader)
                except StopIteration as error:
                    message = f"Incomplete POMM header or maximum row in {pomm_file}"
                    raise RuntimeError(message) from error
            if len(maximum_row) < 2 or "Maximum" not in maximum_row[1]:
                message = f"Maximum row not found in {pomm_file}"
                raise RuntimeError(message)
            lookup = build_column_lookup(result_types, locations)
            for reference in references:
                index = find_column(lookup, "Flow", reference.flow_line, pomm_file)
                if index is None:
                    continue
                if index >= len(maximum_row):
                    message = f"Missing maximum value for {reference.identifier} in {pomm_file.name}"
                    raise RuntimeError(message)
                try:
                    value = float(maximum_row[index])
                except ValueError:
                    value = float("nan")
                if not math.isfinite(value):
                    print(
                        f"WARNING: Ignoring non-finite flow maximum for {reference.identifier} in {pomm_file.name}",
                        file=sys.stderr,
                    )
                    continue
                previous = best[reference.identifier]
                if previous is None or value > previous[0]:
                    best[reference.identifier] = (value, pomm_file)

        for reference in references:
            result = best[reference.identifier]
            if result is None:
                message = f"No finite POMM maximum for reference {reference.identifier} in AEP {folder.name}"
                raise RuntimeError(message)
            value, source = result
            selected[reference.identifier].append(
                PeakFlow(aep_folder=folder.name, aep_label=aep_label(folder.name), value=value, source=source)
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
    if len(flow) < 2:
        message = f"At least two valid flow/water-level/depth rows are required in {source}"
        raise RuntimeError(message)

    point_datum_values = water_level - depth
    point_datum = float(np.median(point_datum_values))
    datum_spread = float(np.max(point_datum_values) - np.min(point_datum_values))

    peak_index = int(np.argmax(flow))
    rise_flow, rise_wl = branch_by_flow(flow[: peak_index + 1], water_level[: peak_index + 1])
    fall_flow, fall_wl = branch_by_flow(flow[peak_index:], water_level[peak_index:])
    # Retain every *observed* rising or falling flow value, not only overlap.
    # Interpolation outside a limb's measured range is explicitly excluded:
    # np.interp's default endpoint behaviour would otherwise invent a flat limb.
    grid = np.union1d(rise_flow, fall_flow)
    rise_wl_grid = np.interp(grid, rise_flow, rise_wl, left=np.nan, right=np.nan)
    fall_wl_grid = np.interp(grid, fall_flow, fall_wl, left=np.nan, right=np.nan)
    overlapping = np.isfinite(rise_wl_grid) & np.isfinite(fall_wl_grid)
    difference = (fall_wl_grid - rise_wl_grid)[overlapping]
    use_falling = np.isfinite(fall_wl_grid) & (
        ~np.isfinite(rise_wl_grid) | (fall_wl_grid > rise_wl_grid)
    )
    envelope_wl = np.fmax(rise_wl_grid, fall_wl_grid)
    if not np.all(np.isfinite(envelope_wl)):
        message = f"Unresolvable flow interval in {source}"
        raise RuntimeError(message)

    comparison: dict[str, float | int] = {
        "envelope_points": len(grid),
        "overlap_points": int(np.count_nonzero(overlapping)),
        "falling_points": int(np.count_nonzero(use_falling)),
        "rising_points": int(np.count_nonzero(~use_falling)),
        "maximum_falling_excess": max(0.0, float(np.max(difference))) if difference.size else 0.0,
        "maximum_rising_excess": max(0.0, float(np.max(-difference))) if difference.size else 0.0,
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
    depth_axis = ax.twinx()  # pyright: ignore[reportUnknownMemberType]
    ax.plot(  # pyright: ignore[reportUnknownMemberType]
        flow, water_level, color="#005A9C", linewidth=1.4, zorder=3
    )

    for peak in sorted(peak_flows, key=lambda item: float(item.aep_label.removesuffix("%")), reverse=True):
        ax.axvline(  # pyright: ignore[reportUnknownMemberType]
            peak.value, color="#C62828", linestyle="--", linewidth=1.15, alpha=0.9, zorder=1
        )
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

    ax.set_title(  # pyright: ignore[reportUnknownMemberType]
        reference.identifier, fontsize=14, fontweight="bold", y=1.095
    )
    ax.set_xlabel("Flow rate (m$^3$/s)")  # pyright: ignore[reportUnknownMemberType]
    ax.set_ylabel("Water level (mAHD)")  # pyright: ignore[reportUnknownMemberType]
    depth_axis.set_ylabel("Depth (m)")  # pyright: ignore[reportUnknownMemberType]
    ax.grid(True, which="major", color="#D9D9D9", linewidth=0.8)  # pyright: ignore[reportUnknownMemberType]
    ax.grid(True, which="minor", color="#EEEEEE", linewidth=0.5)  # pyright: ignore[reportUnknownMemberType]
    ax.minorticks_on()
    depth_axis.grid(False)  # pyright: ignore[reportUnknownMemberType]

    all_x = np.concatenate((flow, np.array([peak.value for peak in peak_flows])))
    x_min, x_max = float(np.min(all_x)), float(np.max(all_x))
    x_span = max(x_max - x_min, 1.0)
    ax.set_xlim(x_min - 0.035 * x_span, x_max + 0.05 * x_span)

    wl_min, wl_max = float(np.min(water_level)), float(np.max(water_level))
    wl_span = max(wl_max - wl_min, 0.1)
    primary_min = wl_min - 0.035 * wl_span
    primary_max = wl_max + 0.10 * wl_span
    ax.set_ylim(primary_min, primary_max)

    fig.canvas.draw()  # pyright: ignore[reportUnknownMemberType]
    primary_ticks = np.array(
        [tick for tick in ax.get_yticks() if primary_min - 1e-9 <= tick <= primary_max + 1e-9],
        dtype=float,
    )
    depth_axis.set_ylim(primary_min - point_datum, primary_max - point_datum)
    depth_axis.set_yticks(primary_ticks - point_datum)  # pyright: ignore[reportUnknownMemberType]
    depth_axis.set_yticklabels(  # pyright: ignore[reportUnknownMemberType]
        [f"{tick - point_datum:.1f}" for tick in primary_ticks]
    )

    fig.savefig(output_path, dpi=200, facecolor="white")  # pyright: ignore[reportUnknownMemberType]
    plt.close(fig)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        message = f"No rows available for {path.name}"
        raise RuntimeError(message)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _validate_output_directory(output_dir: Path, *, overwrite: bool) -> None:
    """Never replace a foreign folder, symlink, or unknown files."""
    if output_dir.is_symlink():
        message = f"Output directory is a symbolic link: {output_dir}"
        raise ValueError(message)
    if not output_dir.exists():
        return
    if not overwrite:
        message = f"Output report directory already exists (use --overwrite): {output_dir}"
        raise FileExistsError(message)
    if not output_dir.is_dir() or not (output_dir / _REPORT_MARKER).is_file():
        message = f"Refusing to replace a folder not created by this script: {output_dir}"
        raise ValueError(message)
    expected = set(_SUMMARY_FILES) | {_REPORT_MARKER}
    for item in output_dir.iterdir():
        if item.is_symlink() or not item.is_file() or not (
            item.name in expected or item.name.endswith("_Stage_Discharge_Curve.png")
        ):
            message = f"Refusing to replace unexpected content in {output_dir}: {item.name}"
            raise ValueError(message)


def _publish_report_directory(stage: Path, output: Path, *, overwrite: bool) -> None:
    """Publish a complete report folder, rolling back if replacement fails."""
    if not output.exists() and not output.is_symlink():
        stage.rename(output)
        return
    if not overwrite:
        message = f"Output report directory already exists: {output}"
        raise FileExistsError(message)
    _validate_output_directory(output, overwrite=True)
    backup = output.with_name(f".{output.name}.{uuid.uuid4().hex}.backup")
    output.rename(backup)
    try:
        stage.rename(output)
    except BaseException:
        # Preserve the old report, including on an interrupted second rename.
        if not output.exists() and not output.is_symlink():
            backup.rename(output)
        raise
    shutil.rmtree(backup)


def main(argv: Sequence[str] | None = None) -> int:
    """Build and publish one complete report, never modifying source files."""
    args = parse_args(argv)
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
    if results_root.resolve(strict=True).is_relative_to(output_dir.resolve(strict=False)):
        message = "Output directory may not contain the source results root"
        raise ValueError(message)
    if reference_path.resolve(strict=True).is_relative_to(output_dir.resolve(strict=False)):
        message = "Output directory may not contain the reference CSV"
        raise ValueError(message)

    references = read_reference(reference_path)
    curve_selections = select_curve_files(curve_folder, references)
    peak_flows = select_peak_flows(results_root, references)

    selection_rows: list[dict[str, object]] = []
    peak_rows: list[dict[str, object]] = []
    comparison_rows: list[dict[str, object]] = []
    plots: list[tuple[Path, ReferenceRow, np.ndarray, np.ndarray, float, list[PeakFlow]]] = []

    for reference in references:
        selection = curve_selections[reference.identifier]
        flow, water_level, datum, comparison = load_curve_data(selection.source, reference)
        output_name = f"{reference.identifier}_Stage_Discharge_Curve.png"
        plots.append((Path(output_name), reference, flow, water_level, datum, peak_flows[reference.identifier]))

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
                "Common Flow Points": comparison["overlap_points"],
                "Falling Limb Selected Points": comparison["falling_points"],
                "Rising Limb Selected Points": comparison["rising_points"],
                "Maximum Falling-Limb Excess (m)": f"{comparison['maximum_falling_excess']:.4f}",
                "Maximum Rising-Limb Excess (m)": f"{comparison['maximum_rising_excess']:.4f}",
                "Depth Datum (mAHD)": f"{datum:.4f}",
                "Depth Datum Spread (m)": f"{comparison['datum_spread']:.6f}",
            }
        )

    _validate_output_directory(output_dir, overwrite=args.overwrite)
    if args.dry_run:
        print(f"Dry run: validated {len(references)} stage-discharge curves; no files created in {output_dir}")
        return 0

    output_dir.parent.mkdir(parents=True, exist_ok=True)
    # Assemble plots and all summaries away from the final report path.
    stage = Path(tempfile.mkdtemp(prefix=f".{output_dir.name}.staging-", dir=output_dir.parent))
    try:
        for filename, reference, flow, water_level, datum, peaks in plots:
            plot_curve(stage / filename, reference, flow, water_level, datum, peaks)
        write_csv(stage / _SUMMARY_FILES[0], selection_rows)
        write_csv(stage / _SUMMARY_FILES[1], peak_rows)
        write_csv(stage / _SUMMARY_FILES[2], comparison_rows)
        (stage / _REPORT_MARKER).write_text(
            f"Created by generate_stage_discharge_curves.py {SCRIPT_VERSION}\n", encoding="utf-8"
        )
        _publish_report_directory(stage, output_dir, overwrite=args.overwrite)
    finally:
        # The published stage was renamed; otherwise clean up all incomplete work.
        if stage.exists():
            shutil.rmtree(stage)
    print(f"Generated {len(references)} stage-discharge plots in {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
