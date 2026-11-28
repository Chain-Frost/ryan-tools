"""Generate a culvert stage-discharge curve from paired TUFLOW 1D CSV outputs.

Edit ``CULVERT_NAME`` to select a different culvert. The script reads flow from
``Q <culvert>`` in a ``_1d_Q.csv`` file and upstream water level from
``H <culvert>.1`` in the matching ``_1d_H.csv`` file.

Dependencies: matplotlib, numpy, pandas
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

# Edit this value to plot another culvert. The output filename uses this name.
CULVERT_NAME = "MLWTP 180.238"

DEFAULT_CSV_DIR = Path(
    r"Q:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO"
    r"\TUFLOW_MLGD\results\v12\FG02\01.0p\plot\csv"
)
DEFAULT_FLOW_CSV = DEFAULT_CSV_DIR / "RTIO_v12_FG02_EXG_CCNone_01.0p_02160m_TP03_04M_NDVI_Base_1d_Q.csv"
DEFAULT_WATER_LEVEL_CSV = DEFAULT_CSV_DIR / "RTIO_v12_FG02_EXG_CCNone_01.0p_02160m_TP03_04M_NDVI_Base_1d_H.csv"
DEFAULT_OUTPUT_DIR = Path(
    r"P:\BGER\PER\RP20181.498 GD AND FORTESCUE RIVER GAP RAIL HYDROLOGY MDL - RTIO"
    r"\4 ENGINEERING\11 HYDROLOGY\Stage Discharge Curve"
)


@dataclass(frozen=True)
class CurveData:
    time: np.ndarray
    flow: np.ndarray
    upstream_water_level: np.ndarray
    flow_column: str
    water_level_column: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--culvert",
        default=CULVERT_NAME,
        help=f"TUFLOW culvert/channel name (default: {CULVERT_NAME}).",
    )
    parser.add_argument(
        "--flow-csv",
        type=Path,
        default=DEFAULT_FLOW_CSV,
        help="TUFLOW _1d_Q.csv flow output.",
    )
    parser.add_argument(
        "--water-level-csv",
        type=Path,
        default=DEFAULT_WATER_LEVEL_CSV,
        help="TUFLOW _1d_H.csv water-level output.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Folder for the stage-discharge plot.",
    )
    return parser.parse_args()


def read_header(path: Path) -> list[str]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return next(csv.reader(handle))


def require_prefixed_column(path: Path, prefix: str) -> str:
    matches = [column for column in read_header(path) if column.startswith(prefix)]
    if len(matches) != 1:
        msg = f"{path.name}: expected one column beginning {prefix!r}, found {len(matches)}"
        raise RuntimeError(msg)
    return matches[0]


def read_curve_data(flow_csv: Path, water_level_csv: Path, culvert_name: str) -> CurveData:
    flow_column = require_prefixed_column(flow_csv, f"Q {culvert_name} [")
    water_level_column = require_prefixed_column(water_level_csv, f"H {culvert_name}.1 [")

    flow_frame = pd.read_csv(flow_csv, usecols=["Time (h)", flow_column], low_memory=False)
    water_level_frame = pd.read_csv(
        water_level_csv,
        usecols=["Time (h)", water_level_column],
        low_memory=False,
    )
    flow_time = pd.to_numeric(flow_frame["Time (h)"], errors="coerce").to_numpy(dtype=float)
    water_level_time = pd.to_numeric(water_level_frame["Time (h)"], errors="coerce").to_numpy(dtype=float)
    flow = pd.to_numeric(flow_frame[flow_column], errors="coerce").to_numpy(dtype=float)
    water_level = pd.to_numeric(water_level_frame[water_level_column], errors="coerce").to_numpy(dtype=float)

    if len(flow_time) != len(water_level_time) or not np.allclose(
        flow_time,
        water_level_time,
        rtol=0.0,
        atol=1e-9,
        equal_nan=True,
    ):
        msg = "Flow and water-level CSV times do not align"
        raise RuntimeError(msg)

    valid = np.isfinite(flow_time) & np.isfinite(flow) & np.isfinite(water_level)
    if not np.any(valid):
        msg = "No finite, time-aligned flow and upstream water-level records were found"
        raise RuntimeError(msg)

    return CurveData(
        time=flow_time[valid],
        flow=flow[valid],
        upstream_water_level=water_level[valid],
        flow_column=flow_column,
        water_level_column=water_level_column,
    )


def output_filename(culvert_name: str) -> str:
    invalid_characters = '<>:"/\\|?*'
    safe_name = "".join("_" if character in invalid_characters else character for character in culvert_name)
    safe_name = safe_name.strip(" .")
    if not safe_name:
        msg = "Culvert name does not contain any filename-safe characters"
        raise RuntimeError(msg)
    return f"{safe_name}_Stage_Discharge_Curve.png"


def plot_curve(output_path: Path, culvert_name: str, data: CurveData) -> None:
    # Matplotlib leaves plotting **kwargs untyped; ignore only that upstream member diagnostic.
    fig, ax = plt.subplots(figsize=(9.0, 6.0))
    fig.subplots_adjust(left=0.11, right=0.88, bottom=0.12, top=0.88)

    points = ax.scatter(  # pyright: ignore[reportUnknownMemberType]
        data.flow,
        data.upstream_water_level,
        c=data.time,
        cmap="viridis",
        s=7,
        alpha=0.75,
        edgecolors="none",
        zorder=1,
    )
    if float(np.min(data.flow)) < 0.0 < float(np.max(data.flow)):
        ax.axvline(0.0, color="#666666", linestyle="--", linewidth=0.8, zorder=0)  # pyright: ignore[reportUnknownMemberType]

    colour_bar = fig.colorbar(points, ax=ax, pad=0.025)  # pyright: ignore[reportUnknownMemberType]
    colour_bar.set_label("Elapsed time (hours)")  # pyright: ignore[reportUnknownMemberType]
    ax.set_title(f"{culvert_name} Upstream Stage-Discharge Curve", fontsize=14, fontweight="bold")  # pyright: ignore[reportUnknownMemberType]
    ax.set_xlabel("Culvert flow (m$^3$/s)")  # pyright: ignore[reportUnknownMemberType]
    ax.set_ylabel("Upstream water level (mAHD)")  # pyright: ignore[reportUnknownMemberType]
    ax.grid(True, which="major", color="#D9D9D9", linewidth=0.8)  # pyright: ignore[reportUnknownMemberType]
    ax.grid(True, which="minor", color="#EEEEEE", linewidth=0.5)  # pyright: ignore[reportUnknownMemberType]
    ax.minorticks_on()

    flow_min, flow_max = float(np.min(data.flow)), float(np.max(data.flow))
    flow_span = max(flow_max - flow_min, 1.0)
    ax.set_xlim(flow_min - 0.035 * flow_span, flow_max + 0.035 * flow_span)
    level_min = float(np.min(data.upstream_water_level))
    level_max = float(np.max(data.upstream_water_level))
    level_span = max(level_max - level_min, 0.1)
    ax.set_ylim(level_min - 0.035 * level_span, level_max + 0.06 * level_span)

    fig.savefig(output_path, dpi=200, facecolor="white")  # pyright: ignore[reportUnknownMemberType]
    plt.close(fig)


def main() -> None:
    args = parse_args()
    flow_csv = args.flow_csv.absolute()
    water_level_csv = args.water_level_csv.absolute()
    output_dir = args.output_dir.absolute()

    if not flow_csv.is_file():
        msg = f"Flow CSV does not exist: {flow_csv}"
        raise RuntimeError(msg)
    if not water_level_csv.is_file():
        msg = f"Water-level CSV does not exist: {water_level_csv}"
        raise RuntimeError(msg)

    output_dir.mkdir(parents=True, exist_ok=True)
    data = read_curve_data(flow_csv, water_level_csv, args.culvert)
    output_path = output_dir / output_filename(args.culvert)
    plot_curve(output_path, args.culvert, data)

    print(
        f"Generated {output_path} from {len(data.time)} paired records "
        f"({data.flow_column!r} and {data.water_level_column!r})"
    )


if __name__ == "__main__":
    main()
