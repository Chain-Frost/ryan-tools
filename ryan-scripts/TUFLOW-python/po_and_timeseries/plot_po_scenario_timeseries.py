"""Plot TUFLOW PO scenarios without mixing measurements or distinct runs.

Edit the settings below and run this maintained, project-specific script directly.
CSV mode processes matching PO files through ryan_library, and can create a
versioned, provenance-checked Parquet plot cache. An existing cache is protected
unless REPLACE_PARQUET_CACHE is explicitly enabled. Cache mode verifies the
current CSV inventory, ingest settings and processor identity before reading it.

One figure is created per location and measurement type, with simulation time in
hours. Set SHOW_PLOTS=False and EXPORT_PNG=True for bounded headless PNG output;
an existing PNG is never overwritten unless OVERWRITE_PNG=True. No TUFLOW
simulation is launched. Do not treat reserved PO no-data values as valid peaks:
their shared normalization is tracked separately in issue #219.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import os
import re
import tempfile
from collections.abc import Callable
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, cast

import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from loguru import logger
from matplotlib.axes import Axes
from matplotlib.backends.backend_agg import FigureCanvasAgg
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize
from matplotlib.figure import Figure
from pandas import DataFrame, Series

from ryan_library.functions.wrapper_utils import print_wrapper_banner
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.other_processors.POProcessor import POProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

# --------------------------------------------------------------------------------------
# Configuration - tweak these constants to target different runs or behaviours.
# --------------------------------------------------------------------------------------
TUFLOW_RESULTS_ROOT = Path(r"Q:\25\RP25232 RR BULK EARTHWORKS - MINRES\TUFLOW_AGIL_RR\results\v05\bigModel")
PARQUET_PATH: Path = TUFLOW_RESULTS_ROOT / "po_timeseries_08M.parquet"
LOAD_FROM_PARQUET = True  # Set True to bypass CSV processing and load from PARQUET_PATH.
FILE_PATTERN = "*_08M_PO.csv"
LOCATION_FILTER: set[str] | None = None  # e.g. {"SW_159", "SW_059"} to trim while loading
INTERNAL_NAME_FILTER = "08M"
PLOT_LOCATIONS: list[str] = ["SW_159", "SW_059"]
SCENARIO_DIMENSIONS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("AEP", ("aep_text", "AEP", "aep_numeric")),
    ("TP", ("tp_text", "TP", "tp_numeric")),
    ("DUR", ("duration_text", "DUR", "duration_numeric")),
)
SCENARIO_DIMENSION_MAP: dict[str, tuple[str, ...]] = dict(SCENARIO_DIMENSIONS)
SCENARIO_VALUE_PLACEHOLDER = "Unknown"
SCENARIO_LEGEND_TITLE = "Scenario (AEP | TP | DUR)"
COLORBAR_LABEL = "AEP"
SCRIPT_VERSION = "2026-11-08.1"
REPLACE_PARQUET_CACHE = False  # Explicit opt-in to replace an existing cache after complete processing.
MEASUREMENT_TYPES: list[str] | None = None  # e.g. ["Flow"]; None plots every type separately.
SHOW_PLOTS = True
EXPORT_PNG = False
PNG_OUTPUT_DIR: Path = TUFLOW_RESULTS_ROOT / "po_scenario_plots"
OVERWRITE_PNG = False
CACHE_MANIFEST_COLUMN = "__plot_po_cache_manifest__"
CACHE_SCHEMA_VERSION = 1


def find_po_files(root: Path) -> list[Path]:
    if not root.is_dir():
        msg = f"TUFLOW root does not exist: {root}"
        raise FileNotFoundError(msg)
    files: list[Path] = sorted({path for path in root.rglob(FILE_PATTERN) if path.is_file()})
    logger.info("Found {} PO files matching pattern {}", len(files), FILE_PATTERN)
    return files


def process_files_sequentially(files: list[Path]) -> ProcessorCollection:
    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(LOCATION_FILTER)
    collection = ProcessorCollection()
    total: int = len(files)
    for index, file_path in enumerate(files, start=1):
        try:
            processor: BaseProcessor = BaseProcessor.from_file(file_path=file_path, entity_filter=normalized_locations)
            processor.process()
            if normalized_locations:
                processor.filter_locations(locations=normalized_locations)
            if processor.processed:
                collection.add_processor(processor=processor)
                logger.success("({}/{}) Processed {}", index, total, processor.log_path)
            else:
                logger.warning("({}/{}) Processor finished without data: {}", index, total, processor.log_path)
        except Exception:
            logger.exception("({}/{}) Failed to process {}", index, total, file_path)
    return collection


def _require_plot_columns(df: pd.DataFrame) -> None:
    required = {"Time", "Value", "Location", "Type", "internalName"}
    missing = sorted(required.difference(df.columns))
    if missing:
        msg = f"PO data missing required plotting column(s): {', '.join(missing)}"
        raise KeyError(msg)
    resolve_dimension_columns(df)


def filter_internal_name(df: pd.DataFrame) -> pd.DataFrame:
    """Apply a case-insensitive literal substring; never restore rejected rows."""
    if not INTERNAL_NAME_FILTER:
        return df
    if "internalName" not in df.columns:
        msg = "INTERNAL_NAME_FILTER requires the internalName run-identity column."
        raise KeyError(msg)
    mask: Series[bool] = (
        df["internalName"].astype("string").str.contains(pat=INTERNAL_NAME_FILTER, case=False, regex=False, na=False)
    )
    filtered: DataFrame = df.loc[mask].copy()
    if filtered.empty:
        msg = f"INTERNAL_NAME_FILTER={INTERNAL_NAME_FILTER!r} matched no PO run identities."
        raise ValueError(msg)
    return filtered


def filter_locations(df: pd.DataFrame) -> pd.DataFrame:
    """Apply identical selected locations to source and cache results."""
    locations = BaseProcessor.normalize_locations(LOCATION_FILTER)
    if not locations:
        return df
    if "Location" not in df.columns:
        msg = "LOCATION_FILTER requires the Location column."
        raise KeyError(msg)
    filtered: DataFrame = df.loc[df["Location"].astype("string").str.strip().isin(locations)].copy()
    if filtered.empty:
        msg = f"LOCATION_FILTER matched no PO locations: {sorted(locations)!r}"
        raise ValueError(msg)
    return filtered


def select_po_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    _require_plot_columns(df)
    return filter_locations(filter_internal_name(df))


def _source_manifest(files: list[Path]) -> str:
    """Keep cache provenance in the same atomic Parquet file, not a racy sidecar."""
    if not files:
        msg = "No source PO CSV files are available to validate the cache."
        raise ValueError(msg)
    try:
        library_version = version("ryan-tools")
    except PackageNotFoundError:
        library_version = "source-checkout"
    processor_file = Path(inspect.getfile(POProcessor))
    processor_hash = hashlib.sha256(processor_file.read_bytes()).hexdigest()
    inventory: list[dict[str, str | int]] = []
    for file in files:
        stat = file.stat()
        inventory.append({"path": str(file.absolute()), "size": stat.st_size, "mtime_ns": stat.st_mtime_ns})
    return json.dumps(
        {
            "cache_schema": CACHE_SCHEMA_VERSION,
            "script_version": SCRIPT_VERSION,
            "ryan_tools_version": library_version,
            "po_processor_sha256": processor_hash,
            "results_root": str(TUFLOW_RESULTS_ROOT.absolute()),
            "file_pattern": FILE_PATTERN,
            "location_filter": sorted(BaseProcessor.normalize_locations(LOCATION_FILTER)),
            "internal_name_filter": INTERNAL_NAME_FILTER,
            "source_files": inventory,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _publish_staged_file(staged: Path, target: Path, *, overwrite: bool) -> None:
    """Publish only a completed file. Exclusive hardlinks fail closed on unsupported shares."""
    if overwrite:
        staged.replace(target)
        return
    try:
        os.link(staged, target)
    except FileExistsError as exc:
        msg = f"Output already exists; explicit overwrite is required: {target}"
        raise FileExistsError(msg) from exc
    except OSError as exc:
        msg = f"Cannot safely create {target} without overwrite; hardlink publication unavailable."
        raise OSError(msg) from exc
    staged.unlink()


def _new_staged_path(target: Path) -> Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{target.stem}.", suffix=target.suffix, dir=target.parent)
    os.close(fd)
    return Path(name)


def _write_parquet_cache(df: pd.DataFrame, manifest: str) -> None:
    target = PARQUET_PATH.absolute()
    if target.exists() and not REPLACE_PARQUET_CACHE:
        msg = f"Cache already exists; set REPLACE_PARQUET_CACHE=True to replace: {target}"
        raise FileExistsError(msg)
    if CACHE_MANIFEST_COLUMN in df.columns:
        msg = f"Source PO data uses reserved cache column: {CACHE_MANIFEST_COLUMN}"
        raise ValueError(msg)
    staged = _new_staged_path(target)
    try:
        cached = df.assign(**{CACHE_MANIFEST_COLUMN: manifest})
        cached.to_parquet(staged, index=False)
        _publish_staged_file(staged, target, overwrite=REPLACE_PARQUET_CACHE)
    finally:
        staged.unlink(missing_ok=True)
    logger.success("Published {:,} plot-cache rows to {}", len(df), target)


def _read_parquet_cache(files: list[Path]) -> pd.DataFrame:
    if not PARQUET_PATH.is_file():
        msg = f"PO plot cache not found: {PARQUET_PATH}"
        raise FileNotFoundError(msg)
    frame: DataFrame = pd.read_parquet(PARQUET_PATH)
    if (
        frame.empty
        or CACHE_MANIFEST_COLUMN not in frame.columns
        or bool(frame[CACHE_MANIFEST_COLUMN].isna().any())
        or not bool(frame[CACHE_MANIFEST_COLUMN].eq(frame[CACHE_MANIFEST_COLUMN].iloc[0]).all())
    ):
        msg = (
            f"PO plot cache has no valid provenance manifest: {PARQUET_PATH}; "
            "reprocess CSV with LOAD_FROM_PARQUET=False and explicit replacement."
        )
        raise ValueError(msg)
    expected = _source_manifest(files)
    recorded = str(frame[CACHE_MANIFEST_COLUMN].iloc[0])
    if recorded != expected:
        msg = (
            f"PO plot cache is stale or its input/config/parser provenance differs: {PARQUET_PATH}; "
            "reprocess CSV with LOAD_FROM_PARQUET=False and REPLACE_PARQUET_CACHE=True."
        )
        raise ValueError(msg)
    output = frame.drop(columns=[CACHE_MANIFEST_COLUMN])
    _require_plot_columns(output)
    return output


def load_po_dataframe() -> pd.DataFrame:
    files = find_po_files(TUFLOW_RESULTS_ROOT)
    if not files:
        msg = f"No PO source files matched FILE_PATTERN={FILE_PATTERN!r}."
        raise RuntimeError(msg)

    manifest_before = _source_manifest(files)
    if LOAD_FROM_PARQUET:
        logger.info("Loading verified plot cache {}", PARQUET_PATH)
        return select_po_dataframe(_read_parquet_cache(files))

    if PARQUET_PATH.exists() and not REPLACE_PARQUET_CACHE:
        msg = (
            f"PO plot cache already exists at {PARQUET_PATH}; set REPLACE_PARQUET_CACHE=True to replace it explicitly."
        )
        raise FileExistsError(msg)
    logger.info("Sequentially processing {} PO file(s).", len(files))
    collection = process_files_sequentially(files)
    if not collection.processors:
        msg = "No PO processors produced data; check LOCATION_FILTER and CSV validity."
        raise RuntimeError(msg)
    combined: DataFrame = collection.po_combine()
    if combined.empty:
        msg = "Combined PO DataFrame is empty."
        raise RuntimeError(msg)
    selected = select_po_dataframe(combined)
    if selected.empty:
        msg = "No PO samples remain after selection."
        raise RuntimeError(msg)
    if _source_manifest(find_po_files(TUFLOW_RESULTS_ROOT)) != manifest_before:
        msg = "Source PO CSV metadata changed during processing; cache publication cancelled."
        raise RuntimeError(msg)
    _write_parquet_cache(selected, manifest_before)
    return selected


def prepare_plot_df(df: pd.DataFrame) -> pd.DataFrame:
    """Convert Time to finite numeric simulation hours, never timestamps or row indices."""
    _require_plot_columns(df)
    plot_df: DataFrame = df.copy()
    for column in ("Location", "Type", "internalName"):
        text = plot_df[column].astype("string").str.strip()
        if bool(text.isna().any()) or bool(text.eq("").fillna(False).any()):
            msg = f"PO plotting requires nonempty {column} values."
            raise ValueError(msg)
        plot_df[column] = text
    for column in ("Time", "Value"):
        numeric = pd.to_numeric(plot_df[column], errors="coerce").astype("float64")
        if not bool(np.isfinite(numeric.to_numpy(dtype=float)).all()):
            msg = f"PO {column} must contain finite numeric values; Time is simulation hours."
            raise ValueError(msg)
        plot_df[column] = numeric
    return plot_df


def resolve_dimension_columns(df: pd.DataFrame) -> dict[str, str]:
    resolved_columns: dict[str, str] = {}
    missing_specs: list[str] = []

    for display_name, options in SCENARIO_DIMENSIONS:
        match: str | None = next((option for option in options if option in df.columns), None)
        if match:
            resolved_columns[display_name] = match
        else:
            option_list: str = " | ".join(options)
            missing_specs.append(f"{display_name} ({option_list})")

    if missing_specs:
        raise KeyError(
            "DataFrame is missing required scenario columns for: "
            + ", ".join(missing_specs)
            + ". Unable to build plot groupings."
        )
    return resolved_columns


def build_scenario_labels(df: pd.DataFrame, dimension_columns: dict[str, str] | None = None) -> pd.Series:
    resolved_columns: dict[str, str] = dimension_columns or resolve_dimension_columns(df)

    labels: Series | None = None
    for display_name, _ in SCENARIO_DIMENSIONS:
        column_name: str = resolved_columns[display_name]
        values: Series = df[column_name].fillna(SCENARIO_VALUE_PLACEHOLDER).astype(str)
        component: Series = display_name + "=" + values
        labels = component if labels is None else labels + " | " + component
    if labels is None:
        msg = "At least one scenario dimension must be configured."
        raise ValueError(msg)
    return labels


def _numeric_dimension_column(df: pd.DataFrame, dimension_name: str) -> str | None:
    options: tuple[str, ...] = SCENARIO_DIMENSION_MAP[dimension_name]
    for option in options:
        if option in df.columns and pd.api.types.is_numeric_dtype(df[option]):
            return option
    return None


def create_aep_colorizer(
    df: pd.DataFrame, dimension_columns: dict[str, str]
) -> tuple[Callable[[Any], Any], dict[str, Any]]:
    aep_label_column: str = dimension_columns["AEP"]
    numeric_column: str | None = _numeric_dimension_column(df, "AEP")
    source_column: str = numeric_column or aep_label_column
    aep_series: Series = pd.to_numeric(df[source_column], errors="coerce") if numeric_column else df[source_column]

    if numeric_column:
        valid_values: Series = aep_series.dropna()
        if valid_values.empty:
            numeric_column = None
        else:
            min_val = float(valid_values.min())
            max_val = float(valid_values.max())
            if min_val == max_val:
                max_val = min_val + 1.0
            norm = Normalize(vmin=min_val, vmax=max_val)
            cmap = plt.get_cmap("viridis")

            def colorize(value: Any) -> Any:
                try:
                    numeric_value: float = float(value)
                except (TypeError, ValueError):
                    return "black"
                if not math.isfinite(numeric_value):
                    return "black"
                return cmap(norm(numeric_value))

            meta = {"type": "numeric", "norm": norm, "cmap": cmap, "column": source_column, "label": COLORBAR_LABEL}
            return colorize, meta

    aep_strings: Series = df[aep_label_column].fillna(SCENARIO_VALUE_PLACEHOLDER).astype(str)
    unique_values: pd.Index = pd.Index(sorted(aep_strings.unique()))
    total = max(len(unique_values), 1)
    cmap = plt.get_cmap("viridis", total)
    color_lookup = {value: cmap(idx) for idx, value in enumerate(unique_values)}

    def colorize(value: Any) -> Any:
        key = SCENARIO_VALUE_PLACEHOLDER if pd.isna(value) else str(value)
        return color_lookup.get(key, "black")

    meta = {"type": "categorical", "values": unique_values, "column": aep_label_column}
    return colorize, meta


def _measurement_ylabel(measurement: str) -> str:
    """Use explicit units only for known TUFLOW measurement names."""
    key = measurement.casefold().strip()
    if key in {"q", "flow", "discharge", "qs"}:
        return "Flow (m³/s)"
    if key in {"h", "hu", "hd", "water level", "u/s structure water level", "d/s structure water level"}:
        return "Water level (m)"
    if key in {"v", "velocity"}:
        return "Velocity (m/s)"
    return f"{measurement} (units unspecified)"


def _png_path(location: str, measurement: str) -> Path:
    """Bound names for Windows and distinguish slug collisions deterministically."""

    def slug(text: str) -> str:
        return re.sub(r"[^A-Za-z0-9_-]+", "_", text).strip("_")[:48] or "unnamed"

    identity = f"{location}\0{measurement}"
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
    return PNG_OUTPUT_DIR / f"po_{slug(location)}_{slug(measurement)}_{digest}.png"


def _save_plot(fig: Figure, location: str, measurement: str) -> None:
    target = _png_path(location, measurement).absolute()
    if target.exists() and not OVERWRITE_PNG:
        msg = f"Plot already exists; set OVERWRITE_PNG=True to replace: {target}"
        raise FileExistsError(msg)
    staged = _new_staged_path(target)
    try:
        fig.savefig(staged, dpi=160, bbox_inches="tight")  # pyright: ignore[reportUnknownMemberType]
        _publish_staged_file(staged, target, overwrite=OVERWRITE_PNG)
    finally:
        staged.unlink(missing_ok=True)
    logger.success("Saved PO plot to {}", target)


def _new_plot_figure() -> tuple[Figure, Axes]:
    """Create Agg-backed headless figures without requiring a desktop display."""
    if SHOW_PLOTS:
        return plt.subplots(figsize=(12, 5))
    fig = Figure(figsize=(12, 5))
    FigureCanvasAgg(fig)
    ax = fig.add_subplot(111)
    return fig, ax


def _draw_measurement_plot(
    measured: pd.DataFrame,
    location: str,
    measurement: str,
    colorize_aep: Callable[[Any], Any],
    color_meta: dict[str, Any],
) -> None:
    fig, ax = _new_plot_figure()
    try:
        fig.subplots_adjust(left=0.067, bottom=0.133, right=0.717, top=0.91)
        series_handles: dict[str, Any] = {}
        for (scenario, run_code), series in measured.groupby(
            ["scenario_label", "internalName"], observed=True, sort=True
        ):
            ordered = series.sort_values("Time")
            label = f"{scenario} | Run={run_code} | Type={measurement}"
            aep_value: Any = series[str(color_meta["column"])].iloc[0]  # pyright: ignore[reportUnknownMemberType]
            color = colorize_aep(aep_value)
            (line,) = ax.plot(  # pyright: ignore[reportUnknownMemberType]
                ordered["Time"], ordered["Value"], label=label, color=color
            )
            series_handles[label] = line

        ax.set_title(f"PO timeseries - {location} - {measurement}")  # pyright: ignore[reportUnknownMemberType]
        ax.set_ylabel(_measurement_ylabel(str(measurement)))  # pyright: ignore[reportUnknownMemberType]
        ax.set_xlabel("Simulation time (h)")  # pyright: ignore[reportUnknownMemberType]
        ax.grid(visible=True)  # pyright: ignore[reportUnknownMemberType]

        labels = sorted(series_handles)
        legend = ax.legend(  # pyright: ignore[reportUnknownMemberType]
            handles=[series_handles[label] for label in labels],
            labels=labels,
            title=SCENARIO_LEGEND_TITLE + " | Run | Type",
            loc="upper left",
            bbox_to_anchor=(1.02, 1),
        )
        ax.add_artist(legend)

        if color_meta["type"] == "numeric":
            scalar_mappable = ScalarMappable(norm=color_meta["norm"], cmap=color_meta["cmap"])
            scalar_mappable.set_array([])
            colorbar = fig.colorbar(  # pyright: ignore[reportUnknownMemberType]
                scalar_mappable, ax=ax, fraction=0.046, pad=0.01
            )
            colorbar.set_label(color_meta.get("label", COLORBAR_LABEL))  # pyright: ignore[reportUnknownMemberType]
        else:
            aep_handles = [
                mpatches.Patch(color=colorize_aep(value), label=str(value)) for value in color_meta["values"]
            ]
            aep_legend = ax.legend(  # pyright: ignore[reportUnknownMemberType]
                handles=aep_handles,
                title=f"{COLORBAR_LABEL} colours",
                loc="lower left",
                bbox_to_anchor=(1.02, 0),
            )
            ax.add_artist(aep_legend)

        if EXPORT_PNG:
            _save_plot(fig, str(location), str(measurement))
        if SHOW_PLOTS:
            plt.show()  # pyright: ignore[reportUnknownMemberType]
    finally:
        plt.close(fig)


def plot_timeseries(df: pd.DataFrame) -> None:
    plot_df: DataFrame = prepare_plot_df(df)
    if plot_df.empty:
        msg = "No PO data to plot after preparation."
        raise RuntimeError(msg)

    if MEASUREMENT_TYPES:
        plot_df = plot_df.loc[plot_df["Type"].isin(MEASUREMENT_TYPES)].copy()
        if plot_df.empty:
            msg = f"MEASUREMENT_TYPES matched no PO types: {MEASUREMENT_TYPES!r}"
            raise ValueError(msg)

    if PLOT_LOCATIONS:
        available = [loc for loc in PLOT_LOCATIONS if loc in plot_df["Location"].unique()]
        if not available:
            msg = f"PLOT_LOCATIONS matched no PO locations: {PLOT_LOCATIONS!r}"
            raise ValueError(msg)
    else:
        available = sorted(plot_df["Location"].unique().tolist())

    subset: DataFrame = plot_df.loc[plot_df["Location"].isin(available)].copy()
    dimension_columns = resolve_dimension_columns(subset)
    subset["scenario_label"] = build_scenario_labels(subset, dimension_columns)
    colorize_aep, color_meta = create_aep_colorizer(subset, dimension_columns)

    for location in available:
        location_subset = subset.loc[subset["Location"] == location]
        if location_subset.empty:
            logger.warning("Configured location {} has no data.", location)
            continue
        for measurement in sorted(location_subset["Type"].unique()):
            measured: DataFrame = cast("DataFrame", location_subset.loc[location_subset["Type"] == measurement])
            _draw_measurement_plot(measured, str(location), str(measurement), colorize_aep, color_meta)


def main() -> None:
    # Maintain the original packaged identity even if this script is copied or renamed.
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=SCRIPT_VERSION,
        wrapper_source="TUFLOW-python/po_and_timeseries/plot_po_scenario_timeseries.py",
    )
    logger.info("PO scenario plot script version {}", SCRIPT_VERSION)
    df: DataFrame = load_po_dataframe()
    logger.info("Selected {:,} PO rows with {} columns.", len(df), len(df.columns))
    plot_timeseries(df)


if __name__ == "__main__":
    main()
