from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Callable

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.cm import ScalarMappable
from matplotlib.colors import Normalize
import pandas as pd
from loguru import logger
from pandas import DataFrame, Index, Series

from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

# --------------------------------------------------------------------------------------
# Configuration – tweak these constants to target different runs or behaviours.
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
SCENARIO_DIMENSION_MAP: dict[str, tuple[str, ...]] = {dimension: options for dimension, options in SCENARIO_DIMENSIONS}
SCENARIO_VALUE_PLACEHOLDER = "Unknown"
SCENARIO_LEGEND_TITLE = "Scenario (AEP | TP | DUR)"
COLORBAR_LABEL = "AEP"


def find_po_files(root: Path) -> list[Path]:
    if not root.is_dir():
        raise FileNotFoundError(f"TUFLOW root does not exist: {root}")
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


def filter_internal_name(df: pd.DataFrame) -> pd.DataFrame:
    if not INTERNAL_NAME_FILTER or "internalName" not in df.columns:
        return df
    mask: Series[bool] = (
        df["internalName"]
        .fillna(value="")  # pyright: ignore[reportUnknownMemberType]
        .str.contains(pat=INTERNAL_NAME_FILTER, case=False, na=False)
    )
    filtered: DataFrame = df.loc[mask].copy()
    if filtered.empty:
        logger.warning("Substring '{}' removed all rows; returning original frame.", INTERNAL_NAME_FILTER)
        return df
    return filtered


def load_po_dataframe() -> pd.DataFrame:
    if LOAD_FROM_PARQUET:
        if not PARQUET_PATH.is_file():
            raise FileNotFoundError(f"Parquet file not found at {PARQUET_PATH}")
        logger.info("Loading cached parquet {}", PARQUET_PATH)
        df: DataFrame = pd.read_parquet(path=PARQUET_PATH)
        return filter_internal_name(df=df)

    files: list[Path] = find_po_files(root=TUFLOW_RESULTS_ROOT)
    if not files:
        raise RuntimeError("No PO files matched the specified pattern.")

    logger.info("Sequentially processing {} file(s)...", len(files))
    collection: ProcessorCollection = process_files_sequentially(files)
    if not collection.processors:
        raise RuntimeError("Processing finished but produced no data.")

    combined: DataFrame = collection.po_combine()
    if combined.empty:
        raise RuntimeError("Combined DataFrame is empty.")

    filtered: DataFrame = filter_internal_name(combined)
    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    filtered.to_parquet(PARQUET_PATH, index=False)
    logger.success("Saved {:,} filtered rows to {}", len(filtered), PARQUET_PATH)
    return filtered


def prepare_plot_df(df: pd.DataFrame) -> pd.DataFrame:
    plot_df: DataFrame = df.copy()
    if "Time" in plot_df.columns:
        if pd.api.types.is_object_dtype(arr_or_dtype=plot_df["Time"]) or pd.api.types.is_string_dtype(
            arr_or_dtype=plot_df["Time"]
        ):
            plot_df["Time"] = pd.to_datetime(arg=plot_df["Time"], errors="coerce")
            plot_df = plot_df.dropna(subset=["Time"])
    else:
        logger.warning("Time column missing; plotting against row index.")
        plot_df["Time"] = pd.RangeIndex(start=0, stop=len(plot_df), step=1)
    return plot_df


def resolve_dimension_columns(df: pd.DataFrame) -> dict[str, str]:
    resolved_columns: dict[str, str] = {}
    missing_specs: list[str] = []

    for display_name, options in SCENARIO_DIMENSIONS:
        match = next((option for option in options if option in df.columns), None)
        if match:
            resolved_columns[display_name] = match
        else:
            option_list = " | ".join(options)
            missing_specs.append(f"{display_name} ({option_list})")

    if missing_specs:
        raise KeyError(
            "DataFrame is missing required scenario columns for: "
            + ", ".join(missing_specs)
            + ". Unable to build plot groupings."
        )
    return resolved_columns


def build_scenario_labels(df: pd.DataFrame, dimension_columns: dict[str, str] | None = None) -> pd.Series:
    resolved_columns = dimension_columns or resolve_dimension_columns(df)

    scenario_components: list[Series] = []
    for display_name, _ in SCENARIO_DIMENSIONS:
        column_name = resolved_columns[display_name]
        component = df[column_name].fillna(SCENARIO_VALUE_PLACEHOLDER).astype(str).rename(display_name)
        scenario_components.append(component)

    scenario_frame: DataFrame = pd.concat(scenario_components, axis=1)
    return scenario_frame.apply(
        lambda row: " | ".join(f"{column}={row[column]}" for column in scenario_frame.columns),
        axis=1,
    )


def _numeric_dimension_column(df: pd.DataFrame, dimension_name: str) -> str | None:
    options = SCENARIO_DIMENSION_MAP[dimension_name]
    for option in options:
        if option in df.columns and pd.api.types.is_numeric_dtype(df[option]):
            return option
    return None


def create_aep_colorizer(
    df: pd.DataFrame, dimension_columns: dict[str, str]
) -> tuple[Callable[[Any], Any], dict[str, Any]]:
    aep_label_column = dimension_columns["AEP"]
    numeric_column = _numeric_dimension_column(df, "AEP")
    source_column = numeric_column or aep_label_column
    aep_series = pd.to_numeric(df[source_column], errors="coerce") if numeric_column else df[source_column]

    if numeric_column:
        valid_values = aep_series.dropna()
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
                numeric_value = pd.to_numeric(value, errors="coerce")
                if pd.isna(numeric_value):
                    return "black"
                return cmap(norm(float(numeric_value)))

            meta = {"type": "numeric", "norm": norm, "cmap": cmap, "column": source_column, "label": COLORBAR_LABEL}
            return colorize, meta

    aep_strings = df[aep_label_column].fillna(SCENARIO_VALUE_PLACEHOLDER).astype(str)
    unique_values = pd.Index(sorted(aep_strings.unique()))
    total = max(len(unique_values), 1)
    cmap = plt.get_cmap("viridis", total)
    color_lookup = {value: cmap(idx) for idx, value in enumerate(unique_values)}

    def colorize(value: Any) -> Any:
        key = SCENARIO_VALUE_PLACEHOLDER if pd.isna(value) else str(value)
        return color_lookup.get(key, "black")

    meta = {"type": "categorical", "values": unique_values, "column": aep_label_column}
    return colorize, meta


def plot_timeseries(df: pd.DataFrame) -> None:
    plot_df: DataFrame = prepare_plot_df(df)
    if plot_df.empty:
        raise RuntimeError("No data to plot after preparation.")

    if PLOT_LOCATIONS:
        available: list[str] = [loc for loc in PLOT_LOCATIONS if loc in plot_df["Location"].dropna().unique()]
        if not available:
            raise RuntimeError("Configured plot locations are missing from the dataset.")
    else:
        available = list(plot_df["Location"].dropna().unique())

    subset: DataFrame = plot_df[plot_df["Location"].isin(values=available)].copy()
    if subset.empty:
        raise RuntimeError("Plot subset is empty.")

    try:
        dimension_columns = resolve_dimension_columns(subset)
        subset["scenario_label"] = build_scenario_labels(subset, dimension_columns)
        colorize_aep, color_meta = create_aep_colorizer(subset, dimension_columns)
    except KeyError as exc:
        raise RuntimeError("Unable to build scenario groupings for plotting.") from exc

    for location in available:
        location_subset = subset[subset["Location"] == location]
        if location_subset.empty:
            logger.warning("Location '{}' configured for plotting but has no available data.", location)
            continue

        fig, ax = plt.subplots(figsize=(12, 5))
        fig.subplots_adjust(left=0.067, bottom=0.133, right=0.717, top=0.91)
        scenario_handles: dict[str, Any] = {}
        for scenario_label, scenario_group in location_subset.groupby("scenario_label"):
            sorted_group = scenario_group.sort_values("Time")
            aep_value = scenario_group[color_meta["column"]].iloc[0]
            color = colorize_aep(aep_value)
            (line,) = ax.plot(
                sorted_group["Time"],
                sorted_group["Value"],
                label=scenario_label,
                color=color,
            )
            scenario_handles.setdefault(scenario_label, line)

        ax.set_title(label=f"PO timeseries – {location}")
        ax.set_ylabel(ylabel="Q (Value)")
        ax.set_xlabel(xlabel="Time")
        ax.grid(visible=True)
        fig.autofmt_xdate()

        legend_labels: list[str] = sorted(scenario_handles.keys())
        scenario_legend = ax.legend(
            handles=[scenario_handles[label] for label in legend_labels],
            labels=legend_labels,
            title=SCENARIO_LEGEND_TITLE,
            loc="upper left",
            bbox_to_anchor=(1.02, 1),
        )
        ax.add_artist(scenario_legend)

        if color_meta["type"] == "numeric":
            scalar_mappable = ScalarMappable(norm=color_meta["norm"], cmap=color_meta["cmap"])
            scalar_mappable.set_array([])
            colorbar = fig.colorbar(scalar_mappable, ax=ax, fraction=0.046, pad=0.01)
            colorbar.set_label(color_meta.get("label", COLORBAR_LABEL))
        else:
            aep_handles = [
                mpatches.Patch(color=colorize_aep(value), label=str(value)) for value in color_meta["values"]
            ]
            aep_legend = ax.legend(
                handles=aep_handles,
                title=f"{COLORBAR_LABEL} colours",
                loc="lower left",
                bbox_to_anchor=(1.02, 0),
            )
            ax.add_artist(aep_legend)

        plt.show()


def main() -> None:
    repo_root: Path = Path(__file__).resolve().parents[1]
    data_root: Path = TUFLOW_RESULTS_ROOT.resolve()
    try:
        common_root = Path(os.path.commonpath([str(repo_root), str(data_root)]))
    except ValueError:
        common_root: Path = repo_root
    os.chdir(common_root)
    df: DataFrame = load_po_dataframe()
    logger.info("DataFrame ready with {:,} rows and {} columns.", len(df), len(df.columns))
    plot_timeseries(df)


if __name__ == "__main__":
    main()
