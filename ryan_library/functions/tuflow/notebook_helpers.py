"""
Helper functions to enable easy usage of TUFLOW workflows in Jupyter Notebooks.
"""

from collections.abc import Collection
from pathlib import Path
from typing import Literal

from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.tuflow.tuflow_common import collect_files, process_files_in_parallel, process_file
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.orchestrators.tuflow.tuflow_culverts_mean import find_culvert_aep_dur_mean, find_culvert_aep_mean_max

import pandas as pd
import matplotlib.pyplot as plt

# Default console log level for notebooks
DEFAULT_NOTEBOOK_LOG_LEVEL = "INFO"


def load_tuflow_data(
    paths: list[str] | list[Path],
    data_types: Collection[str],
    parallel: bool = True,
    log_level: str = DEFAULT_NOTEBOOK_LOG_LEVEL,
    locations: Collection[str] | None = None,
) -> ProcessorCollection:
    """
    Load TUFLOW data from the specified paths into a ProcessorCollection.

    This function abstracts away the complexity of setting up multiprocessing loggers
    and configuration loading, making it easier to use in interactive environments
    like Jupyter Notebooks.

    Args:
        paths: List of directory paths to search for files.
        data_types: List of TUFLOW data types to load (e.g., ["Q", "V", "H", "POMM"]).
        parallel: Whether to use parallel processing (multiprocessing). Defaults to True.
        log_level: Logging verbosity. Defaults to "INFO".
        locations: Optional list of location IDs to filter the results by.

    Returns:
        ProcessorCollection: A collection containing the loaded and processed data.
    """
    # Normalize paths to Path objects
    path_objects: list[Path] = [Path(p) for p in paths]

    # Initialize SuffixesConfig (idempotent Singleton)
    suffixes_config = SuffixesConfig.get_instance()

    # Collect files first
    files: list[Path] = collect_files(
        paths_to_process=path_objects,
        include_data_types=data_types,
        suffixes_config=suffixes_config,
    )

    if not files:
        print(f"No files found matching types {data_types} in {paths}")
        return ProcessorCollection()

    # Process files
    collection = ProcessorCollection()

    if parallel:
        # Use the parallel processing helper with a temporary logger context
        with setup_logger(console_log_level=log_level) as log_queue:
            collection = process_files_in_parallel(
                file_list=files,
                log_queue=log_queue,
                log_level=log_level,
            )
    else:
        # Serial processing
        # We still might want to configure basic logging for serial execution if not already set
        # But commonly in notebooks, users might just rely on print or existing logger config.
        # For consistency with the parallel block, we'll process sequentially.

        for file_path in files:
            proc = process_file(file_path=file_path)
            if proc:
                collection.add_processor(proc)

    # Apply location filtering if requested
    if locations:
        collection.filter_locations(locations)

    return collection


def get_critical_hydrographs(collection: ProcessorCollection, metric: str = "Q") -> dict[tuple[str, str], pd.DataFrame]:
    """
    Identify the critical mean runs and extract their full timeseries.

    1. Calculates AEP/Duration means (scalar).
    2. Identifies the critical duration (max mean) for each AEP.
    3. Identifies the specific simulation run that is closest to the mean for that critical duration.
    4. Extracts the timeseries for that run.

    Args:
        collection: The loaded ProcessorCollection (should contain both Maximums and Timeseries data if possible,
                   but at least Maximums for the mean logic and Timeseries for the result).
        metric: The column to optimize for (default "Q").

    Returns:
        dict: Keys are (AEP_Text, Chan_ID), Values are the timeseries DataFrame for the representative run.
    """
    # 1. Combine maximums to get scalar stats
    max_df = collection.combine_1d_maximums()
    if max_df.empty:
        print("No maximums data found.")
        return {}

    # 2. Calculate Mean Stats
    aep_dur_mean = find_culvert_aep_dur_mean(max_df)

    # 3. Find Critical Duration (Max of Mean)
    # This returns one row per AEP/Culvert corresponding to the critical duration
    # It contains 'adopted_run_code' etc if find_culvert_aep_dur_mean logic supports it?
    # Wait, find_culvert_aep_dur_mean currently just finds the row.
    # The 'adopted' columns in `find_culvert_aep_dur_mean` store values like 'adopted_Q', but do they store the run ID?
    # Let's check `tuflow_culverts_mean.py` again.
    # It seems `find_culvert_aep_dur_mean` implementation I read earlier calculates 'adopted_Q' etc but DOES NOT explicitly store the `internalName` of the adopted run.
    # I need to enhance that or re-implement logic here to capture the RUN ID.

    # Re-implementing simplified logic here to ensure we get the Run ID
    return _extract_critical_timeseries(collection, max_df, metric)


def _extract_critical_timeseries(
    collection: ProcessorCollection, max_df: pd.DataFrame, metric: str
) -> dict[tuple[str, str], pd.DataFrame]:

    # map run_code -> processor (timeseries)
    # This allows fast lookup of the timeseries dataframe once we know the run_code
    ts_processors = {p.name_parser.raw_run_code: p for p in collection.processors if p.dataformat == "Timeseries"}

    # Group max_df by AEP, Duration, Chan ID
    # We need to find the mean Q for each group, then find the run closest to it.

    results = {}

    # columns we need
    group_cols = ["aep_text", "Chan ID"]

    # Only work with compatible data
    if metric not in max_df.columns:
        return {}

    # We want to find the "Global Critical" event for each AEP/ChanID combo.
    # Strategy:
    # 1. Calculate Mean Metric for each Duration
    # 2. Find Duration with Max Mean Metric
    # 3. For that Duration, find the individual Run closest to the Mean
    # 4. Return that Run's timeseries

    # Calculate means per duration
    dur_group_cols = ["aep_text", "duration_text", "Chan ID"]
    if not all(c in max_df.columns for c in dur_group_cols):
        print("Missing required grouping columns.")
        return {}

    # Valid numeric rows
    valid_df = max_df.dropna(subset=[metric]).copy()

    # Mean per duration
    means = valid_df.groupby(dur_group_cols)[metric].mean().reset_index(name="mean_metric")

    # Find max mean per AEP/Chan (Critical Duration)
    # idxmax returns the index of the max value
    crit_idx = means.groupby(group_cols)["mean_metric"].idxmax()
    critical_events = means.loc[crit_idx]
    # critical_events has [aep_text, duration_text, Chan ID, mean_metric] representing the critical duration

    # Now, for each critical event, go back to valid_df and find the specific run closest to mean
    for _, row in critical_events.iterrows():
        aep = row["aep_text"]
        dur = row["duration_text"]
        chan = row["Chan ID"]
        target_mean = row["mean_metric"]

        # Filter raw max rows for this specific bucket
        subset = valid_df[
            (valid_df["aep_text"] == aep) & (valid_df["duration_text"] == dur) & (valid_df["Chan ID"] == chan)
        ]

        if subset.empty:
            continue

        # Find row with metric closest to target_mean
        subset = subset.copy()
        subset["diff"] = (subset[metric] - target_mean).abs()
        closest_row = subset.loc[subset["diff"].idxmin()]

        run_code = closest_row["internalName"]

        # Get timeseries
        proc = ts_processors.get(run_code)
        if proc:
            # We filter the big timeseries DF for just this channel
            # Optimisation: The processor DF contains all channels for that run.
            ts_df = proc.df
            if "Chan ID" in ts_df.columns:
                chan_ts = ts_df[ts_df["Chan ID"] == chan].copy()
                if not chan_ts.empty:
                    results[(aep, chan)] = chan_ts

    return results


def plot_hydrographs(hydrographs: dict[tuple[str, str], pd.DataFrame], title: str = "Hydrographs"):
    """
    Plot the provided hydrographs.

    Args:
        hydrographs: Dictionary returned by get_critical_hydrographs.
    """
    if not hydrographs:
        print("No hydrographs to plot.")
        return

    plt.figure(figsize=(10, 6))

    for (aep, chan), df in hydrographs.items():
        # Ensure numeric
        df = df.sort_values("Time")
        label = f"{chan} - {aep}"
        plt.plot(df["Time"], df["Q"], label=label)

    plt.xlabel("Time (h)")
    plt.ylabel("Flow (m³/s)")
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.show()
