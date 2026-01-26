# ryan_library/orchestrators/pomm_max_items.py
"""
POMM Peak Reporting Utilities.

This module provides functions to generate "Peak Reports" from POMM (Plot Output Maximums/Minimums) data.
It allows extracting Mean or Median peak values across multiple events/durations for specific locations.
It includes both modern workflow functions and deprecated wrappers for backward compatibility.
"""

from collections.abc import Callable, Collection
from datetime import datetime
from pathlib import Path
import warnings

import pandas as pd
from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.functions.tuflow.pomm_utils import (
    aggregated_from_paths,
    save_peak_report_mean,
    save_peak_report_median,
)
from ryan_library.functions.tuflow.wrapper_helpers import normalize_data_types, warn_on_invalid_types

DEFAULT_DATA_TYPES: tuple[str, ...] = ("POMM", "RLL_Qmx")
ACCEPTED_DATA_TYPES: frozenset[str] = frozenset(DEFAULT_DATA_TYPES)


def run_peak_report(script_directory: Path | None = None) -> None:
    """
    Legacy entry point retained for backwards compatibility.

    .. deprecated::
       Use `export_median_peak_report` instead.
    """

    print()
    print("You are using an old wrapper")
    print()
    export_median_peak_report(script_directory=script_directory)


def run_peak_report_workflow(
    *,
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
    include_data_types: Collection[str] | None = None,
    exporter: Callable[..., None],
) -> None:
    """
    Coordinate loading peak data and exporting via a provided `exporter` function.

    This is the core workflow function that:
      1. Normalizes input arguments.
      2. Agreggates data from multiple paths using `aggregated_from_paths`.
      3. Invokes the callback `exporter` (e.g., save mean or median report) with the aggregated data.

    Args:
        script_directory: The root directory to search for files. Defaults to CWD.
        log_level: Logging verbosity level ("INFO", "DEBUG").
        include_pomm: Whether to include the full POMM sheet in the export.
        locations_to_include: List of specific locations to filter by.
        include_data_types: List of data types to include (e.g. "POMM").
        exporter: A callback function that takes the aggregated DataFrame and handles the export logic.
    """

    script_directory = script_directory or Path.cwd()
    resolved_data_types, invalid_types = normalize_data_types(
        requested=include_data_types,
        default=DEFAULT_DATA_TYPES,
        accepted=ACCEPTED_DATA_TYPES,
    )
    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations=locations_to_include)
    location_filter: frozenset[str] | None = normalized_locations if normalized_locations else None

    with setup_logger(console_log_level=log_level) as log_queue:
        logger.info(f"Current Working Directory: {Path.cwd()}")

        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context="POMM peak report",
        )

        if locations_to_include and not normalized_locations:
            logger.warning("Location filter provided but no valid values found. All locations will be included.")

        # Aggregate data from all found files in the directory
        aggregated_df: pd.DataFrame = aggregated_from_paths(
            paths=[script_directory],
            locations_to_include=location_filter,
            include_data_types=resolved_data_types,
            log_queue=log_queue,
        )

        if aggregated_df.empty:
            warn_on_invalid_types(
                invalid_types=invalid_types,
                accepted_types=ACCEPTED_DATA_TYPES,
                context="POMM peak report completed",
            )
            if location_filter:
                logger.warning("No rows remain after applying the Location filter. Exiting.")
            else:
                logger.warning("No POMM CSV files found. Exiting.")
            return

        timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")

        # Execute the export callback
        exporter(
            aggregated_df=aggregated_df,
            script_directory=script_directory,
            timestamp=timestamp,
            include_pomm=include_pomm,
        )

        warn_on_invalid_types(
            invalid_types=invalid_types,
            accepted_types=ACCEPTED_DATA_TYPES,
            context="POMM peak report completed",
        )


def export_median_peak_report(
    *,
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
    include_data_types: Collection[str] | None = None,
) -> None:
    """
    Locate and process POMM files and export **median-based** peak values.

    This function is a wrapper around `run_peak_report_workflow` injecting the `save_peak_report_median` exporter.
    """

    run_peak_report_workflow(
        script_directory=script_directory,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
        include_data_types=include_data_types,
        exporter=save_peak_report_median,
    )


def export_mean_peak_report(
    *,
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
    include_data_types: Collection[str] | None = None,
) -> None:
    """
    Locate and process POMM files and export **mean-based** peak values.

    This function is a wrapper around `run_peak_report_workflow` injecting the `save_peak_report_mean` exporter.
    """

    run_peak_report_workflow(
        script_directory=script_directory,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
        include_data_types=include_data_types,
        exporter=save_peak_report_mean,
    )


def run_median_peak_report(
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
) -> None:
    """
    Deprecated wrapper around :func:`export_median_peak_report`.

    .. deprecated::
       Use `export_median_peak_report` instead.
    """

    warnings.warn(
        message="run_median_peak_report is deprecated; use export_median_peak_report instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    export_median_peak_report(
        script_directory=script_directory,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
    )


def run_mean_peak_report(
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
) -> None:
    """
    Deprecated wrapper around :func:`export_mean_peak_report`.

    .. deprecated::
       Use `export_mean_peak_report` instead.
    """

    warnings.warn(
        message="run_mean_peak_report is deprecated; use export_mean_peak_report instead.",
        category=DeprecationWarning,
        stacklevel=2,
    )
    export_mean_peak_report(
        script_directory=script_directory,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
    )
