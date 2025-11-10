# ryan_library/scripts/pomm_max_items.py

from collections.abc import Collection, Callable
import warnings
from loguru import logger
from pathlib import Path
from datetime import datetime
import pandas as pd

from ryan_library.scripts.pomm_utils import (
    aggregated_from_paths,
    save_peak_report_mean,
    save_peak_report_median,
)
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.processors.tuflow.base_processor import BaseProcessor


def run_peak_report(script_directory: Path | None = None) -> None:
    """Legacy entry point retained for backwards compatibility."""

    print()
    print("You are using an old wrapper")
    print()
    export_median_peak_report(script_directory=script_directory)


def generate_peak_report(
    *,
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
    report_saver: Callable[..., None],
) -> None:
    """Run the shared peak report workflow and delegate saving to ``report_saver``."""

    setup_logger(console_log_level=log_level)
    logger.info(f"Current Working Directory: {Path.cwd()}")
    if script_directory is None:
        script_directory = Path.cwd()

    normalized_locations: frozenset[str] = BaseProcessor.normalize_locations(locations_to_include)

    if locations_to_include and not normalized_locations:
        logger.warning("Location filter provided but no valid values found. All locations will be included.")

    aggregated_df: pd.DataFrame = aggregated_from_paths(
        paths=[script_directory],
        locations_to_include=normalized_locations if normalized_locations else None,
    )

    if aggregated_df.empty:
        if normalized_locations:
            logger.warning("No rows remain after applying the Location filter. Exiting.")
        else:
            logger.warning("No POMM CSV files found. Exiting.")
        return

    timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")
    report_saver(
        aggregated_df=aggregated_df,
        script_directory=script_directory,
        timestamp=timestamp,
        include_pomm=include_pomm,
    )


def export_median_peak_report(
    *,
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
) -> None:
    """Locate and process POMM files and export median-based peak values."""

    generate_peak_report(
        script_directory=script_directory,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
        report_saver=save_peak_report_median,
    )


def export_mean_peak_report(
    *,
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
) -> None:
    """Locate and process POMM files and export mean-based peak values."""

    generate_peak_report(
        script_directory=script_directory,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
        report_saver=save_peak_report_mean,
    )


def run_median_peak_report(
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
) -> None:
    """Deprecated wrapper around :func:`export_median_peak_report`."""

    warnings.warn(
        "run_median_peak_report is deprecated; use export_median_peak_report instead.",
        DeprecationWarning,
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
    """Deprecated wrapper around :func:`export_mean_peak_report`."""

    warnings.warn(
        "run_mean_peak_report is deprecated; use export_mean_peak_report instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    export_mean_peak_report(
        script_directory=script_directory,
        log_level=log_level,
        include_pomm=include_pomm,
        locations_to_include=locations_to_include,
    )
