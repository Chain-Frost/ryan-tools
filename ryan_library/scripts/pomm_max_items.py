# ryan_library/scripts/pomm_max_items.py

from collections.abc import Collection
from loguru import logger
from pathlib import Path
from datetime import datetime
import pandas as pd

from ryan_library.scripts.pomm_utils import (
    aggregated_from_paths,
    save_peak_report_median,
)
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.processors.tuflow.base_processor import BaseProcessor


def run_peak_report(script_directory: Path | None = None) -> None:
    """Run the peak report generation workflow."""
    print()
    print("You are using an old wrapper")
    print()
    run_median_peak_report()


def run_median_peak_report(
    script_directory: Path | None = None,
    log_level: str = "INFO",
    include_pomm: bool = True,
    locations_to_include: Collection[str] | None = None,
) -> None:
    """Locate and process POMM files and export median-based peak values."""

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
    save_peak_report_median(
        aggregated_df=aggregated_df,
        script_directory=script_directory,
        timestamp=timestamp,
        include_pomm=include_pomm,
    )
