# ryan_library/scripts/pomm_max_items.py

from loguru import logger
from pathlib import Path
from datetime import datetime
import pandas as pd

from ryan_library.scripts.pomm_utils import (
    aggregated_from_paths,
    save_peak_report,
    save_peak_report_median,
)
from ryan_library.functions.logging_helpers import setup_logging


def generate_peak_report(pomm_files: list[str], output_path: Path) -> None:
    """Deprecated entry point kept for API compatibility."""
    raise RuntimeError(
        "generate_peak_report has been removed. Use 'run_peak_report_modern' instead."
    )


def run_peak_report(script_directory: Path | None = None) -> None:
    """Run the peak report generation workflow."""
    print()
    print("You are using an old wrapper")
    print()
    run_peak_report_modern()


def run_peak_report_modern(script_directory: Path | None = None) -> None:
    """Deprecated. Use :func:`run_median_peak_report` instead."""

    raise RuntimeError(
        "run_peak_report_modern is deprecated. Use 'POMM-med-max-aep-dur.py' or "
        "'run_median_peak_report' instead."
    )


def run_median_peak_report(
    script_directory: Path | None = None,
    log_level: str | int = "INFO",
) -> None:
    """Locate and process POMM files and export median-based peak values."""

    # Allow log level as string for convenience
    level: int = (
        getattr(logging, log_level.upper(), log_level)
        if isinstance(log_level, str)
        else int(log_level)
    )

    setup_logging(log_level=level)
    logger.info(f"Current Working Directory: {Path.cwd()}")
    if script_directory is None:
        script_directory = Path.cwd()

    aggregated_df: pd.DataFrame = aggregated_from_paths([script_directory])

    if aggregated_df.empty:
        logger.warning("No POMM CSV files found. Exiting.")
        return

    timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")
    save_peak_report_median(
        aggregated_df=aggregated_df,
        script_directory=script_directory,
        timestamp=timestamp,
    )
