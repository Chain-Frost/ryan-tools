# ryan_library/scripts/pomm_max_items.py

from loguru import logger
from pathlib import Path
from datetime import datetime
import pandas as pd

from ryan_library.scripts.pomm_utils import (
    aggregated_from_paths,
    save_peak_report,
)
from ryan_library.functions.logging_helpers import setup_logging


def generate_peak_report(pomm_files: list[str], output_path: Path) -> None:
    """Deprecated entry point kept for API compatibility."""
    raise RuntimeError(
        "generate_peak_report has been removed. Use 'run_peak_report_modern' instead."
    )


def run_peak_report(script_directory: Path | None = None) -> None:
    """Run the peak report generation workflow."""
    raise RuntimeError(
        "run_peak_report has been removed. Use the \n"
        "'POMM-max-aep-dur.py' wrapper in ryan-scripts/TUFLOW-python instead."
    )


def run_peak_report_modern(script_directory: Path | None = None) -> None:
    """Locate and process POMM files and export their peak values."""

    setup_logging()
    logger.info(msg=f"Current Working Directory: {Path.cwd()}")
    if script_directory is None:
        script_directory = Path.cwd()

    aggregated_df: pd.DataFrame = aggregated_from_paths([script_directory])

    if aggregated_df.empty:
        logger.warning(msg="No POMM CSV files found. Exiting.")
        return

    timestamp: str = datetime.now().strftime(format="%Y%m%d-%H%M")
    save_peak_report(
        aggregated_df=aggregated_df,
        script_directory=script_directory,
        timestamp=timestamp,
    )
