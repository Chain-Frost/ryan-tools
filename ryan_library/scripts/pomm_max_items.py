# ryan_library/scripts/pomm_max_items.py

import logging
from pathlib import Path
from datetime import datetime
import pandas as pd

from ryan_library.scripts.pomm_combine import (
    process_pomm_file,
    aggregate_pomm_data,
    save_to_excel,
)
from ryan_library.functions.misc_functions import calculate_pool_size
import multiprocessing
from ryan_library.functions.logging_helpers import setup_logging


def find_and_save_peaks(
    aggregated_df: pd.DataFrame,
    script_directory: Path,
    timestamp: str,
    suffix: str = "_peaks.xlsx",
) -> None:
    """
    Find peak records and save them to an Excel file.

    Args:
        aggregated_df (pd.DataFrame): The aggregated DataFrame.
        script_directory (Path): Directory where the Excel file will be saved.
        timestamp (str): Timestamp string for the filename.
        suffix (str): Suffix for the filename. Default is '_peaks.xlsx'.
    """
    aep_dur_max = find_aep_dur_max(aggregated_df)
    aep_max = find_aep_max(aggregated_df)
    output_filename_peaks = f"{timestamp}{suffix}"
    output_path_peaks = script_directory / output_filename_peaks
    save_to_excel(aep_dur_max, aep_max, output_path_peaks)


def generate_peak_report(pomm_files: list[str], output_path: Path) -> None:
    """
    Generate peak reports by processing POMM CSV files, finding peak records,
    and saving them into an Excel file with separate sheets.

    Args:
        pomm_files (list[str]): List of POMM CSV file paths.
        output_path (Path): Path where the Excel file will be saved.
    """
    if not pomm_files:
        logging.warning("No POMM CSV files found. Exiting.")
        return

    # Calculate the number of worker processes
    pool_size = calculate_pool_size(len(pomm_files))

    # Create a multiprocessing pool and process files
    with multiprocessing.Pool(pool_size) as pool:
        pomm_data = pool.map(process_pomm_file, pomm_files)

    # Aggregate all processed DataFrames
    aggregated_df = aggregate_pomm_data(pomm_data)
    logging.info("Aggregated POMM DataFrame head:")
    logging.info(f"\n{aggregated_df.head()}")

    # Find and save peak records
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    find_and_save_peaks(
        aggregated_df=aggregated_df, script_directory=output_path, timestamp=timestamp
    )


def run_peak_report(script_directory: Path | None = None) -> None:
    """
    Run the peak report generation workflow.

    Args:
        script_directory (Path): Directory to search for POMM CSV files and save reports.
    """
    # Set up logging
    setup_logging()
    logging.info(f"Current Working Directory: {Path.cwd()}")
    if script_directory is None:
        script_directory = Path.cwd()

    # Find all POMM CSV files recursively
    pomm_files: list[str] = [
        str(f) for f in script_directory.rglob("**/*POMM.csv") if f.is_file()
    ]
    logging.info(f"Number of POMM files found: {len(pomm_files)}")

    if not pomm_files:
        logging.warning("No POMM CSV files found. Exiting.")
        return

    # Generate a timestamped filename for the output Excel file with peaks
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    output_path_peaks = script_directory

    # Generate the peak report
    generate_peak_report(pomm_files, output_path_peaks)


def find_aep_dur_max(aggregated_df: pd.DataFrame) -> pd.DataFrame:
    try:
        aep_dur_max = aggregated_df.loc[
            aggregated_df.groupby(
                ["AEP", "Duration", "Location", "Type", "TrimmedRunCode"]
            )["AbsMax"].idxmax()
        ].reset_index(drop=True)
        logging.info(
            "Created 'aep_dur_max' DataFrame with peak records for each AEP-Duration-Location-Type-RunCode group."
        )
    except KeyError as e:
        logging.error(f"Missing expected columns for 'aep_dur_max' grouping: {e}")
        aep_dur_max = pd.DataFrame()
    return aep_dur_max


def find_aep_max(aep_dur_max: pd.DataFrame) -> pd.DataFrame:
    try:
        aep_max = aep_dur_max.loc[
            aep_dur_max.groupby(["AEP", "Location", "Type", "TrimmedRunCode"])[
                "AbsMax"
            ].idxmax()
        ].reset_index(drop=True)
        logging.info(
            "Created 'aep_max' DataFrame with peak records for each AEP group."
        )
    except KeyError as e:
        logging.error(f"Missing expected columns for 'aep_max' grouping: {e}")
        aep_max = pd.DataFrame()
    return aep_max
