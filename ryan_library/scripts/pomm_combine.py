# ryan_library/scripts/POMM_Combine.py

import logging
from pathlib import Path
import pandas as pd
import multiprocessing
from glob import iglob
from datetime import datetime

from ryan_library.functions.data_processing import (
    safe_apply,
    check_string_TP,
    check_string_duration,
    check_string_aep,
)
from ryan_library.functions.misc_functions import setup_logging, calculate_pool_size


def process_pomm_file(file_path: str) -> pd.DataFrame:
    """
    Process a single POMM CSV file and transform it into a standardized DataFrame.

    This includes renaming columns, type casting, extracting run code parts, and
    calculating additional metrics.

    Args:
        file_path (str): Path to the POMM CSV file.

    Returns:
        pd.DataFrame: Transformed DataFrame with necessary calculations.
                      Returns an empty DataFrame if an error occurs.
    """
    logging.info(f"Processing file: {file_path}")

    try:
        # Load the CSV data into a DataFrame
        original_df = pd.read_csv(filepath_or_buffer=file_path, header=None)

        # Extract RunCode from the top left cell
        run_code = original_df.iat[0, 0]

        # Transpose after dropping the first column
        transposed_df = original_df.drop(columns=0).T

        # Promote the first row to headers
        transposed_df.columns = pd.Index(transposed_df.iloc[0], dtype=str)
        transposed_df = transposed_df.drop(index=transposed_df.index[0])

        # Define new column names and their data types
        column_mappings = {
            "Type": ("Location", "string"),
            "Location": ("Time", "string"),
            "Max": ("Maximum (Extracted from Time Series)", "float"),
            "Tmax": ("Time of Maximum", "float"),
            "Min": ("Minimum (Extracted From Time Series)", "float"),
            "Tmin": ("Time of Minimum", "float"),
        }

        # Rename columns and cast to appropriate data types
        for new_col, (old_col, dtype) in column_mappings.items():
            if old_col in transposed_df.columns:
                transposed_df.rename(columns={old_col: new_col}, inplace=True)
                transposed_df[new_col] = transposed_df[new_col].astype(dtype)

        # Extract additional RunCode parts from the filename and insert as new columns
        run_code_parts = run_code.replace("+", "_").split("_")
        for index, part in enumerate(run_code_parts):
            transposed_df.insert(index, f"RunCode_Part_{index}", str(part))

        # Calculate AbsMax column as the maximum absolute value between 'Max' and 'Min'
        transposed_df["AbsMax"] = transposed_df[["Max", "Min"]].abs().max(axis=1)

        # Calculate SignedAbsMax with the sign of the source data
        transposed_df["SignedAbsMax"] = transposed_df.apply(
            lambda row: (
                row["Max"] if abs(row["Max"]) >= abs(row["Min"]) else row["Min"]
            ),
            axis=1,
        )

        # Extract TP, Duration, and AEP using safe_apply
        transposed_df["TP"] = safe_apply(check_string_TP, run_code)
        transposed_df["Duration"] = safe_apply(check_string_duration, run_code)
        transposed_df["AEP"] = safe_apply(check_string_aep, run_code)
        transposed_df["RunCode"] = run_code

        return transposed_df

    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}", exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def aggregate_pomm_data(pomm_data: list[pd.DataFrame]) -> pd.DataFrame:
    """
    Aggregate a list of DataFrames into a single DataFrame.

    Args:
        pomm_data (list[pd.DataFrame]): List of DataFrames to aggregate.

    Returns:
        pd.DataFrame: Combined DataFrame containing all records.
    """
    # Filter out any empty DataFrames resulting from processing errors
    valid_data_frames = [df for df in pomm_data if not df.empty]

    # Concatenate all valid DataFrames into one
    aggregated_df = pd.concat(valid_data_frames, ignore_index=True)
    return aggregated_df


def main_processing():
    """
    Orchestrate the processing of POMM CSV files.

    Steps:
        1. Set up logging.
        2. Discover all POMM CSV files in the directory and subdirectories.
        3. Determine the pool size for multiprocessing.
        4. Process all files in parallel.
        5. Aggregate the processed data.
        6. Save the aggregated data to an Excel file.
        7. Log the runtime.
    """
    start_time = datetime.now()
    setup_logging()

    # Determine the script directory and change the working directory to it
    script_directory = (
        Path(__file__).resolve().parent.parent.parent
    )  # Adjust based on location
    Path.cwd().joinpath(script_directory).resolve()

    # Find all POMM CSV files recursively
    pomm_files = [
        str(f) for f in iglob("**/*POMM.csv", recursive=True) if Path(f).is_file()
    ]
    logging.info(f"Number of POMM files found: {len(pomm_files)}")

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

    # Generate a timestamped filename for the output Excel file
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    output_filename = f"{timestamp}_POMM.xlsx"
    output_path = Path(script_directory) / output_filename

    # Save the aggregated DataFrame to an Excel file
    aggregated_df.to_excel(output_path, index=False)
    logging.info(f"Aggregated data exported to {output_path}")

    # Log the total runtime
    end_time = datetime.now()
    runtime = end_time - start_time
    logging.info(f"Total run time: {runtime}")
