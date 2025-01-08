# ryan_library/scripts/pomm_combine.py

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
from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.functions.logging_helpers import setup_logging


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
        run_code_parts: list[str] = run_code.replace("+", "_").split("_")
        for index, part in enumerate(run_code_parts, start=1):
            transposed_df.insert(
                index, f"R{index:02}", str(part)
            )  # Use zero-padded index

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

        # Clean RunCode by removing AEP, Duration, and TP
        transposed_df["TrimmedRunCode"] = transposed_df.apply(
            lambda row: clean_runcode(
                run_code=row["RunCode"],
                aep=row["AEP"] + "p" if row["AEP"] is not None else None,
                duration=row["Duration"] + "m" if row["Duration"] is not None else None,
                tp="TP" + row["TP"] if row["TP"] is not None else None,
            ),
            axis=1,
        )

        return transposed_df

    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}", exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame in case of error


def clean_runcode(
    run_code: str, aep: str | None, duration: str | None, tp: str | None
) -> str:
    """
    Cleans the RunCode by removing the extracted AEP, Duration, and TP values.

    Args:
        run_code (str): The original RunCode string.
        aep (Optional[str]): The extracted AEP value to remove.
        duration (Optional[str]): The extracted Duration value to remove.
        tp (Optional[str]): The extracted TP value to remove.

    Returns:
        str: The cleaned RunCode string.
    """
    if not isinstance(run_code, str):
        logging.error(f"RunCode must be a string. Received type: {type(run_code)}")
        return ""

    # Replace '+' with '_' to standardize delimiters
    standardized_runcode = run_code.replace("+", "_")
    parts = standardized_runcode.split("_")

    # List of parts to remove
    parts_to_remove = set()
    if aep:
        parts_to_remove.add(aep)
    if duration:
        parts_to_remove.add(duration)
    if tp:
        parts_to_remove.add(tp)

    # Filter out the unwanted parts
    filtered_parts = [
        part for part in parts if part not in parts_to_remove and part.strip() != ""
    ]

    cleaned_runcode = "_".join(filtered_parts)
    logging.debug(f"Original RunCode: {run_code}, Cleaned RunCode: {cleaned_runcode}")

    return cleaned_runcode


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


def save_to_excel(aep_dur_max: pd.DataFrame, aep_max: pd.DataFrame, output_path: Path):
    """
    Save the provided DataFrames to an Excel file with separate sheets.

    Args:
        aep_dur_max (pd.DataFrame): DataFrame for 'aep-dur-max' sheet.
        aep_max (pd.DataFrame): DataFrame for 'aep-max' sheet.
        output_path (Path): Path where the Excel file will be saved.
    """
    try:
        logging.info(f"Output path: {output_path}")
        with pd.ExcelWriter(output_path) as writer:
            aep_dur_max.to_excel(
                writer, sheet_name="aep-dur-max", index=False, merge_cells=False
            )
            aep_max.to_excel(
                writer, sheet_name="aep-max", index=False, merge_cells=False
            )
        logging.info(f"Peak data exported to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save peak data to Excel: {e}")


def save_aggregated_data(aggregated_df: pd.DataFrame, output_path: Path):
    """
    Save the aggregated DataFrame to an Excel file.

    Args:
        aggregated_df (pd.DataFrame): The aggregated DataFrame to save.
        output_path (Path): Path where the Excel file will be saved.
    """
    try:
        aggregated_df.to_excel(output_path, index=False)
        logging.info(f"Aggregated data exported to {output_path}")
    except Exception as e:
        logging.error(f"Failed to save aggregated data to Excel: {e}")


def process_all_files(pomm_files: list[str]) -> pd.DataFrame:
    """
    Process all POMM files and aggregate the data.

    Args:
        pomm_files (list[str]): List of POMM CSV file paths.

    Returns:
        pd.DataFrame: Aggregated DataFrame containing all records.
    """
    if not pomm_files:
        logging.warning("No POMM CSV files found. Exiting.")
        return pd.DataFrame()

    # Calculate the number of worker processes
    pool_size = calculate_pool_size(len(pomm_files))

    # Create a multiprocessing pool and process files
    with multiprocessing.Pool(pool_size) as pool:
        pomm_data = pool.map(process_pomm_file, pomm_files)

    # Aggregate all processed DataFrames
    aggregated_df = aggregate_pomm_data(pomm_data)
    logging.info("Aggregated POMM DataFrame head:")
    logging.info(f"\n{aggregated_df.head()}")
    return aggregated_df


def main_processing():
    """
    Orchestrate the processing of POMM CSV files for legacy scripts.

    Steps:
        1. Set up logging.
        2. Discover all POMM CSV files in the directory and subdirectories.
        3. Process all files in parallel.
        4. Aggregate the processed data.
        5. Save the aggregated data to an Excel file.
        6. Log the runtime.
    """
    start_time = datetime.now()
    setup_logging()

    script_directory = Path.cwd().resolve()

    # Find all POMM CSV files recursively
    pomm_files = [
        str(f) for f in iglob("**/*POMM.csv", recursive=True) if Path(f).is_file()
    ]
    logging.info(f"Number of POMM files found: {len(pomm_files)}")

    # Process all files and aggregate data
    aggregated_df = process_all_files(pomm_files)

    # Save the aggregated DataFrame to an Excel file
    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    output_filename_aggregated = f"{timestamp}_POMM.xlsx"
    output_path_aggregated = script_directory / output_filename_aggregated
    save_aggregated_data(aggregated_df, output_path_aggregated)

    # Log the total runtime
    end_time = datetime.now()
    runtime = end_time - start_time
    logging.info(f"Total run time: {runtime}")


if __name__ == "__main__":
    main_processing()
