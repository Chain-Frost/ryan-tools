import logging
import multiprocessing
from functools import partial
from glob import iglob
from pathlib import Path
from datetime import datetime

import pandas as pd

from ryan_library.functions.misc_functions import (
    setup_logging,
    calculate_pool_size,
)


def read_rorb_hydrograph_csv(filepath: Path) -> pd.DataFrame:
    """
    Reads a RORB hydrograph CSV file with predefined parameters.

    Always skips the first two rows, uses ',' as separator,
    always drops the first column.

    Args:
        filepath (Path): Path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame after processing.
    """
    try:
        # Read the CSV file, skipping the first two rows
        df: pd.DataFrame = pd.read_csv(filepath, sep=",", skiprows=2, header=0)
        # Drop the first column which is presumably an index or unnecessary
        df.drop(df.columns[0], axis=1, inplace=True)
        return df
    except FileNotFoundError:
        logging.error(f"CSV file not found: {filepath}", exc_info=True)
    except pd.errors.ParserError as e:
        logging.error(f"Parsing error in CSV file {filepath}: {e}", exc_info=True)
    except Exception as e:
        logging.error(f"Unexpected error reading CSV file {filepath}: {e}", exc_info=True)
    return pd.DataFrame()


def calculate_central_tendency_statistics(
    freqdb: pd.DataFrame, col_for_stat_calc: str, TP_col: str, duration_col: str
) -> list[float | int | None]:
    """
    Calculate statistics for the frequency database.

    The function finds the duration group with the highest central value of the specified statistic.
    The central value is defined as the value at position n // 2 when the group's values are sorted.
    This allows selecting a specific occurrence (e.g., 5th highest value in a group of 10).

    Args:
        freqdb (pd.DataFrame): DataFrame containing the data grouped by duration.
        col_for_stat_calc (str): The name of the column to perform statistics on.
        TP_col (str): The name of the time pattern column.
        duration_col (str): The name of the duration column.

    Returns:
        list: A list containing:
            - central_value (float): The central value of the statistic.
            - Tcrit (Any): The critical duration corresponding to the highest central value.
            - Tpcrit (Any): The time pattern corresponding to the central value.
            - low (float): The lowest value of the statistic in the group.
            - high (float): The highest value of the statistic in the group.
            - avg_value (float): The average value of the statistic in the group.
            - closest_Tpcrit (Any): The time pattern corresponding to the value closest to the average.
            - closest_value (float): The value of the statistic closest to the average.
    """
    max_central_value = -float("inf")
    Tcrit = Tpcrit = low = high = avg_value = closest_Tpcrit = closest_value = None

    # Group the DataFrame by the duration column
    for duration, group in freqdb.groupby(duration_col):
        # Sort the group by the statistic column
        group_sorted = group.sort_values(col_for_stat_calc).reset_index(drop=True)
        n = len(group_sorted)

        if n == 0:
            continue  # Skip empty groups

        # Calculate the central index (selecting the higher middle item if even number of elements)
        central_index = n // 2

        # Get the central value
        central_value = group_sorted.iloc[central_index][col_for_stat_calc]

        # Update if this group has a higher central value
        if central_value > max_central_value:
            max_central_value = central_value
            Tcrit = duration
            low = group[col_for_stat_calc].min()
            high = group[col_for_stat_calc].max()
            avg_value = group[col_for_stat_calc].mean()

            # Time pattern corresponding to the central value
            Tpcrit = group_sorted.iloc[central_index][TP_col]

            # Find the value closest to the average value
            idx_closest = (group[col_for_stat_calc] - avg_value).abs().idxmin()
            closest_Tpcrit = group.loc[idx_closest, TP_col]
            closest_value = group.loc[idx_closest, col_for_stat_calc]

    return [
        max_central_value,
        Tcrit,
        Tpcrit,
        low,
        high,
        avg_value,
        closest_Tpcrit,
        closest_value,
    ]


def parse_summary_section(raw_summary: list[str]) -> dict[str, str]:
    """
    Process the summary section from batch output.

    Args:
        raw_summary (list[str]): List of strings containing the summary section.

    Returns:
        dict[str, str]: Dictionary mapping hyd number to description.
    """
    summary_dict: dict[str, str] = {}
    for entry in raw_summary[1:]:
        parts = entry.split(" ", 1)
        if len(parts) < 2:
            logging.warning(f"Malformed summary line skipped: {entry}")
            continue  # Skip malformed lines
        rnum, description = parts
        padded = f"Hyd{rnum.zfill(4)}"
        summary_dict[padded] = description.strip()
    return summary_dict


def construct_csv_path(batchout: Path, aep_part: str, duration_part: str, tpat: int) -> Path:
    """
    Construct the CSV file path based on batch output.

    Args:
        batchout (Path): Batch output file name.
        aep_part (str): AEP part of the file name.
        duration_part (str): Duration part of the file name.
        tpat (int): Time pattern number.

    Returns:
        Path: Constructed CSV file path.
    """
    aep = aep_part.replace(".", "p")
    du = duration_part.replace(".", "_")
    # Remove 'batch.out' and prepare the base path
    base_name = batchout.name.replace("batch.out", "")
    # Construct the second part of the filename
    second_part = f" {aep}_du{du}tp{tpat}.csv"
    # Combine to form the full CSV path
    return batchout.parent / (base_name + second_part)


def parse_run_line(line: str, batchout_file: Path) -> list | None:
    """
    Parse a single run line and return the processed data.

    Args:
        line (str): The line from the batchout file.
        batchout_file (Path): Path to the batchout file.

    Returns:
        Optional[list]: Processed run data or None if invalid.
    """
    raw_line = line.strip().split()
    if len(raw_line) < 10:
        logging.warning(f"Invalid run line skipped: {line.strip()}")
        return None

    try:
        # Clean and prepare run data
        raw_line[3] = raw_line[3].strip("%")  # Remove '%' from AEP
        duration_part = raw_line[1] + raw_line[2]
        aep_part = f"aep{raw_line[3]}"
        # Convert 'Y'/'N' to '1'/'0' for the TPat filtering
        raw_line[6] = "1" if raw_line[6].upper() == "Y" else "0"

        # Convert duration to hours if it's in minutes
        if raw_line[2].lower() != "hour":
            try:
                raw_line[1] = float(raw_line[1]) / 60
            except ValueError as e:
                logging.error(f"Error converting duration: {e}")
                return None

        raw_line.pop(2)  # Remove 'hour' or 'min' label

        # Convert relevant fields to appropriate types
        processed_line = []
        for i, el in enumerate(raw_line):
            if i in [0, 3]:  # Assuming Run and TPat are integers
                processed_line.append(int(el))
            else:
                processed_line.append(float(el))

        # Construct the corresponding CSV path
        csv_path = construct_csv_path(
            batchout=batchout_file,
            aep_part=aep_part,
            duration_part=duration_part,
            tpat=processed_line[3],
        )
        # Add CSV path to the run data
        processed_line.append(str(csv_path))
        return processed_line
    except ValueError as e:
        logging.error(f"Error converting line to floats: {raw_line} - {e}")
        return None


def parse_batch_output(batchout_file: Path) -> pd.DataFrame:
    """
    Parse a batch output file to extract run data and parameters.

    Args:
        batchout_file (Path): Path to the batch output file.

    Returns:
        pd.DataFrame: DataFrame containing extracted data.
    """
    basename = batchout_file.name
    try:
        with batchout_file.open("r") as f:
            IL, CL, ROC = (
                0.0,
                0.0,
                1.0,
            )  # Initial Loss, Continuation Loss, Runoff Coefficient
            kc, m = 0.0, 0.0  # Parameters kc and m
            raw_summary: list[str] = []  # Raw summary lines
            rorb_runs: list[list] = []  # List to store run data
            rorb_runs_header: list[str] = []  # Header for run data
            lp_indicator = 0  # Loss Parameter indicator
            found_results = 0  # State variable to track parsing progress

            for line in f:
                if found_results == 20:
                    continue  # Skip lines after reaching the end of relevant data
                elif found_results == 0:
                    # Parsing initial parameters
                    if lp_indicator > 0:
                        # Parsing loss parameters
                        try:
                            values = list(map(float, line.strip().split()))
                            IL = values[0]
                            if lp_indicator == 1:  # CL
                                CL = values[1]
                            elif lp_indicator == 2:  # ROC
                                ROC = values[1]
                        except (IndexError, ValueError) as e:
                            logging.error(f"Error parsing loss parameters: {e}", exc_info=True)
                        lp_indicator = 0  # Reset LP after parsing
                    elif "Parameters" in line:
                        # Parsing 'Parameters' line for kc and m
                        parts = line.strip().split()
                        try:
                            kc = float(parts[3])
                            m = float(parts[-1])
                        except (IndexError, ValueError) as e:
                            logging.error(f"Error parsing Parameters line: {e}", exc_info=True)
                    elif "Loss parameters" in line:
                        # Determine which loss parameter to parse next
                        lp_indicator = 1 if "Cont" in line else 2
                    elif "Peak  Description" in line:
                        # Start of summary section
                        raw_summary = ["rorbNum Description Location"]
                        found_results = 1
                elif found_results == 1:
                    # Parsing summary lines
                    if len(line.strip()) > 3:
                        raw_summary.append(line.strip())
                    else:
                        # End of summary section
                        summary_dict = parse_summary_section(raw_summary)
                        found_results = 10
                elif found_results == 10:
                    # Parsing run data
                    if "Run,    Representative hydrograph" in line:
                        # Reached the end of run data
                        found_results = 20
                    elif " Run        Duration             AEP  " in line:
                        # Header line for runs
                        rorb_runs_header = line.strip().split()
                        rorb_runs_header.append("csv")  # Add 'csv' as an additional column
                    else:
                        # Parsing individual run lines
                        run_data = parse_run_line(line, batchout_file)
                        if run_data:
                            rorb_runs.append(run_data)

            if not rorb_runs:
                logging.warning(f"No valid runs found in {batchout_file}")
                return pd.DataFrame()

            # Create DataFrame from run data
            batch_df = pd.DataFrame(rorb_runs, columns=rorb_runs_header)

            # Add additional parameters to the DataFrame
            batch_df["IL"] = IL
            batch_df["CL"] = CL
            batch_df["m"] = m
            batch_df["kc"] = kc
            batch_df["ROC"] = ROC
            batch_df["file"] = basename
            batch_df["folder"] = str(batchout_file.parent)
            batch_df["Path"] = str(batchout_file)

            return batch_df

    except Exception as e:
        logging.error(f"Error processing batchout file {batchout_file}: {e}", exc_info=True)
        return pd.DataFrame()


def analyze_hydrograph(rline: tuple[str, str, int, str, str], qcheck_list: list[float]) -> pd.DataFrame:
    """
    Analyze hydrograph CSV files to determine durations exceeding specified thresholds.

    Args:
        rline (tuple[str, str, int, str, str]): Tuple containing (AEP, Duration, TP, out_path, csv_read).
        qcheck_list (list[float]): List of threshold flows to check.

    Returns:
        pd.DataFrame: DataFrame containing duration exceeding thresholds.
    """
    aep, dur, tp, out_path, csv_read = rline
    # Initialize an empty DataFrame with specified columns
    durexcdb = pd.DataFrame(
        columns=[
            "AEP",
            "Duration",
            "TP",
            "Location",
            "ThresholdFlow",
            "Duration_Exceeding",
            "out_path",
        ]
    )
    try:
        hydrographs = read_rorb_hydrograph_csv(Path(csv_read))
        if hydrographs.empty:
            logging.info(f"No data in hydrograph CSV: {csv_read}")
            return durexcdb  # Return empty DataFrame if CSV couldn't be read

        # Clean up column names by removing the prefix
        hydrographs.columns = [col.replace("Calculated hydrograph:  ", "") for col in hydrographs.columns]

        # Ensure 'Time (hrs)' column exists and has at least two entries to calculate timestep
        if "Time (hrs)" not in hydrographs.columns or len(hydrographs["Time (hrs)"]) < 2:
            logging.error(f"'Time (hrs)' column missing or insufficient in {csv_read}")
            return durexcdb

        # Calculate the time step based on the first two time entries
        timestep = hydrographs["Time (hrs)"].iloc[1] - hydrographs["Time (hrs)"].iloc[0]

        # Iterate over each threshold flow
        for qch in qcheck_list:
            # Count the number of times each location exceeds the threshold
            counts = (hydrographs.iloc[:, 1:] > qch).sum()
            # Get locations where the threshold is exceeded at least once
            locations = counts[counts > 0].index.tolist()
            # Calculate the duration exceeding the threshold
            dur_exc = (counts[counts > 0] * timestep).tolist()

            if locations:
                # Prepare the dictionary for the DataFrame
                dbdictionary = pd.DataFrame(
                    {
                        "AEP": [aep] * len(locations),
                        "Duration": [dur] * len(locations),
                        "TP": [tp] * len(locations),
                        "Location": locations,
                        "ThresholdFlow": [qch] * len(locations),
                        "Duration_Exceeding": dur_exc,
                        "out_path": [out_path] * len(locations),
                    }
                )
                # Concatenate with the main DataFrame
                durexcdb = pd.concat([durexcdb, dbdictionary], ignore_index=True)
    except FileNotFoundError:
        logging.error(f"File not found: {csv_read}", exc_info=True)
    except Exception as e:
        logging.error(f"Error processing hydrograph CSV {csv_read}: {e}", exc_info=True)
    return durexcdb


def run_processing_pipeline() -> None:
    """
    Orchestrate the processing of batch.out files and hydrograph CSVs.
    """
    # Set up logging configuration
    setup_logging()
    logging.info("Starting processing pipeline...")

    # Recursively search for all batch.out files using pathlib for better path handling
    logging.info("Recursively searching for **/*batch.out files - this may take a while")
    file_list: list[Path] = [Path(f) for f in iglob("**/*batch.out", recursive=True) if Path(f).is_file()]
    logging.info(f"Found {len(file_list)} batch.out files")

    # Process each batch.out file and collect the data
    batch_list: list[pd.DataFrame] = [parse_batch_output(batchout_file) for batchout_file in file_list]

    # Concatenate all DataFrames into one, handling empty lists gracefully
    if batch_list:
        files_df: pd.DataFrame = pd.concat(batch_list, ignore_index=True)
    else:
        logging.warning("No batch.out files were processed successfully.")
        files_df = pd.DataFrame()

    if not files_df.empty:
        # Save the batch outputs to a CSV file with a timestamp
        date_time_string: str = datetime.now().strftime("%Y%m%d-%H%M")
        output = Path(f"{date_time_string}_batchouts.csv")
        files_df.to_csv(output, index=False)
        logging.info(f"Batch outputs saved to {output}")

        # Prepare for processing hydrograph CSVs
        db: pd.DataFrame = files_df
        # Define the threshold flows to check
        qcheck: list[float] = list(range(1, 10)) + list(range(10, 100, 2)) + list(range(100, 2100, 10))
        # Create a list of tuples for each hydrograph CSV to process
        rorb_csvs = list(
            zip(
                db["AEP"],
                db["Duration"],
                db["TPat"],
                db["Path"],
                db["csv"],
            )
        )
        num_files: int = len(rorb_csvs)
        # Calculate the optimal number of threads based on the number of files
        num_threads: int = calculate_pool_size(num_files)
        logging.info(f"Processing {num_files} hydrograph CSV files with {num_threads} threads")

        # Process hydrograph CSVs using multiprocessing for better performance
        with multiprocessing.Pool(num_threads) as pool:
            hydro_csv_partial = partial(analyze_hydrograph, qcheck_list=qcheck)
            # qcheck is a constant list
            rorb_data: list[pd.DataFrame] = pool.map(hydro_csv_partial, rorb_csvs)

        # Concatenate the results into a single DataFrame if any data was returned
        logging.info("Merging the hydrograph results")
        df: pd.DataFrame = pd.concat(rorb_data, ignore_index=True) if rorb_data else pd.DataFrame()

        if not df.empty:
            # Save the duration exceeding thresholds to files with a timestamp
            date_time_string = datetime.now().strftime("%Y%m%d-%H%M")
            output_prefix: str = f"{date_time_string}_durex_5b-zeroes"
            df.to_parquet(f"{output_prefix}.parquet.gzip", compression="gzip")
            df.to_csv(f"{output_prefix}.csv", index=False)
            logging.info(f"Duration exceeding thresholds saved to {output_prefix}.csv and {output_prefix}.parquet.gzip")

            # Prepare to calculate statistics
            finaldb_columns: list[str] = [
                "Path",
                "Location",
                "Threshold_Q",
                "AEP",
                "Central_Value",
                "Critical_Storm",
                "Critical_Tp",
                "Low_Value",
                "High_Value",
                "Average_Value",
                "Closest_Tpcrit",
                "Closest_Value",
            ]
            finaldb = pd.DataFrame(columns=finaldb_columns)

            # Group the data for statistics calculation
            grouped = df.groupby(["out_path", "Location", "ThresholdFlow", "AEP"])
            logging.info("Calculating statistics for each group")

            # Iterate over each group and calculate statistics
            for name, group in grouped:
                stats_result = calculate_central_tendency_statistics(group, "Duration_Exceeding", "TP", "Duration")
                # The result is a list, so we need to concatenate it with the name tuple
                result = list(name) + stats_result
                # Append the result to the final DataFrame
                finaldb.loc[len(finaldb)] = result

            # Save the statistics to a CSV file with a timestamp
            date_time_string = datetime.now().strftime("%Y%m%d-%H%M")
            save_as = Path(f"{date_time_string}_QvsTexc_v5bb-zeroes.csv")
            finaldb.columns = finaldb_columns  # Ensure columns are correctly assigned
            finaldb.to_csv(save_as, index=False)
            logging.info(f"Statistics saved to {save_as}")
        else:
            logging.warning("No hydrograph data was processed successfully.")
    else:
        logging.warning("No batch output data to process.")

    logging.info("Processing complete")


if __name__ == "__main__":
    run_processing_pipeline()
