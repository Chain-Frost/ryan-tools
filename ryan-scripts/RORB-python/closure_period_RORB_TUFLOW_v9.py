import os
import re
import csv
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from glob import iglob
from concurrent.futures import ProcessPoolExecutor

# Importing required functions from ryan_functions
from ryan_functions.data_processing import (
    check_string_TP,
    check_string_duration,
    check_string_aep,
)

from ryan_functions.processTScsv import processTScsv

# in progress - not finished


# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def read_rorb_data(filepath):
    """
    Reads RORB data from a CSV file and returns a standardized DataFrame.
    Args:
        filepath (str): Path to the RORB CSV file.
    Returns:
        pd.DataFrame: DataFrame containing time series data.
    """
    try:
        # Implement reading logic specific to RORB data
        # Placeholder implementation
        df = pd.read_csv(filepath)
        # Standardize DataFrame columns to 'Time', 'Location', 'Value'
        df.rename(
            columns={
                "TimeColumn": "Time",
                "LocationColumn": "Location",
                "ValueColumn": "Value",
            },
            inplace=True,
        )
        return df
    except Exception as e:
        logging.error(f"Error reading RORB data from {filepath}: {e}")
        return pd.DataFrame()


def read_tuflow_po_data(filepath):
    """
    Reads TUFLOW PO data from a CSV file and returns a standardized DataFrame.
    Args:
        filepath (str): Path to the TUFLOW PO CSV file.
    Returns:
        pd.DataFrame: DataFrame containing time series data.
    """
    try:
        # Implement reading logic specific to TUFLOW PO data
        # Placeholder implementation
        df = pd.read_csv(filepath, skiprows=[0], header=0)
        # Extract columns that contain 'Flow' and 'Time'
        flow_columns = [col for col in df.columns if "Flow" in col]
        time_column = "Time"
        df = df[[time_column] + flow_columns]
        # Melt the DataFrame to have 'Time', 'Location', 'Value'
        df = df.melt(id_vars=[time_column], var_name="Location", value_name="Value")
        df.rename(columns={time_column: "Time"}, inplace=True)
        return df
    except Exception as e:
        logging.error(f"Error reading TUFLOW PO data from {filepath}: {e}")
        return pd.DataFrame()


def read_tuflow_1d_data(filepath):
    """
    Reads TUFLOW 1D data from a CSV file and returns a standardized DataFrame.
    Args:
        filepath (str): Path to the TUFLOW 1D CSV file.
    Returns:
        pd.DataFrame: DataFrame containing time series data.
    """
    try:
        # Using the imported processTScsv function
        df, internalName = processTScsv(filepath)
        # Standardize DataFrame columns to 'Time', 'Location', 'Value'
        if "H_US" in df.columns and "H_DS" in df.columns:
            # For water level data, you might need to choose upstream or downstream values
            df["Value"] = df[["H_US", "H_DS"]].mean(axis=1)
        elif "Q" in df.columns:
            df["Value"] = df["Q"]
        else:
            logging.warning(f"Unexpected data format in {filepath}")
            df["Value"] = np.nan
        df.rename(columns={"Time": "Time", "Chan ID": "Location"}, inplace=True)
        df = df[["Time", "Location", "Value"]]
        return df
    except Exception as e:
        logging.error(f"Error reading TUFLOW 1D data from {filepath}: {e}")
        return pd.DataFrame()


def read_input_data(filepath, data_type):
    """
    Reads input data from various sources and returns a standardized DataFrame.
    Args:
        filepath (str): Path to the input file.
        data_type (str): Type of data ('RORB', 'TUFLOWPO', 'TUFLOW1D').
    Returns:
        pd.DataFrame: Standardized DataFrame with time series data.
    """
    if data_type == "RORB":
        df = read_rorb_data(filepath)
    elif data_type == "TUFLOWPO":
        df = read_tuflow_po_data(filepath)
    elif data_type == "TUFLOW1D":
        df = read_tuflow_1d_data(filepath)
    else:
        logging.error(f"Unsupported data type: {data_type}")
        df = pd.DataFrame()
    return df


def extract_metadata(filename):
    """
    Extracts metadata from the filename.
    Args:
        filename (str): Name of the file.
    Returns:
        dict: Dictionary containing metadata.
    """
    metadata = {}
    metadata["TP"] = check_string_TP(filename)
    metadata["Duration"] = check_string_duration(filename)
    metadata["AEP"] = check_string_aep(filename)
    metadata["Runcode"] = os.path.splitext(os.path.basename(filename))[0]
    metadata["TrimRC"] = metadata["Runcode"]
    return metadata


def process_data(df, metadata, thresholds):
    """
    Processes the data to calculate durations exceeding thresholds.
    Args:
        df (pd.DataFrame): Standardized DataFrame with time series data.
        metadata (dict): Dictionary containing metadata.
        thresholds (list): List of threshold values to check.
    Returns:
        pd.DataFrame: DataFrame with results.
    """
    results = []
    if df.empty:
        logging.warning(f"No data to process for {metadata.get('Runcode')}")
        return pd.DataFrame()
    try:
        timestep = df["Time"].iloc[1] - df["Time"].iloc[0]
        for threshold in thresholds:
            exceeding = df[df["Value"] > threshold]
            duration_exceeding = exceeding.groupby("Location").size() * timestep
            for location, duration in duration_exceeding.items():
                result = {
                    "AEP": metadata.get("AEP"),
                    "Duration": metadata.get("Duration"),
                    "TP": metadata.get("TP"),
                    "Location": location,
                    "Threshold": threshold,
                    "Duration_Exceeding": duration,
                    "Runcode": metadata.get("Runcode"),
                    "TrimRC": metadata.get("TrimRC"),
                }
                results.append(result)
        return pd.DataFrame(results)
    except Exception as e:
        logging.error(f"Error processing data for {metadata.get('Runcode')}: {e}")
        return pd.DataFrame()


def process_file(filepath, data_type, thresholds):
    """
    Processes a single file to compute durations exceeding thresholds.
    Args:
        filepath (str): Path to the input file.
        data_type (str): Type of data ('RORB', 'TUFLOWPO', 'TUFLOW1D').
        thresholds (list): List of threshold values.
    Returns:
        pd.DataFrame: DataFrame with computed results.
    """
    logging.info(f"Processing file: {filepath}")
    metadata = extract_metadata(filepath)
    df = read_input_data(filepath, data_type)
    result_df = process_data(df, metadata, thresholds)
    return result_df


def get_file_extension(data_type):
    """
    Maps data types to file extensions.
    Args:
        data_type (str): Data type ('RORB', 'TUFLOWPO', 'TUFLOW1D').
    Returns:
        str: File extension pattern.
    """
    if data_type == "RORB":
        return ".csv"  # Adjust accordingly
    elif data_type == "TUFLOWPO":
        return "_PO.csv"
    elif data_type == "TUFLOW1D":
        return "_1d_*.csv"  # Adjust to include other 1D files if needed
    else:
        raise ValueError(f"Unsupported data type: {data_type}")


def stats(freqdb, statcol, tpcol, durcol):
    """
    Extracts min, max, and median peaks for peaks in the dataframe.
    Args:
        freqdb (pd.DataFrame): DataFrame containing frequency data.
        statcol (str): Statistic column name.
        tpcol (str): TP column name.
        durcol (str): Duration column name.
    Returns:
        list: [median, Tcrit, Tpcrit, low, high]
    """
    try:
        median = -9999
        Tcrit = Tpcrit = low = high = None
        # Group by the duration column
        for _, ensemblestat in freqdb.groupby(by=durcol):
            # Sort the group by the statistic column in ascending order
            ensemblestat_sorted = ensemblestat.sort_values(statcol)
            # Calculate the median position
            medianpos = len(ensemblestat_sorted) // 2
            current_median = ensemblestat_sorted[statcol].iloc[medianpos]
            # Update the median and corresponding values if the current median is higher
            if current_median > median:
                median = current_median
                Tcrit = ensemblestat_sorted[durcol].iloc[medianpos]
                Tpcrit = ensemblestat_sorted[tpcol].iloc[medianpos]
                low = ensemblestat_sorted[statcol].iloc[0]
                high = ensemblestat_sorted[statcol].iloc[-1]
        return [median, Tcrit, Tpcrit, low, high]
    except Exception as e:
        logging.error(f"Error calculating statistics: {e}")
        return [None, None, None, None, None]


def create_finaldb(df):
    """
    Creates the final DataFrame summarizing the results.
    Args:
        df (pd.DataFrame): DataFrame containing processed data.
    """
    try:
        logging.info("Creating the final summary DataFrame...")
        # Initialize the final dataframe
        finaldb = pd.DataFrame(
            columns=[
                "TrimRC",
                "location",
                "Threshold_Q",
                "AEP",
                "Duration_Exceeded",
                "Critical_Storm",
                "Critical_Tp",
                "Low_Duration",
                "High_Duration",
            ]
        )
        # Group by necessary columns
        grouped = df.groupby(["TrimRC", "Location", "Threshold", "AEP"])

        # Apply the stats function to each group and collect the results
        def apply_stats(group):
            return stats(group, "Duration_Exceeding", "TP", "Duration")

        # Apply the function
        results = grouped.apply(apply_stats).reset_index()
        # Concatenate the results
        finaldb = pd.concat(
            [results.drop(columns=0), results[0].apply(pd.Series)], axis=1
        )
        finaldb.columns = [
            "TrimRC",
            "location",
            "Threshold_Q",
            "AEP",
            "Duration_Exceeded",
            "Critical_Storm",
            "Critical_Tp",
            "Low_Duration",
            "High_Duration",
        ]
        # Save the dataframe to a CSV file
        DateTimeString = datetime.now().strftime("%Y%m%d-%H%M")
        save_as = f"{DateTimeString}_HvsTexc.csv"
        logging.info(f"Saving the final dataframe to {save_as}...")
        finaldb.to_csv(save_as, index=False)
        logging.info(f"Final summary saved to {save_as}")
    except Exception as e:
        logging.error(f"Error creating final DataFrame: {e}")


def main():
    """
    Main execution function.
    """
    try:
        # Set working directory
        script_dir = os.path.dirname(os.path.realpath(__file__))
        os.chdir(script_dir)
        # Collect input files
        data_types = ["RORB", "TUFLOWPO", "TUFLOW1D"]
        all_files = []
        for data_type in data_types:
            pattern = "**/*" + get_file_extension(data_type)
            file_list = [f for f in iglob(pattern, recursive=True) if os.path.isfile(f)]
            for filepath in file_list:
                all_files.append((filepath, data_type))
        if not all_files:
            logging.warning("No files found to process.")
            return
        # Define thresholds
        thresholds = list(np.arange(60.0, 75.0, 0.01))  # Example thresholds
        # Process files using multiprocessing
        results = []
        with ProcessPoolExecutor(max_workers=os.cpu_count() - 1) as executor:
            futures = {
                executor.submit(process_file, filepath, data_type, thresholds): (
                    filepath,
                    data_type,
                )
                for filepath, data_type in all_files
            }
            for future in futures:
                filepath, data_type = futures[future]
                try:
                    result = future.result()
                    if not result.empty:
                        results.append(result)
                except Exception as e:
                    logging.error(f"Error processing file {filepath}: {e}")
        if results:
            # Combine results and output
            combined_df = pd.concat(results, ignore_index=True)
            DateTimeString = datetime.now().strftime("%Y%m%d-%H%M")
            output_file = f"{DateTimeString}_output_results.csv"
            combined_df.to_csv(output_file, index=False)
            logging.info(f"Combined results saved to {output_file}")
            # Create final summary
            create_finaldb(combined_df)
        else:
            logging.warning("No results to combine.")
    except Exception as e:
        logging.error(f"An error occurred in the main function: {e}")


if __name__ == "__main__":
    main()
