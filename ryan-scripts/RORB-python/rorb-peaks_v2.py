import pandas as pd
import re
import os
from pathlib import Path
import math
from functools import reduce

# Set script to run in the folder it is saved in
script_dir = Path(__file__).absolute().parent
os.chdir(script_dir)

# Identify all .out files in the directory and subdirectories
out_files = list(script_dir.rglob("*.out"))

# Check if any .out files are found
if not out_files:
    raise FileNotFoundError("No .out files found in the directory and its subdirectories.")

# Initialize a list to collect DataFrames from all files
all_median_data = []

# Initialize counters for processed and skipped files
processed_count = 0
skipped_count = 0

# Iterate over each .out file
for file_path in out_files:
    # Read the file content
    try:
        with open(file_path, "r", encoding="latin1") as file:
            lines = file.readlines()
    except Exception:
        # If there's an error reading the file, skip it
        skipped_count += 1
        continue  # Skip to the next file

    # Initialize dictionaries and lists for capturing parameters and data rows
    parameters = {}
    data_rows = []
    header_index = None

    # Extract relevant rows and parameters
    for i, line in enumerate(lines):
        # Extract parameters from specific lines
        if "Parameters:  kc =" in line:
            params = re.findall(r"[-+]?\d*\.\d+|\d+", line)
            if len(params) >= 2:
                parameters["kc"] = float(params[0])
                parameters["m"] = float(params[1])
        elif "Initial loss (mm)" in line:
            params = re.findall(r"[-+]?\d*\.\d+|\d+", line)
            if len(params) >= 2:
                parameters["Initial_loss_mm"] = float(params[0])
                parameters["Cont_loss_mm_per_h"] = float(params[1])

        # Locate the header line and store the header index
        if "Run        Duration             AEP" in line:
            header_index = i
            headers = re.split(r"\s{2,}", line.strip())

        # Collect data rows after the header is found
        if header_index is not None and i > header_index:
            data_line = re.split(r"\s{2,}", line.strip())
            if len(data_line) == len(headers):
                data_rows.append(data_line)

    # Check if data was found
    if not data_rows:
        skipped_count += 1
        continue  # Skip to the next file

    # Create a DataFrame from the collected data rows
    df = pd.DataFrame(data_rows, columns=headers)

    # Convert numeric columns to appropriate types, excluding specified non-numeric columns
    non_numeric_columns = ["Run", "Duration", "AEP", "TPat"]
    for col in df.columns:
        if col not in non_numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Identify Peak columns (assuming they start with 'Peak')
    peak_columns = [col for col in df.columns if col.startswith("Peak")]
    if not peak_columns:
        skipped_count += 1
        continue  # Skip to the next file

    # Define a function to extract the higher median and corresponding TPat
    def get_higher_median_tpat(group, peak_col):
        """
        For a given group and Peak column:
        - Sort the group by the Peak column in ascending order.
        - Determine the higher median index.
        - Retrieve the Peak value and corresponding TPat at that index.
        """
        # Sort the group by the specified Peak column in ascending order
        sorted_group = group.sort_values(by=peak_col, ascending=True).reset_index(drop=True)
        n = len(sorted_group)

        # Calculate the index for the higher median
        # For n=10, median_index=5 (6th element)
        # For n=9, median_index=4 (5th element)
        median_index = n // 2  # Integer division

        # Handle cases where the group might be empty
        if n == 0:
            return pd.Series({"Median_Peak": None, "Median_TPat": None})

        # Retrieve the Median_Peak and corresponding TPat
        try:
            median_peak = sorted_group.iloc[median_index][peak_col]
            median_tpat = sorted_group.iloc[median_index]["TPat"]
        except IndexError:
            median_peak = None
            median_tpat = None

        return pd.Series({"Median_Peak": median_peak, "Median_TPat": median_tpat})

    # Initialize an empty list to collect median data for all Peak columns
    median_data = []

    # Iterate over each Peak column to compute median and corresponding TPat
    for peak_col in peak_columns:
        # Select only the Peak column and 'TPat' for the apply function to exclude grouping columns
        median_df = (
            df.groupby(["Duration", "AEP"])[[peak_col, "TPat"]]
            .apply(get_higher_median_tpat, peak_col=peak_col)
            .reset_index()
        )

        # Rename the columns to include the Peak column name
        median_df.rename(
            columns={"Median_Peak": f"Median_{peak_col}", "Median_TPat": f"Median_TPat_{peak_col}"}, inplace=True
        )

        # Add the Peak column name to the list for merging
        median_data.append(median_df)

    # Merge all median dataframes on 'Duration' and 'AEP'
    if len(median_data) == 1:
        final_median_df = median_data[0]
    else:
        final_median_df = reduce(
            lambda left, right: pd.merge(left, right, on=["Duration", "AEP"], how="outer"), median_data
        )

    # Add the extracted parameters as new columns to the median dataframe
    for key, value in parameters.items():
        final_median_df[key] = value

    # Add the filename as a new column
    final_median_df["Filename"] = file_path.name

    # Extract additional RunCode parts from the filename
    run_code = file_path.stem  # Get the filename without extension
    run_parts = run_code.replace(" ", "_").split("_")
    for num, part in enumerate(run_parts):
        # Add 'r0', 'r1', etc. as new columns at the end
        final_median_df[f"r{num}"] = str(part)

    # Extract the relative path (excluding the filename)
    try:
        relative_path = file_path.parent.relative_to(script_dir).as_posix()
    except ValueError:
        # If file is in the script_dir itself
        relative_path = "."

    # Add 'Relative_Path' as a new column
    final_median_df["Relative_Path"] = relative_path

    # Reorder columns for better readability (optional)
    # Place 'Filename', 'Relative_Path', 'Duration' and 'AEP' first, followed by medians, parameters, and then 'r*' columns
    median_columns = [col for col in final_median_df.columns if col.startswith("Median_")]
    parameter_columns = list(parameters.keys())
    r_columns = [col for col in final_median_df.columns if col.startswith("r")]
    final_columns_order = (
        ["Filename", "Relative_Path", "Duration", "AEP"] + median_columns + parameter_columns + r_columns
    )
    final_median_df = final_median_df[final_columns_order]

    # Append the processed DataFrame to the list
    all_median_data.append(final_median_df)

    # Increment the processed files counter
    processed_count += 1
    print(f"Processed file: {file_path}")

# Combine all processed DataFrames into a single DataFrame
if not all_median_data:
    raise ValueError("No valid data was processed from the .out files.")

combined_median_df = pd.concat(all_median_data, ignore_index=True)

# Export the combined DataFrame to a CSV file
output_path = "processed_output.csv"
combined_median_df.to_csv(output_path, index=False)

# Print summary of processing
print("\nProcessing Summary:")
print(f"Total files processed: {processed_count}")
print(f"Total files skipped: {skipped_count}")
print(f"Results have been saved to '{output_path}'.")
