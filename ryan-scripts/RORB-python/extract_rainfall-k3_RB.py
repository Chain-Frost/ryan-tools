# I think this was superseded by ryan-scripts\RORB-python\extract_rainfall-to-TUFLOW.py
# but not sure and I have not checked.

import os
import pandas as pd
from pandas import DataFrame


def fix_and_extract(file_path, start_row, end_row, time_increment) -> DataFrame:
    """
    Extract specific rows from a .out file and save them in a CSV format.
    Dynamically adjusts the column count to match the headers.
    """
    with open(file_path, "r", encoding="utf-8", errors="ignore") as file:
        lines = file.readlines()

    # Extract specific rows based on start_row and end_row
    data_lines = lines[start_row - 1 : end_row]  # Python is 0-indexed
    data = [line.split() for line in data_lines if line.strip()]  # Split and ignore empty lines

    # Determine the maximum number of columns in the data
    max_cols = max(len(row) for row in data)
    headers = ["Increments", "Catchment"] + [f"Sub-Area {chr(65 + i)}" for i in range(max_cols - 2)]

    # Normalize rows to match the number of columns
    normalized_data = [row + [""] * (max_cols - len(row)) for row in data]

    # Convert to DataFrame
    df = pd.DataFrame(normalized_data, columns=headers)

    # Convert "Increments" to "Time (hour)"
    df["Time (hour)"] = df["Increments"].astype(float) * time_increment

    # Reorder columns: Increments, Time, Catchment, then Sub-Areas
    sub_area_cols = [col for col in df.columns if col.startswith("Sub-Area")]
    df = df[["Increments", "Time (hour)", "Catchment"] + sub_area_cols]

    # Add an extra row
    last_increment = float(df["Increments"].iloc[-1])
    new_increment = last_increment + 1
    new_time = new_increment * time_increment
    new_row = [new_increment, new_time, 0.0] + [0.0] * len(sub_area_cols)
    df.loc[len(df)] = new_row

    return df


def process_out_files(directory, start_row, end_row, time_increment):
    """
    Process all .out files in the given directory and extract specific rows as CSVs.
    """
    output_directory = os.path.join(directory, "extracted_rainfall")
    os.makedirs(output_directory, exist_ok=True)

    for file_name in os.listdir(directory):
        if file_name.endswith(".out"):
            file_path = os.path.join(directory, file_name)
            try:
                print(f"Processing file: {file_path}")
                extracted_df = fix_and_extract(file_path, start_row, end_row, time_increment)
                output_csv_path = os.path.join(output_directory, f"{os.path.splitext(file_name)[0]}_Rainfall.csv")
                extracted_df.to_csv(output_csv_path, index=False)
                print(f"Extracted and saved: {output_csv_path}")
            except Exception as e:
                print(f"Failed to process {file_path}: {e}")


# Specify the directory containing your .out files, row range, and time increment
directory_path = r"Qfolder"
start_row = 475
end_row = 519
time_increment = 1  # Time increment in hours

# Process the files
process_out_files(directory_path, start_row, end_row, time_increment)
