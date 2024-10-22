import os
import pandas as pd
import re

# Define the lookup data
lookup_data = [
    {
        "raw": "63.2%",
        "percentage": 63.20,
        "1 in X AEP": 1.58,
        "EY": 0.9997,
        "ARI": 1.00,
    },
    {"raw": "50%", "percentage": 50.00, "1 in X AEP": 2.00, "EY": 0.6931, "ARI": 1.44},
    {"raw": "20%", "percentage": 20.00, "1 in X AEP": 5.00, "EY": 0.2231, "ARI": 4.48},
    {"raw": "10%", "percentage": 10.00, "1 in X AEP": 10.00, "EY": 0.1054, "ARI": 9.49},
    {"raw": "5%", "percentage": 5.00, "1 in X AEP": 20.00, "EY": 0.0513, "ARI": 19.50},
    {"raw": "2%", "percentage": 2.00, "1 in X AEP": 50.00, "EY": 0.0202, "ARI": 49.50},
    {"raw": "1%", "percentage": 1.00, "1 in X AEP": 100.00, "EY": 0.0101, "ARI": 99.50},
    {
        "raw": "1 in 100",
        "percentage": 1.00,
        "1 in X AEP": 100,
        "EY": 0.0101,
        "ARI": 99.50,
    },
    {
        "raw": "1 in 200",
        "percentage": 0.50,
        "1 in X AEP": 200,
        "EY": 0.0050,
        "ARI": 199.50,
    },
    {
        "raw": "1 in 500",
        "percentage": 0.20,
        "1 in X AEP": 500,
        "EY": 0.0020,
        "ARI": 499.50,
    },
    {
        "raw": "1 in 1000",
        "percentage": 0.10,
        "1 in X AEP": 1000,
        "EY": 0.0010,
        "ARI": 999.50,
    },
    {
        "raw": "1 in 2000",
        "percentage": 0.05,
        "1 in X AEP": 2000,
        "EY": 0.0005,
        "ARI": 1999.50,
    },
    {
        "raw": "12EY",
        "percentage": 100.00,
        "1 in X AEP": 1.000006,
        "EY": 12,
        "ARI": 0.08,
    },
    {"raw": "6EY", "percentage": 99.75, "1 in X AEP": 1.002, "EY": 6, "ARI": 0.17},
    {"raw": "4EY", "percentage": 98.17, "1 in X AEP": 1.02, "EY": 4, "ARI": 0.25},
    {"raw": "3EY", "percentage": 95.02, "1 in X AEP": 1.05, "EY": 3, "ARI": 0.33},
    {"raw": "2EY", "percentage": 86.47, "1 in X AEP": 1.16, "EY": 2, "ARI": 0.50},
    {"raw": "1EY", "percentage": 63.21, "1 in X AEP": 1.58, "EY": 1, "ARI": 1.00},
    {"raw": "0.5EY", "percentage": 39.35, "1 in X AEP": 2.54, "EY": 0.5, "ARI": 2.00},
    {"raw": "0.2EY", "percentage": 18.13, "1 in X AEP": 5.52, "EY": 0.2, "ARI": 5.00},
]

# Convert lookup_data to a DataFrame for easy merging
lookup_df = pd.DataFrame(lookup_data)

# Optional: If there are duplicate 'raw' entries, ensure uniqueness
lookup_df = lookup_df.drop_duplicates(subset=["raw"])


def process_coordinate(coord: str, coord_type: str) -> float:
    """
    Process coordinate strings by removing directional indicators and converting to float.

    Parameters:
    - coord: The coordinate string (e.g., "33.5(S)", "120.2(E)").
    - coord_type: 'lat' or 'long' to determine processing rules.

    Returns:
    - Processed float value.
    """
    try:
        # Remove any non-numeric characters except for the decimal point and negative sign
        cleaned_coord = re.sub(r"[^\d\.-]", "", coord)
        value = float(cleaned_coord)

        # For latitude, it's always negative
        if coord_type == "lat":
            if value > 0:
                value = -value
        # For longitude, it's always positive
        elif coord_type == "long":
            if value < 0:
                value = -value
        return value
    except Exception as e:
        print(f"Error processing {coord_type} '{coord}': {e}")
        return float("nan")


def extract_data_from_csv(file_path: str, lookup_df: pd.DataFrame) -> pd.DataFrame:
    print(f"Starting extraction for file: {file_path}")
    try:
        # Read the file into a list of lines
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        print(f"File read successfully. Total lines: {len(lines)}")

        # Split lines into columns
        data = [line.strip().split(",") for line in lines]

        # Extract required fields
        req_lat = data[5][2]  # Row 6 (index 5), Column C (index 2)
        req_long = data[5][4]  # Row 6, Column E (index 4)
        grid_lat = data[6][2]  # Row 7, Column C
        grid_long = data[6][4]  # Row 7, Column E
        location_label = data[4][1]  # Row 5, Column B (index 1)

        print(f"Extracted req_lat: {req_lat}")
        print(f"Extracted req_long: {req_long}")
        print(f"Extracted grid_lat: {grid_lat}")
        print(f"Extracted grid_long: {grid_long}")
        print(f"Extracted location_label: {location_label}")

        # Process CSV data starting from the header line
        # Find the index where the header starts
        header_index = None
        for idx, line in enumerate(data):
            if "Duration" in line[0]:
                header_index = idx
                break
        if header_index is None:
            raise ValueError("Header line with 'Duration' not found.")

        # Extract header and data rows
        header = data[header_index]
        data_rows = data[header_index + 1 :]

        # Remove any empty lines
        data_rows = [row for row in data_rows if len(row) > 1]

        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=header)
        print(f"CSV data read successfully. DataFrame shape: {df.shape}")

        # Convert numeric columns to appropriate data types
        for col in df.columns[1:]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Melt the DataFrame to long format
        long_df = df.melt(
            id_vars=["Duration", "Duration in min"],
            var_name="AEP",
            value_name="Value",
        )

        # Add the extracted fields as new columns
        long_df["req_lat"] = req_lat
        long_df["req_long"] = req_long
        long_df["grid_lat"] = grid_lat
        long_df["grid_long"] = grid_long
        long_df["location_label"] = location_label

        print(f"Data reshaped successfully. Long DataFrame shape: {long_df.shape}")

        # Process grid_lat and grid_long
        long_df["grid_lat"] = long_df["grid_lat"].apply(
            lambda x: process_coordinate(x, "lat")
        )
        long_df["grid_long"] = long_df["grid_long"].apply(
            lambda x: process_coordinate(x, "long")
        )

        # Process req_lat and req_long
        long_df["req_lat"] = long_df["req_lat"].apply(
            lambda x: process_coordinate(x, "lat")
        )
        long_df["req_long"] = long_df["req_long"].apply(
            lambda x: process_coordinate(x, "long")
        )

        # Convert 'Duration in min' to integer
        long_df["Duration in min"] = pd.to_numeric(
            long_df["Duration in min"], errors="coerce"
        ).astype("Int64")

        # Merge with lookup_df to add additional AEP details
        merged_df = long_df.merge(
            lookup_df,
            how="left",
            left_on="AEP",
            right_on="raw",
            suffixes=("", "_lookup"),
        )

        # Optionally, drop the 'raw' column from lookup if not needed
        merged_df.drop(columns=["raw"], inplace=True)

        # Enforce data types
        # Define columns to convert
        float_columns = [
            "Value",
            "req_lat",
            "req_long",
            "grid_lat",
            "grid_long",
            "percentage",
            "1 in X AEP",
            "EY",
            "ARI",
        ]
        int_columns = ["Duration in min"]
        text_columns = ["Duration", "AEP", "location_label", "Location", "File_Type"]

        # Convert float columns
        for col in float_columns:
            if col in merged_df.columns:
                merged_df[col] = pd.to_numeric(merged_df[col], errors="coerce")

        # Convert integer columns (already converted above)
        # No action needed as 'Duration in min' is already converted

        # Convert text columns to string
        for col in text_columns:
            if col in merged_df.columns:
                merged_df[col] = merged_df[col].astype(str)

        print(f"Lookup merged successfully. Merged DataFrame shape: {merged_df.shape}")

        return merged_df

    except Exception as e:
        print(f"Unexpected error processing file {file_path}: {e}")
        return pd.DataFrame()


def find_and_process_files(script_dir: str, lookup_df: pd.DataFrame) -> pd.DataFrame:
    print(f"Scanning directory: {script_dir}")
    all_files = os.listdir(script_dir)
    csv_files = [f for f in all_files if f.lower().endswith(".csv")]
    print(f"Found {len(csv_files)} CSV files.")

    # Group files by patterns
    locations = {}
    for file in csv_files:
        if "_ifds" in file:
            location = file.split("_ifds")[0]
            locations.setdefault(location, {})["ifds"] = file
        elif "_rare" in file:
            location = file.split("_rare")[0]
            locations.setdefault(location, {})["rare"] = file
        elif "_very_frequent" in file:
            location = file.split("_very_frequent")[0]
            locations.setdefault(location, {})["very_frequent"] = file
        else:
            print(f"Warning: File '{file}' does not match expected patterns.")

    print(f"Identified {len(locations)} unique locations.")

    # Create an empty list to store all DataFrame entries
    data_frames = []

    # Process each file for each location
    for location, files in locations.items():
        print(f"\nProcessing files for location: {location}")
        for file_type, file_name in files.items():
            file_path = os.path.join(script_dir, file_name)
            print(f"  Processing file type '{file_type}': {file_name}")
            location_data = extract_data_from_csv(file_path, lookup_df)
            if not location_data.empty:
                location_data["Location"] = location
                location_data["File_Type"] = file_type
                data_frames.append(location_data)
                print(f"  Data appended for file type '{file_type}'.")
            else:
                print(f"  No data extracted for file type '{file_type}'.")

    # Concatenate all DataFrames vertically
    if data_frames:
        print(f"\nConcatenating {len(data_frames)} DataFrames.")
        all_data = pd.concat(data_frames, ignore_index=True)
        print(f"All data concatenated successfully. Shape: {all_data.shape}")
    else:
        print("No data frames to concatenate.")
        all_data = pd.DataFrame()

    return all_data


def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Script directory: {script_dir}")

    # Step 1: Find and process CSV files
    print("\nStep 1: Finding and processing CSV files.")
    all_data_long = find_and_process_files(script_dir, lookup_df)

    if not all_data_long.empty:
        # Step 2: Export the complete long data to CSV
        all_data_csv_path = os.path.join(
            script_dir, "all_location_data_with_AEP_lookup.csv"
        )
        all_data_long.to_csv(all_data_csv_path, index=False)
        print(
            f"Step 2: Exported all location data (long format) with AEP lookup to '{all_data_csv_path}'."
        )
    else:
        print("No valid data to export.")


if __name__ == "__main__":
    working_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(working_dir)
    print(f"Changed working directory to: {working_dir}")

    main()
