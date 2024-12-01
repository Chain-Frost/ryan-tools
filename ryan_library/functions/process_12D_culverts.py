# ryan_library.functions/process_12D_culverts.py
import pandas as pd
import re
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)


def get_encoding(file_path):
    """
    Determines the encoding of the file based on BOM.

    Args:
        file_path (str): Path to the file.

    Returns:
        str: 'utf-8' or 'utf-16'
    """
    with open(file_path, "rb") as f:
        first_bytes = f.read(4)
    if first_bytes.startswith(b"\xff\xfe") or first_bytes.startswith(b"\xfe\xff"):
        return "utf-16"
    elif first_bytes.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    else:
        return "utf-8"


def dms_to_decimal(dms_str):
    """
    Converts a DMS (Degrees, Minutes, Seconds) string to decimal degrees.

    Args:
        dms_str (str): Angle in DMS format, e.g., "52°10'14"".

    Returns:
        float: Angle in decimal degrees.
    """
    try:
        dms_str = dms_str.replace('"', "").replace("°", " ").replace("'", " ").strip()
        parts = dms_str.split()
        if len(parts) != 3:
            logging.warning(
                f"Unexpected DMS format '{dms_str}'. Setting angle_degrees to 0.0."
            )
            return 0.0
        degrees, minutes, seconds = map(float, parts)
        decimal_degrees = degrees + minutes / 60 + seconds / 3600
        return decimal_degrees
    except Exception as e:
        logging.error(
            f"Error converting DMS to decimal for '{dms_str}': {e}. Setting angle_degrees to 0.0."
        )
        return 0.0


def get_field(upstream_val, downstream_val, default=None):
    """
    Helper function to get the field value from upstream or downstream.
    Prefers upstream value; if not available, uses downstream value.

    Args:
        upstream_val (str): Value from upstream line.
        downstream_val (str): Value from downstream line.
        default: Default value if both are missing.

    Returns:
        str or float: The selected field value.
    """
    if pd.notna(upstream_val) and upstream_val.strip() != "":
        return upstream_val.strip()
    elif pd.notna(downstream_val) and downstream_val.strip() != "":
        return downstream_val.strip()
    else:
        return default


def extract_numeric(value, field_name, culvert_name, dtype=float, default=0.0):
    """
    Converts a string value to a numeric type with error handling.

    Args:
        value (str): The string value to convert.
        field_name (str): The name of the field (for logging).
        culvert_name (str): The name of the culvert (for logging).
        dtype (type): The desired numeric type (float or int).
        default: The default value if conversion fails.

    Returns:
        float or int: The converted numeric value.
    """
    try:
        return dtype(value)
    except ValueError:
        logging.warning(
            f"Invalid {field_name} value '{value}' for culvert '{culvert_name}'. Setting to {default}."
        )
        return default


def parse_rpt_file(rpt_file_path):
    """
    Parses a .rpt file to extract culvert names and angles.

    Args:
        rpt_file_path (str): Path to the .rpt file.

    Returns:
        list[dict]: A list of dictionaries with 'Name', 'Angle', and 'Angle_Degrees'.
    """
    culverts = []
    # Regex to match data lines
    line_pattern = re.compile(
        r'^\s*\d+\.\d+\s+\d+\.\d+\s+(\d+°\s*\d+\'\s*\d+")\s+\d+\.\d+\s+\d+\.\d+\s+\d+\.\d+\s+\d+\.\d+\s+[-+]?\d+\.\d+\s+"([^"]+)"$'
    )
    encoding = get_encoding(rpt_file_path)
    logging.info(f"Detected encoding for {os.path.relpath(rpt_file_path)}: {encoding}")

    try:
        with open(rpt_file_path, "r", encoding=encoding, errors="ignore") as file:
            for line in file:
                match = line_pattern.match(line)
                if match:
                    angle = match.group(1).strip()
                    full_name = match.group(2).strip()
                    if "->" in full_name:
                        name = full_name.split("->")[-1].strip()
                    else:
                        name = full_name
                    # Remove any null characters and extra spaces
                    name = name.replace("\x00", "").strip()
                    # Default angle to '0°0\'0"' if not found (already handled by regex)
                    if not angle:
                        angle = "0°0'0\""
                    angle_degrees = dms_to_decimal(angle)
                    culverts.append(
                        {"Name": name, "Angle": angle, "Angle_Degrees": angle_degrees}
                    )
                    logging.info(
                        f"Parsed Culvert - Name: {name}, Angle: {angle}, Angle_Degrees: {angle_degrees}"
                    )
    except Exception as e:
        logging.error(f"Error processing {os.path.relpath(rpt_file_path)}: {e}")

    return culverts


def parse_txt_file(txt_file_path):
    """
    Parses a .txt file to extract detailed culvert information.

    Args:
        txt_file_path (str): Path to the .txt file.

    Returns:
        list[dict]: A list of dictionaries with culvert details.
    """
    culverts = []
    encoding = get_encoding(txt_file_path)
    logging.info(f"Detected encoding for {os.path.relpath(txt_file_path)}: {encoding}")

    try:
        # Read the file, skip the first and third rows
        df = pd.read_csv(
            txt_file_path, sep="\t", skiprows=[0, 2], encoding=encoding, dtype=str
        )

        # Normalize column names: strip spaces and convert to lowercase
        df.columns = df.columns.str.strip().str.lower()

        # Define the columns we need, including 'pipe type' and '*direction'
        required_columns = [
            "string name",
            "pit centre x",
            "pit centre y",
            "invert us",
            "invert ds",
            "pipe type",
            "diameter",
            "width",
            "number of pipes",
            "separation",
            "*direction",
        ]

        # Check if all required columns are present
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.warning(
                f"Missing columns {missing_columns} in {os.path.relpath(txt_file_path)}. Skipping this file."
            )
            return culverts

        # Since each culvert has two lines (upstream and downstream), group them
        df = df.reset_index(drop=True)
        num_rows = len(df)
        if num_rows % 2 != 0:
            logging.warning(
                f"Odd number of culvert entries in {os.path.relpath(txt_file_path)}. The last entry will be skipped."
            )
            df = df.iloc[:-1]

        for i in range(0, num_rows, 2):
            upstream = df.iloc[i]
            downstream = df.iloc[i + 1]

            name_upstream = (
                upstream["string name"].strip()
                if pd.notna(upstream["string name"])
                else None
            )
            name_downstream = (
                downstream["string name"].strip()
                if pd.notna(downstream["string name"])
                else None
            )

            # Validate that both lines belong to the same culvert
            if name_upstream != name_downstream:
                logging.warning(
                    f"Mismatched culvert names at lines {i+1} and {i+2} in {os.path.relpath(txt_file_path)}. Skipping these entries."
                )
                continue

            name = name_upstream

            # Extract fields separately for upstream and downstream
            us_x = get_field(upstream["pit centre x"], None, "0.0")
            us_y = get_field(upstream["pit centre y"], None, "0.0")
            ds_x = get_field(downstream["pit centre x"], None, "0.0")
            ds_y = get_field(downstream["pit centre y"], None, "0.0")

            invert_us = get_field(
                downstream["invert us"], upstream["invert us"], "0.0"
            )  # From downstream
            invert_ds = get_field(
                upstream["invert ds"], downstream["invert ds"], "0.0"
            )  # From upstream
            diameter = get_field(upstream["diameter"], downstream["diameter"], "0.0")
            width = get_field(upstream["width"], downstream["width"], "0.0")
            number_of_pipes = get_field(
                upstream["number of pipes"], downstream["number of pipes"], "0"
            )
            separation = get_field(
                upstream["separation"], downstream["separation"], "0.0"
            )
            direction = get_field(
                upstream["*direction"], downstream["*direction"], "Unknown"
            )
            pipe_type = get_field(
                upstream["pipe type"], downstream["pipe type"], "Unknown"
            )

            # Convert numerical fields
            us_x = extract_numeric(us_x, "Pit Centre X (US)", name, float, 0.0)
            us_y = extract_numeric(us_y, "Pit Centre Y (US)", name, float, 0.0)
            ds_x = extract_numeric(ds_x, "Pit Centre X (DS)", name, float, 0.0)
            ds_y = extract_numeric(ds_y, "Pit Centre Y (DS)", name, float, 0.0)
            invert_us = extract_numeric(invert_us, "Invert US", name, float, 0.0)
            invert_ds = extract_numeric(invert_ds, "Invert DS", name, float, 0.0)
            diameter = extract_numeric(diameter, "Diameter", name, float, 0.0)
            width = extract_numeric(width, "Width", name, float, 0.0)
            number_of_pipes = extract_numeric(
                number_of_pipes, "Number of Pipes", name, int, 0
            )
            separation = extract_numeric(separation, "Separation", name, float, 0.0)

            # Create culvert dictionary
            culvert = {
                "Name": name,
                "US_X": us_x,
                "US_Y": us_y,
                "DS_X": ds_x,
                "DS_Y": ds_y,
                "Invert US": invert_us,
                "Invert DS": invert_ds,
                "Diameter": diameter,
                "Width": width,
                "Number of Pipes": number_of_pipes,
                "Separation": separation,
                "Pipe Type": pipe_type,
                "Direction": direction,
            }

            culverts.append(culvert)
            logging.info(
                f"Parsed TXT Culvert - Name: {name}, US_X: {us_x}, US_Y: {us_y}, DS_X: {ds_x}, DS_Y: {ds_y}, Invert DS: {invert_ds}, Pipe Type: {pipe_type}, Direction: {direction}"
            )

    except Exception as e:
        logging.error(f"Error processing {os.path.relpath(txt_file_path)}: {e}")

    return culverts


def combine_data(rpt_data, txt_data):
    """
    Combines .rpt and .txt data based on the 'Name' field.

    Args:
        rpt_data (list[dict]): Data from .rpt files.
        txt_data (list[dict]): Data from .txt files.

    Returns:
        pd.DataFrame: Combined DataFrame with culvert information.
    """
    rpt_df = pd.DataFrame(rpt_data)
    txt_df = pd.DataFrame(txt_data)

    # Drop duplicate entries in rpt_df to ensure 'Name' is unique
    rpt_df = rpt_df.drop_duplicates(subset=["Name"])
    logging.info(f"Unique RPT Culverts: {len(rpt_df)}")

    # Similarly, ensure txt_df has unique 'Name's
    txt_df = txt_df.drop_duplicates(subset=["Name"])
    logging.info(f"Unique TXT Culverts: {len(txt_df)}")

    # Merge on 'Name' using outer join to include all culverts
    combined_df = pd.merge(
        rpt_df, txt_df, on="Name", how="outer", suffixes=("_rpt", "_txt")
    )

    # If 'Angle' is missing or NaN, set it to '0°0\'0"'
    combined_df["Angle"] = combined_df["Angle"].fillna("0°0'0\"")

    # Calculate 'Angle_Degrees' if missing
    combined_df["Angle_Degrees"] = combined_df.apply(
        lambda row: (
            row["Angle_Degrees"]
            if pd.notna(row["Angle_Degrees"])
            else dms_to_decimal(row["Angle"])
        ),
        axis=1,
    )

    return combined_df


def process_culvert_files(rpt_files, txt_files):
    """
    Processes all .rpt and .txt files and combines their data.

    Args:
        rpt_files (list[str]): List of paths to .rpt files.
        txt_files (list[str]): List of paths to .txt files.

    Returns:
        pd.DataFrame: Combined DataFrame with all culverts.
    """
    all_rpt_data = []
    all_txt_data = []

    # Process all .rpt files
    for rpt_file in rpt_files:
        logging.info(f"Processing RPT file: {os.path.relpath(rpt_file)}")
        rpt_data = parse_rpt_file(rpt_file)
        if rpt_data:
            all_rpt_data.extend(rpt_data)
        else:
            logging.warning(f"No culvert data found in {os.path.relpath(rpt_file)}.")

    # Process all .txt files
    for txt_file in txt_files:
        logging.info(f"Processing TXT file: {os.path.relpath(txt_file)}")
        txt_data = parse_txt_file(txt_file)
        if txt_data:
            all_txt_data.extend(txt_data)
        else:
            logging.warning(f"No culvert data found in {os.path.relpath(txt_file)}.")

    # Combine the data
    combined_df = combine_data(all_rpt_data, all_txt_data)

    return combined_df


def process_multiple_files(directory):
    """
    Processes all .rpt and .txt files in the specified directory.

    Args:
        directory (str): Path to the directory containing the files.

    Returns:
        pd.DataFrame: Final combined DataFrame.
    """
    # List all .rpt and .txt files
    rpt_files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.lower().endswith(".rpt")
    ]
    txt_files = [
        os.path.join(directory, f)
        for f in os.listdir(directory)
        if f.lower().endswith(".txt")
    ]

    if not rpt_files and not txt_files:
        logging.warning("No .rpt or .txt files found in the specified directory.")
        return pd.DataFrame()

    combined_df = process_culvert_files(rpt_files, txt_files)

    return combined_df


def report_missing_culverts(combined_df):
    """
    Reports culverts that are missing in either .rpt or .txt data.

    Args:
        combined_df (pd.DataFrame): Combined DataFrame.

    Returns:
        None
    """
    # Culverts missing .rpt data (Angle is '0°0\'0"')
    missing_rpt = combined_df[combined_df["Angle"] == "0°0'0\""]
    if not missing_rpt.empty:
        logging.info(
            "\nCulverts missing or defaulted in .rpt data (Angle set to 0°0'0\"):"
        )
        logging.info(missing_rpt[["Name"]].to_string(index=False))

    # Culverts missing .txt data (US_X or DS_X is 0.0 or NaN)
    missing_txt = combined_df[
        (combined_df["US_X"] == 0.0)
        | (combined_df["DS_X"] == 0.0)
        | (combined_df["US_X"].isna())
        | (combined_df["DS_X"].isna())
    ]
    if not missing_txt.empty:
        logging.info("\nCulverts missing .txt data (US_X or DS_X is 0.0 or NaN):")
        logging.info(missing_txt[["Name"]].to_string(index=False))


def clean_and_convert(combined_df):
    """
    Cleans and converts numerical columns to appropriate data types.

    Args:
        combined_df (pd.DataFrame): Combined DataFrame.

    Returns:
        pd.DataFrame: Cleaned DataFrame with numerical columns converted.
    """
    # Define numerical columns with their desired data types
    numerical_cols = {
        "US_X": "float",
        "US_Y": "float",
        "DS_X": "float",
        "DS_Y": "float",
        "Invert US": "float",
        "Invert DS": "float",
        "Diameter": "float",
        "Width": "float",
        "Number of Pipes": "Int64",  # Allows NaN
        "Separation": "float",
    }

    for col, dtype in numerical_cols.items():
        if col in combined_df.columns:
            try:
                if dtype == "float":
                    combined_df[col] = pd.to_numeric(
                        combined_df[col], errors="coerce"
                    ).fillna(0.0)
                elif dtype == "Int64":
                    combined_df[col] = pd.to_numeric(
                        combined_df[col], errors="coerce"
                    ).astype("Int64")
            except Exception as e:
                logging.error(f"Error converting column '{col}' to {dtype}: {e}")
        else:
            logging.warning(f"Column '{col}' not found in the combined DataFrame.")

    return combined_df


def get_combined_df_from_files(directory):
    """
    Public function to process .rpt and .txt files and obtain combined_df.

    Args:
        directory (str): Path to the directory containing the files.

    Returns:
        pd.DataFrame: Combined DataFrame.
    """
    combined_df = process_multiple_files(directory)

    if combined_df.empty:
        logging.info("No culvert data to process.")
        return combined_df

    # Report culverts missing data in either .rpt or .txt
    report_missing_culverts(combined_df)

    # Clean and convert numerical columns
    combined_df = clean_and_convert(combined_df)

    return combined_df


def get_combined_df_from_csv(csv_path):
    """
    Public function to load combined_df from a CSV file.

    Args:
        csv_path (str): Path to the combined_culverts.csv file.

    Returns:
        pd.DataFrame: Combined DataFrame.
    """
    try:
        combined_df = pd.read_csv(csv_path)
        logging.info(f"Loaded combined DataFrame from {csv_path}")
        # Optionally, perform cleaning and conversion
        combined_df = clean_and_convert(combined_df)
        return combined_df
    except Exception as e:
        logging.error(f"Error loading combined DataFrame from {csv_path}: {e}")
        return pd.DataFrame()
