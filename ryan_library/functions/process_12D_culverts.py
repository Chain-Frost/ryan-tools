# ryan_library/functions/process_12D_culverts.py
from typing import Type
import pandas as pd
import re
from pathlib import Path
from loguru import logger


def get_encoding(file_path: Path) -> str:
    """
    Determines the encoding of the file based on BOM.

    Args:
        file_path (Path): Path to the file.

    Returns:
        str: 'utf-8', 'utf-8-sig', or 'utf-16'
    """
    with file_path.open("rb") as f:
        first_bytes = f.read(4)
    if first_bytes.startswith(b"\xff\xfe") or first_bytes.startswith(b"\xfe\xff"):
        return "utf-16"
    elif first_bytes.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    else:
        return "utf-8"


def dms_to_decimal(dms_str: str) -> float:
    """
    Converts a DMS (Degrees, Minutes, Seconds) string to decimal degrees.

    Args:
        dms_str (str): Angle in DMS format, e.g., "52°10'14"".

    Returns:
        float: Angle in decimal degrees.
    """
    try:
        dms_clean = re.sub(r'[°\'"]', " ", dms_str).strip()
        parts = dms_clean.split()
        if len(parts) != 3:
            logger.warning(
                f"Unexpected DMS format '{dms_str}'. Setting angle_degrees to 0.0."
            )
            return 0.0
        degrees, minutes, seconds = map(float, parts)
        decimal_degrees = degrees + minutes / 60 + seconds / 3600
        return decimal_degrees
    except Exception as e:
        logger.error(
            f"Error converting DMS to decimal for '{dms_str}': {e}. Setting angle_degrees to 0.0."
        )
        return 0.0


def get_field(upstream_val: str, downstream_val: str, default=None) -> str:
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
    if isinstance(upstream_val, str) and upstream_val.strip():
        return upstream_val.strip()
    elif isinstance(downstream_val, str) and downstream_val.strip():
        return downstream_val.strip()
    else:
        return default


def extract_numeric(
    value: str, field_name: str, culvert_name: str, dtype: type, default: float | int
):
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
    except (ValueError, TypeError):
        logger.warning(
            f"Invalid {field_name} value '{value}' for culvert '{culvert_name}'. Setting to {default}."
        )
        return default


def parse_rpt_file(rpt_file_path: Path) -> list[dict[str, any]]:
    """
    Parses a .rpt file to extract culvert names and angles.

    Args:
        rpt_file_path (Path): Path to the .rpt file.

    Returns:
        list[dict[str, any]]: A list of dictionaries with 'Name', 'Angle', and 'Angle_Degrees'.
    """
    culverts = []
    line_pattern = re.compile(
        r'^\s*\d+\.\d+\s+\d+\.\d+\s+(\d+°\s*\d+\'\s*\d+")\s+\d+\.\d+\s+\d+\.\d+\s+\d+\.\d+\s+\d+\.\d+\s+[-+]?\d+\.\d+\s+"([^"]+)"$'
    )
    encoding = get_encoding(rpt_file_path)
    logger.info(
        f"Detected encoding for {rpt_file_path.relative_to(rpt_file_path.parent)}: {encoding}"
    )

    try:
        with rpt_file_path.open("r", encoding=encoding, errors="ignore") as file:
            for line in file:
                match = line_pattern.match(line)
                if match:
                    angle = match.group(1).strip()
                    full_name = match.group(2).strip()
                    name = (
                        full_name.split("->")[-1].strip()
                        if "->" in full_name
                        else full_name
                    )
                    name = name.replace("\x00", "").strip()
                    angle = angle if angle else "0°0'0\""
                    angle_degrees = dms_to_decimal(angle)
                    culverts.append(
                        {"Name": name, "Angle": angle, "Angle_Degrees": angle_degrees}
                    )
                    logger.debug(
                        f"Parsed Culvert - Name: {name}, Angle: {angle}, Angle_Degrees: {angle_degrees}"
                    )
    except Exception as e:
        logger.error(
            f"Error processing {rpt_file_path.relative_to(rpt_file_path.parent)}: {e}"
        )

    return culverts


def parse_txt_file(txt_file_path: Path) -> list[dict[str, any]]:
    """
    Parses a .txt file to extract detailed culvert information.

    Args:
        txt_file_path (Path): Path to the .txt file.

    Returns:
        list[dict[str, any]]: A list of dictionaries with culvert details.
    """
    culverts = []
    encoding = get_encoding(txt_file_path)
    logger.info(
        f"Detected encoding for {txt_file_path.relative_to(txt_file_path.parent)}: {encoding}"
    )

    try:
        # Read the file, skip the first and third rows
        df = pd.read_csv(
            txt_file_path, sep="\t", skiprows=[0, 2], encoding=encoding, dtype=str
        )

        # Normalize column names: strip spaces and convert to lowercase
        df.columns = df.columns.str.strip().str.lower()

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

        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.warning(
                f"Missing columns {missing_columns} in {txt_file_path.relative_to(txt_file_path.parent)}. Skipping this file."
            )
            return culverts

        # Reset index for proper iteration
        df = df.reset_index(drop=True)
        num_rows = len(df)
        if num_rows % 2 != 0:
            logger.warning(
                f"Odd number of culvert entries in {txt_file_path.relative_to(txt_file_path.parent)}. The last entry will be skipped."
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
                logger.warning(
                    f"Mismatched culvert names at lines {i+1} and {i+2} in {txt_file_path.relative_to(txt_file_path.parent)}. Skipping these entries."
                )
                continue

            name = name_upstream

            us_x = get_field(upstream["pit centre x"], None, "0.0")
            us_y = get_field(upstream["pit centre y"], None, "0.0")
            ds_x = get_field(downstream["pit centre x"], None, "0.0")
            ds_y = get_field(downstream["pit centre y"], None, "0.0")

            invert_us = get_field(downstream["invert us"], upstream["invert us"], "0.0")
            invert_ds = get_field(upstream["invert ds"], downstream["invert ds"], "0.0")
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
            logger.debug(f"Parsed TXT Culvert - {culvert}")

    except Exception as e:
        logger.error(
            f"Error processing {txt_file_path.relative_to(txt_file_path.parent)}: {e}"
        )

    return culverts


def combine_data(
    rpt_data: list[dict[str, any]], txt_data: list[dict[str, any]]
) -> pd.DataFrame:
    """
    Combines .rpt and .txt data based on the 'Name' field.

    Args:
        rpt_data (list[dict[str, any]]): Data from .rpt files.
        txt_data (list[dict[str, any]]): Data from .txt files.

    Returns:
        pd.DataFrame: Combined DataFrame with culvert information.
    """
    rpt_df = pd.DataFrame(rpt_data).drop_duplicates(subset=["Name"])
    txt_df = pd.DataFrame(txt_data).drop_duplicates(subset=["Name"])

    logger.info(f"Unique RPT Culverts: {len(rpt_df)}")
    logger.info(f"Unique TXT Culverts: {len(txt_df)}")

    combined_df = pd.merge(
        rpt_df, txt_df, on="Name", how="outer", suffixes=("_rpt", "_txt")
    )

    combined_df["Angle"] = combined_df["Angle"].fillna("0°0'0\"")
    combined_df["Angle_Degrees"] = combined_df.apply(
        lambda row: (
            row["Angle_Degrees"]
            if pd.notna(row["Angle_Degrees"])
            else dms_to_decimal(row["Angle"])
        ),
        axis=1,
    )

    return combined_df


def process_culvert_files(rpt_files: list[Path], txt_files: list[Path]) -> pd.DataFrame:
    """
    Processes all .rpt and .txt files and combines their data.

    Args:
        rpt_files (list[Path]): List of paths to .rpt files.
        txt_files (list[Path]): List of paths to .txt files.

    Returns:
        pd.DataFrame: Combined DataFrame with all culverts.
    """
    all_rpt_data = []
    all_txt_data = []

    for rpt_file in rpt_files:
        logger.info(f"Processing RPT file: {rpt_file.name}")
        rpt_data = parse_rpt_file(rpt_file)
        if rpt_data:
            all_rpt_data.extend(rpt_data)
        else:
            logger.warning(f"No culvert data found in {rpt_file.name}.")

    for txt_file in txt_files:
        logger.info(f"Processing TXT file: {txt_file.name}")
        txt_data = parse_txt_file(txt_file)
        if txt_data:
            all_txt_data.extend(txt_data)
        else:
            logger.warning(f"No culvert data found in {txt_file.name}.")

    combined_df = combine_data(all_rpt_data, all_txt_data)
    return combined_df


def process_multiple_files(directory: Path) -> pd.DataFrame:
    """
    Processes all .rpt and .txt files in the specified directory.

    Args:
        directory (Path): Path to the directory containing the files.

    Returns:
        pd.DataFrame: Final combined DataFrame.
    """
    rpt_files = list(directory.glob("*.rpt"))
    txt_files = list(directory.glob("*.txt"))

    if not rpt_files and not txt_files:
        logger.warning("No .rpt or .txt files found in the specified directory.")
        return pd.DataFrame()

    combined_df = process_culvert_files(rpt_files, txt_files)
    return combined_df


def report_missing_culverts(combined_df: pd.DataFrame) -> None:
    """
    Reports culverts that are missing in either .rpt or .txt data.

    Args:
        combined_df (pd.DataFrame): Combined DataFrame.

    Returns:
        None
    """
    missing_rpt = combined_df[combined_df["Angle"] == "0°0'0\""]
    if not missing_rpt.empty:
        logger.info(
            "\nCulverts missing or defaulted in .rpt data (Angle set to 0°0'0\"):"
        )
        logger.info(missing_rpt["Name"].to_string(index=False))

    missing_txt = combined_df[
        (combined_df["US_X"] == 0.0)
        | (combined_df["DS_X"] == 0.0)
        | (combined_df["US_X"].isna())
        | (combined_df["DS_X"].isna())
    ]
    if not missing_txt.empty:
        logger.info("\nCulverts missing .txt data (US_X or DS_X is 0.0 or NaN):")
        logger.info(missing_txt["Name"].to_string(index=False))


def clean_and_convert(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and converts numerical columns to appropriate data types.

    Args:
        combined_df (pd.DataFrame): Combined DataFrame.

    Returns:
        pd.DataFrame: Cleaned DataFrame with numerical columns converted.
    """
    numerical_cols = {
        "US_X": "float",
        "US_Y": "float",
        "DS_X": "float",
        "DS_Y": "float",
        "Invert US": "float",
        "Invert DS": "float",
        "Diameter": "float",
        "Width": "float",
        "Number of Pipes": "Int64",
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
                logger.error(f"Error converting column '{col}' to {dtype}: {e}")
        else:
            logger.warning(f"Column '{col}' not found in the combined DataFrame.")

    return combined_df


def get_combined_df_from_files(directory: Path) -> pd.DataFrame:
    """
    Public function to process .rpt and .txt files and obtain combined_df.

    Args:
        directory (Path): Path to the directory containing the files.

    Returns:
        pd.DataFrame: Combined DataFrame.
    """
    combined_df = process_multiple_files(directory)

    if combined_df.empty:
        logger.info("No culvert data to process.")
        return combined_df

    report_missing_culverts(combined_df)
    combined_df = clean_and_convert(combined_df)

    return combined_df


def get_combined_df_from_csv(csv_path: Path) -> pd.DataFrame:
    """
    Public function to load combined_df from a CSV file.

    Args:
        csv_path (Path): Path to the combined_culverts.csv file.

    Returns:
        pd.DataFrame: Combined DataFrame.
    """
    try:
        combined_df = pd.read_csv(csv_path)
        logger.info(f"Loaded combined DataFrame from {csv_path}")
        combined_df = clean_and_convert(combined_df)
        return combined_df
    except Exception as e:
        logger.error(f"Error loading combined DataFrame from {csv_path}: {e}")
        return pd.DataFrame()
