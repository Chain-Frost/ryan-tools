import logging
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Optional, Any
import pandas as pd
import csv

# Importing from private libraries
from ryan_functions.data_processing import (
    check_string_TP,
    check_string_duration,
    check_string_aep,
)
from ryan_functions.misc_functions import setup_logging, calculate_pool_size
from ryan_functions.file_utils import is_non_zero_file

# === Configuration Parameters ===
# These parameters are hardcoded for easy modification
SEPARATOR = ","
SKIP_ROW = [0]
HEADER_ROW = 0
SKIP_COLS = 1
DURATION_SKIP_LIST = []  # Example: ['5760', '7200', '8640', '10080']
QC_CHECK_LIST = (
    list(range(2, 30, 1))
    + list(range(30, 100, 5))
    + list(range(100, 500, 20))
    + list(range(500, 2100, 50))
)

# Initialize logging using the existing setup
setup_logging()
logger = logging.getLogger(__name__)


@dataclass
class PODetails:
    """
    Dataclass to store details extracted from PO CSV filenames.
    """

    Runcode: str
    TPat: str
    Duration: str
    AEP: str
    file: str
    folder: str
    csv: Path
    TrimRC: str
    run_params: dict[str, str] = field(default_factory=dict)


def read_csv(
    filepath: Path, separator: str, skip_row: list[int], header_row: int, skip_cols: int
) -> pd.DataFrame:
    """
    Reads a CSV file and extracts the 'Flow' columns along with 'Time'.

    Args:
        filepath (Path): Path to the CSV file.
        separator (str): Delimiter used in the CSV file.
        skip_row (list[int]): Rows to skip at the start of the file.
        header_row (int): Row number to use as the column names.
        skip_cols (int): Number of columns to skip from the start.

    Returns:
        pd.DataFrame: DataFrame containing the 'Time' and 'Flow' columns.
    """
    search_entry = "Flow"
    try:
        with filepath.open("r") as file:
            reader = csv.reader(file)
            first_row = next(reader)
            flow_columns = [
                i for i, column in enumerate(first_row) if column == search_entry
            ]
            if not flow_columns:
                logger.warning(f"No 'Flow' columns found in {filepath}.")
            # Always include the 'Time' column (assuming it's the second column, index 1)
            flow_columns.append(1)
    except Exception as e:
        logger.error(f"Error reading file {filepath}: {e}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(
            filepath,
            sep=separator,
            skiprows=skip_row,
            header=header_row,
            usecols=flow_columns,
            dtype={"Time": float},  # Ensure 'Time' is read as float
            engine="python",  # Use 'python' engine to handle more complex CSVs if needed
        )
        if "Time" not in df.columns:
            # logger.error(f"'Time' column missing in {filepath}. Skipping file.")
            return pd.DataFrame()
        return df
    except Exception as e:
        logger.error(f"Error parsing CSV {filepath}: {e}")
        return pd.DataFrame()


def stats(
    freqdb: pd.DataFrame, statcol: str, tpcol: str, durcol: str
) -> list[Optional[Any]]:
    """
    Extracts statistical metrics from the frequency database.

    Args:
        freqdb (pd.DataFrame): DataFrame containing frequency data.
        statcol (str): Column name for statistical calculations.
        tpcol (str): Column name for TP critical values.
        durcol (str): Column name for duration.

    Returns:
        list[Optional[Any]]: List containing median, Tcrit, Tpcrit, low, and high values.
    """
    try:
        # Group by duration and calculate median
        grouped = freqdb.groupby(durcol)[statcol]
        median_series = grouped.median()
        overall_median = median_series.max()
        if pd.isna(overall_median):
            logger.warning("Overall median is NaN.")
            return [None, None, None, None, None]

        # Get the row corresponding to the overall median
        crit_rows = freqdb[freqdb[statcol] == overall_median]
        if crit_rows.empty:
            # logger.warning("No critical row found for the overall median.")
            return [None, None, None, None, None]

        crit_row = crit_rows.iloc[0]
        Tcrit = crit_row[durcol]
        Tpcrit = crit_row[tpcol]
        low = grouped.min().max()
        high = grouped.max().max()
        return [overall_median, Tcrit, Tpcrit, low, high]
    except Exception as e:
        logger.error(f"Error computing stats: {e}")
        return [None, None, None, None, None]


def process_PO_csv_name(PO_csv_filename: Path) -> Optional[PODetails]:
    """
    Processes the PO CSV filename to extract parameters.

    Args:
        PO_csv_filename (Path): Path to the PO CSV file.

    Returns:
        Optional[PODetails]: PODetails object if successful, None otherwise.
    """
    basename = PO_csv_filename.name
    Runcode = basename[:-7]  # Remove '_PO.csv'
    logger.info(f"Processing file: {Runcode} : {PO_csv_filename}")

    try:
        TPat = check_string_TP(basename)
        Duration = check_string_duration(basename)
        AEP = check_string_aep(basename)
    except ValueError as e:
        logger.error(f"Error parsing filename {PO_csv_filename}: {e}")
        return None

    # Generate TrimRC by removing specific patterns
    TrimRC = Runcode
    TrimRC = TrimRC.replace(f"TP{TPat}", "")
    TrimRC = TrimRC.replace(f"{Duration}m", "")
    TrimRC = TrimRC.replace(f"{AEP}p", "")
    TrimRC = TrimRC.replace("___", "_").replace("__", "_")

    # Extract run parameters dynamically
    run_parts = Runcode.replace("+", "_").split("_")
    run_params = {f"R{idx+1}": part for idx, part in enumerate(run_parts)}
    logger.debug(f"Run parts extracted: {run_params}")

    return PODetails(
        Runcode=Runcode,
        TPat=TPat,
        Duration=Duration,
        AEP=AEP,
        file=basename,
        folder=str(PO_csv_filename.parent),
        csv=PO_csv_filename,
        TrimRC=TrimRC,
        run_params=run_params,
    )


def process_files(file_subset: list[Path], duration_skip: list[str]) -> list[PODetails]:
    """
    Worker function to process a subset of PO files.

    Args:
        file_subset (list[Path]): List of PO file paths.
        duration_skip (list[str]): List of duration strings to skip.

    Returns:
        list[PODetails]: List of PODetails objects.
    """
    PO_details_list = []
    for PO_file in file_subset:
        if any(d in PO_file.name for d in duration_skip):
            logger.info(f"File excluded due to duration filter: {PO_file}")
            continue
        if is_non_zero_file(str(PO_file)):
            PO_details = process_PO_csv_name(PO_csv_filename=PO_file)
            if PO_details:
                PO_details_list.append(PO_details)
        else:
            logger.warning(f"Zero byte or invalid file: {PO_file}")
    return PO_details_list


def process_hydrographCSV(params_dict: dict[str, Any]) -> Optional[pd.DataFrame]:
    """
    Processes a single hydrograph CSV to calculate durations exceeding thresholds.

    Args:
        params_dict (dict[str, Any]): Dictionary containing parameters and file information.

    Returns:
        Optional[pd.DataFrame]: DataFrame with duration exceeding information or None.
    """
    try:
        aep = params_dict["AEP"]
        dur = params_dict["Duration"]
        tp = params_dict["TPat"]
        csv_read = params_dict["csv"]
        Runcode = params_dict["Runcode"]
        TrimRC = params_dict["TrimRC"]
        if "qc" not in params_dict:
            logger.error(f"'qc' key is missing in params_dict: {params_dict}")
            return None
        qcheckList = params_dict["qc"]

        logger.debug(
            f"Processing hydrograph: AEP={aep}, Duration={dur}, TP={tp}, CSV={csv_read}"
        )

        hydrographs = read_csv(
            filepath=csv_read,
            separator=SEPARATOR,
            skip_row=SKIP_ROW,
            header_row=HEADER_ROW,
            skip_cols=SKIP_COLS,
        )

        if hydrographs.empty:
            logger.warning(f"Hydrographs DataFrame is empty for file: {csv_read}")
            return None

        timestep = hydrographs["Time"].diff().iloc[1]
        if pd.isna(timestep) or timestep <= 0:
            logger.error(f"Invalid timestep in file: {csv_read}")
            return None

        db_list = []
        for qch in qcheckList:
            exceeded = hydrographs.iloc[:, 1:] > qch
            locations = exceeded.any(axis=1).index.tolist()
            dur_exc = exceeded.sum(axis=1) * timestep

            data = {
                "AEP": [aep] * len(locations),
                "Duration": [dur] * len(locations),
                "TP": [tp] * len(locations),
                "Location": locations,
                "ThresholdFlow": [qch] * len(locations),
                "Duration_Exceeding": dur_exc.tolist(),
                "Runcode": [Runcode] * len(locations),
                "TrimRC": [TrimRC] * len(locations),
            }
            dbdictionary = pd.DataFrame(data)
            db_list.append(dbdictionary)

        return pd.concat(db_list, ignore_index=True) if db_list else None

    except Exception as e:
        logger.error(
            f"Error processing hydrograph CSV {params_dict.get('csv')}: {e}",
            exc_info=True,
        )
        return None


def generate_finaldb(df: pd.DataFrame) -> pd.DataFrame:
    """
    Generates the final statistics DataFrame from the processed hydrograph data.

    Args:
        df (pd.DataFrame): DataFrame containing hydrograph data.

    Returns:
        pd.DataFrame: Final statistics DataFrame.
    """
    finaldb_records = []

    try:
        grouped = df.groupby(["TrimRC", "Location", "ThresholdFlow", "AEP"])
        for name, group in grouped:
            median, Tcrit, Tpcrit, low, high = stats(
                group, "Duration_Exceeding", "TP", "Duration"
            )
            if None in [median, Tcrit, Tpcrit, low, high]:
                logger.warning(f"Incomplete stats for group {name}. Skipping.")
                print(finaldb_records)
                continue
            finaldb_records.append(
                {
                    "TrimRC": name[0],
                    "Location": name[1],
                    "Threshold_Q": name[2],
                    "AEP": name[3],
                    "Duration_Exceeded": median,
                    "Critical_Storm": Tcrit,
                    "Critical_Tp": Tpcrit,
                    "Low_Duration": low,
                    "High_Duration": high,
                }
            )
    except Exception as e:
        logger.error(f"Error generating finaldb: {e}", exc_info=True)

    finaldb = pd.DataFrame(
        finaldb_records,
        columns=[
            "TrimRC",
            "Location",
            "Threshold_Q",
            "AEP",
            "Duration_Exceeded",
            "Critical_Storm",
            "Critical_Tp",
            "Low_Duration",
            "High_Duration",
        ],
    )
    return finaldb


def plot_stuff(db: pd.DataFrame, working_dir: Path):
    """
    Placeholder for the plotting function. Currently disabled as per instructions.

    Args:
        db (pd.DataFrame): DataFrame containing hydrograph data.
        working_dir (Path): The working directory for saving plots.
    """
    pass  # Plotting is currently disabled


def main(do_plots: bool = False):
    """
    Main function to orchestrate the processing of PO CSV files and hydrographs.

    Args:
        do_plots (bool): Flag to indicate whether to generate plots. Currently ignored.
    """
    try:
        working_dir = Path(__file__).resolve().parent
        logger.info(f"Working directory set to {working_dir}")

        # Recursively search for PO CSV files
        logger.info(
            "Recursively searching for '**/*_PO.csv' files. This may take a while..."
        )
        file_list = list(working_dir.rglob("*_PO.csv"))
        logger.info(f"Found {len(file_list)} PO files.")

        if not file_list:
            logger.warning("No PO files found. Exiting.")
            return

        # Determine MAX_WORKERS based on the actual number of files
        actual_num_files = len(file_list)
        max_workers = calculate_pool_size(num_files=actual_num_files)
        logger.info(f"Calculated MAX_WORKERS: {max_workers}")

        # Process PO files with multiprocessing
        PO_details_list = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Split file_list into chunks for each worker
            chunk_size = max(1, len(file_list) // max_workers)
            file_chunks = [
                file_list[i : i + chunk_size]
                for i in range(0, len(file_list), chunk_size)
            ]
            futures = {
                executor.submit(process_files, chunk, DURATION_SKIP_LIST): chunk
                for chunk in file_chunks
            }
            for future in as_completed(futures):
                result = future.result()
                if result:
                    PO_details_list.extend(result)

        logger.info(f"Extracted parameters from {len(PO_details_list)} PO files.")

        if not PO_details_list:
            logger.warning("No PO details were extracted. Exiting.")
            return

        # Convert PO details to DataFrame
        files_df = pd.DataFrame([po.__dict__ for po in PO_details_list])
        logger.info(f"PO details DataFrame created with {len(files_df)} records.")
        files_df["qc"] = [QC_CHECK_LIST] * len(files_df)

        # Process hydrographs with multiprocessing
        hydro_data_list = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            hydro_futures = {
                executor.submit(process_hydrographCSV, row): row
                for row in files_df.to_dict(orient="records")
            }
            for future in as_completed(hydro_futures):
                result = future.result()
                if result is not None:
                    hydro_data_list.append(result)

        if not hydro_data_list:
            logger.warning("No hydrograph data was processed. Exiting.")
            return

        # Concatenate all hydrograph data into a single DataFrame
        hydro_df = pd.concat(hydro_data_list, ignore_index=True)
        logger.info(f"Hydrograph processing completed with {len(hydro_df)} records.")

        # Save hydrograph data to parquet first
        datetime_string = datetime.now().strftime("%Y%m%d-%H%M")
        output_parquet = working_dir / f"{datetime_string}_durex.parquet.gzip"
        hydro_df.to_parquet(output_parquet, compression="gzip")
        logger.info(f"Saved hydrograph data to {output_parquet}.")

        # Generate final statistics DataFrame
        finaldb = generate_finaldb(hydro_df)
        finaldb_csv = working_dir / f"{datetime_string}_QvsTexc.csv"
        finaldb.to_csv(finaldb_csv, index=False)
        logger.info(f"Saved final statistics to {finaldb_csv}.")

        # Then save to CSV
        output_csv = working_dir / f"{datetime_string}_durex.csv"
        hydro_df.to_csv(output_csv, index=False)
        logger.info(f"Saved hydrograph data to {output_csv}.")

        # Plotting is currently disabled
        if do_plots:
            plot_stuff(hydro_df, working_dir)

        logger.info("Processing completed successfully.")

    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}", exc_info=True)


if __name__ == "__main__":
    main(do_plots=False)
