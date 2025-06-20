# ryan_library/scripts/pomm_combine.py

import os
from multiprocessing import Pool
from pathlib import Path
import pandas as pd
from loguru import logger
import logging
import multiprocessing
from glob import iglob
from datetime import datetime

from ryan_library.functions.file_utils import (
    find_files_parallel,
    is_non_zero_file,
    ensure_output_directory,
)
from ryan_library.functions.misc_functions import calculate_pool_size, ExcelExporter
from ryan_library.functions.data_processing import (
    safe_apply,
    check_string_TP,
    check_string_duration,
    check_string_aep,
)
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import (
    setup_logger,
    log_exception,
    worker_initializer,
)
from ryan_library.functions.logging_helpers import setup_logging


def main_processing(
    paths_to_process: list[Path],
    include_data_types: list[str] = ["POMM"],
    console_log_level: str = "INFO",
) -> None:
    """Generate merged culvert data by processing various TUFLOW CSV and CCA files.

    Args:
        paths_to_process (list[Path]): List of directory paths to search for files.
        include_data_types (list[str] | None): List of data types to include. If None, include all.
        console_log_level (str): Logging level for the console."""

    with setup_logger(console_log_level=console_log_level) as log_queue:
        logger.info("Starting culvert results files processing...")
        logger.info(f"Current working directory: {os.getcwd()}")

        # Step 1: Collect and validate files
        csv_file_list: list[Path] = collect_files(
            paths_to_process=paths_to_process,
            include_data_types=include_data_types,
            suffixes_config=SuffixesConfig.get_instance(),
        )
        if not csv_file_list:
            logger.info("No valid files found to process.")
            return

        logger.debug(f"Files to process: {csv_file_list}")
        logger.info(f"Total files to process: {len(csv_file_list)}")

        # Step 2: Process files in parallel

        results_set: ProcessorCollection = process_files_in_parallel(
            file_list=csv_file_list,
            log_queue=log_queue,
        )

        # Step 3: Combine and export results based on scenarios
        logger.info("Now exporting results...")
        export_results(results=results_set)

        logger.info("End of culvert results combination processing")


def collect_files(
    paths_to_process: list[Path],
    include_data_types: list[str],
    suffixes_config: SuffixesConfig,
) -> list[Path]:
    """Collect and filter files based on specified data types.

    Args:
        paths_to_process (list[Path]): Directories to search.
        include_data_types (list[str] ): List of data types to include.
        suffixes_config (SuffixesConfig ): Suffixes configuration instance.

    Returns:
        list[Path]: List of valid file paths."""

    csv_file_list: list[Path] = []
    suffixes: list[str] = []

    # Determine which suffixes to include based on data types
    # Invert suffixes config
    data_type_to_suffix: dict[str, list[str]] = suffixes_config.invert_suffix_to_type()

    if include_data_types and len(include_data_types) > 0:
        for data_type in include_data_types:
            dt_suffixes: list[str] | None = data_type_to_suffix.get(data_type)
            if not dt_suffixes:
                logger.error(
                    f"No suffixes found for data type '{data_type}'. Skipping."
                )
                continue
            suffixes.extend([s for s in dt_suffixes])  # Preserve capitalization

    if not suffixes:
        logger.error("No suffixes found for the specified data types.")
        return csv_file_list

    # Prepend '*' for wildcard searching
    patterns: list[str] = [f"*{suffix}" for suffix in suffixes]

    root_dirs: list[Path] = [p for p in paths_to_process if p.is_dir()]
    invalid_dirs: set[Path] = set(paths_to_process) - set(root_dirs)
    for invalid_dir in invalid_dirs:
        logger.warning(f"Path {invalid_dir} is not a directory. Skipping.")

    matched_files: list[Path] = find_files_parallel(
        root_dirs=root_dirs, patterns=patterns
    )
    csv_file_list.extend(matched_files)

    # Filter for non-zero files
    csv_file_list = [f for f in csv_file_list if is_non_zero_file(f)]
    # logger.debug(f"Collected files: {csv_file_list}")

    return csv_file_list


def process_files_in_parallel(file_list: list[Path], log_queue) -> ProcessorCollection:
    """Process files using multiprocessing.

    Args:
        file_list (list[Path]): Files to process.
        log_queue (Queue): Logging queue.

    Returns:
        ProcessorCollection: Collection of successfully processed processors."""
    pool_size: int = calculate_pool_size(num_files=len(file_list))
    logger.info(f"Initializing multiprocessing pool with {pool_size} processes.")

    results_set = ProcessorCollection()
    # Initialize the Pool with the worker initializer and pass the log_queue via initargs

    with Pool(
        processes=pool_size,
        initializer=worker_initializer,
        initargs=(log_queue,),
    ) as pool:
        results: list[BaseProcessor] = pool.map(func=process_file, iterable=file_list)

    for result in results:
        if result is not None and result.processed:
            results_set.add_processor(processor=result)

    return results_set


def process_file(file_path: Path) -> BaseProcessor:
    """Process a single file by delegating to BaseProcessor.

    Args:
        file_path (Path): Path to the file to process.

    Returns:
        BaseProcessor: The processed BaseProcessor instance.

    Raises:
        Exception: If processing fails."""
    try:
        # BaseProcessor.from_file determines and instantiates the correct processor
        processor: BaseProcessor = BaseProcessor.from_file(file_path=file_path)
        processor.process()

        if processor.validate_data():
            logger.info(f"Successfully processed file: {file_path}")
        else:
            logger.warning(f"Validation failed for file: {file_path}")
        return processor
    except Exception as e:
        logger.exception(f"Failed to process file {file_path}: {e}")
        raise


def export_results(results: ProcessorCollection) -> None:
    """Export combined DataFrames
    Args:
        results (ProcessorCollection): Collection of processed processors."""
    if not results.processors:
        logger.warning("No results to export.")
        return

    exporter = ExcelExporter()

    # Scenario 1: combine_1d_maximums
    scenario1 = "combined_POMM"
    file_name_prefix1 = "combined_POMM"
    sheet_name1 = "combined_POMM"
    output_path1: Path = Path.cwd()
    # Path("exports/maximums")  # Optional: Specify your desired path

    combined_df1: pd.DataFrame = results.pomm_combine()

    if combined_df1.empty:
        logger.warning(f"No data found for scenario '{scenario1}'. Skipping export.")
    else:
        # Ensure the output directory exists
        ensure_output_directory(output_dir=output_path1)

        # Perform the export for combine_1d_maximums
        exporter.save_to_excel(
            data_frame=combined_df1,
            file_name_prefix=file_name_prefix1,
            sheet_name=sheet_name1,
            output_directory=output_path1,  # Pass the optional path
        )
        logger.info(f"Exported data for scenario '{scenario1}' successfully.")


# ---------------------------------------------------------------------------
# Legacy functions preserved for backward compatibility
# ---------------------------------------------------------------------------


def process_pomm_file(file_path: str) -> pd.DataFrame:
    """Process a single POMM CSV file and transform it into a standardized DataFrame.

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
        original_df = pd.read_csv(filepath_or_buffer=file_path, header=None)

        run_code = original_df.iat[0, 0]

        transposed_df = original_df.drop(columns=0).T

        transposed_df.columns = pd.Index(transposed_df.iloc[0], dtype=str)
        transposed_df = transposed_df.drop(index=transposed_df.index[0])

        column_mappings = {
            "Type": ("Location", "string"),
            "Location": ("Time", "string"),
            "Max": ("Maximum (Extracted from Time Series)", "float"),
            "Tmax": ("Time of Maximum", "float"),
            "Min": ("Minimum (Extracted From Time Series)", "float"),
            "Tmin": ("Time of Minimum", "float"),
        }

        for new_col, (old_col, dtype) in column_mappings.items():
            if old_col in transposed_df.columns:
                transposed_df.rename(columns={old_col: new_col}, inplace=True)
                transposed_df[new_col] = transposed_df[new_col].astype(dtype)

        run_code_parts: list[str] = run_code.replace("+", "_").split("_")
        for index, part in enumerate(run_code_parts, start=1):
            transposed_df.insert(index, f"R{index:02}", str(part))

        transposed_df["AbsMax"] = transposed_df[["Max", "Min"]].abs().max(axis=1)

        transposed_df["SignedAbsMax"] = transposed_df.apply(
            lambda row: row["Max"] if abs(row["Max"]) >= abs(row["Min"]) else row["Min"],
            axis=1,
        )

        transposed_df["TP"] = safe_apply(check_string_TP, run_code)
        transposed_df["Duration"] = safe_apply(check_string_duration, run_code)
        transposed_df["AEP"] = safe_apply(check_string_aep, run_code)
        transposed_df["RunCode"] = run_code

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

    except Exception as e:  # pragma: no cover - safety net
        logging.error(f"Error processing file {file_path}: {e}", exc_info=True)
        return pd.DataFrame()


def clean_runcode(run_code: str, aep: str | None, duration: str | None, tp: str | None) -> str:
    """Clean the RunCode by removing the extracted AEP, Duration, and TP values."""

    if not isinstance(run_code, str):
        logging.error(f"RunCode must be a string. Received type: {type(run_code)}")
        return ""

    standardized_runcode = run_code.replace("+", "_")
    parts = standardized_runcode.split("_")

    parts_to_remove = set()
    if aep:
        parts_to_remove.add(aep)
    if duration:
        parts_to_remove.add(duration)
    if tp:
        parts_to_remove.add(tp)

    filtered_parts = [part for part in parts if part not in parts_to_remove and part.strip() != ""]

    cleaned_runcode = "_".join(filtered_parts)
    logging.debug(f"Original RunCode: {run_code}, Cleaned RunCode: {cleaned_runcode}")

    return cleaned_runcode


def aggregate_pomm_data(pomm_data: list[pd.DataFrame]) -> pd.DataFrame:
    """Aggregate a list of DataFrames into a single DataFrame."""

    valid_data_frames = [df for df in pomm_data if not df.empty]
    aggregated_df = pd.concat(valid_data_frames, ignore_index=True)
    return aggregated_df


def save_to_excel(aep_dur_max: pd.DataFrame, aep_max: pd.DataFrame, output_path: Path):
    """Save the provided DataFrames to an Excel file with separate sheets."""

    try:
        logging.info(f"Output path: {output_path}")
        with pd.ExcelWriter(output_path) as writer:
            aep_dur_max.to_excel(writer, sheet_name="aep-dur-max", index=False, merge_cells=False)
            aep_max.to_excel(writer, sheet_name="aep-max", index=False, merge_cells=False)
        logging.info(f"Peak data exported to {output_path}")
    except Exception as e:  # pragma: no cover - safety net
        logging.error(f"Failed to save peak data to Excel: {e}")


def save_aggregated_data(aggregated_df: pd.DataFrame, output_path: Path):
    """Save the aggregated DataFrame to an Excel file."""

    try:
        aggregated_df.to_excel(output_path, index=False)
        logging.info(f"Aggregated data exported to {output_path}")
    except Exception as e:  # pragma: no cover - safety net
        logging.error(f"Failed to save aggregated data to Excel: {e}")


def process_all_files(pomm_files: list[str]) -> pd.DataFrame:
    """Process all POMM files and aggregate the data."""

    if not pomm_files:
        logging.warning("No POMM CSV files found. Exiting.")
        return pd.DataFrame()

    pool_size = calculate_pool_size(len(pomm_files))

    with multiprocessing.Pool(pool_size) as pool:
        pomm_data = pool.map(process_pomm_file, pomm_files)

    aggregated_df = aggregate_pomm_data(pomm_data)
    logging.info("Aggregated POMM DataFrame head:")
    logging.info(f"\n{aggregated_df.head()}")
    return aggregated_df


def legacy_main_processing() -> None:
    """Legacy main processing for backward compatibility."""

    start_time = datetime.now()
    setup_logging()

    script_directory = Path.cwd().resolve()

    pomm_files = [str(f) for f in iglob("**/*POMM.csv", recursive=True) if Path(f).is_file()]
    logging.info(f"Number of POMM files found: {len(pomm_files)}")

    aggregated_df = process_all_files(pomm_files)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M")
    output_filename_aggregated = f"{timestamp}_POMM.xlsx"
    output_path_aggregated = script_directory / output_filename_aggregated
    save_aggregated_data(aggregated_df, output_path_aggregated)

    end_time = datetime.now()
    runtime = end_time - start_time
    logging.info(f"Total run time: {runtime}")


if __name__ == "__main__":  # pragma: no cover - manual execution
    legacy_main_processing()
