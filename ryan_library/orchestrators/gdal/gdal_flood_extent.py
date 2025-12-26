# ryan_library/orchestrators/gdal/gdal_flood_extent.py

import os
from pathlib import Path
from loguru import logger
from multiprocessing import Pool
from ryan_library.functions.loguru_helpers import (
    setup_logger,
    log_exception,
    worker_initializer,
)
from ryan_library.functions.gdal.gdal_environment import (
    setup_environment,
    check_required_components,
)
from ryan_library.functions.gdal.gdal_runners import run_gdal_calc, run_gdal_polygonize
from ryan_library.functions.file_utils import find_files_parallel
from ryan_library.functions.misc_functions import calculate_pool_size


def main_processing(
    paths_to_process: list[Path],
    console_log_level: str = "INFO",
    qgis_path: Path = None,
) -> None:
    """
    Generate merged flood extent data by processing various GDAL files.

    Args:
        paths_to_process (list[Path]): List of directory paths to search for files.
        console_log_level (str): Logging level for the console.
        qgis_path (Path, optional): Path to the QGIS install folder. Defaults to None.
    """
    with setup_logger(console_log_level=console_log_level) as log_queue:
        try:
            logger.info("Starting GDAL flood extent processing...")
            logger.info(f"Current Working Directory: {os.getcwd()}")

            # Step 1: Setup environment and check components
            setup_environment(qgis_path)  # Pass the QGIS path here
            check_required_components()

            # Step 2: Find files to process
            patterns = "*_d_HR_Max.tif.tif"
            matched_files: list[Path] = find_files_parallel(
                root_dirs=paths_to_process, patterns=patterns, recursive_search=False
            )

            if not matched_files:
                logger.warning("No valid files found to process.")
                return

            logger.debug(f"Matched files: {matched_files}")
            logger.info(f"Total files to process: {len(matched_files)}")
            # import sys

            # print(sys.path)
            # os.system("PAUSE")

            # Remove any paths containing "AppData\\Roaming"
            # sys.path = [p for p in sys.path if "AppData\\Roaming" not in p]
            # os.system("PAUSE")
            # Step 3: Process files in parallel
            pool_size = calculate_pool_size(num_files=len(matched_files))
            with Pool(
                processes=1,  # pool_size,
                initializer=worker_initializer,
                initargs=(log_queue,),
            ) as pool:
                pool.map(process_file, matched_files)

            logger.info("GDAL flood extent processing completed successfully.")
        except Exception as e:
            logger.exception("An error occurred during GDAL flood extent processing.")
            os.system("PAUSE")


def process_file(filepath: Path):
    """
    Process a single file: run gdal_calc and gdal_polygonize.

    Args:
        filepath (Path): Path to the file to process.
    """
    try:
        logger.info(f"Processing file: {filepath}")
        base_name = filepath.stem

        # Define cutoff values (adjust as needed)
        cutoff_values = [0.0]

        for c in cutoff_values:
            formatted_value = format_cutoff_value(c)

            outname = f"{base_name}_FE_{formatted_value}m.tif"
            shpname = f"{base_name}_FE_{formatted_value}m.shp"

            logger.debug(f"Running gdal_calc for cutoff {c}: {outname}")
            run_gdal_calc(filepath, outname, c)

            logger.debug(f"Running gdal_polygonize for output: {shpname}")
            run_gdal_polygonize(outname, shpname)

    except Exception as e:
        logger.error(f"Error processing file {filepath}: {e}")


def format_cutoff_value(value: float) -> str:
    """
    Format the cutoff value for use in filenames.

    Args:
        value (float): The cutoff value.

    Returns:
        str: Formatted cutoff value as a string.
    """
    formatted_value = f"{value}".rstrip("0").rstrip(".") if "." in f"{value}" else f"{value}"
    return formatted_value.replace(".", "")
