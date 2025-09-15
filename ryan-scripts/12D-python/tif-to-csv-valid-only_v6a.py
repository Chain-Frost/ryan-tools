# tif-to-csv-valid-only_v6a.py
# Note that this will round outputs to nearest 0.001
import logging
from pathlib import Path
import pandas as pd
import numpy as np
from ryan_library.functions.terrain_processing import parallel_process_multiple_terrain


def save_tile_csv(tile_df, output_dir, base_filename, i, j):
    """
    Saves a tile DataFrame as a CSV file with precision rounded to 0.001.
    """
    logger = logging.getLogger(__name__)
    tile_filename = f"{base_filename}_tile_{i}_{j}.csv"
    tile_path = output_dir / tile_filename

    try:
        # Round X, Y, Z columns to 3 decimal places
        tile_df = tile_df.round({"X": 3, "Y": 3, "Z": 3})

        # Save the tile DataFrame to CSV
        tile_df.to_csv(tile_path, index=False)
        logger.info(f"Saved tile: {tile_filename}")
    except Exception as e:
        logger.error(f"Failed to save CSV tile {tile_filename}: {e}")


def save_full_csv(df, output_dir, base_filename):
    """
    Saves the full DataFrame as a single CSV file without tiling, with precision rounded to 0.001.
    """
    logger = logging.getLogger(__name__)
    csv_filename = f"{base_filename}.csv"
    output_path = output_dir / csv_filename

    try:
        # Round X, Y, Z columns to 3 decimal places
        df = df.round({"X": 3, "Y": 3, "Z": 3})

        # Save the full DataFrame to CSV
        df.to_csv(output_path, index=False)
        logger.info(f"Saved file without tiling: {csv_filename}")
    except Exception as e:
        logger.error(f"Failed to save CSV file {csv_filename}: {e}")


def main():
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("csv_processing.log", mode="w"),
        ],
    )
    logger = logging.getLogger(__name__)
    logger.info("Starting CSV terrain data processing script.")

    # Configuration parameters
    nodata_values = None  # Replace with your no data values or None
    tile_size = 1000  # Set your desired tile size here (e.g., in coordinate units)
    use_tiling = False  # Set to True to enable tiling

    # Set script_dir to a specific path
    script_dir = Path(__file__).absolute().parent
    logger.info(f"Script directory: {script_dir}")

    # Verify that script_dir exists
    if not script_dir.exists():
        logger.error(f"The specified script directory does not exist: {script_dir}")
        return

    # Define the output directory
    output_dir = script_dir / "output_csv_files"  # Using Path objects
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Find all .tif files in the script_dir
    tif_files = list(script_dir.glob("*.tif"))
    if not tif_files:
        logger.warning("No .tif files found in the script directory.")
        return
    logger.info(f"Found {len(tif_files)} .tif files to process.")

    # Define the saving function based on tiling
    if use_tiling:
        save_function = save_tile_csv
    else:
        save_function = save_full_csv

    # Start processing using the parallel_process_multiple_terrain function
    parallel_process_multiple_terrain(
        files=tif_files,
        output_dir=output_dir,
        nodata_values=nodata_values,
        tile_size=tile_size if use_tiling else None,
        save_function=save_function,
    )

    logger.info("Completed all terrain data processing.")


if __name__ == "__main__":
    main()
