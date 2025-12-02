from collections.abc import Generator
from typing import Any
from pandas import Series
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from pathlib import Path
import os
import pandas as pd
from glob import glob
from multiprocessing import Pool
import numpy as np
from math import ceil

from loguru import logger
from ryan_library.functions.loguru_helpers import setup_logger, worker_initializer
from ryan_library.functions.file_utils import ensure_output_directory
from ryan_library.scripts.wrapper_utils import print_library_version


def thin_data_by_global_selection(df: pd.DataFrame, thinning_factor: int) -> pd.DataFrame:
    """
    Thins the data by keeping only the rows where global row and column indices
    are multiples of the thinning factor.

    Parameters:
    - df: pandas DataFrame containing columns 'row', 'col', and 'Z'.
    - thinning_factor: int, the thinning factor.

    Returns:
    - thinned_data: pandas DataFrame after thinning containing columns 'row', 'col', and 'Z'.
    """
    # Create a mask based on row and column indices
    mask: pd.Series = (df["row"] % thinning_factor == 0) & (df["col"] % thinning_factor == 0)
    thinned_data: pd.DataFrame = df[mask].copy()

    # Drop rows with NaN in 'Z' column
    thinned_data.dropna(subset=["Z"], inplace=True)

    return thinned_data


def save_thinned_data(thinned_data, output_path, tile_id, factor) -> None:
    """
    Saves the thinned data to a CSV file and logs the action.
    Expects thinned_data to have columns: 'X', 'Y', and 'Z'.
    """
    try:
        thinned_data.to_csv(output_path, index=False)
        logger.info(f"{tile_id}: Successfully saved thinned data (factor={factor}) to {output_path}.")
    except Exception as e:
        logger.error(f"{tile_id}: Failed to save thinned data (factor={factor}) to {output_path}. Error: {e}")


def assign_tiles(bounds, tile_size) -> Generator[tuple[int, int, Any, Any, Any, Any], Any, None]:
    """
    Generates tile boundaries based on the raster bounds and tile size.

    Parameters:
    - bounds: tuple, (left, bottom, right, top) in the raster's coordinate system.
    - tile_size: int, size of the tile in meters (default is 5000 for 5km).

    Yields:
    - (i, j, tile_left, tile_bottom, tile_right, tile_top): tuple containing tile indices and boundaries.
    """
    left, bottom, right, top = bounds
    tile_width = tile_size
    tile_height = tile_size

    # Calculate the number of tiles in X and Y directions
    num_tiles_x = ceil((right - left) / tile_width)
    num_tiles_y = ceil((top - bottom) / tile_height)

    for i in range(num_tiles_x):
        for j in range(num_tiles_y):
            tile_left = left + i * tile_width
            tile_right = min(left + (i + 1) * tile_width, right)
            tile_bottom = bottom + j * tile_height
            tile_top = min(bottom + (j + 1) * tile_height, top)

            yield (i, j, tile_left, tile_bottom, tile_right, tile_top)


def determine_global_selection(input_file: str, thinning_factors: list[int]) -> list[int]:
    """
    Determines the thinning factors. Since we're shifting to row/col thinning,
    this function might not be necessary. But keeping it for flexibility.

    Parameters:
    - input_file: str, path to the input GeoTIFF file.
    - thinning_factors: list of int, thinning factors.

    Returns:
    - thinning_factors: list of int, unchanged.
    """
    return thinning_factors


def init_worker(queue) -> None:
    """
    Initializer for worker processes. Sets up logging to use the provided queue.
    """
    worker_initializer(queue)


def process_tile(window, tile_id, input_file, thinning_factors, output_dir, transform):
    """
    Processes a single tile: reads data, thins it for each thinning factor, and saves to the output directory.
    Skips saving if the tile contains only nodata values or has no data after thinning.

    Parameters:
    - window: The raster window to read.
    - tile_id: str, identifier for the tile.
    - input_file: str, path to the input GeoTIFF file.
    - thinning_factors: list of int, thinning factors to apply.
    - output_dir: str, directory to save output CSV files.
    - transform: Affine transformation from raster coordinate space to CRS coordinate space.
    """
    logger.info(f"Processing {tile_id}...")

    try:
        with rasterio.open(input_file) as src:
            # Read the data within the window as a masked array
            data = src.read(1, window=window, masked=True)
            logger.info(f"{tile_id}: Data read successfully.")

            # If the data is masked, replace masked values with NaN
            data = data.filled(np.nan)

            # Retrieve nodata value from the file
            nodata = src.nodata
            logger.info(f"{tile_id}: Nodata value is {nodata}.")

            # Replace specified nodata values with NaN if needed
            if nodata is not None:
                data = np.where(data == nodata, np.nan, data)
                logger.info(f"{tile_id}: Nodata values replaced with NaN.")

        # Get global row and column offsets
        global_row_off = int(window.row_off)
        global_col_off = int(window.col_off)
        rows, cols = data.shape

        # Create global row and column indices for the full tile
        row_indices = np.arange(global_row_off, global_row_off + rows)
        col_indices = np.arange(global_col_off, global_col_off + cols)

        # Create meshgrid of global indices
        global_rows, global_cols = np.meshgrid(row_indices, col_indices, indexing="ij")

        # Flatten the data arrays
        z_flat = data.flatten()
        row_flat = global_rows.flatten()
        col_flat = global_cols.flatten()

        # Create df for real-world coordinates and elevation data
        x_coords, y_coords = transform * (col_flat, row_flat)
        df = pd.DataFrame({"X": x_coords, "Y": y_coords, "Z": z_flat})

        # Drop rows with NaN in 'Z' column (Nodata or masked values)
        df.dropna(subset=["Z"], inplace=True)

        logger.info(f"{tile_id}: DataFrame created with {len(df)} rows after removing nodata.")

        if df.empty:
            logger.warning(f"{tile_id} has no valid data after removing nodata. Skipping.")
            return  # Skip saving if there's no data

        # Data for thinning is based on row/col
        df_thin = pd.DataFrame({"row": row_flat, "col": col_flat, "Z": z_flat})
        df_thin.dropna(subset=["Z"], inplace=True)

        # Apply thinning for each factor
        for factor in thinning_factors:
            logger.info(f"{tile_id}: Applying thinning with factor {factor}.")
            thinned_data = thin_data_by_global_selection(df_thin, factor)
            retained_rows = len(thinned_data)
            logger.info(f"{tile_id}: Thinning with factor {factor} completed. {retained_rows} rows retained.")

            if retained_rows == 0:
                logger.warning(f"{tile_id} has no valid data after thinning with factor {factor}. Skipping save.")
                continue  # Skip saving if there's no data

            # Convert thinned row/col back to real-world coordinates for saving
            x_thinned, y_thinned = transform * (
                thinned_data["col"],
                thinned_data["row"],
            )
            output_df = pd.DataFrame({"X": x_thinned, "Y": y_thinned, "Z": thinned_data["Z"]})

            # Define output filename
            output_filename = f"{tile_id}_GRID_DTM_thinned_{factor}m.csv"
            output_path = os.path.join(output_dir, output_filename)

            # Save the thinned data
            save_thinned_data(output_df, output_path, tile_id, factor)

    except rasterio.errors.RasterioIOError as rio_err:
        logger.error(f"RasterIO error processing {tile_id}: {rio_err}")
    except Exception as e:
        logger.error(f"Unexpected error processing {tile_id}: {e}")


def process_terrain_data(input_file, output_dir, thinning_factors=[10, 5, 2], tile_size=5000, log_queue=None):
    """
    Processes the terrain data from a GeoTIFF file: assigns tiles, thins data for multiple
    thinning factors, and saves to CSV.

    Parameters:
    - input_file: str, path to the input GeoTIFF file.
    - output_dir: str, directory where output CSVs will be saved.
    - thinning_factors: list of int, thinning factors to apply.
    - tile_size: int, size of the tile in meters (default is 5000 for 5km).
    """
    logger.info(f"Processing terrain data from file: {input_file}")

    # Determine the thinning factors (no global selection needed)
    thinning_factors = determine_global_selection(input_file, thinning_factors)

    with rasterio.open(input_file) as src:
        bounds = src.bounds
        transform = src.transform  # The affine transform for the raster
        crs = src.crs
        logger.info(f"Raster bounds: {bounds}")
        logger.info(f"Raster CRS: {crs}")

    # Generate tile boundaries
    tiles = list(assign_tiles(bounds, tile_size))
    total_tiles = len(tiles)
    logger.info(f"Total number of tiles to process: {total_tiles}")

    initializer = worker_initializer if log_queue is not None else None
    initargs = (log_queue,) if log_queue is not None else ()

    # Start a pool of workers for processing tiles
    with Pool(processes=os.cpu_count(), initializer=initializer, initargs=initargs) as pool:
        logger.info("Starting parallel processing of tiles.")
        pool.starmap(
            process_tile,
            [
                (
                    rasterio.windows.from_bounds(
                        tile_left,
                        tile_bottom,
                        tile_right,
                        tile_top,
                        transform=transform,
                    )
                    .round_offsets()
                    .round_lengths(),
                    f"Tile_{i}_{j}",
                    input_file,
                    thinning_factors,
                    output_dir,
                    transform,
                )
                for i, j, tile_left, tile_bottom, tile_right, tile_top in tiles
            ],
        )

    logger.info("All processing complete.")


def main():
    with setup_logger(console_log_level="INFO") as log_queue:
        # Get the directory of the script
        script_dir = Path(__file__).absolute().parent

        # Define the output directory path
        output_dir = os.path.join(script_dir, "thinned-data4")

        # Ensure the output directory exists
        ensure_output_directory(output_dir)

        # For testing purposes, process a single .tif file
        # If you want to process all .tif files in the directory, uncomment:
        # tif_files = glob(os.path.join(script_dir, "*.tif"))

        # Test with a specific .tif file (like "main-section-clip.tif")
        tif_files = glob(os.path.join(script_dir, "main-section-clip.tif"))

        if not tif_files:
            logger.info("No .tif files found in the script directory.")
        else:
            # Process each .tif file
            for file in tif_files:
                process_terrain_data(file, output_dir, log_queue=log_queue)
    print()
    print_library_version()


if __name__ == "__main__":
    main()
