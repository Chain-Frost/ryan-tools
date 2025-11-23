# ryan_library.functions/terrain_processing.py

import pandas as pd
import numpy as np
import rasterio
from loguru import logger
from pathlib import Path
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from ryan_library.functions.loguru_helpers import worker_initializer


def read_geotiff(filename, nodata_values=None):
    """
    Reads a GeoTIFF file and returns a DataFrame with X, Y, Z coordinates.
    """
    logger.info(f"Loading file: {filename}")
    try:
        with rasterio.open(filename) as f:
            band = f.read(1)

            # Use nodata value from file if nodata_values is None
            file_nodata = f.nodata
            if nodata_values is None:
                nodata_values = [file_nodata] if file_nodata is not None else []
            else:
                nodata_values = (
                    nodata_values
                    if isinstance(nodata_values, list)
                    else [nodata_values]
                )

            # Mask out the nodata and unwanted values
            for value in nodata_values:
                band = np.where(band == value, np.nan, band)
            band = np.ma.masked_invalid(band)

            # Get coordinates
            row, col = np.where(~band.mask)
            x, y = f.xy(row, col)

        df = pd.DataFrame({"X": x, "Y": y, "Z": band.compressed()})
        logger.debug("DataFrame shape after loading: {}", df.shape)
        return df

    except Exception as e:
        logger.error(f"Error reading file {filename}: {e}")
        return pd.DataFrame(columns=["X", "Y", "Z"])


def tile_data(df, tile_size):
    """
    Splits the DataFrame into tiles based on the specified tile size.
    Returns a list of tuples containing tile indices and the corresponding tile DataFrame.
    """
    # Determine the range of X and Y
    x_min, x_max = df["X"].min(), df["X"].max()
    y_min, y_max = df["Y"].min(), df["Y"].max()

    # Compute the number of tiles in each direction
    x_tiles = int(np.ceil((x_max - x_min) / tile_size))
    y_tiles = int(np.ceil((y_max - y_min) / tile_size))

    logger.info(f"Tiling data into {x_tiles} x {y_tiles} tiles.")

    tiles = []
    for i in tqdm(range(x_tiles), desc="Processing tiles (X-axis)"):
        for j in range(y_tiles):
            x_start = x_min + i * tile_size
            x_end = x_start + tile_size
            y_start = y_min + j * tile_size
            y_end = y_start + tile_size

            # Filter data within the tile
            tile_df = df[
                (df["X"] >= x_start)
                & (df["X"] < x_end)
                & (df["Y"] >= y_start)
                & (df["Y"] < y_end)
            ]

            if not tile_df.empty:
                tiles.append(((i, j), tile_df))
            else:
                logger.debug("Tile ({}, {}) is empty. Skipping.", i, j)
    logger.info(f"Completed tiling. Generated {len(tiles)} non-empty tiles.")
    return tiles


def process_terrain_file(args_save_function):
    """
    Worker function to process a single terrain file.

    Parameters:
    - args_save_function: Tuple containing (args, save_function)
    """
    args, save_function = args_save_function
    process_terrain_file_inner(*args, save_function)


def process_terrain_file_inner(
    filename, output_dir, nodata_values, tile_size, save_function
):
    """
    Processes a single terrain file: reads, tiles, and saves using the provided save_function.

    Parameters:
    - filename: Path to the GeoTIFF file
    - output_dir: Directory to save the output
    - nodata_values: List of nodata values to mask
    - tile_size: Size of each tile
    - save_function: Function to save the data
    """
    logger.info(f"Processing file: {filename}")

    filename = Path(filename)

    df = read_geotiff(filename, nodata_values)

    # Ensure no NaN values are included
    initial_shape = df.shape
    df.dropna(inplace=True)
    logger.debug(
        "Dropped NaN values. DataFrame shape changed from {} to {}", initial_shape, df.shape
    )

    # Base filename without extension
    base_filename = filename.stem

    if df.empty:
        logger.warning(f"No valid data found in {filename}. Skipping file.")
        return

    if tile_size:
        # Tile the data
        tiles = tile_data(df, tile_size)
        for (i, j), tile_df in tiles:
            save_function(tile_df, output_dir, base_filename, i, j)
    else:
        # Export without tiling
        save_function(df, output_dir, base_filename)


def parallel_process_multiple_terrain(
    files, output_dir, nodata_values, tile_size, save_function, log_queue=None
):
    """
    Orchestrates the processing of multiple terrain files in parallel.

    Parameters:
    - files: List of file paths to process
    - output_dir: Directory to save the output files
    - nodata_values: List of nodata values to mask
    - tile_size: Size of each tile
    - save_function: Function to save the data
    """
    # Create tasks for each file as ((args), save_function)
    tasks = [
        ((str(file), output_dir, nodata_values, tile_size), save_function)
        for file in files
    ]

    initializer = worker_initializer if log_queue is not None else None
    initargs = (log_queue,) if log_queue is not None else ()

    # Use multiprocessing to process files in parallel
    with Pool(processes=cpu_count(), initializer=initializer, initargs=initargs) as pool:
        list(
            tqdm(
                pool.imap_unordered(process_terrain_file, tasks),
                total=len(tasks),
                desc="Processing files",
            )
        )
