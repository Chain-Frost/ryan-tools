"""Shared GeoTIFF-to-tabular terrain processing helpers."""

from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, Protocol, cast

import numpy as np
import numpy.typing as npt
import pandas as pd
import rasterio  # pyright: ignore[reportMissingTypeStubs]
from loguru import logger
from tqdm import tqdm

from ryan_library.functions.loguru_helpers import LogQueue, worker_initializer

type NodataValue = int | float
type NodataValues = NodataValue | Sequence[NodataValue] | None
type TerrainTile = tuple[tuple[int, int], pd.DataFrame]
type FullSaveFunction = Callable[[pd.DataFrame, Path, str], None]
type TileSaveFunction = Callable[[pd.DataFrame, Path, str, int, int], None]
type SaveFunction = FullSaveFunction | TileSaveFunction
type TerrainTaskArguments = tuple[str, Path, NodataValues, float | None]
type TerrainTask = tuple[TerrainTaskArguments, SaveFunction]


class RasterReader(Protocol):
    """Subset of Rasterio's dataset reader used by this module."""

    nodata: float | None

    def read(self, index: int) -> npt.NDArray[np.number[Any]]: ...

    def xy(
        self, rows: npt.NDArray[np.integer[Any]], cols: npt.NDArray[np.integer[Any]]
    ) -> tuple[Sequence[float], Sequence[float]]: ...


type RasterContext = AbstractContextManager[RasterReader]


def read_geotiff(filename: str | Path, nodata_values: NodataValues = None) -> pd.DataFrame:
    """
    Reads a GeoTIFF file and returns a DataFrame with X, Y, Z coordinates.
    """
    logger.info(f"Loading file: {filename}")
    try:
        open_raster = cast(
            Callable[[str | Path], RasterContext],
            rasterio.open,  # pyright: ignore[reportUnknownMemberType]
        )
        with open_raster(filename) as f:
            source_band = f.read(1)

            # Use nodata value from file if nodata_values is None
            file_nodata: float | None = f.nodata
            normalized_nodata_values: Sequence[NodataValue]
            if nodata_values is None:
                normalized_nodata_values = [file_nodata] if file_nodata is not None else []
            else:
                normalized_nodata_values = nodata_values if isinstance(nodata_values, Sequence) else [nodata_values]

            # Mask out the nodata and unwanted values
            band: npt.NDArray[np.float64] = source_band.astype(np.float64)
            for value in normalized_nodata_values:
                band = np.where(band == value, np.nan, band)
            masked_band = np.ma.masked_invalid(band)

            # Get coordinates
            row, col = np.where(~masked_band.mask)
            x, y = f.xy(row, col)

        df = pd.DataFrame({"X": x, "Y": y, "Z": masked_band.compressed()})
        logger.debug("DataFrame shape after loading: {}", df.shape)
        return df

    except Exception as e:
        logger.error(f"Error reading file {filename}: {e}")
        return pd.DataFrame(columns=["X", "Y", "Z"])


def tile_data(df: pd.DataFrame, tile_size: float) -> list[TerrainTile]:
    """
    Splits the DataFrame into tiles based on the specified tile size.
    Returns a list of tuples containing tile indices and the corresponding tile DataFrame.
    """
    if df.empty:
        return []

    # Determine the range of X and Y
    x_min, x_max = float(df["X"].min()), float(df["X"].max())
    y_min, y_max = float(df["Y"].min()), float(df["Y"].max())

    # Compute the number of tiles in each direction
    # Add a small epsilon to ensure the max value is included if it falls exactly on a tile boundary
    x_tiles = int(np.ceil((x_max - x_min + 1e-6) / tile_size))
    y_tiles = int(np.ceil((y_max - y_min + 1e-6) / tile_size))

    # Ensure at least 1 tile if there is data (though the above logic should handle it)
    x_tiles: int = max(1, x_tiles)
    y_tiles: int = max(1, y_tiles)

    logger.info(f"Tiling data into {x_tiles} x {y_tiles} tiles.")

    tiles: list[TerrainTile] = []
    for i in tqdm(range(x_tiles), desc="Processing tiles (X-axis)"):
        for j in range(y_tiles):
            x_start: float = x_min + i * tile_size
            x_end: float = x_start + tile_size
            y_start: float = y_min + j * tile_size
            y_end: float = y_start + tile_size

            # Filter data within the tile
            tile_df = df[(df["X"] >= x_start) & (df["X"] < x_end) & (df["Y"] >= y_start) & (df["Y"] < y_end)]

            if not tile_df.empty:
                tiles.append(((i, j), tile_df))
            else:
                logger.debug("Tile ({}, {}) is empty. Skipping.", i, j)
    logger.info(f"Completed tiling. Generated {len(tiles)} non-empty tiles.")
    return tiles


def process_terrain_file(args_save_function: TerrainTask) -> None:
    """
    Worker function to process a single terrain file.

    Parameters:
    - args_save_function: Tuple containing (args, save_function)
    """
    args, save_function = args_save_function
    process_terrain_file_inner(*args, save_function)


def process_terrain_file_inner(
    filename: str | Path,
    output_dir: Path,
    nodata_values: NodataValues,
    tile_size: float | None,
    save_function: SaveFunction,
) -> None:
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
    initial_shape: tuple[int, int] = df.shape
    df.dropna(inplace=True)
    logger.debug("Dropped NaN values. DataFrame shape changed from {} to {}", initial_shape, df.shape)

    # Base filename without extension
    base_filename: str = filename.stem

    if df.empty:
        logger.warning(f"No valid data found in {filename}. Skipping file.")
        return

    if tile_size:
        # Tile the data
        tiles: list[TerrainTile] = tile_data(df, tile_size)
        for (i, j), tile_df in tiles:
            cast(TileSaveFunction, save_function)(tile_df, output_dir, base_filename, i, j)
    else:
        # Export without tiling
        cast(FullSaveFunction, save_function)(df, output_dir, base_filename)


def parallel_process_multiple_terrain(
    files: Sequence[Path],
    output_dir: Path,
    nodata_values: NodataValues,
    tile_size: float | None,
    save_function: SaveFunction,
    log_queue: LogQueue | None = None,
) -> None:
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
    tasks: list[TerrainTask] = [((str(file), output_dir, nodata_values, tile_size), save_function) for file in files]

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
