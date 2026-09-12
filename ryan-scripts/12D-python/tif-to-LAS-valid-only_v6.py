"""Convert valid GeoTIFF cells to LAS point clouds for 12d.

Before running, replace the project-specific ``script_dir`` in :func:`main`
and review ``nodata_values``, ``tile_size``, and ``use_tiling``. The script
scans that directory for ``*.tif`` and writes LAS 1.2 point-format 3 files to
``output_las_files`` using 0.01-unit XYZ scales.

Run ``python tif-to-LAS-valid-only_v6.py`` from a terminal. The configured
default uses tiling and may create many LAS files; validate coordinate units,
offsets, and a representative output before bulk processing.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false

from pathlib import Path

import laspy  # pyright: ignore[reportMissingTypeStubs]
import numpy as np
import pandas as pd
from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.terrain_processing import parallel_process_multiple_terrain
from ryan_library.functions.wrapper_utils import print_library_version


def save_tile_las(tile_df: pd.DataFrame, output_dir: Path, base_filename: str, i: int, j: int) -> None:
    """Saves a tile DataFrame as a LAS file."""
    tile_filename = f"{base_filename}_tile_{i}_{j}.las"
    tile_path = output_dir / tile_filename

    try:
        # Create LAS header
        header = laspy.LasHeader(point_format=3, version="1.2")

        # Set scales and offsets based on tile data
        header.offsets = np.array([tile_df["X"].min(), tile_df["Y"].min(), tile_df["Z"].min()])
        header.scales = np.array([0.01, 0.01, 0.01])  # Adjust scales as needed

        # Create LasData object
        las = laspy.LasData(header)

        # Assign point data
        las.x = tile_df["X"].to_numpy()
        las.y = tile_df["Y"].to_numpy()
        las.z = tile_df["Z"].to_numpy()

        # Write to LAS file
        las.write(str(tile_path))
        logger.info("Saved tile: {}", tile_filename)
    except Exception as error:
        logger.error("Failed to save LAS tile {}: {}", tile_filename, error)


def save_full_las(df: pd.DataFrame, output_dir: Path, base_filename: str) -> None:
    """Saves the full DataFrame as a single LAS file without tiling."""
    las_filename = f"{base_filename}.las"
    output_path = output_dir / las_filename

    try:
        # Create LAS header
        header = laspy.LasHeader(point_format=3, version="1.2")

        # Set scales and offsets based on full data
        header.offsets = np.array([df["X"].min(), df["Y"].min(), df["Z"].min()])
        header.scales = np.array([0.01, 0.01, 0.01])  # Adjust scales as needed

        # Create LasData object
        las = laspy.LasData(header)

        # Assign point data
        las.x = df["X"].to_numpy()
        las.y = df["Y"].to_numpy()
        las.z = df["Z"].to_numpy()

        # Write to LAS file
        las.write(str(output_path))
        logger.info("Saved file without tiling: {}", las_filename)
    except Exception as error:
        logger.error("Failed to save LAS file {}: {}", las_filename, error)


def main() -> None:
    with setup_logger(console_log_level="INFO", log_file="las_processing.log") as log_queue:
        logger.info("Starting LAS terrain data processing script.")

        # Configuration parameters
        nodata_values = None  # Replace with your no data values or None
        tile_size = 100000  # Set your desired tile size here (e.g., in coordinate units)
        use_tiling = True  # Set to True to enable tiling

        # Set script_dir to a specific path
        script_dir = Path(__file__).absolute().parent
        script_dir = Path(
            r"P:\BGER\PER\RP20180.365 BLACKSMITH SCOPING STUDY - FMG\5 CADD\1 MOD\2 CI"
            r"\12D\Input\2025.06.20_ClippedGIS\h_hr_max"
        )
        logger.info("Script directory: {}", script_dir)

        # Verify that script_dir exists
        if not script_dir.exists():
            logger.error("The specified script directory does not exist: {}", script_dir)
            return

        # Define the output directory
        output_dir = script_dir / "output_las_files"  # Using Path objects
        output_dir.mkdir(parents=True, exist_ok=True)
        logger.info("Output directory: {}", output_dir)

        # Find all .tif files in the script_dir
        tif_files = list(script_dir.glob("*.tif"))
        if not tif_files:
            logger.warning("No .tif files found in the script directory.")
            return
        logger.info("Found {} .tif files to process.", len(tif_files))

        # Define the saving function based on tiling
        if use_tiling:
            save_function = save_tile_las
        else:
            save_function = save_full_las

        # Start processing using the parallel_process_multiple_terrain function
        parallel_process_multiple_terrain(
            files=tif_files,
            output_dir=output_dir,
            nodata_values=nodata_values,
            tile_size=tile_size if use_tiling else None,
            save_function=save_function,
            log_queue=log_queue,
        )

        logger.info("Completed all terrain data processing.")
    print()
    print_library_version()


if __name__ == "__main__":
    main()
