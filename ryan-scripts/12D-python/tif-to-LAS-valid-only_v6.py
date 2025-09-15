# tif-to-LAS-valid-only_v6.py

import logging
from pathlib import Path
import laspy
import numpy as np
from ryan_library.functions.terrain_processing import parallel_process_multiple_terrain


def save_tile_las(tile_df, output_dir, base_filename, i, j) -> None:
    """
    Saves a tile DataFrame as a LAS file.
    """
    logger = logging.getLogger(__name__)
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
        las.x = tile_df["X"].values
        las.y = tile_df["Y"].values
        las.z = tile_df["Z"].values

        # Write to LAS file
        las.write(str(tile_path))
        logger.info(f"Saved tile: {tile_filename}")
    except Exception as e:
        logger.error(f"Failed to save LAS tile {tile_filename}: {e}")


def save_full_las(df, output_dir, base_filename):
    """
    Saves the full DataFrame as a single LAS file without tiling.
    """
    logger = logging.getLogger(__name__)
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
        las.x = df["X"].values
        las.y = df["Y"].values
        las.z = df["Z"].values

        # Write to LAS file
        las.write(str(output_path))
        logger.info(f"Saved file without tiling: {las_filename}")
    except Exception as e:
        logger.error(f"Failed to save LAS file {las_filename}: {e}")


def main() -> None:
    # Set up logging
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    logger = logging.getLogger(__name__)
    logger.info("Starting LAS terrain data processing script.")

    # Configuration parameters
    nodata_values = None  # Replace with your no data values or None
    tile_size = 100000  # Set your desired tile size here (e.g., in coordinate units)
    use_tiling = True  # Set to True to enable tiling

    # Set script_dir to a specific path
    script_dir = Path(__file__).absolute().parent
    script_dir = Path(
        r"P:\BGER\PER\RP20180.365 BLACKSMITH SCOPING STUDY - FMG\5 CADD\1 MOD\2 CI\12D\Input\2025.06.20_ClippedGIS\h_hr_max"
    )
    logger.info(f"Script directory: {script_dir}")

    # Verify that script_dir exists
    if not script_dir.exists():
        logger.error(f"The specified script directory does not exist: {script_dir}")
        return

    # Define the output directory
    output_dir = script_dir / "output_las_files"  # Using Path objects
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
    )

    logger.info("Completed all terrain data processing.")


if __name__ == "__main__":
    main()
