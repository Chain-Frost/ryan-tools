import pandas as pd
import numpy as np
import rasterio
from multiprocessing import Pool, cpu_count
from pathlib import Path
import logging
from tqdm import tqdm
import laspy

# update using this
# https://chatgpt.com/share/6734132d-1d14-800e-967e-771cba1c15aa

def read_geotiff(filename, nodata_values=None):
    logger = logging.getLogger(__name__)
    logger.info(f"Loading file: {filename}")
    try:
        with rasterio.open(filename) as f:
            band = f.read(1)

            # Use nodata value from file if nodata_values is None
            file_nodata = f.nodata
            if nodata_values is None:
                nodata_values = [file_nodata] if file_nodata is not None else []
            else:
                nodata_values = nodata_values if isinstance(nodata_values, list) else [nodata_values]

            # Mask out the nodata and unwanted values
            for value in nodata_values:
                band = np.where(band == value, np.nan, band)
            band = np.ma.masked_invalid(band)

            # Get coordinates
            row, col = np.where(~band.mask)
            x, y = f.xy(row, col)

        df = pd.DataFrame({"X": x, "Y": y, "Z": band.compressed()})
        logger.debug(f"DataFrame shape after loading: {df.shape}")
        return df

    except Exception as e:
        logger.error(f"Error reading file {filename}: {e}")
        return pd.DataFrame(columns=["X", "Y", "Z"])


def tile_and_save(df, tile_size, output_dir, base_filename):
    logger = logging.getLogger(__name__)
    # Determine the range of X and Y
    x_min, x_max = df['X'].min(), df['X'].max()
    y_min, y_max = df['Y'].min(), df['Y'].max()

    # Compute the number of tiles in each direction
    x_tiles = int(np.ceil((x_max - x_min) / tile_size))
    y_tiles = int(np.ceil((y_max - y_min) / tile_size))

    logger.info(f"Tiling data into {x_tiles} x {y_tiles} tiles.")

    # Create tiles and save
    total_tiles = x_tiles * y_tiles
    tile_counter = 0

    for i in tqdm(range(x_tiles), desc="Processing tiles (X-axis)"):
        for j in range(y_tiles):
            x_start = x_min + i * tile_size
            x_end = x_start + tile_size
            y_start = y_min + j * tile_size
            y_end = y_start + tile_size

            # Filter data within the tile
            tile_df = df[(df['X'] >= x_start) & (df['X'] < x_end) &
                         (df['Y'] >= y_start) & (df['Y'] < y_end)]

            if not tile_df.empty:
                # Create a filename for the tile
                tile_filename = f"{base_filename}_tile_{i}_{j}.las"  # Changed extension to .las
                tile_path = output_dir / tile_filename

                # Create LAS header
                header = laspy.LasHeader(point_format=3, version="1.2")

                # Set scales and offsets
                header.offsets = np.array([tile_df['X'].min(), tile_df['Y'].min(), tile_df['Z'].min()])
                header.scales = np.array([0.01, 0.01, 0.01])  # Adjust scales as needed

                # Create LasData object
                las = laspy.LasData(header)

                # Assign point data
                las.x = tile_df['X'].values
                las.y = tile_df['Y'].values
                las.z = tile_df['Z'].values

                # Write to LAS file
                las.write(str(tile_path))

                logger.info(f"Saved tile: {tile_filename}")
            else:
                logger.debug(f"Tile ({i}, {j}) is empty. Skipping.")
            tile_counter += 1
    logger.info(f"Completed tiling. Processed {tile_counter} tiles.")


def process_terrain_data(args):
    filename, output_dir, nodata_values, tile_size = args
    logger = logging.getLogger(__name__)
    logger.info(f"Processing file: {filename}")
    df = read_geotiff(filename, nodata_values)

    # Ensure no NaN values are included
    initial_shape = df.shape
    df.dropna(inplace=True)
    logger.debug(f"Dropped NaN values. DataFrame shape changed from {initial_shape} to {df.shape}")

    # Base filename without extension
    base_filename = Path(filename).stem

    if df.empty:
        logger.warning(f"No valid data found in {filename}. Skipping file.")
        return

    if tile_size:
        # Tile the data and save
        tile_and_save(df, tile_size, output_dir, base_filename)
    else:
        # Export to LAS without tiling
        las_filename = base_filename + ".las"  # Changed extension to .las
        output_path = output_dir / las_filename

        # Create LAS header
        header = laspy.LasHeader(point_format=3, version="1.2")

        # Set scales and offsets
        header.offsets = np.array([df['X'].min(), df['Y'].min(), df['Z'].min()])
        header.scales = np.array([0.01, 0.01, 0.01])  # Adjust scales as needed

        # Create LasData object
        las = laspy.LasData(header)

        # Assign point data
        las.x = df['X'].values
        las.y = df['Y'].values
        las.z = df['Z'].values

        # Write to LAS file
        las.write(str(output_path))

        logger.info(f"Completed processing and saved: {las_filename}")


def main():
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
    logger = logging.getLogger(__name__)
    logger.info("Starting terrain data processing script.")

    # Configuration parameters
    nodata_values = None  # Replace with your no data values or None
    tile_size = 1000  # Set your desired tile size here (e.g., in coordinate units)
    use_tiling = True  # Set to True to enable tiling

    # Get the directory of the script
    script_dir = Path(__file__).resolve().parent
    logger.info(f"Script directory: {script_dir}")

    # Define the output directory (can be the same as script_dir or a subdirectory)
    output_dir = script_dir / "output_las_files"  # Changed directory name to reflect LAS files
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"Output directory: {output_dir}")

    # Find all .tif files in the script's directory
    tif_files = list(script_dir.glob("*.tif"))
    if not tif_files:
        logger.warning("No .tif files found in the script directory.")
        return
    logger.info(f"Found {len(tif_files)} .tif files to process.")

    # Create tasks for each file
    tasks = [(str(file), output_dir, nodata_values, tile_size if use_tiling else None) for file in tif_files]

    # Use multiprocessing to process files in parallel
    with Pool(processes=cpu_count()) as pool:
        list(tqdm(pool.imap_unordered(process_terrain_data, tasks), total=len(tasks), desc="Processing files"))

    logger.info("Completed all terrain data processing.")


if __name__ == "__main__":
    main()
