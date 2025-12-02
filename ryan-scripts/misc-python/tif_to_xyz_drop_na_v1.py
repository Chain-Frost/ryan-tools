from concurrent.futures._base import Future
import os
import subprocess
import pandas as pd
import numpy as np
import rasterio  # type:ignore
from glob import iglob
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

# Set this flag to True if you want to drop rows with missing z values
DROP_NA = True  # Change to True to drop rows with missing z's instead of filling them
WORKING_DIR = None
WORKING_DIR = (
    r"P:\BGER\PER\RP20181.364 YANDI MGD5 DRAIN FS - RTIO\4 ENGINEERING\11 HYDROLOGY\Calcs\20250219 v13 Water Depths"
)
OUT_FOLDER = r"converted_and_trimmed"


def fill_missing_coordinates(df: pd.DataFrame, drop_missing: bool, nodata_value: float) -> pd.DataFrame:
    unique_x = np.unique(df["x"])
    unique_y = np.unique(df["y"])

    # Create a complete grid of x and y coordinates
    complete_grid = pd.DataFrame({"x": np.tile(unique_x, len(unique_y)), "y": np.repeat(unique_y, len(unique_x))})

    # Merge with the original data to find missing coordinates
    merged_df = pd.merge(complete_grid, df, on=["x", "y"], how="left")

    if drop_missing:
        # Drop rows where z is NaN
        merged_df.dropna(subset=["z"], inplace=True)
    else:
        # Fill missing z-values with the nodata_value
        merged_df["z"].fillna(nodata_value, inplace=True)

    return merged_df


def process_tif_file(file: str) -> None:
    try:
        print(f"Processing {file}")
        with rasterio.open(file) as src:
            # Read the first band from the raster
            band1 = src.read(1)
            transform = src.transform
            rows, cols = band1.shape

            # Determine the nodata value: if the source has a nodata, use it; otherwise, use -9999
            src_nodata = src.nodata if src.nodata is not None else -9999

            # If dropping NA values, convert the source's NoData value to np.nan
            if DROP_NA and src.nodata is not None:
                band1 = np.where(band1 == src.nodata, np.nan, band1)

            # Create a grid of column and row indices
            col_indices, row_indices = np.meshgrid(np.arange(cols), np.arange(rows))
            # Convert indices to spatial coordinates using rasterio.transform.xy
            xs, ys = rasterio.transform.xy(transform, row_indices, col_indices)
            xs = np.array(xs)
            ys = np.array(ys)

            # Create a DataFrame with x, y, and z values
            df = pd.DataFrame({"x": xs.flatten(), "y": ys.flatten(), "z": band1.flatten()})

        print("--sorting")
        df.sort_values(["y", "x"], ascending=[True, True], inplace=True)

        # Define the output file path (adjust the directory as needed)
        output_file = f"{OUT_FOLDER}/{os.path.basename(file)[:-4]}_mod.xyz"

        # Fill missing coordinates and either fill or drop missing z-values based on DROP_NA flag.
        # Use the source nodata value if available; otherwise, fallback to -9999.
        df = fill_missing_coordinates(df, drop_missing=DROP_NA, nodata_value=src_nodata)
        df.to_csv(output_file, index=False)
        print(f"Finished processing {file} and saved as {output_file}")

    except Exception as e:
        print(f"Error processing {file}: {str(e)}")


if __name__ == "__main__":
    startTime: datetime = datetime.now()

    if WORKING_DIR is not None:
        os.chdir(WORKING_DIR)

    # List all TIFF files in the current directory
    tifFiles: list[str] = [f for f in iglob("*.tif", recursive=False) if os.path.isfile(f)]
    print("TIFF Files Found:", tifFiles)

    # Create the output directory if it doesn't exist
    os.makedirs(OUT_FOLDER, exist_ok=True)

    # Process files concurrently using ThreadPoolExecutor
    num_threads = 16  # Adjust as needed
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures: list[Future[None]] = [executor.submit(process_tif_file, file) for file in tifFiles]
        for future in futures:
            future.result()

    print("end")
    subprocess.call("pause", shell=True)  # Wait for exit (Windows only)
