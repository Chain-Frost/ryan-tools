import os
import glob
import pandas as pd
import numpy as np
import rasterio
from multiprocessing import Pool, cpu_count


def read_geotiff(filename, nodata_values=None):
    print(f"Loading file: {filename}")  # Print statement for loading a file
    with rasterio.open(filename) as f:
        band = f.read(1)

        # Use nodata value from file if nodata_values is None
        file_nodata = f.nodata
        if nodata_values is None:
            nodata_values = [file_nodata] if file_nodata is not None else []

        # Mask out the nodata and unwanted values
        for value in nodata_values:
            band = np.where(band == value, np.nan, band)
        band = np.ma.masked_invalid(band)

        # Get coordinates
        row, col = np.where(~band.mask)
        x, y = f.xy(row, col)

    return pd.DataFrame({"X": x, "Y": y, "Z": band.compressed()})


def process_terrain_data(filename, output_dir, nodata_values=None):
    df = read_geotiff(filename, nodata_values)

    # Ensure no NaN values are included
    df.dropna(inplace=True)  # This step ensures any lingering NaNs are removed

    # Export to CSV
    csv_filename = os.path.splitext(os.path.basename(filename))[0] + ".csv"
    df.to_csv(os.path.join(output_dir, csv_filename), index=False)
    print(
        f"Completed processing and saved: {csv_filename}"
    )  # Print statement for completion


def worker(task):
    filename, output_dir, nodata_values = task
    process_terrain_data(filename, output_dir, nodata_values)


if __name__ == "__main__":
    # Get the directory of the script
    script_dir = os.path.dirname(os.path.realpath(__file__))

    # Find all .tif files in the script's directory
    tif_files = glob.glob(os.path.join(script_dir, "*.tif"))

    # Optional: Define your nodata_values here, or set to None
    nodata_values = None  # Replace with your no data values or None

    # Create tasks for each file
    tasks = [(file, script_dir, nodata_values) for file in tif_files]

    # Use multiprocessing to process files in parallel
    with Pool(processes=cpu_count()) as pool:
        pool.map(worker, tasks)
