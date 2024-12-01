def fill_missing_coordinates(df) -> pd.DataFrame:
    import numpy as np

    unique_x = np.unique(df["x"])
    unique_y = np.unique(df["y"])

    # Create a complete grid of x and y coordinates
    complete_grid = pd.DataFrame(
        {"x": np.tile(unique_x, len(unique_y)), "y": np.repeat(unique_y, len(unique_x))}
    )

    # Merge with the original data to find missing coordinates
    merged_df = pd.merge(complete_grid, df, on=["x", "y"], how="left")

    # Fill missing z-values with -9999
    merged_df["z"].fillna(-9999, inplace=True)

    return merged_df


import subprocess
import os
import pandas as pd
from glob import iglob
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime


def process_xyz_file(file) -> None:
    try:
        print(f"Processing {file}")
        df: pd.DataFrame = pd.read_csv(file, names=["x", "y", "z"])
        print("--sorting")
        df.sort_values(["y", "x"], ascending=[True, True], inplace=True)
        output_file: str = f"gdal2/{os.path.basename(file)[:-4]}_mod.xyz"  ##
        # Fill missing coordinates with NoData value
        df = fill_missing_coordinates(df)
        df.to_csv(output_file, index=False)
        print(f"Finished processing {file} and saved as {output_file}")
    except Exception as e:
        print(f"Error processing {file}: {str(e)}")


if __name__ == "__main__":
    startTime: datetime = datetime.now()
    xyzFiles: list[str] = [
        f for f in iglob("*.xyz", recursive=False) if os.path.isfile(f)
    ]
    print(xyzFiles)

    # Create a ThreadPoolExecutor with a specified number of threads
    num_threads = 16  # You can adjust this to the desired number of threads
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit each file for processing to run concurrently
        futures = [executor.submit(process_xyz_file, file) for file in xyzFiles]

        # Wait for all tasks to complete
        for future in futures:
            future.result()

    print("end")
    subprocess.call("pause", shell=True)  # wait for exit
