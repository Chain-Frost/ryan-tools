# ryan-scripts\misc-python\parallel-reorder-xyz-updated.py
# Updated 2025-11-18 to suit gdal_buildvrt point order, mulitprocessing instead of multithreading (much faster)
def fill_missing_coordinates(df) -> "pd.DataFrame":
    """Return a DataFrame where the full X/Y grid exists and missing Zs are filled with -9999."""
    import numpy as np

    # Ensure axes sorted so downstream tools (e.g., GDAL) see consistent grid orientation.
    unique_x = np.sort(np.unique(df["x"]))
    unique_y = np.sort(np.unique(df["y"]))[::-1]  # Y must descend for negative NS pixel size.

    # Create a complete grid of x and y coordinates
    complete_grid = pd.DataFrame(
        data={"x": np.tile(A=unique_x, reps=len(unique_y)), "y": np.repeat(a=unique_y, repeats=len(unique_x))}
    )

    # Merge with the original data to find missing coordinates
    merged_df: pd.DataFrame = pd.merge(left=complete_grid, right=df, on=["x", "y"], how="left")

    # Fill missing z-values with -9999
    merged_df["z"] = merged_df["z"].fillna(value=-9999)

    return merged_df


import subprocess
import os
import pandas as pd
from glob import iglob
from multiprocessing import Pool
from datetime import datetime
from ryan_library.functions.misc_functions import calculate_pool_size


def main() -> None:
    startTime: datetime = datetime.now()
    os.chdir(r"C:\Users\Ryan.Brook\Downloads\2019 Unity - AAM - LiDAR - Jan\04_DSM")
    # Mirror the source folder layout by dropping finished files into a local "mod" directory.
    output_dir: str = os.path.join(os.getcwd(), "mod")
    os.makedirs(output_dir, exist_ok=True)
    xyzFiles: list[str] = [f for f in iglob("*.xyz", recursive=False) if os.path.isfile(f)]
    print(xyzFiles)

    if not xyzFiles:
        print("No XYZ files found to process.")
        subprocess.call("pause", shell=True)  # wait for exit
        return

    # Scale the worker count relative to available CPUs and job count (see misc_functions.calculate_pool_size).
    pool_size: int = calculate_pool_size(num_files=len(xyzFiles))
    print(f"Using {pool_size} worker processes")
    with Pool(processes=pool_size) as pool:
        pool.starmap(process_xyz_file, ((file, output_dir) for file in xyzFiles))

    print("end")
    subprocess.call("pause", shell=True)  # wait for exit


def process_xyz_file(file: str, output_dir: str) -> None:
    try:
        print(f"Processing {file}")
        # Input XYZ files are whitespace-delimited without headers, so read them explicitly as such.
        df: pd.DataFrame = pd.read_csv(
            filepath_or_buffer=file,
            names=["x", "y", "z"],
            sep=r"\s+",
            engine="python",
        )
        print("--sorting")
        # GDAL expects rows ordered from max->min Y to avoid "positive NS resolution" warnings.
        df.sort_values(["y", "x"], ascending=[False, True], inplace=True)
        base_name: str = os.path.splitext(os.path.basename(file))[0]
        output_file: str = os.path.join(output_dir, f"{base_name}_mod.xyz")
        # Fill missing coordinates with NoData value
        df = fill_missing_coordinates(df)
        # Preserve XYZ formatting by writing space-delimited rows with no header.
        df.to_csv(path_or_buf=output_file, sep=" ", header=False, index=False)
        print(f"Finished processing {file} and saved as {output_file}")
    except Exception as e:
        print(f"Error processing {file}: {str(e)}")


if __name__ == "__main__":
    main()
