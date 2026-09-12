"""Reorder XYZ grids and fill missing coordinate pairs for downstream GDAL use.

Replace the project-specific working directory in :func:`main`, then run
``python parallel-reorder-xyz-updated.py``. All top-level ``*.xyz`` files are
processed with a multiprocessing pool, sorted into consistent grid order, and
completed with ``-9999`` Z values where coordinate pairs are missing.

Outputs are whitespace-delimited ``*_mod.xyz`` files under the working
directory's ``mod`` folder. Check coordinate spacing and a sample output before
processing a large point-cloud collection.
"""

# pyright: reportUnknownParameterType=false, reportMissingParameterType=false, reportUnknownArgumentType=false


# Updated 2025-11-18 to suit gdal_buildvrt point order, mulitprocessing instead of multithreading (much faster)
def fill_missing_coordinates(df) -> pd.DataFrame:
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
    merged_df: pd.DataFrame = complete_grid.merge(right=df, on=["x", "y"], how="left")

    # Fill missing z-values with -9999
    merged_df["z"] = merged_df["z"].fillna(value=-9999)

    return merged_df


from multiprocessing import Pool
from pathlib import Path

import pandas as pd

from ryan_library.functions.misc_functions import calculate_pool_size
from ryan_library.functions.wrapper_utils import pause_console


def main() -> None:
    working_dir = Path(
        r"P:\25\RP25177.001 UNITY POA - HR\7 DOCUMENT CONTROL\2 RECEIVED DATA\1 CLIENT\20251111 - Previous LiDAR\2019 Unity - AAM - LiDAR - Jan\04_DSM"
    )
    # Mirror the source folder layout by dropping finished files into a local "mod" directory.
    output_dir = working_dir / "mod"
    output_dir.mkdir(parents=True, exist_ok=True)
    xyz_files = [path for path in working_dir.glob("*.xyz") if path.is_file()]
    print(xyz_files)

    if not xyz_files:
        print("No XYZ files found to process.")
        pause_console()
        return

    # Scale the worker count relative to available CPUs and job count (see misc_functions.calculate_pool_size).
    pool_size: int = calculate_pool_size(num_files=len(xyz_files))
    print(f"Using {pool_size} worker processes")
    with Pool(processes=pool_size) as pool:
        pool.starmap(process_xyz_file, ((file, output_dir) for file in xyz_files))

    print("end")
    pause_console()


def process_xyz_file(file: Path, output_dir: Path) -> None:
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
        df = df.sort_values(["y", "x"], ascending=[False, True])
        output_file = output_dir / f"{file.stem}_mod.xyz"
        # Fill missing coordinates with NoData value
        df = fill_missing_coordinates(df)
        # Preserve XYZ formatting by writing space-delimited rows with no header.
        df.to_csv(path_or_buf=output_file, sep=" ", header=False, index=False)
        print(f"Finished processing {file} and saved as {output_file}")
    except Exception as e:
        print(f"Error processing {file}: {e!s}")


if __name__ == "__main__":
    main()
