from concurrent.futures._base import Future
import math
import os
import subprocess
import numpy as np
import rasterio  # type:ignore
from glob import iglob
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from affine import Affine
from rasterio.windows import Window

# Set this flag to True if you want to drop rows with missing z values
DROP_NA = True  # Change to True to drop rows with missing z's instead of filling them
# WORKING_DIR = None
WORKING_DIR = r"folder"
OUT_FOLDER = r"converted_and_trimmed"
TARGET_CELLS_PER_CHUNK = 500_000  # Limits memory use to ~20-30 MB per chunk


def _is_north_up(transform: Affine) -> bool:
    return np.isclose(transform.b, 0.0) and np.isclose(transform.d, 0.0)


def _coordinate_vectors_for_chunk(
    transform: Affine, width: int, row_start: int, chunk_rows: int
) -> tuple[np.ndarray, np.ndarray]:
    """
    Build flattened x/y coordinate arrays for a chunk. Keeps memory bounded by only generating
    coordinates for the portion being processed.
    """
    north_up = _is_north_up(transform)
    if north_up:
        x_coords = transform.c + transform.a * np.arange(width, dtype=np.float64)
        row_indices = np.arange(row_start, row_start + chunk_rows, dtype=np.float64)
        y_coords = transform.f + transform.e * row_indices
        xs = np.tile(x_coords, chunk_rows)
        ys = np.repeat(y_coords, width)
    else:
        rows = np.arange(row_start, row_start + chunk_rows, dtype=np.float64)
        cols = np.arange(width, dtype=np.float64)
        col_grid, row_grid = np.meshgrid(cols, rows)
        xs = transform.c + transform.a * col_grid + transform.b * row_grid
        ys = transform.f + transform.d * col_grid + transform.e * row_grid
        xs = xs.ravel()
        ys = ys.ravel()

    return xs, ys


def _rows_per_chunk(height: int, width: int) -> int:
    """
    Choose the number of raster rows per chunk so that the number of cells (and therefore memory)
    stays close to TARGET_CELLS_PER_CHUNK. Always returns at least 1.
    """
    if width <= 0:
        return max(1, height)

    target_rows = TARGET_CELLS_PER_CHUNK // width
    if target_rows <= 0:
        target_rows = 1
    return min(height, target_rows)


def process_tif_file(file: str) -> None:
    try:
        print(f"Processing {file}")
        with rasterio.open(file) as src:
            rows = src.height
            cols = src.width
            transform = src.transform
            rows_per_chunk = _rows_per_chunk(rows, cols)
            total_chunks = math.ceil(rows / rows_per_chunk) if rows_per_chunk else 1

            # Determine the nodata value: if the source has a nodata, use it; otherwise, use -9999
            nodata_value = src.nodata if src.nodata is not None and not np.isnan(src.nodata) else -9999.0

            output_file = f"{OUT_FOLDER}/{os.path.basename(file)[:-4]}_mod.xyz"
            print(
                f"-- raster shape {rows}x{cols}, chunk rows {rows_per_chunk}, "
                f"{total_chunks} chunk{'s' if total_chunks != 1 else ''}"
            )

            with open(output_file, "w", encoding="utf-8", newline="") as out_file:
                out_file.write("x,y,z\n")
                total_points = 0

                for chunk_index, row_start in enumerate(range(0, rows, rows_per_chunk), start=1):
                    chunk_height = min(rows_per_chunk, rows - row_start)
                    window = Window(col_off=0, row_off=row_start, width=cols, height=chunk_height)
                    band_chunk = src.read(1, window=window, masked=False).astype(np.float64, copy=False)
                    print(
                        f"---- chunk {chunk_index}/{total_chunks} rows {row_start}-{row_start + chunk_height - 1} "
                        f"({chunk_height} rows)"
                    )

                    if DROP_NA and src.nodata is not None:
                        if np.isnan(src.nodata):
                            band_chunk[np.isnan(band_chunk)] = np.nan
                        else:
                            band_chunk[band_chunk == src.nodata] = np.nan

                    xs, ys = _coordinate_vectors_for_chunk(transform, cols, row_start, chunk_height)
                    flat_z = band_chunk.reshape(-1)

                    if DROP_NA:
                        valid_mask = ~np.isnan(flat_z)
                        if not np.any(valid_mask):
                            continue
                        xs = xs[valid_mask]
                        ys = ys[valid_mask]
                        flat_z = flat_z[valid_mask]
                    else:
                        flat_z = np.where(np.isfinite(flat_z), flat_z, nodata_value)

                    if flat_z.size == 0:
                        continue

                    chunk_data = np.column_stack((xs, ys, flat_z))
                    if chunk_data.size == 0:
                        print("------ no valid data in chunk, skipping write")
                        continue

                    np.savetxt(out_file, chunk_data, fmt="%.9f", delimiter=",")
                    total_points += chunk_data.shape[0]
                    print(f"------ wrote {chunk_data.shape[0]:,} points (cumulative {total_points:,})")
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
