"""
Calculate Stage-Storage Volume Curves from DEMs.

Reads DEMs and outputs a stage-storage (elevation-volume) curve as a CSV and PNG.
Utilizes the robust chunked-histogram calculation in ryan_library.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from loguru import logger

WRAPPER_VERSION = "2026-08-11.1"
DEFAULT_WORKING_DIR = Path(".")
DEFAULT_PATTERNS = ["*.tif"]
DEFAULT_STEP_SIZE = 0.01

from ryan_library.functions.gdal.stage_storage import compute_stage_storage
from ryan_library.functions.wrapper_utils import (
    add_execution_cli_arguments,
    change_working_directory,
    pause_console,
    print_wrapper_banner,
)


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Calculate stage-storage volume curves for DEMs."
    )
    
    parser.add_argument(
        "input_directory",
        type=Path,
        help="Directory containing the input DEMs.",
    )
    parser.add_argument(
        "--patterns",
        nargs="+",
        default=DEFAULT_PATTERNS,
        help="Glob patterns to match input files.",
    )
    parser.add_argument(
        "--min-level",
        type=float,
        help="Minimum elevation level (defaults to the DEM's minimum elevation).",
    )
    parser.add_argument(
        "--max-level",
        type=float,
        help="Maximum elevation level (defaults to the DEM's maximum elevation).",
    )
    parser.add_argument(
        "--step",
        type=float,
        default=DEFAULT_STEP_SIZE,
        help=f"Elevation step size for the curve (default: {DEFAULT_STEP_SIZE}m).",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Disable automatic plotting of the stage-storage curve to a PNG file.",
    )
    
    add_execution_cli_arguments(parser)
    return parser.parse_args()


def plot_curve(csv_path: Path, df: pd.DataFrame) -> None:
    plt.figure(figsize=(10, 6))
    plt.plot(df['Level (m)'], df['Volume (m3)'], color='blue', linewidth=2)
    plt.fill_between(df['Level (m)'], df['Volume (m3)'], color='blue', alpha=0.1)
    
    plt.title(f"Stage-Storage Curve: {csv_path.stem.replace('volumes_', '')}")
    plt.xlabel("Elevation Level (mAHD)")
    plt.ylabel("Cumulative Volume (m³)")
    plt.grid(True, linestyle='--', alpha=0.7)
    plt.tight_layout()
    
    png_path = csv_path.with_suffix('.png')
    plt.savefig(png_path, dpi=300)
    plt.close()
    logger.info("Saved plot to {}", png_path.name)


def main(*, working_directory: Path | None = None) -> int:
    args = _parse_cli_arguments()
    target_directory = (working_directory or args.input_directory).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    input_files = []
    for pattern in args.patterns:
        input_files.extend(list(target_directory.glob(pattern)))

    if not input_files:
        logger.warning("No files matching {} found in {}", args.patterns, target_directory)
        return 0
        
    input_files = list(set(input_files))
    logger.info("Found {} DEM files to process.", len(input_files))
    
    success_count = 0
    
    for dem_path in input_files:
        try:
            # Determine min and max if not provided
            min_lvl = args.min_level
            max_lvl = args.max_level
            
            if min_lvl is None or max_lvl is None:
                logger.info("Scanning {} to determine elevation bounds...", dem_path.name)
                with rasterio.open(dem_path) as src:
                    # Request statistics. force=True ensures we get it even if not in metadata.
                    # approx=True will use overviews or a subsample for speed.
                    stats = src.statistics(1, approx=True, force=True)
                    if min_lvl is None:
                        min_lvl = np.floor(stats.min)
                    if max_lvl is None:
                        max_lvl = np.ceil(stats.max)
            
            logger.info("Calculating volumes from {} to {} (step: {})...", min_lvl, max_lvl, args.step)
            
            # Generate levels array
            # Add a small epsilon to max_lvl to ensure it's included due to floating point math
            levels = np.arange(min_lvl, max_lvl + (args.step / 2), args.step)
            
            volumes_dict = compute_stage_storage(dem_path=dem_path, levels=levels)
            
            # Export to CSV
            csv_name = f"volumes_{dem_path.stem}.csv"
            csv_path = target_directory / csv_name
            
            df = pd.DataFrame(list(volumes_dict.items()), columns=['Level (m)', 'Volume (m3)'])
            df.to_csv(csv_path, index=False)
            logger.success("Saved volumes to {}", csv_name)
            
            # Plot
            if not args.no_plot:
                plot_curve(csv_path, df)
                
            success_count += 1
            
        except Exception as e:
            logger.error("Failed to process {}: {}", dem_path.name, e)

    logger.info("Successfully processed {} out of {} files.", success_count, len(input_files))
    return 0 if success_count == len(input_files) else 1


if __name__ == "__main__":
    args = _parse_cli_arguments()
    
    if args.console_log_level:
        logger.remove()
        logger.add(sys.stderr, level=args.console_log_level.upper())
        
    result = main()
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    
    if not getattr(args, "no_pause", False):
        pause_console()
        
    raise SystemExit(result)
