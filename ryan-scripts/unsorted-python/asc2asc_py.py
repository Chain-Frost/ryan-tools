"""
Native Python replacement for TUFLOW asc_to_asc.exe.

Supports:
-max (maximum value across input rasters)
-diff (subtract second raster from first)
-stat (e.g. -statMean, -statMedian, etc)

Examples:
    python asc2asc_py.py -b -diff -out difference.tif after.tif before.tif
    python asc2asc_py.py -b -max -out max.tif 1.tif 2.tif 3.tif
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

WRAPPER_VERSION = "2026-08-11.1"

# Editable defaults
DEFAULT_WORKING_DIR = Path(".")

from loguru import logger
from ryan_library.functions.gdal.asc2asc_logic import compute_max, compute_diff, compute_stat
from ryan_library.functions.wrapper_utils import (
    print_wrapper_banner,
    pause_console,
    change_working_directory,
)

def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Python replacement for TUFLOW asc_to_asc")
    parser.add_argument("-b", action="store_true", help="Batch mode (no pause, no-op for compatibility)")
    parser.add_argument("-max", action="store_true", help="Find maximum across grids")
    parser.add_argument("-diff", action="store_true", help="Subtract second grid from first")
    parser.add_argument("-stat", type=str, help="Calculate stat (e.g. Mean, Median, Min, Max)")
    parser.add_argument("-out", type=str, required=True, help="Output file")
    parser.add_argument("-change", action="store_true", help="For diff: treat nodata as 0")
    parser.add_argument("-nowetdry", action="store_true", help="For diff: no wet/dry test")
    parser.add_argument("-co", action="append", default=[], help="Creation options for rasterio")
    parser.add_argument("input_files", nargs="+", help="Input files")
    
    # Pre-process sys.argv to handle -statMean -> -stat Mean
    argv = sys.argv[1:]
    processed_argv = []
    for arg in argv:
        if arg.lower().startswith("-stat") and len(arg) > 5:
            processed_argv.extend(["-stat", arg[5:]])
        else:
            processed_argv.append(arg)
            
    return parser.parse_known_args(processed_argv)[0]

def main(args: argparse.Namespace, *, working_directory: Path | None = None) -> int:
    target_directory = (working_directory or DEFAULT_WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1
        
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    
    try:
        extra_args = []
        for co in args.co:
            extra_args.extend(["-co", co])
            
        if args.max:
            logger.info("Running compute_max")
            compute_max(args.input_files, args.out, extra_args=extra_args)
        elif args.diff:
            logger.info("Running compute_diff")
            if len(args.input_files) != 2:
                logger.error("-diff requires exactly 2 input files")
                return 2
            compute_diff(
                args.input_files[0], 
                args.input_files[1], 
                args.out, 
                change=args.change, 
                nowetdry=args.nowetdry, 
                extra_args=extra_args
            )
        elif args.stat:
            logger.info("Running compute_stat ({})", args.stat)
            compute_stat(args.stat, args.input_files, args.out, extra_args=extra_args)
        else:
            logger.error("No operation specified (-max, -diff, or -stat)")
            return 2
            
    except Exception:
        logger.exception("Workflow failed.")
        return 1
        
    return 0

if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(args)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not args.b:
        pause_console(collect_before_pause=True)
    raise SystemExit(result)
