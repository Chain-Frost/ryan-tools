from __future__ import annotations

WRAPPER_VERSION = "1.0.0"

import argparse
import sys
import subprocess
import shutil

from loguru import logger

from ryan_library.functions.path_stuff import to_single_path, to_path_list


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"GDAL retile wrapper (v{WRAPPER_VERSION})."
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        type=str,
        help="Input raster files to retile.",
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        required=True,
        help="Output directory for tiles.",
    )
    parser.add_argument(
        "--tile-size",
        "-ps",
        nargs=2,
        type=int,
        default=[5000, 5000],
        help="Tile size in pixels (X Y). Default: 5000 5000",
    )
    parser.add_argument(
        "--overlap",
        type=int,
        default=10,
        help="Overlap in pixels. Default: 10",
    )
    parser.add_argument(
        "--format",
        "-of",
        type=str,
        default="GTiff",
        help="Output format. Default: GTiff",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_cli_arguments()

    input_paths = to_path_list(args.inputs)
    output_dir = to_single_path(args.output_dir)

    for p in input_paths:
        if not p.exists():
            logger.error(f"Input file not found: {p}")
            sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Try to find python in current environment
    python_exe = sys.executable

    # Build the gdal_retile command
    # osgeo_utils.gdal_retile is the standard module path for gdal scripts in recent GDAL python bindings
    cmd = [
        python_exe,
        "-m",
        "osgeo_utils.gdal_retile",
        "-v",
        "-of",
        args.format,
        "-co",
        "COMPRESS=DEFLATE",
        "-co",
        "PREDICTOR=2",
        "-co",
        "NUM_THREADS=ALL_CPUS",
        "-co",
        "SPARSE_OK=TRUE",
        "-co",
        "BIGTIFF=IF_SAFER",
        "-ps",
        str(args.tile_size[0]),
        str(args.tile_size[1]),
        "-overlap",
        str(args.overlap),
        "-r",
        "near",
        "-targetDir",
        str(output_dir),
    ]

    for p in input_paths:
        cmd.append(str(p))

    logger.info("Executing gdal_retile...")
    logger.debug(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=False, text=True, capture_output=True)
        if result.returncode != 0:
            logger.error(f"gdal_retile failed with return code {result.returncode}")
            logger.error(result.stderr)
            
            # Fallback if osgeo_utils is not available, try calling gdal_retile.py directly
            gdal_retile_script = shutil.which("gdal_retile.py")
            if gdal_retile_script:
                logger.info("Falling back to gdal_retile.py direct call...")
                cmd[1:3] = [gdal_retile_script] # Replace '-m osgeo_utils.gdal_retile' with script path
                result2 = subprocess.run(cmd, check=True, text=True)
            else:
                sys.exit(1)
        else:
            logger.success("Retiling complete.")
            logger.debug(result.stdout)
    except Exception as e:
        logger.error(f"Failed to execute gdal_retile: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
