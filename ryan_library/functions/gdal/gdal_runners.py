# ryan_library/functions/gdal/gdal_runners.py

import subprocess
from pathlib import Path
from loguru import logger
import os


def run_gdal_calc(input_file: Path, output_file: str, cutoff: float):
    """
    Run gdal_calc.py with the specified parameters.

    Args:
        input_file (Path): Path to the input TIFF file.
        output_file (str): Filename for the output TIFF.
        cutoff (float): Cutoff value for the calculation.
    """
    commands_calc = '--calc="where(A>=%%%c,1,0)"'
    create_opts = "--NoDataValue=0 --co COMPRESS=DEFLATE --co PREDICTOR=2 --co NUM_THREADS=ALL_CPUS --co SPARSE_OK=TRUE --co BIGTIFF=IF_SAFER"

    gdal_calc_path = os.environ.get("GDAL_CALC_PATH")
    if not gdal_calc_path:
        logger.error("GDAL_CALC_PATH is not set in environment variables.")
        raise EnvironmentError("GDAL_CALC_PATH is not set.")

    gdal_calc_cmd = [
        "python",
        gdal_calc_path,
        commands_calc.replace("%%%c", str(cutoff)),
        "-A",
        str(input_file),
        "--outfile",
        output_file,
    ] + create_opts.split()

    logger.debug(f"Running GDAL Calc Command: {' '.join(gdal_calc_cmd)}")

    try:
        subprocess.run(gdal_calc_cmd, check=True)
        logger.info(f"gdal_calc.py completed for {input_file} -> {output_file}")
    except subprocess.CalledProcessError as e:
        logger.error(f"gdal_calc.py failed for {input_file}: {e}")
        raise


def run_gdal_polygonize(input_file: str, output_shp: str):
    """
    Run gdal_polygonize.py to convert the raster to a polygon.

    Args:
        input_file (str): Path to the input TIFF file.
        output_shp (str): Filename for the output Shapefile.
    """
    gdal_polygonize_path = os.environ.get("GDAL_POLYGONIZE_PATH")
    if not gdal_polygonize_path:
        logger.error("GDAL_POLYGONIZE_PATH is not set in environment variables.")
        raise EnvironmentError("GDAL_POLYGONIZE_PATH is not set.")

    gdal_polygonize_cmd = ["python", gdal_polygonize_path, input_file, output_shp]

    logger.debug(f"Running GDAL Polygonize Command: {' '.join(gdal_polygonize_cmd)}")

    try:
        subprocess.run(gdal_polygonize_cmd, check=True)
        logger.info(f"gdal_polygonize.py completed for {input_file} -> {output_shp}")
    except subprocess.CalledProcessError as e:
        logger.error(f"gdal_polygonize.py failed for {input_file}: {e}")
        raise
