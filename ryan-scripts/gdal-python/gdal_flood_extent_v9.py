import os
import subprocess
import sys
from glob import glob
import multiprocessing  # Added import for multiprocessing


# ========================
# Initialize Values Function
# This function is called by main() to initialize all parameters and configurations.
# ========================
def initialize_values():
    """Initialize and return all configurable parameters and paths."""
    values = {}

    # Parameters and Configurations
    values["file_extension"] = "d_HR_Max.tif"
    values["cutoff_values"] = [0.0]
    values["commands_calc"] = '--calc="where(A>=%%%c,1,0)"'
    values["instances"] = os.cpu_count()
    values["create_opts"] = (
        "--NoDataValue=0 --co COMPRESS=DEFLATE --co PREDICTOR=2 --co NUM_THREADS=ALL_CPUS --co SPARSE_OK=TRUE --co BIGTIFF=IF_SAFER"
    )

    # Specify paths to custom Python modules and executables
    values["gdal_calc_path"] = "C:/OSGeo4W/apps/Python312/Scripts/gdal_calc.py"
    values["gdal_polygonize_path"] = "C:/OSGeo4W/apps/Python312/Scripts/gdal_polygonize.py"
    values["gdal_translate_path"] = "C:/OSGeo4W/bin/gdal_translate.exe"

    return values


# ========================
# Functions for environment setup and checks
# ========================
def setup_environment(values):
    """Set up the environment variables needed for the script."""
    os.environ["OSGEO4W_ROOT"] = "C:\\OSGeo4W"
    os.environ["GDAL_DATA"] = os.path.join(os.environ["OSGEO4W_ROOT"], "apps", "gdal", "share", "gdal")
    os.environ["GDAL_DRIVER_PATH"] = os.path.join(os.environ["OSGEO4W_ROOT"], "apps", "gdal", "lib", "gdalplugins")
    os.environ["GS_LIB"] = os.path.join(os.environ["OSGEO4W_ROOT"], "apps", "gs", "lib")
    os.environ["OPENSSL_ENGINES"] = os.path.join(os.environ["OSGEO4W_ROOT"], "lib", "engines-3")
    os.environ["SSL_CERT_FILE"] = os.path.join(os.environ["OSGEO4W_ROOT"], "bin", "curl-ca-bundle.crt")
    os.environ["SSL_CERT_DIR"] = os.path.join(os.environ["OSGEO4W_ROOT"], "apps", "openssl", "certs")
    os.environ["PDAL_DRIVER_PATH"] = os.path.join(os.environ["OSGEO4W_ROOT"], "apps", "pdal", "plugins")
    os.environ["PROJ_DATA"] = os.path.join(os.environ["OSGEO4W_ROOT"], "share", "proj")
    os.environ["PYTHONHOME"] = os.path.join(os.environ["OSGEO4W_ROOT"], "apps", "Python312")
    os.environ["PYTHONUTF8"] = "1"
    os.environ["QT_PLUGIN_PATH"] = os.path.join(os.environ["OSGEO4W_ROOT"], "apps", "Qt5", "plugins")
    os.environ["PATH"] = (
        f"{os.path.join(os.environ['OSGEO4W_ROOT'], 'apps', 'Python312', 'Scripts')};{os.path.join(os.environ['OSGEO4W_ROOT'], 'bin')};{os.environ['PATH']}"
    )

    # Ensure the correct Python paths are used
    sys.path.insert(0, os.path.dirname(values["gdal_calc_path"]))
    sys.path.insert(0, os.path.dirname(values["gdal_polygonize_path"]))


def check_executable(path, name):
    """Check if the specified executable or script exists."""
    if not os.path.exists(path):
        print(f"Error: {name} not found. Ensure it is correctly installed.")
        sys.exit(1)


def check_required_components(values):
    """Check that all required components are available."""
    check_executable(values["gdal_translate_path"], "gdal_translate")
    check_executable(values["gdal_calc_path"], "gdal_calc.py")
    check_executable(values["gdal_polygonize_path"], "gdal_polygonize.py")


# ========================
# Functions for file processing
# ========================
def format_cutoff_value(value):
    """Format the cutoff value for use in filenames."""
    formatted_value = f"{value}".rstrip("0").rstrip(".") if "." in f"{value}" else f"{value}"
    return formatted_value.replace(".", "")


def process_file(args):
    """Process a single file with the specified cutoff values."""
    filepath, values = args  # Modified to accept a tuple of (filepath, values)
    filename = os.path.basename(filepath)
    base_name = os.path.splitext(filename)[0]

    for c in values["cutoff_values"]:
        formatted_value = format_cutoff_value(c)

        outname = f"{base_name}_FE_{formatted_value}m.tif"
        shpname = f"{base_name}_FE_{formatted_value}m.shp"

        print(f"Processing cutoff {c} for file {filepath}")

        run_gdal_calc(filepath, outname, c, values)
        run_gdal_polygonize(outname, shpname, values)


def run_gdal_calc(filepath, outname, cutoff, values):
    """Run gdal_calc.py with the specified parameters."""
    gdal_calc_cmd = [
        "python",
        values["gdal_calc_path"],
        values["commands_calc"].replace("%%%c", str(cutoff)),
        "-A",
        filepath,
        "--outfile",
        outname,
    ] + values["create_opts"].split()
    subprocess.run(gdal_calc_cmd, check=True)


def run_gdal_polygonize(outname, shpname, values):
    """Run gdal_polygonize.py to convert the raster to a polygon."""
    gdal_polygonize_cmd = ["python", values["gdal_polygonize_path"], outname, shpname]
    subprocess.run(gdal_polygonize_cmd, check=True)


# ========================
# Main processing function
# ========================
def main():
    """Main function to process all files with the specified extension."""
    # Initialize values and configurations
    values = initialize_values()

    # Set up the environment and check required components
    setup_environment(values)
    check_required_components(values)

    # Process specific filenames or all files with the given extension
    files_to_process = glob(f"*{values['file_extension']}")

    # Prepare arguments for multiprocessing
    args = [(filepath, values) for filepath in files_to_process]

    # Create a multiprocessing pool and process files in parallel
    with multiprocessing.Pool(processes=values["instances"]) as pool:
        pool.map(process_file, args)

    print("\nComplete")


if __name__ == "__main__":
    main()
