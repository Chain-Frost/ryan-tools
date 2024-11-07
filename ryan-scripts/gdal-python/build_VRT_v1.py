import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
import re
from multiprocessing import Pool, current_process
import logging


# ========================
# Initialize Values Function
# This function initializes all parameters and configurations.
# ========================
def initialize_values():
    """Initialize and return all configurable parameters and paths."""
    values = {}

    # Parameters and Configurations
    values["file_extension"] = ".tif"

    # Grouping configuration: specify which field number to remove (1-based index)
    # For example, 2 to remove the second field after splitting by '_' and '+'
    values["group_remove_index"] = 2  # Set to 2 to remove the second part

    # Allowed suffixes: only process files ending with these suffixes before the extension
    values["allowed_suffixes"] = [
        "d_HR_Max",
        "h_HR_Max",
        "V_Max",
        "DEM_Z_HR",
    ]  # Add or modify suffixes as needed

    # GDAL Options
    values["gdal_translate_options"] = [
        "-stats",
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
    ]
    values["gdaladdo_options"] = [
        "--config",
        "COMPRESS_OVERVIEW",
        "DEFLATE",
        "--config",
        "PREDICTOR_OVERVIEW",
        "2",
        "--config",
        "NUM_THREADS",
        "ALL_CPUS",
        "--config",
        "SPARSE_OK",
        "TRUE",
    ]

    # Specify paths to GDAL executables
    values["gdalbuildvrt_path"] = r"C:\OSGeo4W\bin\gdalbuildvrt.exe"
    values["gdal_translate_path"] = r"C:\OSGeo4W\bin\gdal_translate.exe"
    values["gdaladdo_path"] = r"C:\OSGeo4W\bin\gdaladdo.exe"

    return values


# ========================
# Setup Logging
# ========================
def setup_logging():
    """Configure logging for the script."""
    logging.basicConfig(
        filename="processing.log",
        filemode="a",
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter("%(levelname)s - %(message)s")
    console.setFormatter(formatter)
    logging.getLogger("").addHandler(console)


# ========================
# Functions for environment setup and checks
# ========================
def setup_environment(values):
    """Set up the environment variables needed for the script."""
    osg_root = "C:\\OSGeo4W"
    os.environ["OSGEO4W_ROOT"] = osg_root
    os.environ["GDAL_DATA"] = os.path.join(osg_root, "share", "gdal")
    os.environ["GDAL_DRIVER_PATH"] = os.path.join(osg_root, "bin")
    os.environ["GS_LIB"] = os.path.join(osg_root, "bin", "lib")
    os.environ["OPENSSL_ENGINES"] = os.path.join(osg_root, "lib", "engines-3")
    os.environ["SSL_CERT_FILE"] = os.path.join(osg_root, "bin", "curl-ca-bundle.crt")
    os.environ["SSL_CERT_DIR"] = os.path.join(osg_root, "apps", "openssl", "certs")
    os.environ["PDAL_DRIVER_PATH"] = os.path.join(osg_root, "apps", "pdal", "plugins")
    os.environ["PROJ_DATA"] = os.path.join(osg_root, "share", "proj")
    os.environ["PYTHONHOME"] = os.path.join(osg_root, "apps", "Python312")
    os.environ["PYTHONUTF8"] = "1"
    os.environ["QT_PLUGIN_PATH"] = os.path.join(osg_root, "apps", "Qt5", "plugins")
    # Prepend necessary paths to PATH
    python_scripts = os.path.join(osg_root, "apps", "Python312", "Scripts")
    gdal_bin = os.path.join(osg_root, "bin")
    os.environ["PATH"] = f"{python_scripts};{gdal_bin};{os.environ['PATH']}"


def check_executable(path, name):
    """Check if the specified executable or script exists."""
    if not os.path.exists(path):
        logging.error(f"{name} not found at {path}. Ensure it is correctly installed.")
        sys.exit(1)


def check_required_components(values):
    """Check that all required GDAL components are available."""
    check_executable(values["gdalbuildvrt_path"], "gdalbuildvrt.exe")
    check_executable(values["gdal_translate_path"], "gdal_translate.exe")
    check_executable(values["gdaladdo_path"], "gdaladdo.exe")


# ========================
# Functions for file processing
# ========================
def find_tif_files(root_dir, extension, allowed_suffixes):
    """
    Recursively find all .tif files in root_dir that end with allowed suffixes.

    Parameters:
    - root_dir (Path): The root directory to search.
    - extension (str): The file extension to look for (e.g., '.tif').
    - allowed_suffixes (list): List of suffixes that filenames must end with before the extension.

    Returns:
    - List of Path objects that match the criteria.
    """
    tif_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.lower().endswith(extension):
                name_without_ext = os.path.splitext(filename)[0]
                if any(
                    name_without_ext.endswith(suffix) for suffix in allowed_suffixes
                ):
                    full_path = os.path.join(dirpath, filename)
                    tif_files.append(full_path)
                else:
                    logging.info(
                        f"Skipping file (does not match allowed suffixes): {filename}"
                    )
    return tif_files


def extract_base_name(filename, group_remove_index):
    """
    Extract the base name by removing the specified field.

    Parameters:
    - filename (str): The name of the file.
    - group_remove_index (int): The field index to remove (1-based).

    Example:
    If filename = '01_X143_DEV_01.00p_triangles_10M_d_HR_Max.tif' and group_remove_index = 2,
    the base name would be '01_DEV_01.00p_triangles_10M_d_HR_Max.tif'
    """
    # Remove file extension
    name_without_ext = os.path.splitext(filename)[0]
    # Split by '_' and '+'
    parts = re.split(r"[+_]", name_without_ext)
    # Adjust for 1-based indexing
    index = group_remove_index - 1
    if index < 0 or index >= len(parts):
        logging.warning(
            f"Filename '{filename}' does not have field {group_remove_index}. Skipping."
        )
        return None
    # Remove the specified field
    del parts[index]
    # Rejoin the remaining parts
    base_name = "_".join(parts) + ".tif"
    return base_name


def group_files_by_base(tif_files, group_remove_index):
    """
    Group TIFF files by their base names after removing the specified field.

    Parameters:
    - tif_files (list): List of TIFF file paths.
    - group_remove_index (int): The field index to remove for grouping.

    Returns:
    - defaultdict(list): Groups of files keyed by their base names.
    """
    groups = defaultdict(list)
    for file in tif_files:
        filename = os.path.basename(file)
        base_name = extract_base_name(filename, group_remove_index)
        if base_name:
            groups[base_name].append(file)
    return groups


# ========================
# Worker Function for Multiprocessing
# ========================
def process_group(args):
    """
    Process a single group: create VRT, translate to TIFF, and build overviews.

    Parameters:
    - args (tuple): Contains (base_name, files, values, script_dir)
    """
    base_name, files, values, script_dir = args
    try:
        logging.info(f"Processing group: {base_name}")

        # Define output VRT and TIFF paths
        vrt_filename = f"{base_name}.vrt"
        vrt_path = os.path.join(script_dir, vrt_filename)

        tif_filename = f"merged_{base_name}"
        tif_path = os.path.join(script_dir, tif_filename)

        # Check if output TIFF already exists
        if os.path.exists(tif_path):
            logging.info(f"TIFF already exists: {tif_path}. Skipping.")
            return

        # Create VRT
        create_vrt(vrt_path, files, values["gdalbuildvrt_path"])

        # Translate VRT to TIFF with compression
        translate_vrt_to_tif(
            vrt_path,
            tif_path,
            values["gdal_translate_path"],
            values["gdal_translate_options"],
        )

        # Build overviews (pyramids)
        build_overviews(tif_path, values["gdaladdo_path"], values["gdaladdo_options"])

        logging.info(f"Successfully created: {tif_path}")

    except subprocess.CalledProcessError as e:
        logging.error(f"Error processing group '{base_name}': {e}")
    except Exception as ex:
        logging.error(f"Unexpected error processing group '{base_name}': {ex}")
    finally:
        # Remove the VRT file if it exists
        if os.path.exists(vrt_path):
            try:
                os.remove(vrt_path)
                logging.info(f"Removed temporary VRT file: {vrt_path}")
            except Exception as ex:
                logging.warning(f"Could not remove VRT file '{vrt_path}': {ex}")


# ========================
# VRT Creation Function
# ========================
import tempfile


def create_vrt(vrt_path, input_files, gdalbuildvrt_path) -> None:
    """Create a VRT from input_files using a unique temporary file."""
    try:
        # Create a unique temporary file list using tempfile
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".txt", dir=os.path.dirname(vrt_path)
        ) as temp_file:
            for file in input_files:
                temp_file.write(f"{file}\n")
            file_list_path = temp_file.name
            logging.info(f"Temporary file list created at: {file_list_path}")

        logging.info(f"Creating VRT: {vrt_path}")
        cmd = [gdalbuildvrt_path, "-input_file_list", file_list_path, vrt_path]
        subprocess.run(cmd, check=True)
        logging.info(f"Created VRT: {vrt_path}")

    except subprocess.CalledProcessError as e:
        logging.error(f"GDAL build VRT failed for '{vrt_path}': {e}")
        return
    except PermissionError:
        logging.error(
            f"Access denied when writing to '{file_list_path}' or creating VRT '{vrt_path}'."
        )
        return
    except Exception as e:
        logging.error(f"Unexpected error during VRT creation for '{vrt_path}': {e}")
        return
    finally:
        # Attempt to remove the temporary file list
        if "file_list_path" in locals() and os.path.exists(file_list_path):
            try:
                os.remove(file_list_path)
                logging.info(f"Removed temporary file list: {file_list_path}")
            except PermissionError:
                logging.warning(
                    f"Access denied when trying to remove temporary file list: {file_list_path}"
                )
            except Exception as e:
                logging.warning(
                    f"Error removing temporary file list '{file_list_path}': {e}"
                )


# ========================
# GDAL Translate Function
# ========================
def translate_vrt_to_tif(vrt_path, tif_path, gdal_translate_path, translate_options):
    """Translate VRT to compressed TIFF."""
    cmd = [gdal_translate_path] + translate_options + [vrt_path, tif_path]
    logging.info(f"Translating VRT to TIFF: {tif_path}")
    subprocess.run(cmd, check=True)
    logging.info(f"Translated VRT to TIFF: {tif_path}")


# ========================
# GDAL Add Overviews Function
# ========================
def build_overviews(tif_path, gdaladdo_path, addo_options):
    """Build pyramids (overviews) for the TIFF."""
    overview_levels = ["2", "4", "8", "16", "32"]
    cmd = [gdaladdo_path] + addo_options + [tif_path] + overview_levels
    logging.info(f"Building overviews for: {tif_path}")
    subprocess.run(cmd, check=True)
    logging.info(f"Built overviews for: {tif_path}")


def main():
    """Main function to merge TIFF files and build overviews."""
    # Setup logging
    setup_logging()

    # Initialize values and configurations
    values = initialize_values()

    # Set up the environment and check required components
    setup_environment(values)
    check_required_components(values)

    # Get the directory where the script is located and change to it
    script_dir = Path(__file__).resolve().parent
    os.chdir(script_dir)
    logging.info(f"Changed working directory to: {script_dir}")

    logging.info(f"Searching for .tif files in: {script_dir}")

    # Step 1: Find all .tif files that end with allowed suffixes
    tif_files = find_tif_files(
        script_dir, values["file_extension"], values["allowed_suffixes"]
    )
    logging.info(f"Found {len(tif_files)} .tif files matching allowed suffixes.")

    if not tif_files:
        logging.info("No .tif files found matching the allowed suffixes. Exiting.")
        return

    # Step 2: Group files by base name after removing the specified field
    group_remove_index = values["group_remove_index"]
    groups = group_files_by_base(tif_files, group_remove_index)
    logging.info(
        f"Grouped into {len(groups)} sets based on removing field {group_remove_index}."
    )

    # Output all groups to a text file
    groups_output_path = os.path.join(script_dir, "groups.txt")
    with open(groups_output_path, "w") as f:
        for idx, (base_name, files) in enumerate(groups.items(), start=1):
            f.write(f"Group {idx}: {base_name}\n")
            for file in files:
                f.write(f"  - {file}\n")
            f.write("\n")
    logging.info(f"All groups have been written to '{groups_output_path}'.")

    # Prepare arguments for multiprocessing
    tasks = []
    for base_name, files in groups.items():
        tasks.append((base_name, files, values, script_dir))

    # Determine the number of processes to use (e.g., number of CPU cores)
    num_processes = os.cpu_count() or 4  # Default to 4 if os.cpu_count() is None

    logging.info(f"Starting multiprocessing with {num_processes} processes.")

    # Initialize multiprocessing Pool
    with Pool(processes=num_processes) as pool:
        pool.map(process_group, tasks)

    logging.info("All processing complete.")


if __name__ == "__main__":
    main()
