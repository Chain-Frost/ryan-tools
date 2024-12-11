# GDAL_Flood_Extent.py

from pathlib import Path
import os
import sys
from ryan_library.scripts.gdal.gdal_flood_extent import main_processing


def main():
    """
    Wrapper script to process GDAL flood extent.
    By default, it processes files in the directory where the script is located.
    Optionally, a path to the QGIS install folder can be provided.
    """
    try:
        # Determine the script directory
        script_directory: Path = Path(__file__).resolve().parent

        # Optionally, override the script directory
        # Example:
        # script_directory = Path("path/to/user/specified/directory")

        os.chdir(script_directory)
        print(f"Working directory set to: {script_directory}")

        # Define directories to process; modify as needed
        directories_to_process = [script_directory]
        # Example: directories_to_process = [Path("dir1"), Path("dir2")]

        # Optional: Specify the QGIS install path here
        # If not specified, the main_processing function will attempt to find it
        qgis_install_path = None  # e.g., Path("C:/Program Files/QGIS 3.28")

        # Call the main processing function with the optional QGIS path
        main_processing(
            paths_to_process=directories_to_process,
            console_log_level="DEBUG",
            qgis_path=qgis_install_path,  # Pass the QGIS path
        )
    except Exception as e:
        print(f"An error occurred in the wrapper: {e}")
        os.system("PAUSE")
        sys.exit(1)


if __name__ == "__main__":
    main()
    os.system("PAUSE")
