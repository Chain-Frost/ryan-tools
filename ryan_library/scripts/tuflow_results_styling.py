# ryan_library.scripts/tuflow_results_styling.py
import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from pathlib import Path
import importlib.resources as pkg_resources
import logging

from ryan_library.functions.logging_helpers import setup_logging

# Path to the default styles within the package
default_styles_path = Path(__file__).parent.parent.parent / "QGIS-Styles" / "TUFLOW"

# Configure logging using the library's logging helpers
setup_logging(
    log_level=logging.INFO,
    log_file=None,
    use_rotating_file=False,
    enable_color=True,
)

logger = logging.getLogger(__name__)


def get_file_mappings(default_styles_path, user_qml_paths=None):
    """
    Returns a mapping of file keys to their extensions and QML paths.
    Allows user to override default QML paths.

    Args:
        default_styles_path (Path): Path to the default QML styles within the package.
        user_qml_paths (dict, optional): Dictionary of user overrides for QML paths.

    Returns:
        dict: Mapping of file keys to their extensions and QML paths.
    """
    raster_exts = ["flt", "tif"]
    vector_exts = ["shp", "gpkg"]

    mapping_dict = {
        "d_Max": {
            "exts": raster_exts,
            "qml": default_styles_path / "depth_for_legend_max2m.qml",
        },
        "h_Max": {
            "exts": raster_exts,
            "qml": default_styles_path / "hmax.qml",
        },
        "V_Max": {
            "exts": raster_exts,
            "qml": default_styles_path / "velocities scour protection mrwa.qml",
        },
        "DEM_Z": {
            "exts": raster_exts,
            "qml": default_styles_path / "hillshade.qml",
        },
        "1d_ccA_L": {
            "exts": vector_exts,
            "qml": default_styles_path / "_1d_ccA.qml",
        },
        "DIFF_P2-P1": {
            "exts": raster_exts,
            "qml": default_styles_path / "Depth Diff GOOOD.qml",
        },
        "Results1D": {
            "exts": vector_exts,
            "layer_name": "1d_ccA_L",
            "qml": default_styles_path / "1d_ccA.qml",
        },
    }

    # Add HR variants
    mapping_dict["d_HR_Max"] = mapping_dict["d_Max"]
    mapping_dict["h_HR_Max"] = mapping_dict["h_Max"]
    mapping_dict["DEM_Z_HR"] = mapping_dict["DEM_Z"]

    # Override with user-provided QML paths if available
    if user_qml_paths:
        for key, qml_path in user_qml_paths.items():
            if key in mapping_dict:
                mapping_dict[key]["qml"] = Path(qml_path)

    return mapping_dict


def get_qml_content(qml_path):
    """
    Retrieves the content of a QML file, either from user-provided path or default styles path.

    Args:
        qml_path (Path): Path to the QML file.

    Returns:
        str: Content of the QML file.
    """
    if qml_path.is_absolute() and qml_path.exists():
        logger.debug(f"Loading QML from user path: {qml_path}")
        with qml_path.open("r", encoding="utf-8") as file:
            return file.read()
    else:
        # Use the global default_styles_path
        full_qml_path = default_styles_path / qml_path.name
        if full_qml_path.exists():
            logger.debug(f"Loading QML from default styles path: {full_qml_path}")
            with full_qml_path.open("r", encoding="utf-8") as file:
                return file.read()
        else:
            logger.error(
                f"QML file '{qml_path.name}' not found in default styles path."
            )
            return ""


def process_data(filename, ext, current_path, qml_path):
    """
    Processes raster and vector data by applying the QML style.

    Args:
        filename (str): Name of the file to process.
        ext (str): Target file extension (e.g., 'qml').
        current_path (Path): Current directory path.
        qml_path (Path): Path to the QML file.
    """
    try:
        source_file = current_path / filename
        new_filename = f"{source_file.stem}.{ext}"
        destination = current_path / new_filename
        logger.info(f"Copying {source_file} to {destination}")

        # Retrieve QML content
        qml_content = get_qml_content(qml_path)
        if not qml_content:
            logger.warning(f"No QML content for {filename}. Skipping.")
            return

        # Write QML content to destination
        with destination.open("w", encoding="utf-8") as f:
            f.write(qml_content)

    except Exception as e:
        logger.error(f"Error processing data for {filename}: {e}")


def process_gpkg(filename, layer_name, current_path, qml_path):
    """
    Processes GeoPackage files by applying styles to specific layers.

    Args:
        filename (str): Name of the GeoPackage file.
        layer_name (str): Name of the layer within the GeoPackage.
        current_path (Path): Current directory path.
        qml_path (Path): Path to the QML file.
    """
    try:
        gpkg_path = current_path / filename
        logger.info(f"Processing GeoPackage: {gpkg_path}")

        conn = sqlite3.connect(gpkg_path)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT styleName, styleQML FROM layer_styles WHERE f_table_name = ?;",
            (layer_name,),
        )
        styles = cursor.fetchall()

        for style_name, style_qml in styles:
            logger.info(
                f"NOT YET applying style '{style_name}' to layer '{layer_name}'"
            )
            # Implement style application logic here
            # For example, you could update the styleQML in the database or apply it using QGIS APIs

        conn.close()
    except Exception as e:
        logger.error(f"Error processing GeoPackage {filename}: {e}")


def tree_process(current_path, mappings):
    """
    Recursively processes directories to apply QML styles based on file mappings.

    Args:
        current_path (Path): Current directory path.
        mappings (dict): File mappings with extensions and QML paths.
    """
    logger.info(f"Processing directory: {current_path}")
    with ThreadPoolExecutor() as executor:
        futures = []
        for item in current_path.iterdir():
            if item.is_file():
                for key, value in mappings.items():
                    for ext in value["exts"]:
                        if item.name.lower().endswith(f"{key.lower()}.{ext.lower()}"):
                            if ext.lower() == "gpkg":
                                futures.append(
                                    executor.submit(
                                        process_gpkg,
                                        item.name,
                                        value.get("layer_name", ""),
                                        current_path,
                                        value["qml"],
                                    )
                                )
                            else:
                                futures.append(
                                    executor.submit(
                                        process_data,
                                        item.name,
                                        "qml",
                                        current_path,
                                        value["qml"],
                                    )
                                )
            elif item.is_dir():
                futures.append(executor.submit(tree_process, item, mappings))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logger.error(f"Error in thread execution: {e}")


def validate_qml_paths(mappings, user_qml_overrides):
    """
    Validates that user-provided QML paths exist.

    Args:
        mappings (dict): File mappings with extensions and QML paths.
        user_qml_overrides (dict): User-provided QML path overrides.
    """
    for key, qml_path in user_qml_overrides.items():
        qml_path = Path(qml_path)
        if qml_path.is_absolute():
            if not qml_path.exists():
                logger.warning(
                    f"User-provided QML path for '{key}' does not exist: {qml_path}"
                )


def apply_styles(user_qml_overrides=None) -> None:
    """
    Main function to apply QML styles to QGIS results.

    Args:
        user_qml_overrides (dict, optional): Dictionary to override default QML paths.
    """
    mappings: dict[
        str, dict[str, list[str] | Path] | dict[str, list[str] | str | Path]
    ] = get_file_mappings(
        default_styles_path=default_styles_path, user_qml_paths=user_qml_overrides
    )

    # Validate QML paths only for user overrides
    if user_qml_overrides:
        validate_qml_paths(mappings, user_qml_overrides)

    # Set current path to the script's directory or the desired processing directory
    current_path = Path.cwd()
    logger.info(f"Current working directory: {current_path}")
    logger.debug("File mappings:")
    logger.debug(mappings)

    # Start processing
    try:
        tree_process(current_path, mappings)
    except Exception as e:
        logger.critical(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    apply_styles()
