# ryan_library/scripts/tuflow_results_styling.py

import os
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from pathlib import Path
import logging

# Get the logger for this module
logger = logging.getLogger(__name__)


class TUFLOWResultsStyler:
    def __init__(self, user_qml_overrides=None):
        """
        Initializes the TUFLOWResultsStyler with default styles path and user overrides.
        """
        self.default_styles_path = (
            Path(__file__).parent.parent.parent / "QGIS-Styles" / "TUFLOW"
        )
        self.user_qml_overrides = user_qml_overrides
        self.mappings = self.get_file_mappings()

    def get_file_mappings(self):
        """
        Returns a mapping of file keys to their extensions and QML paths.
        Allows user to override default QML paths.
        """
        raster_exts = ["flt", "tif"]
        vector_exts = ["shp", "gpkg"]

        mapping_dict = {
            "d_Max": {
                "exts": raster_exts,
                "qml": self.default_styles_path / "depth_for_legend_max2m.qml",
            },
            "h_Max": {
                "exts": raster_exts,
                "qml": self.default_styles_path / "hmax.qml",
            },
            "V_Max": {
                "exts": raster_exts,
                "qml": self.default_styles_path
                / "velocities scour protection mrwa.qml",
            },
            "DEM_Z": {
                "exts": raster_exts,
                "qml": self.default_styles_path / "hillshade.qml",
            },
            "1d_ccA_L": {
                "exts": vector_exts,
                "qml": self.default_styles_path / "_1d_ccA.qml",
            },
            "DIFF_P2-P1": {
                "exts": raster_exts,
                "qml": self.default_styles_path / "Depth Diff GOOOD.qml",
            },
            "Results1D": {
                "exts": vector_exts,
                "layer_name": "1d_ccA_L",
                "qml": self.default_styles_path / "1d_ccA.qml",
            },
        }

        # Add HR variants
        mapping_dict["d_HR_Max"] = mapping_dict["d_Max"]
        mapping_dict["h_HR_Max"] = mapping_dict["h_Max"]
        mapping_dict["DEM_Z_HR"] = mapping_dict["DEM_Z"]

        # Override with user-provided QML paths if available
        if self.user_qml_overrides:
            for key, qml_path in self.user_qml_overrides.items():
                if key in mapping_dict:
                    mapping_dict[key]["qml"] = Path(qml_path)

        return mapping_dict

    def get_qml_content(self, qml_path):
        """
        Retrieves the content of a QML file, either from user-provided path or default styles path.
        """
        if qml_path.is_absolute() and qml_path.exists():
            logger.debug(f"Loading QML from user path: {qml_path}")
            with qml_path.open("r", encoding="utf-8") as file:
                return file.read()
        else:
            full_qml_path = self.default_styles_path / qml_path.name
            if full_qml_path.exists():
                logger.debug(f"Loading QML from default styles path: {full_qml_path}")
                with full_qml_path.open("r", encoding="utf-8") as file:
                    return file.read()
            else:
                logger.error(
                    f"QML file '{qml_path.name}' not found in default styles path."
                )
                return ""

    def process_data(self, filename, ext, current_path, qml_path):
        """
        Processes raster and vector data by applying the QML style.
        """
        try:
            source_file = current_path / filename
            new_filename = f"{source_file.stem}.{ext}"
            destination = current_path / new_filename
            logger.info(
                f"Copying {source_file} to {destination}", extra={"simple_format": True}
            )

            # Retrieve QML content
            qml_content = self.get_qml_content(qml_path)
            if not qml_content:
                logger.warning(f"No QML content for {filename}. Skipping.")
                return

            # Write QML content to destination
            with destination.open("w", encoding="utf-8") as f:
                f.write(qml_content)

        except Exception as e:
            logger.error(f"Error processing data for {filename}: {e}")

    def process_gpkg(self, filename, layer_name, current_path, qml_path):
        """
        Processes GeoPackage files by applying styles to specific layers.
        """
        try:
            gpkg_path = current_path / filename
            logger.info(
                f"Processing GeoPackage: {gpkg_path}", extra={"simple_format": True}
            )

            conn = sqlite3.connect(gpkg_path)
            cursor = conn.cursor()

            cursor.execute(
                "SELECT styleName, styleQML FROM layer_styles WHERE f_table_name = ?;",
                (layer_name,),
            )
            styles = cursor.fetchall()

            for style_name, style_qml in styles:
                logger.info(
                    f"Applying style '{style_name}' to layer '{layer_name}'",
                    extra={"simple_format": True},
                )
                # Implement style application logic here
                # For example, you could update the styleQML in the database or apply it using QGIS APIs

            conn.close()
        except Exception as e:
            logger.error(f"Error processing GeoPackage {filename}: {e}")

    def tree_process(self, current_path):
        """
        Recursively processes directories to apply QML styles based on file mappings.
        """
        logger.info(
            f"Processing directory: {current_path}", extra={"simple_format": True}
        )
        with ThreadPoolExecutor() as executor:
            futures = []
            for item in current_path.iterdir():
                if item.is_file():
                    for key, value in self.mappings.items():
                        for ext in value["exts"]:
                            if item.name.lower().endswith(
                                f"{key.lower()}.{ext.lower()}"
                            ):
                                if ext.lower() == "gpkg":
                                    futures.append(
                                        executor.submit(
                                            self.process_gpkg,
                                            item.name,
                                            value.get("layer_name", ""),
                                            current_path,
                                            value["qml"],
                                        )
                                    )
                                else:
                                    futures.append(
                                        executor.submit(
                                            self.process_data,
                                            item.name,
                                            "qml",
                                            current_path,
                                            value["qml"],
                                        )
                                    )
                elif item.is_dir():
                    futures.append(executor.submit(self.tree_process, item))

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error in thread execution: {e}")

    def validate_qml_paths(self):
        """
        Validates that user-provided QML paths exist.
        """
        for key, qml_path in self.user_qml_overrides.items():
            qml_path = Path(qml_path)
            if qml_path.is_absolute():
                if not qml_path.exists():
                    logger.warning(
                        f"User-provided QML path for '{key}' does not exist: {qml_path}"
                    )

    def apply_styles(self):
        """
        Main function to apply QML styles to QGIS results.
        """
        # Validate QML paths only for user overrides
        if self.user_qml_overrides:
            self.validate_qml_paths()

        # Set current path to the script's directory or the desired processing directory
        current_path = Path.cwd()
        logger.info(
            f"Current working directory: {current_path}", extra={"simple_format": True}
        )
        logger.debug("File mappings:")
        logger.debug(self.mappings)

        # Start processing
        try:
            self.tree_process(current_path)
        except Exception as e:
            logger.critical(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    # Initialize the LoggerConfigurator
    from ryan_library.functions.logging_helpers import LoggerConfigurator

    logger_config = LoggerConfigurator(
        log_level=logging.INFO,
        log_file=None,
        use_rotating_file=False,
        enable_color=True,
    )
    logger_config.configure()

    # Get the logger after configuring logging
    logger = logging.getLogger(__name__)
    logger.info("Starting TUFLOW Results Styling...", extra={"simple_format": True})

    # Initialize and apply styles
    styler = TUFLOWResultsStyler()
    styler.apply_styles()
