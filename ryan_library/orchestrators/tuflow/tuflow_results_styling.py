# ryan_library/orchestrators/tuflow/tuflow_results_styling.py
"""
TUFLOW Results Styling.

This module automates the application of QGIS layer styles (.qml) to TUFLOW result files (e.g., raster .flt/.tif,
vector .shp/.gpkg). It recursively scans a directory and copies the appropriate .qml file next to the result file
based on filename matching (e.g. *_d_Max.flt gets depth_for_legend_max2m.qml).
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
from pathlib import Path
from typing import Any, TypedDict

from loguru import logger


class BaseMappingEntry(TypedDict):
    """
    Base type for style mapping configuration.

    Attributes:
        exts: List of file extensions to apply this style to (e.g. ["flt", "tif"]).
        qml: Path to the QML style file.
    """

    exts: list[str]
    qml: Path


class MappingEntry(BaseMappingEntry, total=False):
    """
    Extended mapping type allowing for optional layer name (for GPKG/database styles).
    """

    layer_name: str


class TUFLOWResultsStyler:
    """
    Handles the recursive scanning and application of QML styles to TUFLOW results.
    """

    def __init__(self, user_qml_overrides: dict[str, str] | None = None) -> None:
        """Initializes the TUFLOWResultsStyler with default styles path and user overrides."""
        # __file__ resolves to ryan_library/orchestrators/tuflow/tuflow_results_styling.py
        # We need the repository root to locate QML files under QGIS-Styles/TUFLOW
        self.default_styles_path: Path = Path(__file__).absolute().parents[3] / "QGIS-Styles" / "TUFLOW"
        logger.debug("Default styles path: {}", self.default_styles_path)
        self.user_qml_overrides: dict[str, str] = user_qml_overrides or {}
        self.mappings: dict[str, MappingEntry] = self.get_file_mappings()

    def get_file_mappings(self) -> dict[str, MappingEntry]:
        """Returns a mapping of file keys to their extensions and QML paths.
        Allows user to override default QML paths."""
        raster_exts: list[str] = ["flt", "tif"]
        vector_exts: list[str] = ["shp", "gpkg"]

        mapping_dict: dict[str, MappingEntry] = {
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
                "qml": self.default_styles_path / "velocities scour protection mrwa.qml",
            },
            "DEM_Z": {
                "exts": raster_exts,
                "qml": self.default_styles_path / "hillshade.qml",
            },
            # "1d_ccA_L": {
            #     "exts": vector_exts,
            #     "qml": self.default_styles_path / "_1d_ccA.qml",
            # },
            # appears tuflow has a default now
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
        for key, qml_path in self.user_qml_overrides.items():
            if key in mapping_dict:
                mapping_dict[key]["qml"] = Path(qml_path)

        return mapping_dict

    def get_qml_content(self, qml_path: Path) -> str:
        """Retrieves the content of a QML file, either from user-provided path or default styles path."""
        if qml_path.is_absolute() and qml_path.exists():
            logger.debug("Loading QML from user path: {}", qml_path)
            with qml_path.open("r", encoding="utf-8") as file:
                return file.read()
        else:
            full_qml_path = self.default_styles_path / qml_path.name
            if full_qml_path.exists():
                logger.debug("Loading QML from default styles path: {}", full_qml_path)
                with full_qml_path.open("r", encoding="utf-8") as file:
                    return file.read()
            else:
                logger.error(f"QML file '{qml_path.name}' not found in default styles path.")
                return ""

    def process_data(self, filename: str, ext: str, current_path: Path, qml_path: Path) -> None:
        """Processes raster and vector data by applying the QML style."""
        try:
            source_file: Path = current_path / filename
            new_filename: str = f"{source_file.stem}.{ext}"
            destination: Path = current_path / new_filename
            logger.info(f"Copying {source_file} to {destination}")

            # Retrieve QML content
            qml_content: str = self.get_qml_content(qml_path)
            if not qml_content:
                logger.warning(f"No QML content for {filename}. Skipping.")
                return

            # Write QML content to destination
            with destination.open("w", encoding="utf-8") as f:
                f.write(qml_content)

        except Exception as e:
            logger.error(f"Error processing data for {filename}: {e}")

    def process_gpkg(self, filename: str, layer_name: str, current_path: Path, qml_path: Path) -> None:
        """
        Processes GeoPackage files by applying styles to specific layers.

        Currently a placeholder implementation pending future development (SQL logic).
        """
        # not implemented
        try:
            gpkg_path: Path = current_path / filename
            logger.info(f"Processing GeoPackage: {gpkg_path}")

            conn: sqlite3.Connection = sqlite3.connect(gpkg_path)
            cursor: sqlite3.Cursor = conn.cursor()

            cursor.execute(
                "SELECT styleName, styleQML FROM layer_styles WHERE f_table_name = ?;",
                (layer_name,),
            )
            styles: list[tuple[str, str]] = cursor.fetchall()

            for style_name, _style_qml in styles:
                logger.info(
                    f"Applying style '{style_name}' to layer '{layer_name}'",
                )
                # Implement style application logic here
                # For example, you could update the styleQML in the database or apply it using QGIS APIs

            conn.close()
        except Exception as e:
            logger.error(f"Error processing GeoPackage {filename}: {e}")

    def tree_process(self, current_path: Path) -> None:
        """Recursively processes directories to apply QML styles based on file mappings."""
        logger.info(f"Processing directory: {current_path}")
        with ThreadPoolExecutor() as executor:
            futures: list[Any] = []
            for item in current_path.iterdir():
                if item.is_file():
                    for key, value in self.mappings.items():
                        for ext in value["exts"]:  # Now recognized as list[str]
                            if item.name.lower().endswith(f"{key.lower()}.{ext.lower()}"):
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

    def validate_qml_paths(self) -> None:
        """Validates that user-provided QML paths exist."""
        for key, qml_path_str in self.user_qml_overrides.items():
            qml_path: Path = Path(qml_path_str)
            if qml_path.is_absolute() and not qml_path.exists():
                logger.warning(f"User-provided QML path for '{key}' does not exist: {qml_path}")

    def apply_styles(self) -> None:
        """Main function to apply QML styles to QGIS results."""
        # Validate QML paths only for user overrides
        if self.user_qml_overrides:
            self.validate_qml_paths()

        # Set current path to the script's directory or the desired processing directory
        current_path = Path.cwd()
        logger.info(f"Current working directory: {current_path}")
        logger.debug("File mappings:")
        logger.debug(self.mappings)

        # Start processing
        try:
            self.tree_process(current_path=current_path)
        except Exception as e:
            logger.critical(f"An unexpected error occurred: {e}")


def main() -> None:
    """Wrapper entry point for direct execution."""
    from ryan_library.functions.loguru_helpers import is_loguru_configured, setup_logger

    # If the caller already configured Loguru, reuse it instead of reconfiguring.
    if is_loguru_configured():
        logger.info("Starting TUFLOW Results Styling...")
        styler = TUFLOWResultsStyler()
        styler.apply_styles()
        return

    with setup_logger(console_log_level="INFO"):
        logger.info("Starting TUFLOW Results Styling...")
        styler = TUFLOWResultsStyler()
        styler.apply_styles()


if __name__ == "__main__":
    main()
