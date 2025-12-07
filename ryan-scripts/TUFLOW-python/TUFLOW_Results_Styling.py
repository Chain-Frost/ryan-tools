# ryan-scripts\TUFLOW-python\TUFLOW_Results_Styling.py
"""
Wrapper Script: TUFLOW Results Styling.

This script acts as a mutable wrapper for `ryan_library.scripts.tuflow.tuflow_results_styling`.
It applies QGIS styles (.qml) to TUFLOW results (rasters/vectors) found in the target directory.
Users can define custom QML overrides in the `user_qml_overrides` dictionary within this file.
"""

from pathlib import Path
import gc
import os
import sys
from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.scripts.wrapper_utils import (
    change_working_directory,
    print_library_version,
)

# Now import the TUFLOWResultsStyler class
from ryan_library.scripts.tuflow.tuflow_results_styling import TUFLOWResultsStyler

# User Overrides: Define your custom QML paths here
user_qml_overrides: dict[str, str] = {
    # "d_Max": "/path/to/custom/depth_for_legend_max2m.qml",
    # "h_Max": "/path/to/custom/hmax.qml",
    # Add other overrides as needed
}


def main() -> None:
    """
    Main entry point for the TUFLOW Results Styling script.

    This function sets the working directory to the script's location (or CWD),
    initializes the logger, and applies the configured styles using `TUFLOWResultsStyler`.
    It handles basic error logging and keeps the console window open upon completion/error.
    """
    try:
        with setup_logger(console_log_level="INFO"):
            # Set working directory to the location of the script
            # In some execution contexts (like frozen executables), __file__ might not exist
            # Fallback to CWD is a safe default for a wrapper script running in-place
            script_location = Path(__file__).parent if "__file__" in globals() else Path.cwd()

            if not change_working_directory(target_dir=script_location):
                return

            # Initialize and apply styles
            styler = TUFLOWResultsStyler(user_qml_overrides=user_qml_overrides)
            styler.apply_styles()

            logger.error(f"Styles were sourced from: {styler.default_styles_path}")
            print()
            print_library_version()
            gc.collect()
            os.system("PAUSE")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        gc.collect()
        os.system("PAUSE")
        sys.exit(1)


if __name__ == "__main__":
    main()
