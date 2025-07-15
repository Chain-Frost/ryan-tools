from pathlib import Path
import os
import sys
import logging

# Import the LoggerConfigurator
from ryan_library.functions.logging_helpers import LoggerConfigurator

# Configure logging before importing the module that uses logging
logger_config = LoggerConfigurator(
    log_level=logging.INFO,
    log_file=None,
    use_rotating_file=False,
    enable_color=True,
)
logger_config.configure()

# Now import the TUFLOWResultsStyler class
from ryan_library.scripts.tuflow.tuflow_results_styling import TUFLOWResultsStyler

# Get the logger
logger = logging.getLogger(__name__)

# User Overrides: Define your custom QML paths here
user_qml_overrides: dict = {
    # "d_Max": "/path/to/custom/depth_for_legend_max2m.qml",
    # "h_Max": "/path/to/custom/hmax.qml",
    # Add other overrides as needed
}


def main() -> None:
    """
    Entry point for the TUFLOWResultsStyling script.
    Sets the working directory to the location of this script.
    """
    try:
        # Set working directory to the location of the script
        script_location = Path(__file__).parent if "__file__" in globals() else Path.cwd()
        os.chdir(script_location)

        # Initialize and apply styles
        styler = TUFLOWResultsStyler(user_qml_overrides=user_qml_overrides)
        styler.apply_styles()
        logger.error(f"Styles were sourced from: {styler.default_styles_path}")
        os.system("PAUSE")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        os.system("PAUSE")
        sys.exit(1)


if __name__ == "__main__":
    main()
