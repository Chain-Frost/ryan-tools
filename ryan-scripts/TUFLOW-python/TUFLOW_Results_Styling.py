# ryan-scripts\TUFLOW-python\TUFLOW_Results_Styling.py
"""
Wrapper Script: TUFLOW Results Styling.

This script acts as a mutable wrapper for `ryan_library.orchestrators.tuflow.tuflow_results_styling`.
It applies QGIS styles (.qml) to TUFLOW results (rasters/vectors) found in the target directory.
Users can define custom QML overrides in the `user_qml_overrides` dictionary within this file.
"""

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.1"

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).resolve().parent

# User Overrides: Define your custom QML paths here
user_qml_overrides: dict[str, str] = {
    # "d_Max": "/path/to/custom/depth_for_legend_max2m.qml",
    # "h_Max": "/path/to/custom/hmax.qml",
    # Add other overrides as needed
}

import argparse

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_execution_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.tuflow.tuflow_results_styling import TUFLOWResultsStyler


def main(*, console_log_level: str | None = None, working_directory: Path | None = None) -> int:
    """
    Main entry point for the TUFLOW Results Styling script.

    This function sets the working directory to the script's location (or CWD),
    initializes the logger, and applies the configured styles using `TUFLOWResultsStyler`.
    It handles basic error logging and keeps the console window open upon completion/error.
    """
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        with setup_logger(console_log_level=console_log_level or CONSOLE_LOG_LEVEL):
            script_location: Path = working_directory or WORKING_DIR
            if not change_working_directory(target_dir=script_location):
                return 1

            # The orchestrator locates packaged styles, with a source-checkout fallback.
            styler = TUFLOWResultsStyler(user_qml_overrides=user_qml_overrides)
            styler.apply_styles()

            logger.error(f"Styles were sourced from: {styler.default_styles_path}")
            return 0

    except Exception:
        logger.exception("TUFLOW result styling failed.")
        return 1


def _parse_cli_arguments() -> CommonWrapperOptions:
    """Parse common execution overrides for the styling wrapper."""
    parser = argparse.ArgumentParser(description="Apply configured QGIS styles to TUFLOW result files.")
    add_execution_cli_arguments(parser=parser)
    return parse_common_cli_arguments(args=parser.parse_args())


if __name__ == "__main__":
    common_options: CommonWrapperOptions = _parse_cli_arguments()
    result: int = main(
        console_log_level=common_options.console_log_level,
        working_directory=common_options.working_directory,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not common_options.no_pause:
        pause_console()
    raise SystemExit(result)
