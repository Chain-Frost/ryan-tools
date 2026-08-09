"""
Wrapper Script: TUFLOW Culverts to HY-8.

Converts TUFLOW culvert maximums exports into an HY-8 project file.

Note:
    This bridge has a known limitation where it primarily supports mapping
    circular ("C") and rectangular ("R") box culverts. Other shapes may require
    manual adjustment in HY-8 after export.
"""

from __future__ import annotations

from pathlib import Path
import argparse
from loguru import logger
import pandas as pd

WRAPPER_VERSION = "2026-08-09.1"

CONSOLE_LOG_LEVEL = "INFO"
WORKING_DIR: Path = Path(__file__).absolute().parent

from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.functions.loguru_helpers import configure_serial_logging
from ryan_library.functions.hy8.run_hy8_bridge import Hy8Project, maximums_dataframe_to_project


def main(
    input_csv: Path,
    output_hy8: Path,
    project_title: str,
    *,
    console_log_level: str | None = None,
    working_directory: Path | None = None,
) -> int:
    """
    Main entry point for converting TUFLOW culverts to HY-8.

    Args:
        input_csv: Path to the TUFLOW culvert maximums CSV export.
        output_hy8: Path to write the .hy8 project file.
        project_title: The title of the HY-8 project.
        console_log_level: Overrides the CONSOLE_LOG_LEVEL constant.
        working_directory: Overrides the default WORKING_DIR.
    """
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    script_directory: Path = working_directory or WORKING_DIR

    if not change_working_directory(target_dir=script_directory):
        return 1

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL

    configure_serial_logging(
        console_log_level=effective_console_log_level,
        log_file=str(output_hy8.with_suffix(".log")),
    )

    try:
        logger.info(f"Reading TUFLOW culvert maximums from {input_csv}...")
        df = pd.read_csv(input_csv)

        logger.info("Converting DataFrame to HY-8 project...")
        hy8_project: Hy8Project = maximums_dataframe_to_project(
            maximums=df,
            project_title=project_title,
        )

        logger.info(f"Saving HY-8 project to {output_hy8}...")
        hy8_project.save(output_hy8)
        logger.success(f"Successfully generated {output_hy8}")

    except Exception:
        logger.exception("Failed to convert TUFLOW culverts to HY-8.")
        return 1

    return 0


def _parse_cli_arguments() -> tuple[argparse.Namespace, CommonWrapperOptions]:
    parser = argparse.ArgumentParser(description="Convert TUFLOW culvert maximums into an HY-8 project.")

    parser.add_argument(
        "-i",
        "--input-csv",
        type=Path,
        required=True,
        help="Path to the TUFLOW culvert maximums CSV.",
    )
    parser.add_argument(
        "-o",
        "--output-hy8",
        type=Path,
        required=True,
        help="Path to save the generated .hy8 project file.",
    )
    parser.add_argument(
        "-t",
        "--title",
        type=str,
        default="TUFLOW Culverts Export",
        help="Title of the HY-8 project.",
    )

    add_common_cli_arguments(parser=parser)
    args: argparse.Namespace = parser.parse_args()
    return args, parse_common_cli_arguments(args=args)


if __name__ == "__main__":
    args, common_options = _parse_cli_arguments()
    result: int = main(
        input_csv=args.input_csv,
        output_hy8=args.output_hy8,
        project_title=args.title,
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
