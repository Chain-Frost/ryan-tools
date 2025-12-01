# ryan-scripts\\TUFLOW-python\\TUFLOW-find-closure-durations.py
"""Wrapper for computing closure durations from TUFLOW PO files.

Update ``paths`` or ``thresholds`` below to customise processing.
``data_type`` defaults to ``"Flow"`` and ``allowed_locations`` can
restrict which locations are loaded from the CSV files.
"""

# TODO. expand this to work on a variety of file formats and data types.
# Ultimately, we are just looking at exceedance duration of some form so should not be that hard.

import argparse
import gc
import os
from pathlib import Path

from ryan_library.scripts.tuflow.closure_durations import run_closure_durations
from ryan_library.scripts.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    print_library_version,
)

CONSOLE_LOG_LEVEL = "INFO"
LOCATIONS_TO_INCLUDE: tuple[str, ...] = ()
WORKING_DIR: Path = Path(__file__).absolute().parent
# WORKING_DIR: Path = Path(r"E:\path\to\custom\directory")


def main(
    *,
    console_log_level: str | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    working_directory: Path | None = None,
) -> None:
    print_library_version()
    script_directory: Path = working_directory or WORKING_DIR
    if not change_working_directory(target_dir=script_directory):
        return

    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_locations: tuple[str, ...] | None = (
        locations_to_include if locations_to_include else (LOCATIONS_TO_INCLUDE or None)
    )
    run_closure_durations(
        paths=[script_directory],
        thresholds=None,
        data_type="Flow",
        allowed_locations=effective_locations,
        log_level=effective_console_log_level,
    )
    print()
    print_library_version()


def _parse_cli_arguments() -> CommonWrapperOptions:
    parser = argparse.ArgumentParser(
        description="Compute closure durations from PO files. Command-line options override the script defaults."
    )
    add_common_cli_arguments(parser=parser)
    args: argparse.Namespace = parser.parse_args()
    return parse_common_cli_arguments(args=args)


if __name__ == "__main__":
    common_options: CommonWrapperOptions = _parse_cli_arguments()
    main(
        console_log_level=common_options.console_log_level,
        locations_to_include=common_options.locations_to_include,
        working_directory=common_options.working_directory,
    )
    gc.collect()
    os.system("PAUSE")
