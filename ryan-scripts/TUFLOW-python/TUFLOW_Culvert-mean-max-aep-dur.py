# ryan-scripts\TUFLOW-python\TUFLOW_Culvert-mean-max-aep-dur.py
from pathlib import Path

CONSOLE_LOG_LEVEL = "INFO"  # or "DEBUG"
# Toggle the specific culvert data types to collect. Leave empty to accept the library defaults.
INCLUDED_DATA_TYPES: tuple[str, ...] = ("Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF")
# Toggle the export of the raw culvert-maximums sheet (may be extremely large).
EXPORT_RAW_MAXIMUMS: bool = True
# Change the working directory
WORKING_DIR: Path = Path(__file__).absolute().parent
# WORKING_DIR: Path = Path(r"E:\path\to\custom\directory")

import argparse
import gc
import os

from ryan_library.scripts.tuflow.tuflow_culverts_mean import run_culvert_mean_report
from ryan_library.scripts.wrapper_utils import (
    CommonWrapperOptions,
    add_common_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    print_library_version,
)


def main(
    *,
    console_log_level: str | None = None,
    include_data_types: tuple[str, ...] | None = None,
    locations_to_include: tuple[str, ...] | None = None,
    working_directory: Path | None = None,
) -> None:
    """Wrapper script to create culvert mean peak reports."""

    print_library_version()
    script_directory: Path = working_directory or WORKING_DIR

    if not change_working_directory(target_dir=script_directory):
        return

    to_include_data_types: tuple[str, ...] | None = include_data_types or INCLUDED_DATA_TYPES or None
    effective_console_log_level: str = console_log_level or CONSOLE_LOG_LEVEL
    effective_locations: tuple[str, ...] | None = locations_to_include

    run_culvert_mean_report(
        script_directory=script_directory,
        log_level=effective_console_log_level,
        include_data_types=to_include_data_types,
        locations_to_include=effective_locations,
        export_raw=EXPORT_RAW_MAXIMUMS,
    )

    print()
    print_library_version()


def _parse_cli_arguments() -> CommonWrapperOptions:
    parser = argparse.ArgumentParser(
        description="Create culvert mean peak reports. Command-line options override the script defaults."
    )
    add_common_cli_arguments(parser=parser)
    args: argparse.Namespace = parser.parse_args()
    return parse_common_cli_arguments(args=args)


if __name__ == "__main__":
    common_options: CommonWrapperOptions = _parse_cli_arguments()
    main(
        console_log_level=common_options.console_log_level,
        include_data_types=common_options.data_types,
        locations_to_include=common_options.locations_to_include,
        working_directory=common_options.working_directory,
    )
    gc.collect()
    os.system("PAUSE")
