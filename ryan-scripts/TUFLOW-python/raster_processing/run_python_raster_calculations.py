"""Run experimental native-Python maximum, difference or statistical raster operations.

Edit the defaults below for ordinary copied-wrapper use. Command-line values
override those defaults when supplied.
"""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

from pathlib import Path
from typing import Literal, NamedTuple

WRAPPER_VERSION = "2026-08-20.1"

DEFAULT_WORKING_DIR = Path(".")
DEFAULT_OPERATION: Literal["max", "diff", "mean", "median", "min"] = "diff"
DEFAULT_INPUT_FILES: list[Path] = [Path("after.tif"), Path("before.tif")]
DEFAULT_OUTPUT_FILE = Path("difference.tif")
DEFAULT_CHANGE = False
DEFAULT_NO_WET_DRY = False
DEFAULT_MEAN_VALUE_METHOD: Literal["closest_source", "arithmetic"] = "closest_source"
DEFAULT_WRITE_SOURCE = False
DEFAULT_CREATION_OPTIONS: list[str] = []
DEFAULT_PAUSE = True

import argparse
import sys

from loguru import logger

from ryan_library.functions.tuflow.asc_to_asc_raster_operations import (
    MeanValueMethod,
    compute_diff,
    compute_max,
    compute_stat,
)
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


class WrapperConfiguration(NamedTuple):
    operation: str
    input_files: list[str]
    output_file: str
    change: bool
    no_wet_dry: bool
    mean_value_method: MeanValueMethod
    write_source: bool
    creation_options: list[str]
    pause: bool


def resolve_configuration(args: argparse.Namespace) -> WrapperConfiguration:
    """Apply explicit CLI overrides to the editable wrapper defaults."""
    if args.max:
        operation = "max"
    elif args.diff:
        operation = "diff"
    elif args.stat is not None:
        operation: str = str(args.stat).lower()
    else:
        operation = DEFAULT_OPERATION

    input_values: list[Path] = args.input_files if args.input_files else DEFAULT_INPUT_FILES
    output_value: Path = args.output if args.output is not None else DEFAULT_OUTPUT_FILE
    change: bool = args.change if args.change is not None else DEFAULT_CHANGE
    no_wet_dry: bool = args.no_wet_dry if args.no_wet_dry is not None else DEFAULT_NO_WET_DRY
    mean_value_method: MeanValueMethod = args.mean_value_method or DEFAULT_MEAN_VALUE_METHOD
    write_source: bool = args.source or DEFAULT_WRITE_SOURCE
    creation_options: list[str] = (
        args.creation_options if args.creation_options is not None else DEFAULT_CREATION_OPTIONS
    )
    pause: bool = DEFAULT_PAUSE and not args.no_pause
    return WrapperConfiguration(
        operation=operation,
        input_files=[str(Path(value)) for value in input_values],
        output_file=str(Path(output_value)),
        change=change,
        no_wet_dry=no_wet_dry,
        mean_value_method=mean_value_method,
        write_source=write_source,
        creation_options=list(creation_options),
        pause=pause,
    )


def main(args: argparse.Namespace, *, working_directory: Path | None = None) -> int:
    """Resolve configuration, run one raster operation and return a process exit code."""
    target_directory: Path = (working_directory or DEFAULT_WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    configuration: WrapperConfiguration = resolve_configuration(args)
    creation_arguments: list[str] = [item for option in configuration.creation_options for item in ("-co", option)]

    try:
        if configuration.operation == "max":
            compute_max(
                input_files=configuration.input_files,
                output_file=configuration.output_file,
                extra_args=creation_arguments,
            )
        elif configuration.operation == "diff":
            if len(configuration.input_files) != 2:
                logger.error("Difference mode requires exactly two input files.")
                return 2
            compute_diff(
                file1=configuration.input_files[0],
                file2=configuration.input_files[1],
                output_file=configuration.output_file,
                change=configuration.change,
                nowetdry=configuration.no_wet_dry,
                extra_args=creation_arguments,
            )
        elif configuration.operation in {"mean", "median", "min"}:
            compute_stat(
                stat_type=configuration.operation,
                input_files=configuration.input_files,
                output_file=configuration.output_file,
                extra_args=creation_arguments,
                mean_value_method=configuration.mean_value_method,
                write_source=configuration.write_source,
            )
        else:
            logger.error("Unsupported operation: {}", configuration.operation)
            return 2
    except Exception:
        logger.exception("Workflow failed.")
        return 1
    return 0


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Python replacement candidate for TUFLOW asc_to_asc")
    operations = parser.add_mutually_exclusive_group()
    operations.add_argument("-max", action="store_true", help="Find the maximum across grids.")
    operations.add_argument("-diff", action="store_true", help="Subtract the second grid from the first.")
    operations.add_argument("-stat", choices=["mean", "median", "min"], help="Calculate a cell-wise statistic.")
    parser.add_argument("-out", "--output", type=Path, help="Output file; overrides DEFAULT_OUTPUT_FILE.")
    parser.add_argument("-change", dest="change", action="store_const", const=True, default=None)
    parser.add_argument("--no-change", dest="change", action="store_const", const=False)
    parser.add_argument("-nowetdry", dest="no_wet_dry", action="store_const", const=True, default=None)
    parser.add_argument("--wet-dry", dest="no_wet_dry", action="store_const", const=False)
    parser.add_argument(
        "--mean-value-method",
        choices=["closest_source", "arithmetic"],
        help="For mean, return the nearest contributing value (default) or the numeric arithmetic mean.",
    )
    parser.add_argument(
        "--source",
        action="store_true",
        help="Write the statistic source raster and source legend.",
    )
    parser.add_argument(
        "-co",
        dest="creation_options",
        action="append",
        default=None,
        help="Raster creation option NAME=VALUE; repeat as required.",
    )
    parser.add_argument("-b", "--no-pause", action="store_true", help="Do not pause when processing finishes.")
    parser.add_argument("input_files", nargs="*", type=Path, help="Input files; override DEFAULT_INPUT_FILES.")

    values: list[str] = list(sys.argv[1:] if argv is None else argv)
    processed_values: list[str] = []
    for value in values:
        if value.lower().startswith("-stat") and len(value) > 5:
            processed_values.extend(["-stat", value[5:].lower()])
        else:
            processed_values.append(value)
    return parser.parse_args(processed_values)


if __name__ == "__main__":
    cli_args: argparse.Namespace = _parse_cli_arguments()
    result: int = main(args=cli_args)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if resolve_configuration(args=cli_args).pause:
        pause_console(collect_before_pause=True)
    raise SystemExit(result)
