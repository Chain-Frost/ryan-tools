"""
Plots peak flows and exceedance durations for ensemble model results (like RORB).
"""

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-08-12.1"

# EDITABLE DEFAULTS
DEFAULT_INPUT = Path(".")
DEFAULT_OUTPUT_DIR = Path(".")
DEFAULT_LOCATIONS: list[str] | None = None
DEFAULT_CAPACITY_THRESHOLD: float | None = None
DEFAULT_SOURCE = "rorb"
# ==============================================================================

import argparse

from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import to_single_path
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner
from plot_ensemble_orchestrator import orchestrate_ensemble_plotting


def main(args: argparse.Namespace, *, working_directory: Path | None = None) -> int:
    target_directory = (working_directory or Path.cwd()).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    # Resolve parameters defensively
    target_input = to_single_path(args.input) if args.input is not None else Path(DEFAULT_INPUT).resolve()
    target_output = (
        to_single_path(args.output_dir) if args.output_dir is not None else Path(DEFAULT_OUTPUT_DIR).resolve()
    )

    target_capacity = args.capacity if args.capacity is not None else DEFAULT_CAPACITY_THRESHOLD
    target_source = args.source if args.source is not None else DEFAULT_SOURCE

    if not target_input.exists() or not target_input.is_file():
        logger.error("Input file does not exist or is not a file: {}", target_input)
        return 1

    target_output.mkdir(parents=True, exist_ok=True)

    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    try:
        orchestrate_ensemble_plotting(
            input_path=target_input,
            output_dir=target_output,
            locations=args.locations if args.locations is not None else DEFAULT_LOCATIONS,
            capacity_threshold=target_capacity,
            source=target_source,
        )
        logger.success("All plotting operations completed.")
        return 0
    except Exception as e:
        logger.exception("An error occurred during plotting: {}", e)
        return 1


def _parse_cli_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generates box-and-whisker plots for ensemble model outputs (e.g. RORB).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-i",
        "--input",
        type=str,
        help="Path to the input results file (e.g., merged RORB parquet).",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        help="Path to the output directory where plots will be saved.",
    )
    parser.add_argument(
        "-l",
        "--locations",
        nargs="+",
        type=str,
        help="Specific locations to plot (if omitted, all locations are processed).",
    )
    parser.add_argument(
        "-c",
        "--capacity",
        type=float,
        help="Capacity threshold (m3/s) for calculating closure times/exceedance durations.",
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["rorb"],
        default=None,
        help="The data source format.",
    )
    parser.add_argument(
        "--no-pause",
        action="store_true",
        help="Do not pause the console when finishing (useful for batch running).",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    cli_args = _parse_cli_arguments()
    with setup_logger(log_file="plot_ensemble_results.log"):
        return_code = main(cli_args)

    if not cli_args.no_pause:
        pause_console(collect_before_pause=True)
    raise SystemExit(return_code)
