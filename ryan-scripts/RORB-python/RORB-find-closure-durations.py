r"""Calculate flow-threshold exceedance durations from RORB batch results.

The wrapper recursively discovers RORB ``batch.out`` files below each input
path, reads their referenced hydrographs, and calculates how long each location
exceeds each configured flow threshold. The working directory receives a
timestamped detailed CSV, a gzip-compressed detailed Parquet file, and a
summary ``QvsTexc`` CSV.

Edit the defaults below for normal double-click use, or override them from a
terminal. Leave ``THRESHOLDS`` as ``None`` to use the library's broad default
range.

Examples::

    py -3.14 RORB-find-closure-durations.py --working-directory "D:\RORB\results"
    py -3.14 RORB-find-closure-durations.py "D:\RORB\run1" "D:\RORB\run2" --thresholds 10 25 50
    py -3.14 RORB-find-closure-durations.py --working-directory "D:\RORB\results" --no-pause
"""

from pathlib import Path

WRAPPER_VERSION = "2026-08-02.1"

# Editable defaults for normal double-click or IDE execution.
WORKING_DIR: Path = Path(__file__).resolve().parent
PATHS_TO_PROCESS: tuple[Path, ...] = ()
THRESHOLDS: tuple[float, ...] | None = None
CONSOLE_LOG_LEVEL = "INFO"

import argparse

from ryan_library.functions.wrapper_utils import (
    CommonWrapperOptions,
    add_execution_cli_arguments,
    change_working_directory,
    parse_common_cli_arguments,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.rorb.closure_durations import run_closure_durations


def main(
    *,
    working_directory: Path | None = None,
    paths_to_process: tuple[Path, ...] | None = None,
    thresholds: tuple[float, ...] | None = None,
    console_log_level: str | None = None,
) -> int:
    """Resolve wrapper settings and run the shared RORB closure workflow.

    Args:
        working_directory: Output and process working directory. Defaults to
            ``WORKING_DIR``.
        paths_to_process: Folder roots searched recursively for ``batch.out``
            files. Defaults to ``PATHS_TO_PROCESS`` or the working directory.
        thresholds: Flow thresholds used for exceedance calculations. ``None``
            selects the library's default range.
        console_log_level: Loguru console level such as ``INFO`` or ``DEBUG``.

    Returns:
        ``0`` after the orchestrator completes, or ``1`` when the working
        directory cannot be selected.
    """
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)

    target_directory: Path = (working_directory or WORKING_DIR).resolve()
    configured_paths: tuple[Path, ...] | tuple[Path] = paths_to_process or PATHS_TO_PROCESS or (target_directory,)
    effective_paths: tuple[Path, ...] = tuple(path.resolve() for path in configured_paths)
    if not change_working_directory(target_dir=target_directory):
        return 1

    effective_thresholds: tuple[float, ...] | None = THRESHOLDS if thresholds is None else thresholds
    run_closure_durations(
        paths=effective_paths,
        thresholds=list(effective_thresholds) if effective_thresholds is not None else None,
        log_level=console_log_level or CONSOLE_LOG_LEVEL,
    )
    return 0


def _parse_cli_arguments() -> tuple[CommonWrapperOptions, tuple[Path, ...] | None, tuple[float, ...] | None]:
    """Parse CLI overrides while retaining editable defaults for omitted values."""
    parser = argparse.ArgumentParser(
        description="Calculate threshold exceedance durations from RORB batch.out hydrographs.",
        epilog=r"""Examples:
  py -3.14 RORB-find-closure-durations.py --working-directory "D:\RORB\results"
  py -3.14 RORB-find-closure-durations.py "D:\RORB\run1" "D:\RORB\run2" --thresholds 10 25 50""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help="Folder roots containing RORB batch.out results; default: working directory.",
    )
    add_execution_cli_arguments(parser=parser)
    parser.add_argument(
        "--thresholds",
        nargs="+",
        type=float,
        metavar="FLOW",
        help="Flow thresholds; default: THRESHOLDS or the library's generated range.",
    )
    args: argparse.Namespace = parser.parse_args()
    return (
        parse_common_cli_arguments(args=args),
        tuple(args.paths) if args.paths else None,
        tuple(args.thresholds) if args.thresholds else None,
    )


if __name__ == "__main__":
    common_options, cli_paths, cli_thresholds = _parse_cli_arguments()
    result: int = main(
        working_directory=common_options.working_directory,
        paths_to_process=cli_paths,
        thresholds=cli_thresholds,
        console_log_level=common_options.console_log_level,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not common_options.no_pause:
        pause_console()
    raise SystemExit(result)
