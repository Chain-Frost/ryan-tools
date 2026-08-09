# ryan_library/functions/wrapper_utils.py
"""Utility functions shared by wrapper scripts."""

from argparse import ArgumentParser, Namespace
from collections.abc import Collection
from dataclasses import dataclass
from importlib.metadata import PackageNotFoundError, version
import os
from pathlib import Path
import subprocess
import sys
from typing import Protocol, Sequence
import warnings


@dataclass(slots=True)
class CommonWrapperOptions:
    """Container for CLI-provided overrides that most wrappers share."""

    console_log_level: str | None = None
    data_types: tuple[str, ...] | None = None
    live_max_rows: int | None = None
    live_refresh_per_second: float | None = None
    locations_to_include: tuple[str, ...] | None = None
    no_pause: bool = False
    paths_to_process: tuple[Path, ...] | None = None
    use_live_dashboard: bool | None = None
    working_directory: Path | None = None


def pause_console(message: str = "Press Enter to continue . . .", *, collect_before_pause: bool = True) -> None:
    """Pause an interactive console, optionally collecting garbage first.

    Collection occurs only when standard input is interactive and the function
    is actually about to wait. This can release unreachable cyclic objects
    while a completed wrapper remains open for a user to read its output,
    without adding work to headless runs.
    """
    if not sys.stdin.isatty():
        return
    if collect_before_pause:
        # Import locally because automated runs return above and do not need it.
        import gc

        gc.collect()
    if os.name == "nt":
        subprocess.run(["cmd.exe", "/C", "PAUSE"], check=False)
        return
    try:
        input(message)
    except EOFError:
        pass


def change_working_directory(target_dir: Path) -> bool:
    """Change the working directory, returning whether the operation succeeded.

    This helper never pauses or terminates the process. User-facing wrappers
    remain responsible for their own interactive pause and process exit code.
    """
    try:
        os.chdir(target_dir)
        print(f"Current Working Directory: {Path.cwd()}")
    except OSError as exc:
        print(f"Failed to change working directory to {target_dir}: {exc}")
        return False
    return True


def print_library_version(package_name: str = "ryan_functions") -> None:
    """Display the installed version of *package_name* if available."""
    try:
        print(f"{package_name} version: {version(distribution_name=package_name)}")
    except PackageNotFoundError:
        print(f"{package_name} version: unknown")


def print_wrapper_identity(*, wrapper_file: Path, wrapper_version: str) -> None:
    """Display the running wrapper path and its embedded revision.

    The revision is stored in the wrapper itself, so it remains available when
    that file is copied away from the repository and loses its Git history.
    """
    print(f"Wrapper: {wrapper_file.resolve()} (version {wrapper_version})")


def print_wrapper_banner(
    *,
    wrapper_file: Path,
    wrapper_version: str,
    leading_blank_line: bool = False,
    package_name: str = "ryan_functions",
) -> None:
    """Display the wrapper identity and installed library version together.

    Args:
        wrapper_file: ``__file__`` from the calling wrapper.
        wrapper_version: Embedded revision from the calling wrapper.
        leading_blank_line: Print a blank separator before the banner. Use this
            for the closing banner after ``main()`` returns.
        package_name: Installed distribution whose version should be displayed.
    """
    if leading_blank_line:
        print()
    print_wrapper_identity(wrapper_file=wrapper_file, wrapper_version=wrapper_version)
    print_library_version(package_name=package_name)


def add_common_cli_arguments(parser: ArgumentParser) -> None:
    """Add execution, filtering, and live-dashboard argument groups."""
    add_execution_cli_arguments(parser=parser)
    add_filter_cli_arguments(parser=parser)
    add_live_dashboard_cli_arguments(parser=parser)


def add_execution_cli_arguments(parser: ArgumentParser) -> None:
    """Add working-directory, logging, and pause controls used by wrappers."""
    parser.add_argument(
        "--console-log-level",
        dest="console_log_level",
        help=(
            "Set log verbosity (e.g., DEBUG, INFO, or SUCCESS). SUCCESS keeps completions, warnings, and errors "
            "while suppressing routine progress. Defaults to the script value."
        ),
    )
    parser.add_argument(
        "--working-directory",
        type=Path,
        help="Directory to process instead of the script's location.",
    )
    add_no_pause_cli_argument(parser=parser)


def add_filter_cli_arguments(parser: ArgumentParser) -> None:
    """Add data-type and location filters shared by result-processing wrappers."""
    parser.add_argument(
        "--data-types",
        nargs="+",
        metavar="TYPE",
        help="Override the data types to load (e.g., POMM RLL_Qmx). Defaults to the script value.",
    )
    parser.add_argument(
        "--locations",
        nargs="+",
        metavar="LOCATION",
        help="Limit processing to one or more PO/Location/Channel identifiers.",
    )


def add_live_dashboard_cli_arguments(parser: ArgumentParser) -> None:
    """Add Rich live-dashboard controls used by long-running wrappers."""
    live_group = parser.add_mutually_exclusive_group()
    live_group.add_argument(
        "--live-dashboard",
        action="store_const",
        const=True,
        default=None,
        dest="use_live_dashboard",
        help="Enable the Rich live status dashboard for wrappers that support it.",
    )
    live_group.add_argument(
        "--no-live-dashboard",
        action="store_const",
        const=False,
        default=None,
        dest="use_live_dashboard",
        help="Disable the Rich live status dashboard for wrappers that support it.",
    )
    parser.add_argument(
        "--live-refresh-per-second",
        type=float,
        help="Override the Rich live dashboard refresh rate for wrappers that support it.",
    )
    parser.add_argument(
        "--live-max-rows",
        type=int,
        help="Override the maximum live dashboard rows for wrappers that support it.",
    )


def add_no_pause_cli_argument(parser: ArgumentParser) -> None:
    """Add the standard automation-friendly switch for skipping console pauses."""
    parser.add_argument(
        "--no-pause",
        action="store_true",
        help="Exit immediately after processing instead of waiting for interactive input.",
    )


def add_export_mode_cli_argument(parser: ArgumentParser) -> None:
    """Add the common Excel/Parquet output selection used by TUFLOW wrappers."""
    parser.add_argument(
        "--export-mode",
        choices=("excel", "parquet", "both"),
        help="Select the export format. Defaults to the editable wrapper value.",
    )


def parse_common_cli_arguments(args: Namespace) -> CommonWrapperOptions:
    """Map argparse results to :class:`CommonWrapperOptions`."""
    locations_argument = getattr(args, "locations", None)
    data_types_argument = getattr(args, "data_types", None)
    return CommonWrapperOptions(
        console_log_level=getattr(args, "console_log_level", None),
        data_types=_coerce_sequence_argument(raw_values=data_types_argument),
        live_max_rows=getattr(args, "live_max_rows", None),
        live_refresh_per_second=getattr(args, "live_refresh_per_second", None),
        locations_to_include=_coerce_locations_argument(raw_locations=locations_argument),
        no_pause=bool(getattr(args, "no_pause", False)),
        use_live_dashboard=getattr(args, "use_live_dashboard", None),
        working_directory=getattr(args, "working_directory", None),
    )


def _coerce_locations_argument(
    raw_locations: Sequence[str] | None,
) -> tuple[str, ...] | None:
    return _coerce_sequence_argument(raw_values=raw_locations)


def _coerce_sequence_argument(raw_values: Sequence[str] | None) -> tuple[str, ...] | None:
    if not raw_values:
        return None
    normalized: tuple[str, ...] = tuple(value.strip() for value in raw_values if value.strip())
    return normalized or None


# Deprecated POMM compatibility API. Maintained wrappers call the POMM
# orchestrator directly; these names remain available for copied older wrappers
# until 31 December 2026.
_POMM_WRAPPER_COMPATIBILITY_END = "31 December 2026"


def _warn_deprecated_pomm_wrapper_api(api_name: str) -> None:
    warnings.warn(
        (
            f"ryan_library.functions.wrapper_utils.{api_name} is deprecated. "
            "Maintained POMM wrappers now call "
            "ryan_library.orchestrators.tuflow.pomm_max_items directly. "
            f"Backwards compatibility is supported until {_POMM_WRAPPER_COMPATIBILITY_END}."
        ),
        DeprecationWarning,
        stacklevel=3,
    )


class PeakReportExporter(Protocol):
    """Legacy callable signature retained for older POMM wrappers.

    .. deprecated:: 2026-12-31
       Maintained wrappers call the POMM orchestrator directly.
    """

    def __call__(
        self,
        *,
        script_directory: Path,
        paths_to_process: Sequence[Path] | None,
        log_level: str,
        include_pomm: bool,
        locations_to_include: Collection[str] | None,
        include_data_types: Collection[str] | None,
    ) -> None: ...


@dataclass(slots=True, frozen=True)
class PommPeakWrapperDefaults:
    """Legacy default container retained for older POMM wrappers.

    .. deprecated:: 2026-12-31
       Keep editable defaults in each maintained wrapper.
    """

    console_log_level: str
    include_pomm: bool
    include_data_types: tuple[str, ...]
    locations_to_include: tuple[str, ...]
    paths_to_process: tuple[Path, ...]
    working_directory: Path

    def __post_init__(self) -> None:
        _warn_deprecated_pomm_wrapper_api(api_name=type(self).__name__)


def run_pomm_peak_report_wrapper(
    *,
    exporter: PeakReportExporter,
    defaults: PommPeakWrapperDefaults,
    overrides: CommonWrapperOptions,
) -> int:
    """Run an older POMM peak wrapper.

    .. deprecated:: 2026-12-31
       Maintained wrappers call the POMM orchestrator directly.
    """
    _warn_deprecated_pomm_wrapper_api(api_name=run_pomm_peak_report_wrapper.__name__)

    script_directory: Path = overrides.working_directory or defaults.working_directory
    if not change_working_directory(target_dir=script_directory):
        return 1

    effective_console_log_level: str = overrides.console_log_level or defaults.console_log_level
    effective_data_types: tuple[str, ...] | None = overrides.data_types or defaults.include_data_types or None
    effective_locations: tuple[str, ...] | None = (
        overrides.locations_to_include if overrides.locations_to_include else (defaults.locations_to_include or None)
    )
    effective_paths_to_process: tuple[Path, ...] | None = (
        overrides.paths_to_process or defaults.paths_to_process or None
    )

    exporter(
        script_directory=script_directory,
        paths_to_process=effective_paths_to_process,
        log_level=effective_console_log_level,
        include_pomm=defaults.include_pomm,
        locations_to_include=effective_locations,
        include_data_types=list(effective_data_types) if effective_data_types else None,
    )
    return 0
