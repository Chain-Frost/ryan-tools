"""Utility functions shared by wrapper scripts."""

from argparse import ArgumentParser, Namespace
from collections.abc import Collection
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Protocol, Sequence
from importlib.metadata import PackageNotFoundError, version


@dataclass(slots=True)
class CommonWrapperOptions:
    """Container for CLI-provided overrides that most wrappers share."""

    console_log_level: str | None = None
    data_types: tuple[str, ...] | None = None
    live_max_rows: int | None = None
    live_refresh_per_second: float | None = None
    locations_to_include: tuple[str, ...] | None = None
    paths_to_process: tuple[Path, ...] | None = None
    use_live_dashboard: bool | None = None
    working_directory: Path | None = None


class PeakReportExporter(Protocol):
    """Callable signature for POMM peak report exporters."""

    def __call__(
        self,
        *,
        script_directory: Path,
        paths_to_process: Collection[Path] | None,
        log_level: str,
        include_pomm: bool,
        locations_to_include: Collection[str] | None,
        include_data_types: Collection[str] | None,
    ) -> None: ...


@dataclass(slots=True, frozen=True)
class PommPeakWrapperDefaults:
    """Default configuration values for POMM peak report wrappers."""

    console_log_level: str
    include_pomm: bool
    include_data_types: tuple[str, ...]
    locations_to_include: tuple[str, ...]
    paths_to_process: tuple[Path, ...]
    working_directory: Path


def change_working_directory(target_dir: Path) -> bool:
    """Change the working directory and handle failures."""
    try:
        os.chdir(target_dir)
        print(f"Current Working Directory: {Path.cwd()}")
    except OSError as exc:
        print(f"Failed to change working directory to {target_dir}: {exc}")
        if os.name == "nt":
            os.system("PAUSE")
        return False
    return True


def print_library_version(package_name: str = "ryan_functions") -> None:
    """Display the installed version of *package_name* if available."""
    try:
        print(f"{package_name} version: {version(distribution_name=package_name)}")
    except PackageNotFoundError:
        print(f"{package_name} version: unknown")


def add_common_cli_arguments(parser: ArgumentParser) -> None:
    """Inject shared CLI arguments that most wrappers support."""
    parser.add_argument(
        "--console-log-level",
        dest="console_log_level",
        help="Set log verbosity (e.g., INFO or DEBUG). Defaults to the script value.",
    )
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
    parser.add_argument(
        "--working-directory",
        type=Path,
        help="Directory to process instead of the script's location.",
    )
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
        use_live_dashboard=getattr(args, "use_live_dashboard", None),
        working_directory=getattr(args, "working_directory", None),
    )


def run_pomm_peak_report_wrapper(
    *,
    exporter: PeakReportExporter,
    defaults: PommPeakWrapperDefaults,
    overrides: CommonWrapperOptions,
) -> None:
    """Run a POMM peak report wrapper using common defaults and CLI overrides."""
    print_library_version()

    script_directory: Path = overrides.working_directory or defaults.working_directory
    if not change_working_directory(target_dir=script_directory):
        return

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
    print()
    print_library_version()


def _coerce_locations_argument(
    raw_locations: Sequence[str] | None,
) -> tuple[str, ...] | None:
    return _coerce_sequence_argument(raw_values=raw_locations)


def _coerce_sequence_argument(raw_values: Sequence[str] | None) -> tuple[str, ...] | None:
    if not raw_values:
        return None
    normalized: tuple[str, ...] = tuple(value.strip() for value in raw_values if value.strip())
    return normalized or None
