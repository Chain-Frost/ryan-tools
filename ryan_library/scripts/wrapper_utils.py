"""Utility functions shared by wrapper scripts."""

from argparse import ArgumentParser, Namespace
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Sequence
from importlib.metadata import PackageNotFoundError, version


@dataclass(slots=True)
class CommonWrapperOptions:
    """Container for CLI-provided overrides that most wrappers share."""

    console_log_level: str | None = None
    locations_to_include: tuple[str, ...] | None = None
    working_directory: Path | None = None


def change_working_directory(target_dir: Path) -> bool:
    """Change the working directory and handle failures."""
    try:
        os.chdir(target_dir)
        print(f"Current Working Directory: {Path.cwd()}")
    except OSError as exc:
        print(f"Failed to change working directory to {target_dir}: {exc}")
        os.system("PAUSE")
        return False
    return True


def print_library_version(package_name: str = "ryan_functions") -> None:
    """Display the installed version of *package_name* if available."""
    try:
        print(f"{package_name} version: {version(package_name)}")
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


def parse_common_cli_arguments(args: Namespace) -> CommonWrapperOptions:
    """Map argparse results to :class:`CommonWrapperOptions`."""
    locations_argument = getattr(args, "locations", None)
    return CommonWrapperOptions(
        console_log_level=getattr(args, "console_log_level", None),
        locations_to_include=_coerce_locations_argument(raw_locations=locations_argument),
        working_directory=getattr(args, "working_directory", None),
    )


def _coerce_locations_argument(
    raw_locations: Sequence[str] | None,
) -> tuple[str, ...] | None:
    if not raw_locations:
        return None
    normalized: tuple[str, ...] = tuple(location.strip() for location in raw_locations if location.strip())
    return normalized or None
