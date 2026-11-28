# ryan_library/functions/wrapper_utils.py
"""Utility functions shared by wrapper scripts."""

import ast
import json
import os
import re
import sys
from argparse import ArgumentParser, Namespace
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files
from pathlib import Path
from typing import cast


@dataclass(slots=True)
class CommonWrapperOptions:
    """Container for CLI-provided overrides that most wrappers share."""

    console_log_level: str | None = None
    data_types: tuple[str, ...] | None = None
    file_log_level: str | None = None
    log_file: Path | None = None
    live_max_rows: int | None = None
    live_refresh_per_second: float | None = None
    locations_to_include: tuple[str, ...] | None = None
    no_pause: bool = False
    pause_on_finish: bool | None = None
    paths_to_process: tuple[Path, ...] | None = None
    use_live_dashboard: bool | None = None
    working_directory: Path | None = None


def _pause_windows_console() -> None:
    """Wait for a console keystroke without starting CMD.EXE from a UNC directory."""
    import msvcrt

    print("Press any key to continue . . .", end="", flush=True)
    key = msvcrt.getwch()
    if key in ("\x00", "\xe0"):
        # Arrow/function keys emit a prefix plus a second key code.
        msvcrt.getwch()
    print()


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
        _pause_windows_console()
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
    requested_path = target_dir.absolute()
    try:
        os.chdir(requested_path)
        # Report the requested spelling, not a filesystem-normalised UNC alias.
        print(f"Current Working Directory: {requested_path}")
    except OSError as exc:
        print(f"Failed to change working directory to {target_dir}: {exc}")
        return False
    return True


def print_library_version(package_name: str = "ryan-tools") -> None:
    """Display the installed version of *package_name* if available."""
    try:
        print(f"{package_name} version: {version(distribution_name=package_name)}")
    except PackageNotFoundError:
        print(f"{package_name} version: unknown")


def print_wrapper_identity(*, wrapper_file: Path, wrapper_version: str, display_base: Path | None = None) -> None:
    """Display the running wrapper path and its embedded revision.

    The revision is stored in the wrapper itself, so it remains available when
    that file is copied away from the repository and loses its Git history.
    """
    display_path = wrapper_file.absolute()
    if display_base is not None:
        try:
            display_path = display_path.relative_to(display_base.absolute())
        except ValueError:
            pass
    print(f"Wrapper: {display_path} (version {wrapper_version})")


def print_wrapper_banner(
    *,
    wrapper_file: Path | None = None,
    wrapper_version: str | None = None,
    leading_blank_line: bool = False,
    package_name: str = "ryan-tools",
    wrapper_source: str | None = None,
    display_base: Path | None = None,
) -> None:
    """Display the wrapper identity and installed library version together.

    Args:
        wrapper_file: ``__file__`` from the calling wrapper. Legacy calls that
            omit this fall back to the process entry point and print a warning.
        wrapper_version: Embedded revision from the calling wrapper. Missing or
            unsupported revisions print a warning without blocking execution.
        leading_blank_line: Print a blank separator before the banner. Use this
            for the closing banner after ``main()`` returns.
        package_name: Installed distribution whose version should be displayed.
        wrapper_source: Original path relative to ``ryan-scripts``. Supply this
            for renamed copies; otherwise a unique installed filename is used.
        display_base: Optionally display the wrapper relative to this directory.
    """
    if leading_blank_line:
        print()
    actual_file = wrapper_file if isinstance(wrapper_file, Path) else Path(sys.argv[0])
    actual_version = wrapper_version if isinstance(wrapper_version, str) else "unknown"
    print_wrapper_identity(wrapper_file=actual_file, wrapper_version=actual_version, display_base=display_base)
    print_library_version(package_name=package_name)
    if package_name == "ryan-tools":
        if wrapper_file is None or _wrapper_revision(actual_version) is None or not wrapper_source:
            print(
                "WARNING: This script uses an outdated banner interface or unsupported revision. "
                "Update the script to supply its original script path and embedded revision. "
                "Execution will continue."
            )
        _warn_if_wrapper_is_older(
            wrapper_file=actual_file, wrapper_version=actual_version, wrapper_source=wrapper_source
        )


def _wrapper_revision(value: str) -> tuple[date, int] | None:
    """Parse the maintained wrapper revision numerically, including its date."""
    match = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})\.([1-9]\d*)", value)
    if match is None:
        return None
    try:
        return date(int(match[1]), int(match[2]), int(match[3])), int(match[4])
    except ValueError:
        return None


def _warn_if_wrapper_is_older(*, wrapper_file: Path, wrapper_version: str, wrapper_source: str | None) -> None:
    """Compare allowlisted installed source without executing it or using a network."""
    current = _wrapper_revision(wrapper_version)
    if current is None:
        return
    try:
        resource = files("ryan_library.resources.mcp").joinpath("installed_scripts.json")
        metadata: object = json.loads(resource.read_text(encoding="utf-8"))
        if not isinstance(metadata, dict):
            return
        manifest = cast("dict[str, object]", metadata)
        if manifest.get("schema_version") != 1:
            return
        entries: object = manifest.get("installed_files")
        if not isinstance(entries, list):
            return
        candidates = [
            entry
            for entry in cast("list[object]", entries)
            if isinstance(entry, str)
            and entry.endswith(".py")
            and (entry == wrapper_source if wrapper_source is not None else Path(entry).name == wrapper_file.name)
        ]
        if len(candidates) != 1:
            return
        source = candidates[0]
        relative = Path(source)
        if relative.is_absolute() or ".." in relative.parts:
            return
        original = Path(__file__).absolute().parents[2] / "ryan-scripts" / relative
        latest_text = _read_wrapper_revision(original)
        latest = _wrapper_revision(latest_text) if latest_text is not None else None
        if latest is not None and current < latest:
            print(
                f"WARNING: This wrapper ({wrapper_version}) is older than the installed package's "
                f"{source} ({latest_text}). Copy the newer script and transfer your settings before using it."
            )
    except (OSError, UnicodeError, ValueError, SyntaxError, RecursionError):
        # A best-effort banner check must not prevent the wrapper from running.
        return


def _read_wrapper_revision(original: Path) -> str | None:
    """Read a literal script revision without importing a human-facing entry point."""
    tree = ast.parse(original.read_text(encoding="utf-8"))
    for statement in tree.body:
        if isinstance(statement, ast.Assign):
            targets = statement.targets
            value = statement.value
        elif isinstance(statement, ast.AnnAssign):
            targets = [statement.target]
            value = statement.value
        else:
            continue
        if any(
            isinstance(target, ast.Name) and target.id in {"WRAPPER_VERSION", "SCRIPT_VERSION"} for target in targets
        ):
            return value.value if isinstance(value, ast.Constant) and isinstance(value.value, str) else None
    return None


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
    raw_no_pause = getattr(args, "no_pause", None)
    return CommonWrapperOptions(
        console_log_level=getattr(args, "console_log_level", None),
        data_types=_coerce_sequence_argument(raw_values=data_types_argument),
        file_log_level=getattr(args, "file_log_level", None),
        log_file=getattr(args, "log_file", None),
        live_max_rows=getattr(args, "live_max_rows", None),
        live_refresh_per_second=getattr(args, "live_refresh_per_second", None),
        locations_to_include=_coerce_locations_argument(raw_locations=locations_argument),
        no_pause=bool(raw_no_pause),
        pause_on_finish=False if raw_no_pause else None,
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
