r"""Wrapper Script: TUFLOW user, model, GPU, and CodeMeter licence activity.

This read-only wrapper checks recent per-user activity under
``%ProgramData%\TUFLOW\log``, inspects each referenced TLF independently, and
optionally reads the local CodeMeter WebAdmin service. It reports active licence
allocations, utilisation, recent denials, model paths, completion, runtime, and
GPU device IDs.

Examples:
    py -3.14 check_tuflow_user_activity.py --no-pause
    py -3.14 check_tuflow_user_activity.py --recent-hours 48 --no-pause
    py -3.14 check_tuflow_user_activity.py --no-codemeter --no-pause
"""

from __future__ import annotations

import os
from pathlib import Path

WRAPPER_VERSION = "2026-10-20.1"

DEFAULT_LOG_ROOT = Path(os.environ.get("ProgramData", r"C:\ProgramData")) / "TUFLOW" / "log"
DEFAULT_RECENT_HOURS = 24.0
DEFAULT_CODEMETER_URL = "http://localhost:22352"
DEFAULT_CODEMETER_TIMEOUT_SECONDS = 5.0
DEFAULT_CHECK_CODEMETER = True
DEFAULT_CONSOLE_LOG_LEVEL = "WARNING"

import argparse
import sys
from dataclasses import dataclass

from ryan_library.functions.loguru_helpers import configure_serial_logging
from ryan_library.functions.wrapper_utils import add_no_pause_cli_argument, pause_console, print_wrapper_banner
from ryan_library.orchestrators.tuflow.tuflow_user_activity import (
    MAX_RECENT_HOURS,
    build_activity_report,
    render_activity_report,
)


@dataclass(frozen=True, slots=True)
class WrapperOptions:
    """Resolved command-line overrides for the activity wrapper."""

    log_root: Path
    recent_hours: float
    codemeter_url: str | None
    codemeter_timeout_seconds: float
    console_log_level: str
    no_pause: bool


def _recent_hours(value: str) -> float:
    number = float(value)
    if number <= 0:
        message = "must be greater than zero"
        raise argparse.ArgumentTypeError(message)
    if number > MAX_RECENT_HOURS:
        message = f"must not exceed {MAX_RECENT_HOURS} hours (7 days)"
        raise argparse.ArgumentTypeError(message)
    return number


def _positive_float(value: str) -> float:
    number = float(value)
    if number <= 0:
        message = "must be greater than zero"
        raise argparse.ArgumentTypeError(message)
    return number


def main(
    *,
    log_root: Path = DEFAULT_LOG_ROOT,
    recent_hours: float = DEFAULT_RECENT_HOURS,
    codemeter_url: str | None = DEFAULT_CODEMETER_URL,
    codemeter_timeout_seconds: float = DEFAULT_CODEMETER_TIMEOUT_SECONDS,
    console_log_level: str = DEFAULT_CONSOLE_LOG_LEVEL,
) -> int:
    """Build and print the integrated read-only activity report."""
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    try:
        configure_serial_logging(console_log_level=console_log_level)
        report = build_activity_report(
            log_root=log_root,
            recent_hours=recent_hours,
            codemeter_url=codemeter_url,
            codemeter_timeout_seconds=codemeter_timeout_seconds,
        )
    except (FileNotFoundError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(render_activity_report(report))
    return 0


def _parse_arguments() -> WrapperOptions:
    parser = argparse.ArgumentParser(
        description=(
            "Report recent TUFLOW simulations and current CodeMeter licence allocations. "
            "Command-line options override the editable defaults."
        )
    )
    parser.add_argument(
        "--log-root",
        type=Path,
        help=f"TUFLOW per-user global log folder (default: {DEFAULT_LOG_ROOT})",
    )
    parser.add_argument(
        "--recent-hours",
        type=_recent_hours,
        help=f"Activity window, up to {MAX_RECENT_HOURS} hours / 7 days (default: {DEFAULT_RECENT_HOURS:g})",
    )
    parser.add_argument(
        "--codemeter-url",
        help=f"CodeMeter WebAdmin base URL (default: {DEFAULT_CODEMETER_URL})",
    )
    parser.add_argument(
        "--codemeter-timeout",
        type=_positive_float,
        help=f"CodeMeter request timeout in seconds (default: {DEFAULT_CODEMETER_TIMEOUT_SECONDS:g})",
    )
    parser.add_argument(
        "--codemeter",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable or disable CodeMeter WebAdmin checks (enabled by default).",
    )
    parser.add_argument(
        "--console-log-level",
        help=f"Loguru console level used by TLF parsing (default: {DEFAULT_CONSOLE_LOG_LEVEL}).",
    )
    add_no_pause_cli_argument(parser=parser)
    args = parser.parse_args()
    check_codemeter = DEFAULT_CHECK_CODEMETER if args.codemeter is None else bool(args.codemeter)
    effective_codemeter_url = (args.codemeter_url or DEFAULT_CODEMETER_URL) if check_codemeter else None
    return WrapperOptions(
        log_root=args.log_root or DEFAULT_LOG_ROOT,
        recent_hours=args.recent_hours or DEFAULT_RECENT_HOURS,
        codemeter_url=effective_codemeter_url,
        codemeter_timeout_seconds=args.codemeter_timeout or DEFAULT_CODEMETER_TIMEOUT_SECONDS,
        console_log_level=args.console_log_level or DEFAULT_CONSOLE_LOG_LEVEL,
        no_pause=bool(args.no_pause),
    )


if __name__ == "__main__":
    options = _parse_arguments()
    result = main(
        log_root=options.log_root,
        recent_hours=options.recent_hours,
        codemeter_url=options.codemeter_url,
        codemeter_timeout_seconds=options.codemeter_timeout_seconds,
        console_log_level=options.console_log_level,
    )
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not options.no_pause:
        pause_console()
    raise SystemExit(result)
