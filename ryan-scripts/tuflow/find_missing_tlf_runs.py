r"""Infer missing TUFLOW filename combinations from a results folder.

Scan only by default; optional CSV output never overwrites. See MISSING_RUNS.md.
Legacy --input-table remains available. Requires Python 3.14 and installed ryan-tools.
Example: python find_missing_tlf_runs.py "T:\results\04" --no-pause
"""

from pathlib import Path

WRAPPER_VERSION = "2026-11-07.5"
# Relative paths use the launch directory, before WORKING_DIR is applied.
RESULTS_FOLDER: Path = Path.cwd()
# None selects folder mode; otherwise select a CSV/XLSX/XLS tracking table.
INPUT_TABLE: Path | None = None
# Excel sheet name or zero-based index; unused for folder mode and CSV.
SHEET: str | int = 0
RECURSIVE = True
# False counts filenames only. True reads TLFs and counts confirmed successful simulations.
VERIFY_SUCCESS = False
# None means no folder report; a path creates a new CSV and never overwrites.
# Table mode always exports beside its input; leave OUTPUT_CSV as None.
OUTPUT_CSV: Path | None = None
# Positive upper bound on inferred combinations.
MAX_COMBINATIONS = 1_000_000
CONSOLE_LOG_LEVEL = "SUCCESS"
WORKING_DIR: Path = Path.cwd()
PAUSE_ON_FINISH = True

import argparse
import csv

from loguru import logger
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ryan_library.functions.loguru_helpers import configure_serial_logging
from ryan_library.functions.tlf_ensemble_inventory import EnsembleInventory, scan_ensemble_inventory
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner
from ryan_library.orchestrators.tuflow.tlf_missing_runs import orchestrate_missing_runs_check


def _parse_cli_arguments() -> argparse.Namespace:
    """Resolve settings before changing directory and run the selected inventory."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("folder", nargs="?", type=Path, default=None, help="Results root; defaults to RESULTS_FOLDER.")
    parser.add_argument(
        "--input-table", "-i", type=Path, default=INPUT_TABLE, help="CSV/Excel tracking table instead of folder mode."
    )
    parser.add_argument("--sheet", "-s", default=SHEET, help="Excel sheet name or zero-based index.")
    parser.add_argument(
        "--verify-success",
        action=argparse.BooleanOptionalAction,
        default=VERIFY_SUCCESS,
        help="Read TLFs and require Simulation FINISHED; default: VERIFY_SUCCESS (False).",
    )
    parser.add_argument(
        "--recursive",
        action=argparse.BooleanOptionalAction,
        default=RECURSIVE,
        help="Include subdirectories; defaults to RECURSIVE.",
    )
    parser.add_argument("--output-csv", type=Path, default=OUTPUT_CSV, help="New folder-report CSV; refuses overwrite.")
    parser.add_argument(
        "--max-combinations", type=int, default=MAX_COMBINATIONS, help="Positive limit on inferred combinations."
    )
    parser.add_argument("--console-log-level", default=CONSOLE_LOG_LEVEL, help="Loguru level, e.g. SUCCESS or DEBUG.")
    parser.add_argument("--working-directory", type=Path, default=WORKING_DIR, help="Existing execution directory.")
    parser.add_argument(
        "--pause",
        action=argparse.BooleanOptionalAction,
        default=PAUSE_ON_FINISH,
        help="Pause after interactive success; --no-pause for automation.",
    )
    args: argparse.Namespace = parser.parse_args()
    if args.input_table is not None and (args.folder is not None or args.output_csv is not None or args.verify_success):
        parser.error("--input-table cannot be combined with a folder, --output-csv or --verify-success")
    return args


def _path_setting(value: object, name: str) -> Path:
    if not isinstance(value, (str, Path)) or not str(value).strip():
        message = f"{name} must be a nonblank path, e.g. Path('results'); got {value!r}"
        raise ValueError(message)
    return Path(value).absolute()


def _validate_settings(args: argparse.Namespace) -> tuple[Path, Path | None, Path | None, Path]:
    """Validate all effective values before changing directory or writing reports."""
    for name, value in (
        ("RECURSIVE", args.recursive),
        ("PAUSE_ON_FINISH", args.pause),
        ("VERIFY_SUCCESS", args.verify_success),
    ):
        if not isinstance(value, bool):
            message = f"{name} must be True or False; got {value!r}"
            raise ValueError(message)
    if type(args.max_combinations) is not int or args.max_combinations < 1:
        message = f"MAX_COMBINATIONS must be a positive integer, e.g. 1000000; got {args.max_combinations!r}"
        raise ValueError(message)
    if not isinstance(args.console_log_level, str):
        message = "CONSOLE_LOG_LEVEL must be a level name, e.g. 'SUCCESS'"
        raise ValueError(message)
    logger.level(args.console_log_level)
    folder = _path_setting(args.folder if args.folder is not None else RESULTS_FOLDER, "RESULTS_FOLDER")
    table = _path_setting(args.input_table, "INPUT_TABLE") if args.input_table is not None else None
    output = _path_setting(args.output_csv, "OUTPUT_CSV") if args.output_csv is not None else None
    working_dir = _path_setting(args.working_directory, "WORKING_DIR")
    _validate_table(table, args.sheet)
    if output is not None and (output.suffix.lower() != ".csv" or not output.parent.is_dir()):
        message = f"OUTPUT_CSV must be a CSV path in an existing directory; got {output}"
        raise ValueError(message)
    return folder, table, output, working_dir


def _validate_table(table: Path | None, sheet: object) -> None:
    """Reject unsupported table selections before workflow side effects."""
    if table is not None:
        if table.suffix.lower() not in {".csv", ".xlsx", ".xls"} or not table.is_file():
            message = f"INPUT_TABLE must be an existing CSV/Excel file; got {table}"
            raise ValueError(message)
        if isinstance(sheet, bool) or not isinstance(sheet, (str, int)) or not str(sheet).strip():
            message = f"SHEET must be a sheet name or nonnegative index, e.g. 0; got {sheet!r}"
            raise ValueError(message)
        if isinstance(sheet, int) and sheet < 0:
            message = "SHEET index must be nonnegative, e.g. 0"
            raise ValueError(message)


def _show_inventory(inventory: EnsembleInventory) -> None:
    """Display TP coverage by scenario, retaining a fallback for filenames without TP."""
    console = Console()
    for coverage in inventory.tp_tables:
        # Split wide grids instead of truncating duration headings or cell counts.
        column_width = max(8, *(len(duration) + 2 for duration in coverage.durations))
        per_page = max(1, (console.width - 16) // (column_width + 1))
        for start in range(0, len(coverage.durations), per_page):
            durations = coverage.durations[start : start + per_page]
            label = "Successful TPs" if inventory.verified_count is not None else "TPs found"
            table = Table(title=Text(coverage.scenario), caption=f"{label} / {coverage.expected_tps} expected")
            table.add_column("AEP", no_wrap=True)
            for duration in durations:
                table.add_column(Text(duration), justify="right", no_wrap=True)
            for aep, counts in zip(coverage.aeps, coverage.counts, strict=True):
                table.add_row(
                    Text(aep),
                    *(
                        Text(str(count), style="grey50" if count == coverage.expected_tps else "red")
                        for count in counts[start : start + per_page]
                    ),
                )
            console.print(table)
        console.print(Text("Missing: " + (" | ".join(coverage.missing_summary) or "none")))
    # TP tables replace per-run console lines; the CSV still includes every missing filename.
    if inventory.tp_tables:
        logger.success("Use --output-csv for the full list of inferred missing filenames")
    if inventory.ungridded_missing:
        for name in inventory.ungridded_missing[:50]:
            logger.success("Inferred missing: {}", name)
        if len(inventory.ungridded_missing) > 50:
            logger.warning("Console limited to 50 gaps; use --output-csv to save every inferred missing run")


def main(args: argparse.Namespace) -> int:
    """Validate effective settings and run without pausing or terminating the process."""
    print_wrapper_banner(
        wrapper_source="tuflow/find_missing_tlf_runs.py", wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION
    )
    result = 0
    try:
        folder, table, output, working_dir = _validate_settings(args)
        if not change_working_directory(target_dir=working_dir):
            return 1
        configure_serial_logging(
            console_log_level=args.console_log_level,
            console_format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
        )
        if table is not None:
            sheet = int(args.sheet) if str(args.sheet).isdigit() else args.sheet
            orchestrate_missing_runs_check(input_path=table, sheet_name=sheet)
        else:
            inventory: EnsembleInventory = scan_ensemble_inventory(
                root=folder,
                recursive=args.recursive,
                max_combinations=args.max_combinations,
                verify_success=args.verify_success,
            )
            logger.success(
                "{} logs; {} unique runs; {} filename families; {} inferred expected; {} inferred missing",
                inventory.log_count,
                inventory.run_count,
                inventory.family_count,
                inventory.expected_count,
                len(inventory.missing),
            )
            if inventory.verified_count is None:
                logger.warning("Filename presence does not establish successful completion. Expectations are inferred.")
            else:
                logger.success(
                    "{} confirmed successful; {} logs present without confirmed success",
                    inventory.verified_count,
                    inventory.run_count - inventory.verified_count,
                )
                logger.warning(
                    "Gaps include absent logs and runs without confirmed success. Expectations are inferred."
                )
            _show_inventory(inventory)
            if output is not None:
                with output.open("x", newline="", encoding="utf-8") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(
                        ["inferred_missing_or_unsuccessful_run" if args.verify_success else "inferred_missing_run"]
                    )
                    writer.writerows((name,) for name in inventory.missing)
                logger.success("Wrote inferred missing runs to {}", output)
    except Exception:
        logger.exception("Missing runs analysis failed")
        result = 1
    return result


if __name__ == "__main__":
    options = _parse_cli_arguments()
    exit_code = main(options)
    print_wrapper_banner(
        wrapper_source="tuflow/find_missing_tlf_runs.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if exit_code == 0 and options.pause:
        pause_console()
    raise SystemExit(exit_code)
