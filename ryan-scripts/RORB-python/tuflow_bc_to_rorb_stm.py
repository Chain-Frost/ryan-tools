"""Convert TUFLOW rainfall bc_dbase CSV columns to RORB DESIGN .stm files.

Edit the defaults below or override them on the command line.

Single file:
    py -3.14 tuflow_bc_to_rorb_stm.py "GD08_RF_01.00p00120m.CSV" ^
      --column TP07_2030_SSP3-7.0 --model-name MLGD --output-dir "RORB/stm"

Critical-storm batch:
    py -3.14 tuflow_bc_to_rorb_stm.py --critical-storms "critical_storms.csv" ^
      --bc-database "TUFLOW_MLGD/bc_dbase" --output-dir "RORB/stm" --dry-run

TUFLOW time-zero rainfall is a placeholder, not a RORB interval. The library
validates the input and preserves rainfall depth and individual interval timing.
"""

from pathlib import Path

WRAPPER_VERSION = "2026-10-15.1"
DEFAULT_OUTPUT_DIR = Path()
DEFAULT_RUN_INCREMENTS = 200  # RORBwin 6.52 default; review routing tail.
DEFAULT_CLIMATE_SUFFIX = "2030_SSP3-7.0"

import argparse
import sys
from collections.abc import Sequence
from typing import cast

from ryan_library.functions.RORB.storm_files import (
    ConversionError,
    TerminalZeroPolicy,
    write_stm,
)
from ryan_library.functions.wrapper_utils import (
    add_no_pause_cli_argument,
    pause_console,
    print_wrapper_banner,
)
from ryan_library.orchestrators.rorb.storm_conversion import (
    DEFAULT_COLUMN_TEMPLATE,
    DEFAULT_INPUT_TEMPLATE,
    DEFAULT_OUTPUT_TEMPLATE,
    prepare_batch,
    prepare_single,
    write_batch,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert TUFLOW interval rainfall to RORB DESIGN STM.")
    parser.add_argument("input_csv", nargs="?", type=Path, help="CSV for single-file conversion")
    parser.add_argument("--column", help="Exact rainfall column name for single-file mode")
    parser.add_argument("--critical-storms", type=Path, help="Critical-storm table for batch mode")
    parser.add_argument("--bc-database", type=Path, help="Root of the rainfall input tree for batch mode")
    parser.add_argument("--output-dir", type=Path, default=None, help="Output directory (default: current folder)")
    parser.add_argument("--output-name", help="Output filename override for single-file mode")
    parser.add_argument("--model-name", help="Storm identifier / RORB model name (otherwise infer TUFLOW_* ancestor)")
    parser.add_argument(
        "--climate-suffix",
        default=None,
        help="Climate column suffix, default 2030_SSP3-7.0; used in the column template",
    )
    parser.add_argument(
        "--run-increments",
        type=int,
        default=None,
        help="RORB simulation length, 1-2000 (default 200). Ensure the runoff tail fits.",
    )
    parser.add_argument(
        "--terminal-zero",
        choices=("auto", "keep", "drop"),
        default="auto",
        help="Auto removes only a zero beyond the named storm duration; keep/drop override for non-standard series",
    )
    parser.add_argument(
        "--input-template",
        default=DEFAULT_INPUT_TEMPLATE,
        help="Batch input relative to --bc-database; placeholders include model, aep, duration, tp",
    )
    parser.add_argument(
        "--output-template",
        default=DEFAULT_OUTPUT_TEMPLATE,
        help="Batch output relative to --output-dir; supports model, aep_normalised, duration_minutes, tp_lower",
    )
    parser.add_argument(
        "--column-template",
        default=DEFAULT_COLUMN_TEMPLATE,
        help="Batch rainfall column; default '{tp}_{climate_suffix}'",
    )
    parser.add_argument("--overwrite", action="store_true", help="Replace existing STM files")
    parser.add_argument("--dry-run", action="store_true", help="Validate all outputs but do not write them")
    add_no_pause_cli_argument(parser=parser)
    return parser.parse_args(argv)


def _run_batch(
    args: argparse.Namespace,
    output_dir: Path,
    run_increments: int,
    terminal_zero: TerminalZeroPolicy,
) -> int:
    if args.input_csv is not None or args.column is not None or args.output_name is not None:
        message = "Batch mode cannot use input_csv, --column or --output-name."
        raise ConversionError(message)
    if args.bc_database is None:
        message = "--bc-database is required with --critical-storms."
        raise ConversionError(message)
    plan = prepare_batch(
        table_path=args.critical_storms,
        bc_database=args.bc_database,
        output_dir=output_dir,
        climate_suffix=args.climate_suffix or DEFAULT_CLIMATE_SUFFIX,
        model_name=args.model_name,
        run_increments=run_increments,
        terminal_zero=terminal_zero,
        input_template=args.input_template,
        output_template=args.output_template,
        column_template=args.column_template,
        overwrite=args.overwrite,
    )
    print(f"Valid jobs: {len(plan.jobs)}; skipped: {len(plan.skipped)}; errors: {len(plan.errors)}")
    for skipped in plan.skipped:
        print(f"SKIP: {skipped}")
    for error in plan.errors:
        print(f"ERROR: {error}", file=sys.stderr)
    for job in plan.jobs:
        print(f"{job.input_csv} [{job.column}] -> {job.output_path}")
    if plan.errors:
        print("Validation failed: no outputs written.", file=sys.stderr)
        return 1
    if args.dry_run:
        print("Dry run: no outputs written.")
        return 0
    outputs = write_batch(plan, overwrite=args.overwrite)
    print(f"Wrote {len(outputs)} RORB STM file(s).")
    return 0


def _run_single(
    args: argparse.Namespace,
    output_dir: Path,
    run_increments: int,
    terminal_zero: TerminalZeroPolicy,
) -> int:
    if args.input_csv is None or args.column is None:
        message = "Single-file mode requires input_csv and --column."
        raise ConversionError(message)
    if args.bc_database is not None:
        message = "--bc-database is only used in batch mode."
        raise ConversionError(message)

    job = prepare_single(
        args.input_csv,
        column=args.column,
        output_dir=output_dir,
        output_name=args.output_name,
        model_name=args.model_name,
        run_increments=run_increments,
        terminal_zero=terminal_zero,
    )
    if job.output_path.exists() and not args.overwrite:
        message = f"Output exists: {job.output_path} (pass --overwrite)."
        raise ConversionError(message)
    print(f"Input: {job.input_csv}; column: {job.column}; output: {job.output_path}")
    if args.dry_run:
        print("Dry run: no output written.")
        return 0
    write_stm(job.output_path, job.stm_text, overwrite=args.overwrite)
    print("STM created.")
    return 0


def main(args: argparse.Namespace) -> int:
    output_dir: Path = args.output_dir if args.output_dir is not None else DEFAULT_OUTPUT_DIR
    run_increments: int = args.run_increments if args.run_increments is not None else DEFAULT_RUN_INCREMENTS
    terminal_zero = cast("TerminalZeroPolicy", args.terminal_zero)
    try:
        if args.critical_storms is not None:
            return _run_batch(args, output_dir, run_increments, terminal_zero)
        return _run_single(args, output_dir, run_increments, terminal_zero)
    except (ConversionError, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    arguments = parse_args()
    result = main(arguments)
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    if not arguments.no_pause:
        pause_console()
    raise SystemExit(result)
