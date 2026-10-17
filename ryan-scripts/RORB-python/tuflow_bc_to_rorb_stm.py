#!/usr/bin/env python3
"""Convert TUFLOW rainfall bc_dbase CSV temporal patterns to RORB STM files.

The TUFLOW CSV is expected to contain an ``Event ID`` row followed by a
``Time (hour)`` header row.  One temporal-pattern column is copied into a
single-pluviograph RORB design-storm file.

Example
-------
python tuflow_bc_to_rorb_stm.py ^
  "...\\GD08_RF_01.00p01440m.CSV" ^
  --column "TP07_2030_SSP3-7.0" ^
  --output-dir "...\\RORB\\ROG stm" ^
  --run-increments 100

Batch example
-------------
python tuflow_bc_to_rorb_stm.py ^
  --critical-storms "...\\critical_storms.csv" ^
  --bc-database "...\\TUFLOW_MLGD\\bc_dbase" ^
  --output-dir "...\\RORB\\ROG stm"
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


CSV_NAME_RE = re.compile(
    r"^(?P<catchment>.+?)_RF_(?P<aep>.+?)p(?P<duration>\d+)m\.csv$",
    re.IGNORECASE,
)


class ConversionError(ValueError):
    """Raised when an input cannot be converted safely."""


@dataclass(frozen=True)
class RainfallSeries:
    column_name: str
    timestep_hours: float
    values: tuple[float, ...]


@dataclass(frozen=True)
class BatchJob:
    table_line: int
    model: str
    input_csv: Path
    column_name: str
    output_path: Path


def _clean_cell(value: str) -> str:
    return value.strip().lstrip("\ufeff")


def read_rainfall_series(csv_path: Path, column_name: str) -> RainfallSeries:
    """Read and validate one rainfall temporal-pattern column."""
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as stream:
            rows = [[_clean_cell(cell) for cell in row] for row in csv.reader(stream)]
    except OSError as exc:
        raise ConversionError(f"Could not read input CSV: {exc}") from exc

    header_index = next(
        (
            index
            for index, row in enumerate(rows)
            if row and row[0].casefold() in {"time (hour)", "time (hours)"}
        ),
        None,
    )
    if header_index is None:
        raise ConversionError(
            "Could not find the CSV header row beginning with 'Time (hour)'."
        )

    header = rows[header_index]
    matches = [
        index
        for index, name in enumerate(header)
        if name.casefold() == column_name.casefold()
    ]
    if not matches:
        available = ", ".join(header[1:])
        raise ConversionError(
            f"Column {column_name!r} was not found. Available rainfall columns: "
            f"{available}"
        )
    if len(matches) > 1:
        raise ConversionError(f"Column {column_name!r} occurs more than once.")
    column_index = matches[0]

    times: list[float] = []
    values: list[float] = []
    for file_line, row in enumerate(rows[header_index + 1 :], start=header_index + 2):
        if not row or not any(row):
            continue
        if len(row) <= column_index:
            raise ConversionError(
                f"CSV line {file_line} has no value for {header[column_index]!r}."
            )
        try:
            time = float(row[0])
            value = float(row[column_index])
        except ValueError as exc:
            raise ConversionError(
                f"CSV line {file_line} contains a non-numeric time or rainfall value."
            ) from exc
        if not math.isfinite(time) or not math.isfinite(value):
            raise ConversionError(f"CSV line {file_line} contains NaN or infinity.")
        times.append(time)
        values.append(value)

    if len(values) < 2:
        raise ConversionError("At least two rainfall rows are required.")
    if not math.isclose(times[0], 0.0, abs_tol=1e-9):
        raise ConversionError(f"The first rainfall time must be 0; found {times[0]:g}.")

    timestep = times[1] - times[0]
    if timestep <= 0:
        raise ConversionError("Rainfall times must increase.")
    tolerance = max(1e-9, abs(timestep) * 1e-9)
    for index, (previous, current) in enumerate(zip(times, times[1:]), start=2):
        increment = current - previous
        if not math.isclose(increment, timestep, rel_tol=1e-9, abs_tol=tolerance):
            raise ConversionError(
                f"Irregular timestep at rainfall row {index}: expected {timestep:g} h, "
                f"found {increment:g} h."
            )

    return RainfallSeries(header[column_index], timestep, tuple(values))


def infer_output_details(csv_path: Path, column_name: str) -> tuple[str, str, str, int]:
    """Infer catchment, AEP, duration and TP from standard TUFLOW naming."""
    match = CSV_NAME_RE.match(csv_path.name)
    if not match:
        raise ConversionError(
            "Cannot infer the output name. Expected an input name such as "
            "GD08_RF_01.00p01440m.CSV; use --output-name to override it."
        )

    tp_match = re.match(r"^TP(?P<number>\d+)(?:_|$)", column_name, re.IGNORECASE)
    if not tp_match:
        raise ConversionError(
            f"Cannot infer a temporal-pattern number from column {column_name!r}; "
            "use --output-name to override the output filename."
        )

    aep = re.sub(r"\.0+$", "", match.group("aep"))
    duration = str(int(match.group("duration")))
    return match.group("catchment"), aep, duration, int(tp_match.group("number"))


def infer_model_name(csv_path: Path) -> str:
    """Infer MLGD from an ancestor directory such as TUFLOW_MLGD."""
    for parent in csv_path.parents:
        if parent.name.upper().startswith("TUFLOW_") and len(parent.name) > 7:
            return parent.name[7:]
    raise ConversionError(
        "Cannot infer the RORB model name from a TUFLOW_* parent directory; "
        "provide --model-name."
    )


def format_number(value: float) -> str:
    """Match the example STM's concise 10-significant-digit formatting."""
    if value == 0:
        return "0"
    return format(value, ".10g")


def format_timestep(value: float) -> str:
    """Keep an explicit decimal for whole-hour RORB timesteps (for example 1.0)."""
    if value.is_integer():
        return f"{value:.1f}"
    return format_number(value)


def build_stm(model_name: str, series: RainfallSeries, run_increments: int) -> str:
    rainfall_rows = len(series.values)
    if run_increments < rainfall_rows:
        raise ConversionError(
            f"--run-increments ({run_increments}) cannot be shorter than the "
            f"rainfall series ({rainfall_rows})."
        )

    lines = [
        model_name,
        "DESIGN",
        (
            "C time incr (hrs), no of time incr, no of rainfall bursts, "
            "no of pluvios, flag =0 for uniform spatial pattern"
        ),
        f"{format_timestep(series.timestep_hours)},{run_increments},1,1,0,-99",
        "C Pluviograph 1 - start and finishing increment of rainfall burst",
        f"0,{rainfall_rows}",
        "Pluviograph 1",
        *(format_number(value) for value in series.values),
        "-99",
    ]
    return "\n".join(lines) + "\n"


def normalise_aep(aep: str) -> str:
    """Convert a TUFLOW AEP token such as 01.00p to the STM token 01p."""
    if not re.fullmatch(r"\d+(?:\.\d+)?p", aep, re.IGNORECASE):
        raise ConversionError(f"Invalid TUFLOW AEP value {aep!r}.")
    number = re.sub(r"\.0+$", "", aep[:-1])
    return f"{number}p"


def read_batch_jobs(
    table_path: Path,
    bc_database: Path,
    output_dir: Path,
    climate_suffix: str,
) -> tuple[list[BatchJob], list[str]]:
    """Read the critical-storm table and construct batch conversion jobs."""
    try:
        with table_path.open("r", encoding="utf-8-sig", newline="") as stream:
            reader = csv.DictReader(stream)
            required = {"Model", "TUFLOW_AEP", "TUFLOW_Duration", "TP"}
            actual = set(reader.fieldnames or ())
            missing = sorted(required - actual)
            if missing:
                raise ConversionError(
                    "Critical-storm table is missing column(s): " + ", ".join(missing)
                )
            table_rows = list(reader)
    except OSError as exc:
        raise ConversionError(f"Could not read critical-storm table: {exc}") from exc

    jobs: list[BatchJob] = []
    skipped: list[str] = []
    seen_outputs: dict[Path, int] = {}
    for table_line, row in enumerate(table_rows, start=2):
        model = (row.get("Model") or "").strip()
        aep = (row.get("TUFLOW_AEP") or "").strip()
        duration = (row.get("TUFLOW_Duration") or "").strip()
        tp = (row.get("TP") or "").strip().upper()

        missing_values = [
            name
            for name, value in (
                ("Model", model),
                ("TUFLOW_AEP", aep),
                ("TUFLOW_Duration", duration),
                ("TP", tp),
            )
            if not value
        ]
        if missing_values:
            skipped.append(
                f"line {table_line}: skipped incomplete row ({', '.join(missing_values)} blank)"
            )
            continue
        if not re.fullmatch(r"\d{5}m", duration, re.IGNORECASE):
            raise ConversionError(
                f"Critical-storm line {table_line}: invalid duration {duration!r}."
            )
        tp_match = re.fullmatch(r"TP(\d+)", tp, re.IGNORECASE)
        if not tp_match:
            raise ConversionError(
                f"Critical-storm line {table_line}: invalid TP value {tp!r}."
            )

        input_name = f"{model}_RF_{aep}{duration}.CSV"
        input_csv = bc_database / model / "rf_inflow" / input_name
        output_name = (
            f"Storm_{model}_{normalise_aep(aep)}_{int(duration[:-1])}m_"
            f"tp{int(tp_match.group(1)):02d}.stm"
        )
        output_path = output_dir / model / output_name
        key = Path(str(output_path).casefold())
        if key in seen_outputs:
            raise ConversionError(
                f"Critical-storm lines {seen_outputs[key]} and {table_line} produce "
                f"the same output: {output_path}"
            )
        seen_outputs[key] = table_line
        jobs.append(
            BatchJob(
                table_line=table_line,
                model=model,
                input_csv=input_csv,
                column_name=f"{tp}_{climate_suffix}",
                output_path=output_path,
            )
        )
    return jobs, skipped


def write_stm(output_path: Path, stm_text: str, overwrite: bool) -> None:
    if output_path.exists() and not overwrite:
        raise ConversionError(
            f"Output already exists: {output_path}. Use --overwrite to replace it."
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output_path.open("w", encoding="utf-8", newline="\n") as stream:
            stream.write(stm_text)
    except OSError as exc:
        raise ConversionError(f"Could not write output STM: {exc}") from exc


def run_batch(args: argparse.Namespace) -> int:
    table_path = args.critical_storms.resolve()
    bc_database = args.bc_database.resolve()
    output_dir = args.output_dir.resolve()
    jobs, skipped = read_batch_jobs(
        table_path, bc_database, output_dir, args.climate_suffix
    )
    model_name = args.model_name or infer_model_name(bc_database)

    prepared: list[tuple[BatchJob, str]] = []
    errors: list[str] = []
    for job in jobs:
        try:
            if not job.input_csv.is_file():
                raise ConversionError(f"input CSV not found: {job.input_csv}")
            series = read_rainfall_series(job.input_csv, job.column_name)
            stm_text = build_stm(model_name, series, args.run_increments)
            if job.output_path.exists() and not args.overwrite:
                raise ConversionError(
                    f"output already exists: {job.output_path} (use --overwrite)"
                )
            prepared.append((job, stm_text))
        except ConversionError as exc:
            errors.append(f"line {job.table_line}: {exc}")

    print(f"Critical-storm table: {table_path}")
    print(f"Climate columns:      TP##_{{{args.climate_suffix}}} (CSV columns L-U)")
    print(f"Complete rows:        {len(jobs)}")
    print(f"Incomplete rows:      {len(skipped)} (skipped)")
    print(f"Valid conversions:    {len(prepared)}")
    print(f"Invalid conversions:  {len(errors)}")
    print(f"Output root:          {output_dir}")

    for message in skipped:
        print(f"SKIP: {message}")
    for message in errors:
        print(f"ERROR: {message}", file=sys.stderr)

    if args.dry_run:
        print("Dry run: no files written.")
    else:
        for job, stm_text in prepared:
            write_stm(job.output_path, stm_text, args.overwrite)
        print(f"Created {len(prepared)} STM file(s).")
    return 2 if errors else 0


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert one TUFLOW rainfall bc_dbase CSV column to a RORB STM."
    )
    parser.add_argument(
        "input_csv",
        type=Path,
        nargs="?",
        help="TUFLOW rainfall CSV (single-file mode)",
    )
    parser.add_argument(
        "--column",
        help="Exact rainfall column heading, e.g. TP07_2030_SSP3-7.0",
    )
    parser.add_argument(
        "--critical-storms",
        type=Path,
        help="Critical-storm CSV for batch mode",
    )
    parser.add_argument(
        "--bc-database",
        type=Path,
        help="TUFLOW bc_dbase directory (required in batch mode)",
    )
    parser.add_argument(
        "--climate-suffix",
        default="2030_SSP3-7.0",
        help="Suffix of the climate-adjusted TP columns (default: 2030_SSP3-7.0)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        required=True,
        help="Directory in which to create the STM",
    )
    parser.add_argument(
        "--output-name",
        help="STM filename; inferred from the input filename and TP if omitted",
    )
    parser.add_argument(
        "--model-name",
        help="Text for STM line 1; inferred from a TUFLOW_* parent if omitted",
    )
    parser.add_argument(
        "--run-increments",
        type=int,
        default=100,
        help="RORB total time increments on line 4 (default: 100; use 150 if needed)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Replace an existing output STM",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and show the intended output without writing it",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.critical_storms:
            if args.input_csv or args.column:
                raise ConversionError(
                    "Do not supply input_csv or --column with --critical-storms."
                )
            if not args.bc_database:
                raise ConversionError("--bc-database is required in batch mode.")
            return run_batch(args)
        if not args.input_csv or not args.column:
            raise ConversionError(
                "Single-file mode requires input_csv and --column, or use "
                "--critical-storms with --bc-database."
            )
        if args.bc_database:
            raise ConversionError("--bc-database is only used in batch mode.")

        input_csv = args.input_csv.resolve()
        series = read_rainfall_series(input_csv, args.column)
        model_name = args.model_name or infer_model_name(input_csv)

        if args.output_name:
            output_name = args.output_name
        else:
            catchment, aep, duration, tp_number = infer_output_details(
                input_csv, series.column_name
            )
            output_name = (
                f"Storm_{catchment}_{aep}p_{duration}m_tp{tp_number:02d}.stm"
            )
        if Path(output_name).name != output_name:
            raise ConversionError("--output-name must be a filename, not a path.")
        if not output_name.lower().endswith(".stm"):
            output_name += ".stm"

        output_path = args.output_dir.resolve() / output_name
        stm_text = build_stm(model_name, series, args.run_increments)

        print(f"Input:          {input_csv}")
        print(f"Column:         {series.column_name}")
        print(f"Timestep:       {format_timestep(series.timestep_hours)} hours")
        print(f"Rainfall rows:  {len(series.values)}")
        print(f"Run increments: {args.run_increments}")
        print(f"Output:         {output_path}")

        if args.dry_run:
            print("Dry run: no file written.")
            return 0
        write_stm(output_path, stm_text, args.overwrite)
        print("STM created successfully.")
        return 0
    except ConversionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())