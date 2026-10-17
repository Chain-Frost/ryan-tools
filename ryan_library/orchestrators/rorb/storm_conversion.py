"""Coordinate one-file and critical-storm-table TUFLOW → RORB conversions.

All batch inputs are validated and rendered in memory before any output is
created. Filesystem failures during the write stage cannot be rolled back.
"""

import csv
import re
from dataclasses import dataclass
from pathlib import Path

from ryan_library.functions.RORB.storm_files import (
    DEFAULT_RUN_INCREMENTS,
    ConversionError,
    TerminalZeroPolicy,
    build_stm,
    infer_model_name,
    normalise_aep,
    normalise_tp,
    output_filename,
    read_rainfall_series,
    write_stm,
)
from ryan_library.functions.tuflow.tuflow_result_naming import (
    format_user_template,
    validate_output_filename,
)

DEFAULT_INPUT_TEMPLATE = "{model}/rf_inflow/{model}_RF_{aep}{duration}.CSV"
DEFAULT_OUTPUT_TEMPLATE = "{model}/Storm_{model}_{aep_normalised}_{duration_minutes}m_{tp_lower}.stm"
DEFAULT_COLUMN_TEMPLATE = "{tp}_{climate_suffix}"


@dataclass(frozen=True, slots=True)
class PreparedJob:
    input_csv: Path
    column: str
    output_path: Path
    stm_text: str
    table_line: int | None = None


@dataclass(frozen=True, slots=True)
class BatchPlan:
    jobs: tuple[PreparedJob, ...]
    skipped: tuple[str, ...]
    errors: tuple[str, ...]


def _relative_template_path(rendered: str, *, output: bool) -> Path:
    """Permit only paths below the supplied root, including on Windows."""
    # Path.absolute() is used for normalisation, never Path.resolve().
    name = rendered.replace("\\", "/")
    pieces = name.split("/")
    if (
        not name
        or name.startswith("/")
        or any(part in {"", ".", ".."} for part in pieces)
        or ":" in name
        or any(char in name for char in '<>"|?*')
    ):
        message = f"Unsafe {'output' if output else 'input'} template path: {rendered!r}"
        raise ConversionError(message)
    if output:
        if not name.lower().endswith(".stm"):
            message = f"STM output template must end with .stm: {name!r}"
            raise ConversionError(message)
        try:
            validate_output_filename(pieces[-1])
        except ValueError as exc:
            raise ConversionError(str(exc)) from exc
    return Path(*pieces)


def _render_path(template: str, values: dict[str, str], *, output: bool) -> Path:
    try:
        rendered = format_user_template(
            template=template,
            values=values,
            description="STM output template" if output else "TUFLOW CSV input template",
        )
    except ValueError as exc:
        raise ConversionError(str(exc)) from exc
    return _relative_template_path(rendered, output=output)


def prepare_single(
    input_csv: Path,
    *,
    column: str,
    output_dir: Path,
    output_name: str | None = None,
    model_name: str | None = None,
    run_increments: int = DEFAULT_RUN_INCREMENTS,
    terminal_zero: TerminalZeroPolicy = "auto",
) -> PreparedJob:
    """Validate one rainfall CSV, without writing files."""
    input_csv = input_csv.absolute()
    series = read_rainfall_series(input_csv, column, terminal_zero=terminal_zero)
    selected_model = model_name if model_name is not None else infer_model_name(input_csv)
    filename = output_filename(input_csv, series.column_name, override=output_name)
    text = build_stm(selected_model, series, run_increments)
    return PreparedJob(input_csv, series.column_name, output_dir.absolute() / filename, text)


def read_batch_rows(table_path: Path) -> list[tuple[int, dict[str, str | None]]]:
    """Read a critical-storm table, retaining its 1-based physical row indices."""
    try:
        with table_path.open(encoding="utf-8-sig", newline="") as stream:
            reader: csv.DictReader[str] = csv.DictReader(stream)
            required = {"Model", "TUFLOW_AEP", "TUFLOW_Duration", "TP"}
            missing = sorted(required - set(reader.fieldnames or ()))
            if missing:
                message = f"Critical-storm table missing columns: {', '.join(missing)}"
                raise ConversionError(message)
            result = [(line, row) for line, row in enumerate(reader, start=2)]
    except OSError as exc:
        message = f"Cannot read critical-storm table: {exc}"
        raise ConversionError(message) from exc
    return result


def prepare_batch(
    table_path: Path,
    *,
    bc_database: Path,
    output_dir: Path,
    climate_suffix: str = "2030_SSP3-7.0",
    model_name: str | None = None,
    run_increments: int = DEFAULT_RUN_INCREMENTS,
    terminal_zero: TerminalZeroPolicy = "auto",
    input_template: str = DEFAULT_INPUT_TEMPLATE,
    output_template: str = DEFAULT_OUTPUT_TEMPLATE,
    column_template: str = DEFAULT_COLUMN_TEMPLATE,
    overwrite: bool = False,
) -> BatchPlan:
    """Preflight every complete table entry; no writes occur at this stage."""
    bc_database = bc_database.absolute()
    output_dir = output_dir.absolute()
    rows = read_batch_rows(table_path.absolute())
    if model_name is None:
        model_name = infer_model_name(bc_database)

    jobs: list[PreparedJob] = []
    skipped: list[str] = []
    errors: list[str] = []
    seen: dict[str, int] = {}

    for line, row in rows:
        cells = {key: (row.get(key) or "").strip() for key in ("Model", "TUFLOW_AEP", "TUFLOW_Duration", "TP")}
        missing = [key for key, value in cells.items() if not value]
        if missing:
            skipped.append(f"line {line}: incomplete row ({', '.join(missing)})")
            continue

        try:
            model, aep, duration = cells["Model"], cells["TUFLOW_AEP"], cells["TUFLOW_Duration"]
            if not re.fullmatch(r"\d{3,5}m", duration, re.IGNORECASE):
                message = f"Invalid duration {duration!r}, expected e.g. 01440m."
                raise ConversionError(message)
            tp = normalise_tp(cells["TP"])
            values = {
                "model": model,
                "aep": aep,
                "aep_normalised": normalise_aep(aep),
                "duration": duration,
                "duration_minutes": str(int(duration[:-1])),
                "tp": tp,
                "tp_lower": tp.lower(),
                "climate_suffix": climate_suffix,
            }
            input_path = bc_database / _render_path(input_template, values, output=False)
            output_path = output_dir / _render_path(output_template, values, output=True)
            column = format_user_template(
                template=column_template, values=values, description="rainfall column template"
            )
            if not column:
                message = "Rainfall column template produced an empty name."
                raise ConversionError(message)
            key = str(output_path).casefold()
            if key in seen:
                message = f"Output duplicates critical-storm line {seen[key]}: {output_path}"
                raise ConversionError(message)
            seen[key] = line
            if output_path.exists() and not overwrite:
                message = f"Output already exists: {output_path} (use --overwrite)."
                raise ConversionError(message)
            series = read_rainfall_series(input_path, column, terminal_zero=terminal_zero)
            stm_text = build_stm(model_name, series, run_increments)
            jobs.append(PreparedJob(input_path, column, output_path, stm_text, table_line=line))
        except ValueError as exc:
            errors.append(f"line {line}: {exc}")

    return BatchPlan(tuple(jobs), tuple(skipped), tuple(errors))


def write_batch(plan: BatchPlan, *, overwrite: bool = False) -> tuple[Path, ...]:
    """Write only a fully validated plan; never write a partial *validation* batch."""
    if plan.errors:
        message = (
            f"Batch contains {len(plan.errors)} invalid conversion(s); "
            "no outputs were written. Correct errors before executing."
        )
        raise ConversionError(message)
    outputs: list[Path] = []
    for job in plan.jobs:
        write_stm(job.output_path, job.stm_text, overwrite=overwrite)
        outputs.append(job.output_path)
    return tuple(outputs)
