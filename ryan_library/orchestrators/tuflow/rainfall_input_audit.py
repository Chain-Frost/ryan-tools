"""Read-only inventory of selected TUFLOW rainfall and material/soil inputs.

TUFLOW control parsing, event resolution, BC arithmetic, and GIS feature reading
are delegated to PyTUFLOW. Defined database rows are never presented as verified
active cell assignments; no routing, loss simulation, or TUFLOW launch occurs.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from math import isfinite
from numbers import Integral
from pathlib import Path
from typing import Any

from ...functions.tuflow.command_plan import CommandPlan
from ...functions.tuflow.soil_loss_validation import validated_soil_parameters


@dataclass(frozen=True, slots=True)
class Finding:
    run_index: int
    code: str
    detail: str


@dataclass(frozen=True, slots=True)
class RainfallBoundary:
    name: str
    source: str
    column: str
    database_multiplier: str
    database_addition: str
    gis_f1: str | None
    gis_f2: str | None
    bc_adjusted_sum_mm: float | None
    spatially_adjusted_sum_mm: float | None
    status: str


@dataclass(frozen=True, slots=True)
class MaterialDefinition:
    material_id: int
    initial_loss_mm: float | None
    continuing_loss_mm_h: float | None
    fraction_impervious: float | None
    status: str


@dataclass(frozen=True, slots=True)
class SoilDefinition:
    soil_id: int
    method: str
    parameters: tuple[tuple[str, str], ...]
    status: str


@dataclass(frozen=True, slots=True)
class RunAudit:
    run_index: int
    tcf: str
    flags: tuple[tuple[str, str], ...]
    source: str
    rainfall: tuple[RainfallBoundary, ...]
    materials: tuple[MaterialDefinition, ...]
    soils: tuple[SoilDefinition, ...]
    findings: tuple[Finding, ...]
    # These are configured DB records, NOT a Cartesian product of active cells.
    spatial_combinations_verified: bool = False


@dataclass(frozen=True, slots=True)
class RainfallAudit:
    runs: tuple[RunAudit, ...]

    def to_dict(self) -> dict[str, object]:
        """Return unambiguous structured evidence suitable for a JSON report."""
        return {"schema_version": 1, "model_execution": False, "runs": [asdict(run) for run in self.runs]}


def _text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return "" if text.casefold() in {"nan", "none", "<na>"} else text


def _float(value: object) -> float | None:
    if not _text(value):
        return None
    try:
        number = float(str(value))
    except ValueError, TypeError:
        return None
    return number if isfinite(number) else None


def _row_value(row: Mapping[str, object], name: str) -> object:
    return next((value for key, value in row.items() if key.casefold() == name.casefold()), None)


def _material_definition(material_id: int, data: Mapping[str, object]) -> MaterialDefinition:
    """Read material IL/CL, applying only documented TUFLOW optional defaults.

    Missing optional rainfall losses and fraction impervious mean 0, not an
    unknown value. Supplied but malformed values remain unresolved.
    """
    if data.get("__schema_unresolved__"):
        return MaterialDefinition(material_id, None, None, None, "unresolved")
    if not any(key.casefold() in {"rainfall losses", "il", "cl"} for key in data):
        return MaterialDefinition(material_id, None, None, None, "unresolved")
    pair = _text(_row_value(data, "Rainfall Losses"))
    if pair:
        values = pair.strip('"').split(",")
        il, cl = (_float(value) for value in values) if len(values) == 2 else (None, None)
    else:
        raw_il = _text(_row_value(data, "IL"))
        raw_cl = _text(_row_value(data, "CL"))
        if raw_il or raw_cl:
            il, cl = _float(raw_il), _float(raw_cl)
        else:
            il, cl = 0.0, 0.0  # Omitted material rainfall-loss fields default to no loss.
    raw_impervious = _text(_row_value(data, "Fraction Impervious"))
    impervious = _float(raw_impervious) if raw_impervious else 0.0
    valid = il is not None and cl is not None and il >= 0 and cl >= 0
    if impervious is None or not 0 <= impervious <= 1:
        valid = False
    return MaterialDefinition(material_id, il, cl, impervious, "defined" if valid else "unresolved")


def _soil_definition(soil_id: int, data: Mapping[str, object]) -> SoilDefinition:
    method = _text(data.get("method")).upper()
    fields = tuple(sorted((str(key), _text(value)) for key, value in data.items() if str(key).casefold() != "method"))
    try:
        validated_soil_parameters(method, data)
    except ValueError:
        return SoilDefinition(soil_id, method, fields, "unresolved")
    return SoilDefinition(soil_id, method, fields, "defined")


def _db_rows(db: Any) -> list[tuple[object, dict[str, object]]]:
    frame = db.df
    columns: list[str] = [str(key) for key in frame.columns]
    if columns and all(isinstance(key, Integral) for key in frame.columns):
        # The pinned PyTUFLOW reader may retain positional numeric column names
        # for a headerless materials CSV. Use only a recognised database schema.
        schema = getattr(getattr(db, "bs", db), "COLUMN_NAMES", None)
        if (
            isinstance(schema, (tuple, list))
            and len(schema) >= len(columns)
            and all(isinstance(name, str) for name in schema)
            and ("Rainfall Losses" in schema or {"IL", "CL"}.issubset(schema))
        ):
            columns = list(schema[: len(columns)])
        else:
            return [(index, {"__schema_unresolved__": True}) for index, _ in frame.iterrows()]
    return [(index, dict(zip(columns, row.tolist(), strict=True))) for index, row in frame.iterrows()]


def _material_rows(context: Any, findings: list[Finding], index: int) -> tuple[MaterialDefinition, ...]:
    try:
        mat = context.mat_file()
    except KeyError:
        findings.append(
            Finding(index, "NO_MATERIAL_DATABASE", "No active materials file; material losses not established")
        )
        return ()
    result: list[MaterialDefinition] = []
    for raw_id, row in _db_rows(mat):
        try:
            result.append(_material_definition(int(raw_id), row))
        except (TypeError, ValueError) as exc:
            findings.append(Finding(index, "MATERIAL_UNRESOLVED", f"Material ID {raw_id}: {exc}"))
    if any(m.status == "unresolved" for m in result):
        findings.append(Finding(index, "MATERIAL_LOSS_UNRESOLVED", "Some defined material losses cannot be evaluated"))
    return tuple(result)


def _soil_rows(context: Any, findings: list[Finding], index: int) -> tuple[SoilDefinition, ...]:
    try:
        db = context.soils_file()
    except KeyError:
        return ()  # No soil infiltration database selected.
    result: list[SoilDefinition] = []
    for raw_id, _ in _db_rows(db):
        try:
            soil_id = int(raw_id)
            detail = db.value(soil_id)
            if not isinstance(detail, Mapping):
                message = "PyTUFLOW did not return soil parameters"
                raise ValueError(message)
            definition = _soil_definition(soil_id, detail)
            result.append(definition)
            if definition.status != "defined":
                findings.append(
                    Finding(
                        index,
                        "SOIL_UNRESOLVED",
                        f"Soil {soil_id}: invalid or incomplete {definition.method} parameters",
                    )
                )
        except (ValueError, TypeError, KeyError, NotImplementedError) as exc:
            findings.append(Finding(index, "SOIL_UNRESOLVED", f"Soil {raw_id}: {exc}"))
    return tuple(result)


def _gis_records(path: object) -> tuple[Mapping[str, object], ...]:
    """Read selected GIS attributes using PyTUFLOW's public context manager."""
    from pytuflow.util.gis import GISAttributes

    with GISAttributes(path) as source:
        return tuple(dict(record) for record in source)


def _add_rainfall_location(
    locations: dict[str, tuple[str | None, str | None]],
    record: Mapping[str, object],
    source: object,
    index: int,
    findings: list[Finding],
) -> None:
    item = {str(key).casefold(): val for key, val in record.items()}
    name = _text(item.get("name"))
    if not name:
        findings.append(Finding(index, "BLANK_RF_NAME", f"Blank polygon rainfall name in {source}"))
        return
    factors = (_text(item.get("f1")) or None, _text(item.get("f2")) or None)
    if name in locations and locations[name] != factors:
        findings.append(Finding(index, "RF_FACTOR_CONFLICT", f"Different GIS factors for RF name {name}"))
        return
    for field, value in zip(("f1", "f2"), factors, strict=True):
        if value is not None and _float(value) is None:
            findings.append(Finding(index, "RF_FACTOR_UNRESOLVED", f"Non-numeric {field} for RF name {name}"))
    locations.setdefault(name, factors)


def _active_rainfall_names(
    context: Any, index: int, findings: list[Finding]
) -> dict[str, tuple[str | None, str | None]]:
    """Get names from active GIS RF polygon features, not all BC database rows."""
    locations: dict[str, tuple[str | None, str | None]] = {}
    inputs = context.find_input(recursive=True)
    rf_inputs = [inp for inp in inputs if _text(getattr(inp, "lhs", "")).casefold() == "read gis rf"]
    unsupported = [
        _text(getattr(inp, "lhs", ""))
        for inp in inputs
        if any(word in _text(getattr(inp, "lhs", "")).casefold() for word in ("rainfall", "read gis rf"))
        and inp not in rf_inputs
    ]
    if unsupported:
        findings.append(
            Finding(index, "OTHER_RAINFALL_INPUT", "Unassessed rainfall commands: " + ", ".join(unsupported))
        )
    for inp in rf_inputs:
        try:
            for path in inp.files:
                for record in _gis_records(path):
                    _add_rainfall_location(locations, record, path, index, findings)
        except (OSError, ValueError, AttributeError, ImportError) as exc:
            findings.append(Finding(index, "RF_GIS_UNRESOLVED", f"Cannot inspect {inp}: {exc}"))
    if not rf_inputs:
        findings.append(
            Finding(index, "NO_SUPPORTED_RF_GIS", "No Read GIS RF polygon found; other RF methods not inferred")
        )
    return locations


def _bc_series_total(database: Any, name: str, row: Mapping[str, object], column: str) -> float:
    """Delegate adjusted series to PyTUFLOW, rejecting its silent invalid-field fallbacks."""
    for field in ("Mult Col 2", "Add Col 1", "Add Col 2"):
        raw = _row_value(row, field)
        if raw is not None and _text(raw) and _float(raw) is None:
            message = f"Nonfinite/invalid explicit BC adjustment {field}={raw!r}"
            raise ValueError(message)
    resolved = database.value(name)
    if not column or not hasattr(resolved, "columns"):
        message = "Rainfall series or value column not resolved"
        raise ValueError(message)
    candidates = [key for key in resolved.columns if str(key).casefold() == column.casefold()]
    if len(candidates) != 1:
        message = f"Expected exactly one rainfall column {column!r}"
        raise ValueError(message)
    values = [float(val) for val in resolved[candidates[0]]]
    if not values or not all(isfinite(val) for val in values):
        message = "Nonfinite or empty rainfall values"
        raise ValueError(message)
    return sum(values)


def _rainfall_rows(context: Any, findings: list[Finding], index: int) -> tuple[RainfallBoundary, ...]:
    selected = _active_rainfall_names(context, index, findings)
    if not selected:
        return ()
    try:
        database = context.bc_dbase()
    except KeyError:
        findings.append(
            Finding(index, "NO_BC_DATABASE", "RF GIS names cannot be resolved without an active BC database")
        )
        return ()
    by_name = {str(key).casefold(): row for key, row in _db_rows(database)}
    rainfall: list[RainfallBoundary] = []
    for name, (f1, f2) in sorted(selected.items()):
        selector, group = name.split("|", 1) if "|" in name else ("", name)
        row = by_name.get(group.strip().casefold())
        if row is None:
            findings.append(Finding(index, "RF_BC_MISSING", f"No active BC database row for RF name {name}"))
            continue
        data_sum: float | None = None
        column = _text(_row_value(row, "Column 2")) or selector.strip()
        try:
            data_sum = _bc_series_total(database, name, row, column)
        except (ValueError, TypeError, KeyError, OSError, AttributeError) as exc:
            findings.append(Finding(index, "RF_SERIES_UNRESOLVED", f"{name}: {exc}"))
        rainfall.append(
            RainfallBoundary(
                name=name,
                source=_text(_row_value(row, "Source")),
                column=column,
                database_multiplier=_text(_row_value(row, "Mult Col 2")) or "1",
                database_addition=_text(_row_value(row, "Add Col 2")) or "0",
                gis_f1=f1,
                gis_f2=f2,
                bc_adjusted_sum_mm=data_sum,
                spatially_adjusted_sum_mm=None,
                status="bc-adjusted-only" if data_sum is not None else "unresolved",
            )
        )
    if rainfall:
        findings.append(
            Finding(
                index,
                "SPATIAL_RF_UNVERIFIED",
                "GIS/global factors and effective cell-level rainfall are not yet verified",
            )
        )
    return tuple(rainfall)


def audit_command_plan(
    plan: CommandPlan,
    *,
    model_factory: Callable[[Path], Any] | None = None,
) -> RainfallAudit:
    """Inspect selected model inputs for each inert planned run, without launching TUFLOW.

    ``model_factory`` permits fixture injection; production uses PyTUFLOW.TCF.
    Failures are retained per run rather than presenting partial evidence as PASS.
    """
    if model_factory is None:
        from pytuflow import TCF

        model_factory = TCF
    cached: dict[str, Any] = {}
    audited: list[RunAudit] = []
    for run in plan.runs:
        findings: list[Finding] = []
        rainfall: tuple[RainfallBoundary, ...] = ()
        materials: tuple[MaterialDefinition, ...] = ()
        soils: tuple[SoilDefinition, ...] = ()
        try:
            key = str(run.tcf)
            if key not in cached:
                cached[key] = model_factory(Path(key))
            context = cached[key].context(run.flags)
            rainfall = _rainfall_rows(context, findings, run.index)
            materials = _material_rows(context, findings, run.index)
            soils = _soil_rows(context, findings, run.index)
            findings.append(
                Finding(
                    run.index,
                    "SPATIAL_COMBINATION_UNRESOLVED",
                    "Defined material/soil rows are not proof of active cell pairings",
                )
            )
        except Exception as exc:  # noqa: BLE001 - external PyTUFLOW parsing errors are per-run findings
            findings.append(Finding(run.index, "MODEL_CONTEXT_UNRESOLVED", f"{type(exc).__name__}: {exc}"))
        audited.append(
            RunAudit(
                run_index=run.index,
                tcf=str(run.tcf),
                flags=run.flag_pairs,
                source=f"{run.source}:{run.line_start}",
                rainfall=rainfall,
                materials=materials,
                soils=soils,
                findings=tuple(findings),
            )
        )
    return RainfallAudit(tuple(audited))


def factor_groups(audit: RainfallAudit) -> dict[tuple[tuple[str, str, str, str, str], ...], tuple[int, ...]]:
    """Group same selected factors, deliberately ignoring event-specific depths."""
    groups: dict[tuple[tuple[str, str, str, str, str], ...], list[int]] = defaultdict(list)
    for run in audit.runs:
        # Incomplete spatial/BC inventories must never be treated as
        # factor-equivalent to complete runs that happen to share the
        # successfully resolved subset.
        invalid_codes = {
            "BLANK_RF_NAME",
            "RF_FACTOR_CONFLICT",
            "RF_FACTOR_UNRESOLVED",
            "RF_GIS_UNRESOLVED",
            "RF_BC_MISSING",
            "RF_SERIES_UNRESOLVED",
            "NO_BC_DATABASE",
            "NO_SUPPORTED_RF_GIS",
            "OTHER_RAINFALL_INPUT",
            "MODEL_CONTEXT_UNRESOLVED",
        }
        if any(finding.code in invalid_codes for finding in run.findings):
            continue
        if not run.rainfall or any(item.status != "bc-adjusted-only" for item in run.rainfall):
            continue
        signature = tuple(
            sorted(
                (b.name, b.database_multiplier, b.database_addition, b.gis_f1 or "", b.gis_f2 or "")
                for b in run.rainfall
            )
        )
        groups[signature].append(run.run_index)
    return {key: tuple(values) for key, values in groups.items()}
