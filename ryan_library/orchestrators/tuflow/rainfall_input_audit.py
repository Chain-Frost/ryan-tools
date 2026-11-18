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
from pathlib import Path
from typing import Any

from ...functions.tuflow.command_plan import CommandPlan


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
    except (ValueError, TypeError):
        return None
    return number if isfinite(number) else None


def _row_value(row: Mapping[str, object], name: str) -> object:
    return next((value for key, value in row.items() if key.casefold() == name.casefold()), None)


def _material_definition(material_id: int, data: Mapping[str, object]) -> MaterialDefinition:
    """Read the PyTUFLOW resolved material schema; never infer missing IL/CL as zero."""
    pair = _text(_row_value(data, "Rainfall Losses"))
    if pair:
        values = pair.strip('"').split(",")
        il, cl = (_float(x) for x in values) if len(values) == 2 else (None, None)
    else:
        il, cl = _float(_row_value(data, "IL")), _float(_row_value(data, "CL"))
    impervious = _float(_row_value(data, "Fraction Impervious"))
    valid = il is not None and cl is not None and il >= 0 and cl >= 0
    if impervious is not None and not 0 <= impervious <= 1:
        valid = False
    return MaterialDefinition(material_id, il, cl, impervious, "defined" if valid else "unresolved")


def _soil_definition(soil_id: int, data: Mapping[str, object]) -> SoilDefinition:
    method = _text(data.get("method")).upper()
    fields = tuple(sorted((str(key), _text(value)) for key, value in data.items() if str(key).casefold() != "method"))
    return SoilDefinition(soil_id, method, fields, "defined" if method else "unresolved")


def _db_rows(db: Any) -> list[tuple[object, dict[str, object]]]:
    frame = db.df
    return [(index, {str(key): val for key, val in row.items()}) for index, row in frame.iterrows()]


def _material_rows(context: Any, findings: list[Finding], index: int) -> tuple[MaterialDefinition, ...]:
    try:
        mat = context.mat_file()
    except KeyError:
        findings.append(Finding(index, "NO_MATERIAL_DATABASE", "No active materials file; material losses not established"))
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
            result.append(_soil_definition(soil_id, detail))
        except (ValueError, TypeError, KeyError, NotImplementedError) as exc:
            findings.append(Finding(index, "SOIL_UNRESOLVED", f"Soil {raw_id}: {exc}"))
    return tuple(result)


def _active_rainfall_names(context: Any, index: int, findings: list[Finding]) -> dict[str, tuple[str | None, str | None]]:
    """Get names from active GIS RF polygon features, not all BC database rows."""
    locations: dict[str, tuple[str | None, str | None]] = {}
    inputs = context.get_inputs()
    rf_inputs = [inp for inp in inputs if _text(getattr(inp, "lhs", "")).casefold() == "read gis rf"]
    unsupported = [
        _text(getattr(inp, "lhs", ""))
        for inp in inputs
        if any(word in _text(getattr(inp, "lhs", "")).casefold() for word in ("rainfall", "read gis rf"))
        and inp not in rf_inputs
    ]
    if unsupported:
        findings.append(Finding(index, "OTHER_RAINFALL_INPUT", "Unassessed rainfall commands: " + ", ".join(unsupported)))
    for inp in rf_inputs:
        try:
            for path in inp.files:
                for item in path.gis_attributes():
                    record = {str(k).casefold(): v for k, v in item.items()}
                    name = _text(record.get("name"))
                    if not name:
                        findings.append(Finding(index, "BLANK_RF_NAME", f"Blank polygon rainfall name in {path}"))
                        continue
                    factors = (_text(record.get("f1")) or None, _text(record.get("f2")) or None)
                    if name in locations and locations[name] != factors:
                        findings.append(Finding(index, "RF_FACTOR_CONFLICT", f"Different GIS factors for RF name {name}"))
                    locations[name] = factors
        except (OSError, ValueError, AttributeError, ImportError) as exc:
            findings.append(Finding(index, "RF_GIS_UNRESOLVED", f"Cannot inspect {inp}: {exc}"))
    if not rf_inputs:
        findings.append(Finding(index, "NO_SUPPORTED_RF_GIS", "No Read GIS RF polygon found; other RF methods not inferred"))
    return locations


def _rainfall_rows(context: Any, findings: list[Finding], index: int) -> tuple[RainfallBoundary, ...]:
    selected = _active_rainfall_names(context, index, findings)
    if not selected:
        return ()
    try:
        database = context.bc_dbase()
    except KeyError:
        findings.append(Finding(index, "NO_BC_DATABASE", "RF GIS names cannot be resolved without an active BC database"))
        return ()
    by_name = {str(key).casefold(): row for key, row in _db_rows(database)}
    rainfall: list[RainfallBoundary] = []
    for name, (f1, f2) in sorted(selected.items()):
        row = by_name.get(name.casefold())
        if row is None:
            findings.append(Finding(index, "RF_BC_MISSING", f"No active BC database row for RF name {name}"))
            continue
        data_sum: float | None = None
        try:
            resolved = database.value(name)
            # PyTUFLOW applies the DB time shift, multiplication and addition.
            column = _text(_row_value(row, "Column 2"))
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
            data_sum = sum(values)
        except (ValueError, TypeError, KeyError, OSError, AttributeError) as exc:
            findings.append(Finding(index, "RF_SERIES_UNRESOLVED", f"{name}: {exc}"))
        # GIS f1/f2 and TUFLOW global rainfall factors are recorded but NOT
        # conflated with a verified applied rainfall total.
        status = "bc-adjusted-only" if data_sum is not None else "unresolved"
        rainfall.append(
            RainfallBoundary(
                name=name,
                source=_text(_row_value(row, "Source")),
                column=_text(_row_value(row, "Column 2")),
                database_multiplier=_text(_row_value(row, "Mult Col 2")) or "1",
                database_addition=_text(_row_value(row, "Add Col 2")) or "0",
                gis_f1=f1,
                gis_f2=f2,
                bc_adjusted_sum_mm=data_sum,
                spatially_adjusted_sum_mm=None,
                status=status,
            )
        )
    if rainfall:
        findings.append(
            Finding(index, "SPATIAL_RF_UNVERIFIED", "GIS/global factors and effective cell-level rainfall are not yet verified")
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
                Finding(run.index, "SPATIAL_COMBINATION_UNRESOLVED", "Defined material/soil rows are not proof of active cell pairings")
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
        signature = tuple(
            sorted((b.name, b.database_multiplier, b.database_addition, b.gis_f1 or "", b.gis_f2 or "") for b in run.rainfall)
        )
        if run.rainfall:
            groups[signature].append(run.run_index)
    return {key: tuple(values) for key, values in groups.items()}
