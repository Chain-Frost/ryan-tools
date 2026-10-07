"""Evaluate circular culverts from a TUFLOW 1d_nwk GIS layer.

Input is a canonical SI 1d_nwk/1d_nwkb-compatible GIS layer. Output is a
long-form CSV of inverse HW/D capacity checks using either HY-8 or the native
ryan-culverts backend.

Example:
    python tuflow_culvert_from_1d_nwk.py model.gpkg --layer 1d_nwk --engine ryan-culverts
"""

from pathlib import Path

WRAPPER_VERSION = "2026-10-07.5"

import argparse
import csv
import hashlib
import shutil
from math import isfinite, pi, sqrt
from typing import Any, cast

import geopandas as gpd
from pandas import isna
from pandas.api.types import is_numeric_dtype

from ryan_library.classes.culvert import CulvertMaterialName
from ryan_library.functions.culvert.tuflow_engines import (
    CulvertEngine,
    CulvertEngineResult,
    TuflowCircularCulvert,
    solve_tuflow_culvert_inverse,
)
from ryan_library.functions.wrapper_utils import print_wrapper_banner

DIAMETER_FIELDS = ("Width_or_D", "Width_or_Diameter", "Width_or_Dia")
BARREL_FIELDS = ("Number_of", "num_barrels", "Barrels")
IGNORED_VALUES = frozenset({"T", "Y"})
AUTO_INVERT_SENTINEL = -99999.0
SOURCE_ROW_KEY = "__source_row__"
BLOCKAGE_NUMERIC_KEY = "__pblockage_numeric__"


def _float(row: dict[str, Any], key: str, default: float | None = None) -> float | None:
    raw = row.get(key)
    if raw is None or (isinstance(raw, str) and not raw.strip()) or bool(isna(raw)):
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        msg = f"{key} must contain a numeric value."
        raise ValueError(msg) from exc
    if not isfinite(value):
        msg = f"{key} must contain a finite numeric value."
        raise ValueError(msg)
    return value


def _text(row: dict[str, Any], key: str) -> str:
    raw = row.get(key)
    if raw is None or bool(isna(raw)):
        return ""
    return str(raw).strip()


def _first_float(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _float(row, key)
        if value is not None:
            return value
    return None


def _int_first(row: dict[str, Any], keys: tuple[str, ...], default: int = 1) -> int:
    for key in keys:
        if key not in row:
            continue
        raw = row.get(key)
        if raw is None or (isinstance(raw, str) and not raw.strip()) or bool(isna(raw)):
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError) as exc:
            msg = f"{key} must contain a numeric integer barrel count."
            raise ValueError(msg) from exc
        if not isfinite(value):
            msg = f"{key} must contain a finite integer barrel count."
            raise ValueError(msg)
        rounded = round(value)
        if abs(value - rounded) > 1e-9:
            msg = f"{key} must contain an integer barrel count."
            raise ValueError(msg)
        if key == "Number_of" and rounded == 0:
            return 1
        if rounded <= 0:
            msg = f"{key} must contain a strictly positive integer barrel count."
            raise ValueError(msg)
        return int(rounded)
    return default


def _blockage_percent(row: dict[str, Any]) -> float:
    raw = row.get("pBlockage")
    if raw is None or (isinstance(raw, str) and not raw.strip()) or bool(isna(raw)):
        if row.get(BLOCKAGE_NUMERIC_KEY) is False:
            msg = (
                "Blank categorical pBlockage is unresolved because TUFLOW may apply "
                "Blockage Default; resolve the effective event blockage before evaluation."
            )
            raise ValueError(msg)
        return 0.0
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        msg = (
            "pBlockage must be a numeric percentage for this workflow; "
            "category-based blockage must be resolved before evaluation."
        )
        raise ValueError(msg) from exc
    if not isfinite(value) or value < 0.0 or value >= 100.0:
        msg = "pBlockage must be at least 0 and less than 100 percent for this workflow."
        raise ValueError(msg)
    return value


def _headwater_ratios(values: list[float]) -> tuple[float, ...]:
    ratios = tuple(float(value) for value in values)
    if not ratios:
        msg = "--headwater-ratios requires at least one value."
        raise ValueError(msg)
    if any(not isfinite(value) or value <= 0.0 for value in ratios):
        msg = "--headwater-ratios values must be finite and strictly positive."
        raise ValueError(msg)
    if len(set(ratios)) != len(ratios):
        msg = "--headwater-ratios values must be unique."
        raise ValueError(msg)
    return ratios


def _seed_flow_hint(definition: TuflowCircularCulvert) -> float:
    area = pi * definition.diameter_m**2 / 4.0
    return max(area * definition.barrels, 0.05)


def _is_ignored(row: dict[str, Any]) -> bool:
    return _text(row, "Ignore").upper() in IGNORED_VALUES


def _require_metric_projected_crs(crs: Any) -> None:
    if crs is None or not bool(getattr(crs, "is_projected", False)):
        msg = "Geometry-derived Len_or_ANA requires a projected CRS with metre units."
        raise ValueError(msg)
    axes = tuple(getattr(crs, "axis_info", ()) or ())
    if not axes:
        msg = "Unable to confirm metre units for geometry-derived Len_or_ANA."
        raise ValueError(msg)
    for axis in axes[:2]:
        try:
            factor = float(axis.unit_conversion_factor)
        except (AttributeError, TypeError, ValueError) as exc:
            msg = "Unable to confirm metre units for geometry-derived Len_or_ANA."
            raise ValueError(msg) from exc
        if not isfinite(factor) or abs(factor - 1.0) > 1e-12:
            msg = "Geometry-derived Len_or_ANA requires CRS axis units in metres."
            raise ValueError(msg)


def _geometry_length(row: dict[str, Any]) -> float:
    geometry = row.get("geometry")
    try:
        length = float(getattr(geometry, "length", float("nan")))
    except AttributeError, TypeError, ValueError:
        length = float("nan")
    if not isfinite(length) or length <= 0.0:
        msg = "Negative Len_or_ANA requires a feature geometry with a positive digitized length."
        raise ValueError(msg)
    return length


def _resolve_layer(path: Path, requested: str | None) -> str | None:
    if requested:
        return requested
    if path.suffix.lower() != ".gpkg":
        return None
    layers = gpd.list_layers(path)
    names = [str(name).strip() for name in layers["name"].tolist() if str(name).strip()]
    candidates = [name for name in names if "1d_nwk" in name.lower()]
    if len(candidates) == 1:
        return candidates[0]
    if len(names) == 1:
        return names[0]
    if not names:
        msg = f"No layers found in GeoPackage: {path}."
        raise ValueError(msg)
    joined = ", ".join(names)
    msg = (
        "GeoPackage layer is ambiguous; pass --layer explicitly. "
        f"Available layers: {joined}"
    )
    raise ValueError(msg)


def _validate_unique_ids(rows: list[dict[str, Any]]) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for row in rows:
        identifier = _text(row, "ID")
        if not identifier:
            continue
        if identifier in seen:
            duplicates.add(identifier)
        seen.add(identifier)
    if duplicates:
        joined = ", ".join(sorted(duplicates))
        msg = f"TUFLOW culvert IDs must be unique; duplicate IDs: {joined}."
        raise ValueError(msg)


def _select_active_rows(rows: list[dict[str, Any]], crossing: str | None) -> list[dict[str, Any]]:
    selected = rows
    if crossing:
        selected = [row for row in rows if _text(row, "ID") == crossing]
        if not selected:
            msg = f"Crossing {crossing!r} was not found."
            raise ValueError(msg)
    active = [row for row in selected if not _is_ignored(row)]
    if not active:
        if crossing:
            msg = f"Crossing {crossing!r} is marked ignored in the TUFLOW 1d_nwk layer."
        else:
            msg = "No active culvert features remain after applying the TUFLOW Ignore field."
        raise ValueError(msg)
    return active


def _ensure_output_available(path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        msg = f"Output already exists: {path}. Pass --overwrite to replace it."
        raise FileExistsError(msg)


def _validate_supported_losses(row: dict[str, Any]) -> None:
    unsupported: list[str] = []

    form_loss = _float(row, "Form_Loss")
    if form_loss is not None and abs(form_loss) > 1e-12:
        unsupported.append(f"Form_Loss={form_loss:g}")

    width_contraction = _float(row, "WConF_or_WEx")
    if width_contraction is not None:
        tuflow_width_contraction = 1.0 if width_contraction <= 0.0 or width_contraction > 1.0 else width_contraction
        if abs(tuflow_width_contraction - 1.0) > 1e-12:
            unsupported.append(f"WConF_or_WEx={width_contraction:g}")

    entry_loss = _float(row, "EntryC_or_WSa")
    if entry_loss is not None and abs(entry_loss - 0.5) > 1e-12:
        unsupported.append(f"EntryC_or_WSa={entry_loss:g}")

    exit_loss = _float(row, "ExitC_or_WSb")
    if exit_loss is not None and abs(exit_loss - 1.0) > 1e-12:
        unsupported.append(f"ExitC_or_WSb={exit_loss:g}")

    if unsupported:
        joined = ", ".join(unsupported)
        msg = (
            "This migrated workflow supports only zero additional form loss, "
            "circular width-contraction factor 1.0, entry loss 0.5 and exit loss 1.0; "
            f"unsupported TUFLOW coefficients: {joined}."
        )
        raise ValueError(msg)


def _definition(row: dict[str, Any], source_row: int) -> TuflowCircularCulvert:
    source_type = _text(row, "Type").upper()
    if source_type != "C":
        msg = f"Unsupported TUFLOW Type {source_type or '<blank>'!r}; migrated workflow supports Type 'C'."
        raise ValueError(msg)
    nominal_diameter = _first_float(row, DIAMETER_FIELDS)
    length = _float(row, "Len_or_ANA")
    if length is not None and length < 0.0:
        length = _geometry_length(row)
    inlet = _float(row, "US_Invert")
    outlet = _float(row, "DS_Invert")
    if nominal_diameter is None or nominal_diameter <= 0.0:
        msg = f"Expected a positive diameter in one of: {', '.join(DIAMETER_FIELDS)}."
        raise ValueError(msg)
    blockage_percent = _blockage_percent(row)
    diameter = nominal_diameter * sqrt(1.0 - blockage_percent / 100.0)
    if length is None or length <= 0.0:
        msg = "Len_or_ANA must contain a positive culvert length."
        raise ValueError(msg)
    if inlet is None or outlet is None:
        msg = "US_Invert and DS_Invert are required."
        raise ValueError(msg)
    if inlet == AUTO_INVERT_SENTINEL or outlet == AUTO_INVERT_SENTINEL:
        msg = (
            "US_Invert/DS_Invert contains the unresolved TUFLOW -99999 sentinel; "
            "resolve effective inverts from processed TUFLOW data before evaluation."
        )
        raise ValueError(msg)
    _validate_supported_losses(row)
    roughness = _float(row, "n_nF_Cd")
    if roughness is None or roughness <= 0.0:
        msg = "n_nF_Cd must contain a positive Manning roughness for a Type C culvert."
        raise ValueError(msg)
    barrels = _int_first(row, BARREL_FIELDS)
    name = _text(row, "ID")
    if not name:
        msg = f"ID is required for TUFLOW culvert source row {source_row}."
        raise ValueError(msg)
    return TuflowCircularCulvert(
        name=name,
        diameter_m=diameter,
        length_m=length,
        inlet_invert_m=inlet,
        outlet_invert_m=outlet,
        roughness_manning_n=roughness,
        barrels=barrels,
        material=CulvertMaterialName.CONCRETE_PIPE,
        nominal_diameter_m=nominal_diameter,
    )


def _record(
    result: CulvertEngineResult | None,
    *,
    source: str,
    source_row: int,
    crossing: str,
    scenario: str,
    engine: CulvertEngine,
    error: str = "",
) -> dict[str, str | int | float | None]:
    if result is None:
        return {
            "Source": source,
            "Source Row": source_row,
            "Crossing": crossing,
            "Scenario": scenario,
            "Engine": engine.value,
            "Requested Flow (m3/s)": None,
            "Requested Headwater (m)": None,
            "Computed Flow (m3/s)": None,
            "Headwater Elevation (m)": None,
            "HW:D": None,
            "Outlet Velocity (m/s)": None,
            "Flow Type": "",
            "Roadway Discharge (m3/s)": None,
            "Overtopping": "",
            "Status": "failed",
            "Warnings": "",
            "Workspace": "",
            "Error": error,
        }
    return {
        "Source": source,
        "Source Row": source_row,
        "Crossing": crossing,
        "Scenario": result.scenario,
        "Engine": result.engine.value,
        "Requested Flow (m3/s)": result.requested_discharge_m3s,
        "Requested Headwater (m)": result.requested_headwater_m,
        "Computed Flow (m3/s)": result.computed_discharge_m3s,
        "Headwater Elevation (m)": result.headwater_elevation_m,
        "HW:D": result.headwater_ratio,
        "Outlet Velocity (m/s)": result.outlet_velocity_mps,
        "Flow Type": result.flow_type,
        "Roadway Discharge (m3/s)": result.roadway_discharge_m3s,
        "Overtopping": result.overtopping,
        "Status": result.status,
        "Warnings": ";".join(result.warnings),
        "Workspace": str(result.workspace or ""),
        "Error": "",
    }


def _workspace(
    root: Path | None,
    crossing: str,
    scenario: str,
    *,
    overwrite: bool,
) -> Path | None:
    if root is None:
        return None
    run_key = f"{crossing}_{scenario}"
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in run_key).strip(" ._")
    safe = (safe or "run")[:120].rstrip(" ._")
    digest = hashlib.sha256(run_key.encode("utf-8")).hexdigest()[:12]
    path = root / f"{safe}__{digest}"
    if path.exists():
        if not overwrite:
            msg = f"HY-8 workspace already exists: {path}. Pass --overwrite to replace it."
            raise FileExistsError(msg)
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=False)
    return path


def run(args: argparse.Namespace) -> int:
    print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
    engine = CulvertEngine(args.engine)
    _ensure_output_available(args.output_csv, overwrite=args.overwrite)
    # GeoPandas stubs leave backend keyword arguments untyped.
    layer = _resolve_layer(args.input_gis, args.layer)
    frame = gpd.read_file(args.input_gis, layer=layer)  # pyright: ignore[reportUnknownMemberType]
    if frame.empty:
        msg = f"No features found in {args.input_gis}."
        raise ValueError(msg)
    frame = frame.where(frame.notna(), None)
    rows = cast("list[dict[str, Any]]", frame.to_dict(orient="records"))
    blockage_is_numeric = "pBlockage" not in frame.columns or is_numeric_dtype(frame["pBlockage"].dtype)
    for source_row, row in enumerate(rows, start=1):
        row[SOURCE_ROW_KEY] = source_row
        row[BLOCKAGE_NUMERIC_KEY] = blockage_is_numeric
    rows = _select_active_rows(rows, args.crossing)
    _validate_unique_ids(rows)
    uses_geometry_length = any(
        (value := _float(row, "Len_or_ANA")) is not None and value < 0.0 for row in rows
    )
    if uses_geometry_length:
        _require_metric_projected_crs(getattr(frame, "crs", None))
    headwater_ratios = _headwater_ratios(args.headwater_ratios)

    workspace_root: Path | None = args.workspace
    if engine is CulvertEngine.HY8 and args.keep_workspace and workspace_root is None:
        workspace_root = Path("hy8-workspaces")
    if engine is CulvertEngine.HY8 and workspace_root is not None:
        workspace_root.mkdir(parents=True, exist_ok=True)

    output_rows: list[dict[str, str | int | float | None]] = []
    had_failures = False
    for row in rows:
        source_row = int(row[SOURCE_ROW_KEY])
        source = str(args.input_gis)
        fallback_name = _text(row, "ID") or f"<source row {source_row}>"
        try:
            definition = _definition(row, source_row)
            q_hint = _seed_flow_hint(definition)
            for ratio in headwater_ratios:
                scenario = f"HW:D = {ratio:g}"
                target = definition.inlet_invert_m + ratio * definition.hw_diameter_m
                try:
                    work = (
                        _workspace(
                            workspace_root,
                            definition.name,
                            scenario,
                            overwrite=args.overwrite,
                        )
                        if engine is CulvertEngine.HY8
                        else None
                    )
                    result = solve_tuflow_culvert_inverse(
                        definition,
                        scenario=scenario,
                        headwater_elevation_m=target,
                        tailwater_elevation_m=definition.outlet_invert_m,
                        engine=engine,
                        q_hint_m3s=q_hint,
                        hy8=args.hy8_exe,
                        workspace=work,
                        keep_workspace=args.keep_workspace,
                    )
                    if result.status.strip().lower() == "unresolved":
                        had_failures = True
                    output_rows.append(
                        _record(
                            result,
                            source=source,
                            source_row=source_row,
                            crossing=definition.name,
                            scenario=scenario,
                            engine=engine,
                        )
                    )
                except Exception as exc:
                    had_failures = True
                    output_rows.append(
                        _record(
                            None,
                            source=source,
                            source_row=source_row,
                            crossing=definition.name,
                            scenario=scenario,
                            engine=engine,
                            error=str(exc),
                        )
                    )
        except Exception as exc:
            had_failures = True
            output_rows.append(
                _record(
                    None,
                    source=source,
                    source_row=source_row,
                    crossing=fallback_name,
                    scenario="input mapping",
                    engine=engine,
                    error=str(exc),
                )
            )

    if not output_rows:
        msg = "No output rows were produced."
        raise ValueError(msg)
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(output_rows[0]))
        writer.writeheader()
        writer.writerows(output_rows)
    return 1 if had_failures else 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_gis", type=Path)
    parser.add_argument("--layer")
    parser.add_argument("--output-csv", type=Path, default=Path("tuflow-1d-nwk-culvert-results.csv"))
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--crossing")
    parser.add_argument("--engine", choices=[item.value for item in CulvertEngine], default=CulvertEngine.HY8.value)
    parser.add_argument("--headwater-ratios", type=float, nargs="+", default=[1.5, 2.0])
    parser.add_argument("--hy8-exe", type=Path)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--keep-workspace", action="store_true")
    return parser


if __name__ == "__main__":
    args = _parser().parse_args()
    try:
        exit_code = run(args)
    except Exception as exc:
        print(f"Evaluation failed: {exc}")
        exit_code = 1
    finally:
        print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION, leading_blank_line=True)
    raise SystemExit(exit_code)
