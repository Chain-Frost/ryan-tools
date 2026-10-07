"""Evaluate circular culverts from a TUFLOW Maximums workbook.

Input is a processed Maximums Excel workbook containing merged hydraulic and
culvert-geometry attributes. Output is a long-form CSV of forward and inverse
checks using either HY-8 or the native ryan-culverts backend.

Example:
    python tuflow_culvert_from_maximums.py maximums.xlsx --engine ryan-culverts
"""

from pathlib import Path

WRAPPER_VERSION = "2026-10-08.1"

import argparse
import csv
import hashlib
import shutil
from math import isfinite, sqrt
from typing import Any, cast

import pandas as pd

from ryan_library.classes.culvert import CulvertMaterialName
from ryan_library.functions.culvert.tuflow_engines import (
    CulvertEngine,
    CulvertEngineResult,
    TuflowCircularCulvert,
    solve_tuflow_culvert_forward,
    solve_tuflow_culvert_inverse,
)
from ryan_library.functions.wrapper_utils import print_wrapper_banner


def _float(row: dict[str, Any], key: str, default: float | None = None) -> float | None:
    raw = row.get(key)
    if raw is None or (isinstance(raw, str) and not raw.strip()) or bool(pd.isna(raw)):
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


def _blockage_percent(row: dict[str, Any]) -> float:
    raw = row.get("pBlockage")
    if raw is None or (isinstance(raw, str) and not raw.strip()) or bool(pd.isna(raw)):
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


def _headwater_ratio(value: float) -> float:
    ratio = float(value)
    if not isfinite(ratio) or ratio <= 0.0:
        msg = "--headwater-ratio must be finite and strictly positive."
        raise ValueError(msg)
    return ratio


def _run_identity(row: dict[str, Any]) -> str:
    """Return the comparable base run identity retained in Maximums exports."""
    trim_runcode = str(row.get("trim_runcode") or "").strip()
    if trim_runcode:
        return trim_runcode
    return str(row.get("internalName") or "").strip()


def _material_from_roughness(roughness: float) -> CulvertMaterialName:
    """Infer the supported circular material from the source Manning roughness."""
    if roughness <= 0.013:
        return CulvertMaterialName.CONCRETE_PIPE
    if roughness >= 0.016:
        return CulvertMaterialName.CORRUGATED_STEEL
    msg = (
        "Manning roughness is in the ambiguous 0.013-0.016 range where concrete, "
        "smooth HDPE and small CSP cannot be distinguished reliably from TUFLOW output alone."
    )
    raise ValueError(msg)


def _int_first(row: dict[str, Any], keys: tuple[str, ...]) -> int:
    for key in keys:
        if key not in row:
            continue
        raw = row.get(key)
        if raw is None or (isinstance(raw, str) and not raw.strip()) or bool(pd.isna(raw)):
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
        if abs(value - rounded) > 1e-9 or rounded <= 0:
            msg = f"{key} must contain a strictly positive integer barrel count."
            raise ValueError(msg)
        return int(rounded)
    joined = ", ".join(keys)
    msg = f"A barrel count is required in one of: {joined}."
    raise ValueError(msg)


def _definition(row: dict[str, Any]) -> TuflowCircularCulvert:
    source_type = str(row.get("Type") or "").strip().upper()
    if source_type != "C":
        msg = f"Unsupported Maximums Type value {source_type or '<blank>'!r}; this migrated workflow supports circular Type 'C' rows only."
        raise ValueError(msg)
    nominal_diameter = _float(row, "Height")
    if nominal_diameter is None or nominal_diameter <= 0.0:
        msg = "Height must contain a positive circular culvert diameter."
        raise ValueError(msg)
    blockage_percent = _blockage_percent(row)
    diameter = nominal_diameter * sqrt(1.0 - blockage_percent / 100.0)
    length = _float(row, "Length")
    inlet = _float(row, "US Invert")
    outlet = _float(row, "DS Invert")
    if length is None or inlet is None or outlet is None:
        msg = "Length, US Invert and DS Invert are required Maximums geometry fields."
        raise ValueError(msg)
    roughness = _float(row, "n or Cd")
    if roughness is None or roughness <= 0.0:
        msg = "n or Cd must contain a positive Manning roughness for circular culverts."
        raise ValueError(msg)
    return TuflowCircularCulvert(
        name=str(row.get("Chan ID") or "").strip(),
        diameter_m=diameter,
        length_m=length,
        inlet_invert_m=inlet,
        outlet_invert_m=outlet,
        roughness_manning_n=roughness,
        barrels=_int_first(row, ("Num_barrels", "num_barrels", "Barrels")),
        material=_material_from_roughness(roughness),
        nominal_diameter_m=nominal_diameter,
    )


def _record(
    result: CulvertEngineResult | None,
    *,
    source: str,
    crossing: str,
    run: str,
    aep: str,
    scenario: str,
    engine: CulvertEngine,
    error: str = "",
) -> dict[str, str | float | None]:
    if result is None:
        return {
            "Source": source,
            "Crossing": crossing,
            "Run": run,
            "AEP": aep,
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
        "Crossing": crossing,
        "Run": run,
        "AEP": aep,
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


def _selected_rows(path: Path, sheet_name: str, crossing: str | None) -> list[dict[str, Any]]:
    # Pandas stubs include optional workbook types without complete typing.
    frame = pd.read_excel(path, sheet_name=sheet_name)  # pyright: ignore[reportUnknownMemberType]
    required = {"Chan ID", "Q", "Height", "Type"}
    missing = required - set(frame.columns)
    if missing:
        msg = f"Missing required Maximums columns: {', '.join(sorted(missing))}"
        raise ValueError(msg)
    frame["Chan ID"] = frame["Chan ID"].fillna("").astype(str).str.strip()
    frame = frame[frame["Chan ID"].astype(bool)]
    if crossing is not None:
        frame = frame[frame["Chan ID"] == crossing]
    frame["Q"] = pd.to_numeric(frame["Q"], errors="coerce")
    frame = frame[frame["Q"] > 0.0]
    if frame.empty:
        msg = "No positive-flow Maximums rows matched the selection."
        raise ValueError(msg)
    if "aep_text" not in frame.columns:
        frame["aep_text"] = ""
    if "trim_runcode" not in frame.columns:
        frame["trim_runcode"] = ""
    if "internalName" not in frame.columns:
        frame["internalName"] = ""
    frame["aep_text"] = frame["aep_text"].fillna("").astype(str).str.strip()
    frame["trim_runcode"] = frame["trim_runcode"].fillna("").astype(str).str.strip()
    frame["internalName"] = frame["internalName"].fillna("").astype(str).str.strip()
    frame["Run"] = frame["trim_runcode"]
    missing_run = ~frame["Run"].astype(bool)
    frame.loc[missing_run, "Run"] = frame.loc[missing_run, "internalName"]
    frame = frame.sort_values(["Chan ID", "Run", "aep_text", "Q"], ascending=[True, True, True, False])
    frame = frame.drop_duplicates(subset=["Chan ID", "Run", "aep_text"], keep="first")
    records = frame.where(pd.notna(frame), None).to_dict(orient="records")
    return cast("list[dict[str, Any]]", records)


def _ensure_output_available(path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        msg = f"Output already exists: {path}. Pass --overwrite to replace it."
        raise FileExistsError(msg)


def _workspace(
    root: Path | None,
    crossing: str,
    run: str,
    aep: str,
    scenario: str,
    *,
    overwrite: bool,
) -> Path | None:
    if root is None:
        return None
    run_key = f"{crossing}_{run or 'no-run'}_{aep or 'no-aep'}_{scenario}"
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
    headwater_ratio = _headwater_ratio(args.headwater_ratio)
    rows = _selected_rows(args.input_workbook, args.sheet_name, args.crossing)
    workspace_root: Path | None = args.workspace
    if engine is CulvertEngine.HY8 and args.keep_workspace and workspace_root is None:
        workspace_root = Path("hy8-workspaces")
    if engine is CulvertEngine.HY8 and workspace_root is not None:
        workspace_root.mkdir(parents=True, exist_ok=True)

    output_rows: list[dict[str, str | float | None]] = []
    had_failures = False
    for row in rows:
        crossing = str(row.get("Chan ID") or "").strip()
        run_identity = _run_identity(row)
        aep = str(row.get("aep_text") or "").strip()
        source = str(args.input_workbook)
        try:
            definition = _definition(row)
            flow = _float(row, "Q")
            if flow is None:
                msg = "Q must contain a positive finite discharge after row selection."
                raise ValueError(msg)
            ds_invert = definition.outlet_invert_m
            ds_headwater = _float(row, "DS_h", ds_invert)
            if ds_headwater is None:
                ds_headwater = ds_invert
            cases: list[tuple[str, str, float, float]] = [
                ("forward", "Q @ DS_h TW", flow, ds_headwater),
                ("forward", "Q @ invert TW", flow, ds_invert),
            ]
            us_headwater = _float(row, "US_h")
            if us_headwater is not None:
                cases.append(("inverse", "HW = US_h", us_headwater, ds_headwater))
            ratio_headwater = definition.inlet_invert_m + headwater_ratio * definition.hw_diameter_m
            cases.append(("inverse", f"HW:D = {headwater_ratio:g}", ratio_headwater, ds_invert))

            for mode, scenario, value, tailwater in cases:
                try:
                    work = (
                        _workspace(
                            workspace_root,
                            crossing,
                            run_identity,
                            aep,
                            scenario,
                            overwrite=args.overwrite,
                        )
                        if engine is CulvertEngine.HY8
                        else None
                    )
                    if mode == "forward":
                        result = solve_tuflow_culvert_forward(
                            definition,
                            scenario=scenario,
                            discharge_m3s=value,
                            tailwater_elevation_m=tailwater,
                            engine=engine,
                            hy8=args.hy8_exe,
                            workspace=work,
                            keep_workspace=args.keep_workspace,
                        )
                    else:
                        result = solve_tuflow_culvert_inverse(
                            definition,
                            scenario=scenario,
                            headwater_elevation_m=value,
                            tailwater_elevation_m=tailwater,
                            engine=engine,
                            q_hint_m3s=flow,
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
                            crossing=crossing,
                            run=run_identity,
                            aep=aep,
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
                            crossing=crossing,
                            run=run_identity,
                            aep=aep,
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
                    crossing=crossing or "<unknown>",
                    run=run_identity,
                    aep=aep,
                    scenario="input mapping",
                    engine=engine,
                    error=str(exc),
                )
            )

    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    with args.output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(output_rows[0]))
        writer.writeheader()
        writer.writerows(output_rows)
    return 1 if had_failures else 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input_workbook", type=Path)
    parser.add_argument("--sheet-name", default="Maximums")
    parser.add_argument("--output-csv", type=Path, default=Path("tuflow-culvert-results.csv"))
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--crossing")
    parser.add_argument("--engine", choices=[item.value for item in CulvertEngine], default=CulvertEngine.HY8.value)
    parser.add_argument("--headwater-ratio", type=float, default=1.5)
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
