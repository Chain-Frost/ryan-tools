"""Evaluate TUFLOW Maximums culverts with HY-8 or ryan-culverts."""

import argparse
import csv
from math import isfinite
from pathlib import Path
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

DEFAULT_N = 0.024
DEFAULT_LENGTH_M = 30.0
DEFAULT_INLET_INVERT_M = 0.15
DEFAULT_OUTLET_INVERT_M = 0.0


def _float(row: dict[str, Any], key: str, default: float | None = None) -> float | None:
    try:
        value = float(row.get(key))
    except (TypeError, ValueError):
        return default
    return value if isfinite(value) else default


def _int_first(row: dict[str, Any], keys: tuple[str, ...], default: int = 1) -> int:
    for key in keys:
        value = _float(row, key)
        if value is not None:
            rounded = round(value)
            if abs(value - rounded) > 1e-9 or rounded <= 0:
                msg = f"{key} must contain a strictly positive integer barrel count."
                raise ValueError(msg)
            return int(rounded)
    return default


def _definition(row: dict[str, Any]) -> TuflowCircularCulvert:
    source_type = str(row.get("Flags") or "").strip().upper()
    if source_type != "C":
        msg = f"Unsupported Maximums Flags value {source_type or '<blank>'!r}; this migrated workflow supports circular 'C' rows only."
        raise ValueError(msg)
    diameter = _float(row, "Height")
    if diameter is None or diameter <= 0.0:
        msg = "Height must contain a positive circular culvert diameter."
        raise ValueError(msg)
    length = _float(row, "Length", DEFAULT_LENGTH_M)
    inlet = _float(row, "US Invert", DEFAULT_INLET_INVERT_M)
    outlet = _float(row, "DS Invert", DEFAULT_OUTLET_INVERT_M)
    roughness = _float(row, "n or Cd", DEFAULT_N)
    return TuflowCircularCulvert(
        name=str(row.get("Chan ID") or "").strip(),
        diameter_m=diameter,
        length_m=DEFAULT_LENGTH_M if length is None else length,
        inlet_invert_m=DEFAULT_INLET_INVERT_M if inlet is None else inlet,
        outlet_invert_m=DEFAULT_OUTLET_INVERT_M if outlet is None else outlet,
        roughness_manning_n=DEFAULT_N if roughness is None else roughness,
        barrels=_int_first(row, ("Num_barrels", "num_barrels", "Barrels")),
        material=CulvertMaterialName.CORRUGATED_STEEL,
    )


def _record(
    result: CulvertEngineResult | None,
    *,
    source: str,
    crossing: str,
    aep: str,
    scenario: str,
    engine: CulvertEngine,
    error: str = "",
) -> dict[str, str | float | None]:
    if result is None:
        return {
            "Source": source,
            "Crossing": crossing,
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
    frame = pd.read_excel(path, sheet_name=sheet_name)
    required = {"Chan ID", "Q", "Height", "Flags"}
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
    frame["aep_text"] = frame["aep_text"].fillna("").astype(str).str.strip()
    frame = frame.sort_values(["Chan ID", "aep_text", "Q"], ascending=[True, True, False])
    frame = frame.drop_duplicates(subset=["Chan ID", "aep_text"], keep="first")
    records = frame.where(pd.notna(frame), None).to_dict(orient="records")
    return cast("list[dict[str, Any]]", records)


def _ensure_output_available(path: Path, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        msg = f"Output already exists: {path}. Pass --overwrite to replace it."
        raise FileExistsError(msg)


def _workspace(root: Path | None, crossing: str, scenario: str) -> Path | None:
    if root is None:
        return None
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in f"{crossing}_{scenario}")
    path = root / safe
    path.mkdir(parents=True, exist_ok=True)
    return path


def run(args: argparse.Namespace) -> int:
    engine = CulvertEngine(args.engine)
    _ensure_output_available(args.output_csv, overwrite=args.overwrite)
    rows = _selected_rows(args.input_workbook, args.sheet_name, args.crossing)
    workspace_root: Path | None = args.workspace
    if args.keep_workspace and workspace_root is None:
        workspace_root = Path("hy8-workspaces")
    if workspace_root is not None:
        workspace_root.mkdir(parents=True, exist_ok=True)

    output_rows: list[dict[str, str | float | None]] = []
    had_failures = False
    for row in rows:
        crossing = str(row.get("Chan ID") or "").strip()
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
            ratio_headwater = definition.inlet_invert_m + args.headwater_ratio * definition.diameter_m
            cases.append(("inverse", f"HW:D = {args.headwater_ratio:g}", ratio_headwater, ds_invert))

            for mode, scenario, value, tailwater in cases:
                try:
                    work = _workspace(workspace_root, crossing, scenario)
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
                    output_rows.append(
                        _record(result, source=source, crossing=crossing, aep=aep, scenario=scenario, engine=engine)
                    )
                except Exception as exc:
                    had_failures = True
                    output_rows.append(
                        _record(
                            None,
                            source=source,
                            crossing=crossing,
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
    raise SystemExit(run(_parser().parse_args()))
