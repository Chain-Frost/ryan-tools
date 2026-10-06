"""Evaluate TUFLOW 1d_nwk circular culverts with HY-8 or ryan-culverts."""

import argparse
import csv
from math import isfinite
from pathlib import Path
from typing import Any

import geopandas as gpd

from ryan_library.classes.culvert import CulvertMaterialName
from ryan_library.functions.culvert.tuflow_engines import (
    CulvertEngine,
    CulvertEngineResult,
    TuflowCircularCulvert,
    solve_tuflow_culvert_inverse,
)

DIAMETER_FIELDS = ("Width_or_D", "Width_or_Diameter", "Width_or_Dia")
BARREL_FIELDS = ("Number_of", "num_barrels", "Barrels")
DEFAULT_N = 0.024


def _float(row: dict[str, Any], key: str, default: float | None = None) -> float | None:
    try:
        value = float(row.get(key))
    except (TypeError, ValueError):
        return default
    return value if isfinite(value) else default


def _first_float(row: dict[str, Any], keys: tuple[str, ...]) -> float | None:
    for key in keys:
        value = _float(row, key)
        if value is not None:
            return value
    return None


def _definition(row: dict[str, Any], source_row: int) -> TuflowCircularCulvert:
    source_type = str(row.get("Type") or "").strip().upper()
    if source_type != "C":
        raise ValueError(f"Unsupported TUFLOW Type {source_type or '<blank>'!r}; migrated workflow supports Type 'C'.")
    diameter = _first_float(row, DIAMETER_FIELDS)
    length = _float(row, "Len_or_ANA")
    inlet = _float(row, "US_Invert")
    outlet = _float(row, "DS_Invert")
    if diameter is None or diameter <= 0.0:
        raise ValueError(f"Expected a positive diameter in one of: {', '.join(DIAMETER_FIELDS)}.")
    if length is None or length <= 0.0:
        raise ValueError("Len_or_ANA must contain a positive culvert length.")
    if inlet is None or outlet is None:
        raise ValueError("US_Invert and DS_Invert are required.")
    roughness = _float(row, "n_nF_Cd", DEFAULT_N) or DEFAULT_N
    barrels_raw = _first_float(row, BARREL_FIELDS)
    barrels = 1 if barrels_raw is None else max(1, int(barrels_raw))
    name = str(row.get("ID") or "").strip() or f"culvert_{source_row:04d}"
    return TuflowCircularCulvert(
        name=name,
        diameter_m=diameter,
        length_m=length,
        inlet_invert_m=inlet,
        outlet_invert_m=outlet,
        roughness_manning_n=roughness,
        barrels=barrels,
        material=CulvertMaterialName.CONCRETE_PIPE,
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


def _workspace(root: Path | None, crossing: str, scenario: str) -> Path | None:
    if root is None:
        return None
    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in f"{crossing}_{scenario}")
    path = root / safe
    path.mkdir(parents=True, exist_ok=True)
    return path


def run(args: argparse.Namespace) -> int:
    engine = CulvertEngine(args.engine)
    kwargs: dict[str, str] = {}
    if args.layer:
        kwargs["layer"] = args.layer
    frame = gpd.read_file(args.input_gis, **kwargs)
    if frame.empty:
        raise ValueError(f"No features found in {args.input_gis}.")
    frame = frame.where(frame.notna(), None)
    rows: list[dict[str, Any]] = frame.to_dict(orient="records")
    if args.crossing:
        rows = [row for row in rows if str(row.get("ID") or "").strip() == args.crossing]
        if not rows:
            raise ValueError(f"Crossing {args.crossing!r} was not found.")

    workspace_root: Path | None = args.workspace
    if args.keep_workspace and workspace_root is None:
        workspace_root = Path("hy8-workspaces")
    if workspace_root is not None:
        workspace_root.mkdir(parents=True, exist_ok=True)

    output_rows: list[dict[str, str | int | float | None]] = []
    had_failures = False
    for source_row, row in enumerate(rows, start=1):
        source = str(args.input_gis)
        fallback_name = str(row.get("ID") or "").strip() or f"culvert_{source_row:04d}"
        try:
            definition = _definition(row, source_row)
            q_hint = max(definition.diameter_m**2 * definition.barrels, 0.05)
            for ratio in args.headwater_ratios:
                scenario = f"HW:D = {ratio:g}"
                target = definition.inlet_invert_m + ratio * definition.diameter_m
                try:
                    result = solve_tuflow_culvert_inverse(
                        definition,
                        scenario=scenario,
                        headwater_elevation_m=target,
                        tailwater_elevation_m=definition.outlet_invert_m,
                        engine=engine,
                        q_hint_m3s=q_hint,
                        hy8=args.hy8_exe,
                        workspace=_workspace(workspace_root, definition.name, scenario),
                        keep_workspace=args.keep_workspace,
                    )
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
        raise ValueError("No output rows were produced.")
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
    parser.add_argument("--crossing")
    parser.add_argument("--engine", choices=[item.value for item in CulvertEngine], default=CulvertEngine.HY8.value)
    parser.add_argument("--headwater-ratios", type=float, nargs="+", default=[1.5, 2.0])
    parser.add_argument("--hy8-exe", type=Path)
    parser.add_argument("--workspace", type=Path)
    parser.add_argument("--keep-workspace", action="store_true")
    return parser


if __name__ == "__main__":
    raise SystemExit(run(_parser().parse_args()))
