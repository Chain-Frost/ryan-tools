"""Experimental nested frequency storms from saved inputs.

See docs/work/2026-11-28-nested-frequency-storms.md. No network or model execution.
"""

from __future__ import annotations

from pathlib import Path

SCRIPT_VERSION = "2026.11.28.2"
CONFIG: Path | None = None
OUTPUT: Path | None = None
KIND = "gross"
BOUNDARY_NAME: str | None = None
WRITE = False

import argparse
import json
from math import fsum
from typing import Any, Literal, cast

from ryan_library.functions.hydrology.nested_storm_sources import (
    ArrResponse,
    apply_arr_loss_factors,
    arr_preburst_ratio,
    curve_from_arr_response,
    curve_from_bom_file,
    read_arr_response,
)
from ryan_library.functions.hydrology.nested_storm_tuflow import write_tuflow_bundle
from ryan_library.functions.hydrology.nested_storms import (
    DepthAnchor,
    EventLosses,
    RainfallCurve,
    StormResult,
    generate_storm,
    write_tuflow_csv,
)
from ryan_library.functions.hydrology.rainfall_loss_screening import LossSpec


def _source_curve(  # noqa: C901 - dispatch source-specific contracts
    raw: dict[str, Any], directory: Path, cache: dict[Path, ArrResponse]
) -> tuple[RainfallCurve, ArrResponse | None, tuple[int, str] | None]:
    if "curve" in raw:
        if "source" in raw:
            msg = "Specify curve or source, not both"
            raise ValueError(msg)
        data: dict[str, Any] = dict(raw["curve"])
        data["anchors"] = tuple(DepthAnchor(**anchor) for anchor in data["anchors"])
        return RainfallCurve(**data), None, None
    settings = cast("dict[str, Any]", raw["source"])
    response: ArrResponse | None = None
    response_path: Path | None = None
    if "arr_response" in settings:
        response_path = (directory / Path(settings["arr_response"])).absolute()
        if response_path not in cache:
            cache[response_path] = read_arr_response(response_path)
        response = cache[response_path]
    climate = None
    if "climate" in settings:
        year, ssp = settings["climate"]
        climate = (int(year), str(ssp))
    area = float(settings.get("area_km2", 0))
    override = settings.get("arf_override")
    if settings["kind"] == "arr":
        if response is None or response_path is None:
            msg = "ARR source requires saved arr_response"
            raise ValueError(msg)
        curve = curve_from_arr_response(
            response,
            settings["aep"],
            source=str(response_path),
            baseline_year=int(settings.get("baseline_year", 2030)),
            climate=climate,
            area_km2=area,
            arf_override=override,
        )
    elif settings["kind"] == "bom":
        if climate is not None:
            msg = "BoM source requires explicit rainfall factors, not ARR baseline/SSP selection"
            raise ValueError(msg)
        factors = settings.get("rainfall_factors")
        curve = curve_from_bom_file(
            (directory / Path(settings["path"])).absolute(),
            settings["aep"],
            response=response,
            area_km2=area,
            arf_override=override,
            rainfall_factors={int(k): float(v) for k, v in factors.items()} if factors is not None else None,
            scenario=settings.get("scenario"),
        )
    else:
        msg = "Source kind must be arr or bom"
        raise ValueError(msg)
    return curve, response, climate


def _loss_options(
    data: dict[str, Any],
    response: ArrResponse | None,
    climate: tuple[int, str] | None,
    curve: RainfallCurve,
    duration: int,
) -> EventLosses:
    data = dict(data)
    data["spec"] = LossSpec(**data["spec"])
    if "preburst_increments_mm" in data:
        data["preburst_increments_mm"] = tuple(data["preburst_increments_mm"])
    percentile = data.pop("arr_preburst_percentile", None)
    if percentile is not None:
        if response is None or "preburst_ratio" in data or "preburst_depth_mm" in data:
            msg = "ARR percentile requires response and no additional PB depth/ratio"
            raise ValueError(msg)
        data["preburst_ratio"] = arr_preburst_ratio(response, curve.aep, duration, percentile=percentile)
    adjust = data.pop("arr_climate_loss_factors", False)
    losses = EventLosses(**data)
    if adjust:
        if response is None or climate is None:
            msg = "ARR climate loss factors require saved ARR response and selected scenario"
            raise ValueError(msg)
        losses = apply_arr_loss_factors(response, losses, climate)
    return losses


def _event(raw: dict[str, Any], directory: Path, cache: dict[Path, ArrResponse]) -> StormResult:
    curve, response, climate = _source_curve(raw, directory, cache)
    options: dict[str, Any] = dict(raw.get("options", {}))
    duration = raw["duration_min"]
    if "losses" in options:
        options["losses"] = _loss_options(options["losses"], response, climate, curve, duration)
    return generate_storm(curve, duration, **options)


def _read_events(path: Path) -> dict[str, StormResult]:
    rows: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or not rows:
        msg = "Config must be a nonempty JSON event list"
        raise ValueError(msg)
    storms: dict[str, StormResult] = {}
    cache: dict[Path, ArrResponse] = {}
    for raw in cast("list[dict[str, Any]]", rows):
        name = str(raw["name"])
        if name in storms:
            msg = f"Duplicate event name: {name}"
            raise ValueError(msg)
        storms[name] = _event(raw, path.parent, cache)
    return storms


def _report(storms: dict[str, StormResult], kind: str) -> None:
    print(f"Nested storms {SCRIPT_VERSION} (experimental; {kind} rainfall)")
    for name, storm in storms.items():
        if kind == "excess" and storm.excess_mm is None:
            msg = f"No loss selection for {name}"
            raise ValueError(msg)
        print(
            f"{name}: source={storm.curve.source}, AEP={storm.curve.aep}, scenario={storm.curve.scenario}, "
            f"burst={storm.burst_total_mm:g} mm, total={fsum(storm.gross_mm):g} mm, "
            f"PB={storm.preburst_mm:g} mm, IL/CL={storm.applied_losses}, ARF override={storm.arf_override}"
        )
        for caution in storm.curve.cautions:
            print(f"  Caution: {caution}")
        for check in storm.depth_checks:
            print(f"  {check.duration_min} min: target={check.target_mm:g}, max={check.maximum_rolling_mm:g} mm")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=SCRIPT_VERSION)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--kind", choices=("gross", "excess"), default=KIND)
    parser.add_argument("--boundary-name", default=BOUNDARY_NAME)
    parser.add_argument("--write", action=argparse.BooleanOptionalAction, default=WRITE)
    args = parser.parse_args()
    try:
        if args.config is None or (args.write and args.output is None):
            msg = "Supply --config and, with --write, --output"
            raise ValueError(msg)
        storms = _read_events(Path(args.config).absolute())
        kind = cast('Literal["gross", "excess"]', args.kind)
        _report(storms, kind)
        if not args.write:
            print("Preview only; no files created.")
        elif args.boundary_name is not None:
            write_tuflow_bundle(Path(args.output), storms, boundary_name=args.boundary_name, kind=kind)
        else:
            write_tuflow_csv(Path(args.output), storms, kind=kind)
    except Exception as exc:  # noqa: BLE001 - entry-point boundary
        parser.exit(1, f"Nested storm generation failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
