"""Experimental nested frequency storms from saved inputs or explicit live ARR requests.

See docs/work/2026-11-28-nested-frequency-storms.md. No model execution.
"""

from __future__ import annotations

from pathlib import Path

SCRIPT_VERSION = "2026.11.28.3"
CONFIG: Path | None = None
OUTPUT: Path | None = None
KIND = "gross"
BOUNDARY_NAME: str | None = None
WRITE = False
ALLOW_NETWORK = False

import argparse
import json
from math import fsum
from typing import Any, Literal, cast

from ryan_library.functions.hydrology.nested_storm_sources import (
    ArrResponse,
    apply_arr_loss_factors,
    constant_preburst_increments,
    curve_from_arr_response,
    curve_from_bom_file,
    fetch_arr_response,
    read_arr_response,
    regional_storm_losses,
    selected_preburst_ratio,
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

ResponseCache = dict[Path | tuple[float, float, bool], ArrResponse]


def _response(
    settings: dict[str, Any],
    directory: Path,
    cache: ResponseCache,
    climate: tuple[int, str] | None,
    duration: int,
    *,
    allow_network: bool,
) -> tuple[ArrResponse | None, str | None]:
    has_point = "latitude" in settings or "longitude" in settings
    if "arr_response" in settings:
        if has_point:
            msg = "Specify saved ARR response or live coordinates, not both"
            raise ValueError(msg)
        path = (directory / Path(settings["arr_response"])).absolute()
        if path not in cache:
            cache[path] = read_arr_response(path)
        return cache[path], str(path)
    if not has_point:
        return None, None
    if not allow_network:
        msg = "Live ARR coordinates require explicit --allow-network"
        raise ValueError(msg)
    latitude, longitude = float(settings["latitude"]), float(settings["longitude"])
    key = (latitude, longitude, climate is not None)
    if key not in cache:
        cache[key] = fetch_arr_response(
            latitude, longitude, aep=settings["aep"], duration_min=duration, climate=climate
        )
    return cache[key], f"ARR point {latitude},{longitude}"


def _source_curve(
    raw: dict[str, Any], directory: Path, cache: ResponseCache, *, allow_network: bool
) -> tuple[RainfallCurve, ArrResponse | None, tuple[int, str] | None]:
    if "curve" in raw:
        if "source" in raw:
            msg = "Specify curve or source, not both"
            raise ValueError(msg)
        data: dict[str, Any] = dict(raw["curve"])
        data["anchors"] = tuple(DepthAnchor(**anchor) for anchor in data["anchors"])
        return RainfallCurve(**data), None, None
    settings = cast("dict[str, Any]", raw["source"])
    climate = None
    if "climate" in settings:
        year, ssp = settings["climate"]
        climate = (int(year), str(ssp))
    response, response_source = _response(
        settings, directory, cache, climate, raw["duration_min"], allow_network=allow_network
    )
    area = float(settings.get("area_km2", 0))
    override = settings.get("arf_override")
    if settings["kind"] == "arr":
        if response is None or response_source is None:
            msg = "ARR source requires saved arr_response or explicitly enabled live coordinates"
            raise ValueError(msg)
        curve = curve_from_arr_response(
            response,
            settings["aep"],
            source=response_source,
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


def _select_loss_spec(data: dict[str, Any], response: ArrResponse | None, aep: str, *, regional: bool) -> None:
    if regional:
        if response is None or "spec" in data:
            msg = "Regional losses require ARR data and no user spec"
            raise ValueError(msg)
        timing = data.pop("continuing_loss_timing", "fractional")
        spec, source = regional_storm_losses(response, aep, continuing_loss_timing=timing)
        data["spec"], data["source"] = spec, source
        data.setdefault("mode", "storm_reduce")
        if data["mode"] == "burst":
            msg = "Regional losses are full-storm losses; choose storm_reduce or complete"
            raise ValueError(msg)
        if data["mode"] == "storm_reduce" and not any(
            key in data for key in ("arr_preburst_percentile", "preburst_ratio", "preburst_depth_mm")
        ):
            data["arr_preburst_percentile"] = "recommended"
    else:
        if "spec" not in data:
            msg = "Supply user loss spec or explicitly opt in to arr_regional_losses"
            raise ValueError(msg)
        data["spec"] = LossSpec(**data["spec"])


def _select_preburst(
    data: dict[str, Any], response: ArrResponse | None, curve: RainfallCurve, duration: int, timestep: int
) -> None:
    if "preburst_increments_mm" in data:
        data["preburst_increments_mm"] = tuple(data["preburst_increments_mm"])
    percentile = data.pop("arr_preburst_percentile", None)
    prefix_duration = data.pop("preburst_duration_min", None)
    if percentile is None:
        if prefix_duration is not None:
            msg = "Constant preburst duration requires an ARR preburst percentile"
            raise ValueError(msg)
        return
    if response is None or any(
        key in data for key in ("preburst_ratio", "preburst_depth_mm", "preburst_increments_mm")
    ):
        msg = "ARR percentile requires a response and no additional PB depth, ratio or increments"
        raise ValueError(msg)
    ratio, layer = selected_preburst_ratio(response, curve.aep, duration, percentile=percentile)
    data["preburst_source"] = f"ARR {layer}"
    if data.get("mode") == "complete":
        if prefix_duration is None:
            msg = "Complete storm with ARR percentile requires explicit preburst_duration_min"
            raise ValueError(msg)
        data["preburst_increments_mm"] = constant_preburst_increments(
            curve, duration, timestep_min=timestep, preburst_duration_min=prefix_duration, ratio=ratio
        )
    elif prefix_duration is not None:
        msg = "Preburst duration is only applicable to complete storms"
        raise ValueError(msg)
    else:
        data["preburst_ratio"] = ratio


def _loss_options(
    data: dict[str, Any],
    response: ArrResponse | None,
    climate: tuple[int, str] | None,
    curve: RainfallCurve,
    duration: int,
    timestep: int,
) -> EventLosses:
    data = dict(data)
    regional = data.pop("arr_regional_losses", False)
    if not isinstance(regional, bool):
        msg = "arr_regional_losses must be a boolean"
        raise ValueError(msg)
    _select_loss_spec(data, response, curve.aep, regional=regional)
    _select_preburst(data, response, curve, duration, timestep)
    adjust = data.pop("arr_climate_loss_factors", regional and climate is not None)
    if not isinstance(adjust, bool):
        msg = "arr_climate_loss_factors must be a boolean"
        raise ValueError(msg)
    losses = EventLosses(**data)
    if adjust:
        if response is None or climate is None:
            msg = "ARR climate loss factors require ARR response and selected scenario"
            raise ValueError(msg)
        losses = apply_arr_loss_factors(response, losses, climate)
    return losses


def _event(raw: dict[str, Any], directory: Path, cache: ResponseCache, *, allow_network: bool) -> StormResult:
    curve, response, climate = _source_curve(raw, directory, cache, allow_network=allow_network)
    options: dict[str, Any] = dict(raw.get("options", {}))
    duration = raw["duration_min"]
    if "losses" in options:
        options["losses"] = _loss_options(
            options["losses"], response, climate, curve, duration, options.get("timestep_min", 1)
        )
    return generate_storm(curve, duration, **options)


def _read_events(path: Path, *, allow_network: bool = False) -> dict[str, StormResult]:
    rows: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(rows, list) or not rows:
        msg = "Config must be a nonempty JSON event list"
        raise ValueError(msg)
    storms: dict[str, StormResult] = {}
    cache: ResponseCache = {}
    for raw in cast("list[dict[str, Any]]", rows):
        name = str(raw["name"])
        if name in storms:
            msg = f"Duplicate event name: {name}"
            raise ValueError(msg)
        storms[name] = _event(raw, path.parent, cache, allow_network=allow_network)
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
        if storm.loss_selection is not None:
            selection = storm.loss_selection
            print(
                f"  Loss source: {selection.source}; PB source: {selection.preburst_source}; "
                f"CC loss factors: {selection.climate_factors_source}"
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
    parser.add_argument("--allow-network", action=argparse.BooleanOptionalAction, default=ALLOW_NETWORK)
    args = parser.parse_args()
    try:
        if args.config is None or (args.write and args.output is None):
            msg = "Supply --config and, with --write, --output"
            raise ValueError(msg)
        storms = _read_events(Path(args.config).absolute(), allow_network=args.allow_network)
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
