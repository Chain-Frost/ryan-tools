"""Experimental offline nested frequency rainfall generator.

Pass a normalized JSON event list; see docs/work/2026-11-28-nested-frequency-storms.md.
Source-specific rainfall readers are intentionally outside this wrapper.
"""

from __future__ import annotations

from pathlib import Path

SCRIPT_VERSION = "2026.11.28.1"
CONFIG: Path | None = None
OUTPUT: Path | None = None
KIND = "gross"
WRITE = False

import argparse
import json
from math import fsum
from typing import Any, Literal, cast

from ryan_library.functions.hydrology.nested_storms import (
    DepthAnchor,
    EventLosses,
    Loading,
    RainfallCurve,
    StormResult,
    generate_storm,
    write_tuflow_csv,
)
from ryan_library.functions.hydrology.rainfall_loss_screening import LossSpec


def _event(raw: dict[str, Any]) -> StormResult:
    curve_data = dict(raw["curve"])
    curve_data["anchors"] = tuple(DepthAnchor(**anchor) for anchor in curve_data["anchors"])
    options = dict(raw.get("options", {}))
    if "losses" in options:
        loss_data = dict(options["losses"])
        loss_data["spec"] = LossSpec(**loss_data["spec"])
        if "preburst_increments_mm" in loss_data:
            loss_data["preburst_increments_mm"] = tuple(loss_data["preburst_increments_mm"])
        options["losses"] = EventLosses(**loss_data)
    if "loading" in options:
        options["loading"] = cast("Loading", options["loading"])
    return generate_storm(RainfallCurve(**curve_data), raw["duration_min"], **options)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=SCRIPT_VERSION)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--output", type=Path, default=OUTPUT)
    parser.add_argument("--kind", choices=("gross", "excess"), default=KIND)
    parser.add_argument("--write", action=argparse.BooleanOptionalAction, default=WRITE)
    args = parser.parse_args()
    try:
        if args.config is None or (args.write and args.output is None):
            msg = "Supply --config and, with --write, --output"
            raise ValueError(msg)
        rows: object = json.loads(Path(args.config).read_text(encoding="utf-8"))
        if not isinstance(rows, list) or not rows:
            msg = "Config must be a nonempty JSON event list"
            raise ValueError(msg)
        storms: dict[str, StormResult] = {}
        for raw in cast("list[dict[str, Any]]", rows):
            name = str(raw["name"])
            if name in storms:
                msg = f"Duplicate event name: {name}"
                raise ValueError(msg)
            storms[name] = _event(raw)
        kind = cast('Literal["gross", "excess"]', args.kind)
        print(f"Nested storms {SCRIPT_VERSION} (experimental; {kind} rainfall)")
        for name, storm in storms.items():
            if kind == "excess" and storm.excess_mm is None:
                msg = f"No loss selection for {name}"
                raise ValueError(msg)
            print(
                f"{name}: source={storm.curve.source}, AEP={storm.curve.aep}, scenario={storm.curve.scenario}, "
                f"burst={storm.burst_total_mm:g} mm, total={fsum(storm.gross_mm):g} mm, "
                f"PB={storm.preburst_mm:g} mm, IL/CL={storm.applied_losses}, "
                f"ARF override={storm.arf_override}"
            )
            for check in storm.depth_checks:
                print(f"  {check.duration_min} min: target={check.target_mm:g}, max={check.maximum_rolling_mm:g} mm")
        if args.write:
            write_tuflow_csv(Path(args.output), storms, kind=kind)
        else:
            print("Preview only; no files created.")
    except (ArithmeticError, AttributeError, KeyError, OSError, TypeError, ValueError) as exc:
        parser.exit(1, f"Nested storm generation failed: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
