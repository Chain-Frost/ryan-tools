"""Candidate TUFLOW inputs for a single boundary and composite nested event axis."""

from __future__ import annotations

import csv
import json
import re
from collections.abc import Mapping
from dataclasses import asdict
from math import fsum, isfinite
from pathlib import Path
from typing import Literal

from .nested_storms import StormResult, write_tuflow_csv

RainKind = Literal["gross", "excess"]
_TOKEN = re.compile(r"[A-Za-z0-9_]{1,100}")


def _loss_values(storm: StormResult, kind: RainKind) -> tuple[float, float]:
    if storm.applied_losses is None or storm.loss_selection is None:
        msg = "Bundle requires explicit losses (zero is allowed)"
        raise ValueError(msg)
    if kind == "excess":
        if storm.excess_mm is None:
            msg = "Excess bundle requires calculated rainfall excess"
            raise ValueError(msg)
        return 0.0, 0.0
    cl = storm.applied_losses.continuing_loss_mm_per_hour
    if cl is None or storm.loss_selection.proportional_loss_fraction != 0:
        msg = "Gross IL/CL bundle cannot encode proportional or coefficient loss; use explicit excess instead"
        raise ValueError(msg)
    return storm.applied_losses.initial_loss_mm, cl


def _validate_bundle(storms: Mapping[str, StormResult], boundary_name: str, kind: RainKind) -> None:
    if kind not in ("gross", "excess") or not storms or _TOKEN.fullmatch(boundary_name) is None:
        msg = "Select gross/excess and nonempty events; boundary requires a safe ASCII token"
        raise ValueError(msg)
    if len({name.casefold() for name in storms}) != len(storms):
        msg = "Event tokens must be case-insensitively unique"
        raise ValueError(msg)
    if len({storm.curve.source for storm in storms.values()}) != 1:
        msg = "Bundle represents one site/source and one rainfall boundary"
        raise ValueError(msg)
    for name, storm in storms.items():
        if _TOKEN.fullmatch(name) is None:
            msg = f"Unsafe TUFLOW event token: {name!r}"
            raise ValueError(msg)
        values = storm.gross_mm if kind == "gross" else storm.excess_mm
        if not values or any(not isfinite(value) or value < 0 for value in values):
            msg = f"Missing or invalid rainfall series: {name}"
            raise ValueError(msg)
        _loss_values(storm, kind)


def _write_controls(folder: Path, storms: Mapping[str, StormResult], boundary_name: str, kind: RainKind) -> None:
    with (folder / "bc_dbase.csv").open("x", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(["Name", "Source", "Column 1", "Column 2"])
        writer.writerow([boundary_name, r"rf_inflow\rain_~NSTORM~.csv", "Time", "Rainfall"])
    with (folder / "Event_File.tef").open("x", encoding="utf-8") as stream:
        for name in storms:
            stream.write(f"Define Event == {name}\n    BC Event Source == ~NSTORM~ | {name}\nEnd Define\n\n")
    with (folder / "rainfall_losses.trd").open("x", encoding="utf-8") as stream:
        stream.write("! Candidate loss variables only; explicitly connect to the intended model loss mechanism.\n")
        if kind == "excess":
            stream.write("! Already excess rainfall: disable duplicate rainfall loss and review soil infiltration.\n")
        for index, (name, storm) in enumerate(storms.items()):
            initial, continuing = _loss_values(storm, kind)
            condition = "If" if index == 0 else "Else If"
            stream.write(f"{condition} Event == {name}\n")
            stream.write(f"    Set Variable IL_{boundary_name} == {initial}\n")
            stream.write(f"    Set Variable CL_{boundary_name} == {continuing}\n")
        stream.write("Else\n    Pause == Nested Storm Event Not Recognised\nEnd If\n")


def _event_manifest(storm: StormResult, kind: RainKind) -> dict[str, object]:
    selected = storm.gross_mm if kind == "gross" else storm.excess_mm
    if selected is None:
        msg = "Missing rainfall series"
        raise ValueError(msg)
    initial, continuing = _loss_values(storm, kind)
    return {
        "curve": asdict(storm.curve),
        "duration_min": storm.duration_min,
        "timestep_min": storm.timestep_min,
        "loading": storm.loading,
        "arf_override": storm.arf_override,
        "gross_burst_mm": storm.burst_total_mm,
        "gross_total_mm": fsum(storm.gross_mm),
        "exported_total_mm": fsum(selected),
        "preburst_mm": storm.preburst_mm,
        "preburst_intervals": storm.preburst_intervals,
        "loss_selection": asdict(storm.loss_selection) if storm.loss_selection is not None else None,
        "applied_losses": asdict(storm.applied_losses) if storm.applied_losses is not None else None,
        "model_il_mm": initial,
        "model_cl_mm_per_hour": continuing,
        "depth_checks": [asdict(check) for check in storm.depth_checks],
    }


def write_tuflow_bundle(
    folder: Path, storms: Mapping[str, StormResult], *, boundary_name: str, kind: RainKind
) -> None:
    """Create a new bundle, one CSV per event; never change an existing model.

    TRD sets loss variables only; it does not edit materials or soils.
    Gross mode requires ordinary IL/CL. Excess mode sets both model loss
    variables to zero. Native RF/ILCL equivalence remains unqualified.
    """
    _validate_bundle(storms, boundary_name, kind)
    manifest = {
        "rainfall_kind": kind,
        "boundary_name": boundary_name,
        "native_qualified": False,
        "events": {name: _event_manifest(storm, kind) for name, storm in storms.items()},
    }
    encoded = json.dumps(manifest, indent=2, allow_nan=False) + "\n"
    folder.mkdir(exist_ok=False)
    inflow = folder / "rf_inflow"
    inflow.mkdir()
    for name, storm in storms.items():
        write_tuflow_csv(inflow / f"rain_{name}.csv", {"Rainfall": storm}, kind=kind)
    _write_controls(folder, storms, boundary_name, kind)
    with (folder / "manifest.json").open("x", encoding="utf-8") as stream:
        stream.write(encoded)
