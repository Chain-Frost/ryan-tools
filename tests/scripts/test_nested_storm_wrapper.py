"""Offline tests for the experimental repository-only nested storm CLI."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

from ryan_library.functions.hydrology.rainfall_loss_screening import LossSpec

SCRIPT = Path(__file__).absolute().parents[2] / "ryan-scripts" / "hydrology-python" / "generate_nested_storms.py"


def _wrapper() -> Any:
    spec = importlib.util.spec_from_file_location("nested_storm_wrapper", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Response:
    def ifd_table(self, year: int) -> dict[str, Any]:
        assert year in (1990, 2030)
        return {"index": [5, 60], "columns": [1], "data": [[5], [20]]}

    def layer(self, name: str, required: bool = False) -> dict[str, Any] | None:
        layers: dict[str, dict[str, Any]] = {
            "StormLossesNonNSW": {"Storm Initial Losses (mm)": 12, "Storm Continuing Losses (mm/h)": 3},
            "Preburst50": {"index": [60], "columns": [1], "data": [[0.1]]},
        }
        result = layers.get(name)
        if result is None and required:
            msg = f"Missing {name}"
            raise ValueError(msg)
        return result

    def climate_change_rainfall_factors(self, baseline_year: int, ssp: str) -> tuple[list[float], list[float]]:
        assert (baseline_year, ssp) == (2050, "SSP2")
        return [1, 24], [1.2, 1.2]

    def climate_change_loss_factors(self, baseline_year: int, ssp: str) -> tuple[float, float]:
        assert (baseline_year, ssp) == (2050, "SSP2")
        return 0.8, 0.5


def _fake_read_response(_path: Path) -> _Response:
    return _Response()


def _row(name: str, source: dict[str, Any], losses: dict[str, Any] | None) -> dict[str, Any]:
    options: dict[str, Any] = {"timestep_min": 5}
    if losses is not None:
        options["losses"] = losses
    return {"name": name, "source": source, "duration_min": 60, "options": options}


def test_user_losses_primary_and_regional_opt_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _wrapper()
    monkeypatch.setattr(module, "read_arr_response", _fake_read_response)
    source = {"kind": "arr", "arr_response": "site.json", "aep": "1%", "climate": [2050, "SSP2"]}
    manual = {"spec": {"initial_loss_mm": 25, "continuing_loss_mm_per_hour": 4}}
    regional = {"arr_regional_losses": True}
    path = tmp_path / "events.json"
    rows = [_row("manual", source, manual), _row("regional", source, regional)]
    path.write_text(json.dumps(rows), encoding="utf-8")
    storms = module._read_events(path)
    assert storms["manual"].applied_losses == LossSpec(25, continuing_loss_mm_per_hour=4)
    assert storms["manual"].loss_selection.source == "user"
    assert storms["regional"].applied_losses.initial_loss_mm == pytest.approx(7.2)
    assert storms["regional"].applied_losses.continuing_loss_mm_per_hour == 1.5
    assert storms["regional"].loss_selection.preburst_source == "ARR Preburst50"
    output = tmp_path / "bundle"
    monkeypatch.setattr(
        sys, "argv", [str(SCRIPT), "--config", str(path), "--boundary-name", "RF", "--output", str(output)]
    )
    assert module.main() == 0
    assert not output.exists()
    assert "Loss source: ARR StormLossesNonNSW" in capsys.readouterr().out
    monkeypatch.setattr(sys, "argv", [*sys.argv, "--write"])
    assert module.main() == 0
    manifest = json.loads((output / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["events"]["manual"]["model_il_mm"] == 25
    assert manifest["events"]["regional"]["model_il_mm"] == pytest.approx(7.2)


def test_no_implicit_regional_fallback_and_complete_prefix(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _wrapper()
    monkeypatch.setattr(module, "read_arr_response", _fake_read_response)
    source = {"kind": "arr", "arr_response": "site.json", "aep": "1%"}
    path = tmp_path / "events.json"
    path.write_text(json.dumps([_row("gross", source, None)]), encoding="utf-8")
    assert module._read_events(path)["gross"].loss_selection is None
    for losses in (
        {"arr_regional_losses": True, "spec": {"initial_loss_mm": 1, "continuing_loss_mm_per_hour": 0}},
        {"mode": "burst"},
        {"arr_regional_losses": True, "mode": "burst"},
    ):
        path.write_text(json.dumps([_row("invalid", source, losses)]), encoding="utf-8")
        with pytest.raises(ValueError):
            module._read_events(path)
    complete = {
        "arr_regional_losses": True,
        "mode": "complete",
        "arr_preburst_percentile": "50%",
        "preburst_duration_min": 10,
    }
    path.write_text(json.dumps([_row("complete", source, complete)]), encoding="utf-8")
    storm = module._read_events(path)["complete"]
    assert storm.gross_mm[:2] == (1, 1)
    assert storm.applied_losses == LossSpec(12, continuing_loss_mm_per_hour=3)


def test_live_requests_require_opt_in_and_are_cached(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _wrapper()
    requests: list[tuple[float, float, str, int, tuple[int, str] | None]] = []

    def fake_fetch(
        latitude: float, longitude: float, *, aep: str, duration_min: int, climate: tuple[int, str] | None = None
    ) -> _Response:
        requests.append((latitude, longitude, aep, duration_min, climate))
        return _Response()

    monkeypatch.setattr(module, "fetch_arr_response", fake_fetch)
    source = {"kind": "arr", "latitude": -30.0, "longitude": 150.0, "aep": "1%"}
    path = tmp_path / "events.json"
    rows = [_row("first", source, None), _row("second", source, None)]
    path.write_text(json.dumps(rows), encoding="utf-8")
    with pytest.raises(ValueError, match="allow-network"):
        module._read_events(path)
    assert not requests
    storms = module._read_events(path, allow_network=True)
    assert len(storms) == 2
    assert requests == [(-30.0, 150.0, "1%", 60, None)]
    path.write_text(json.dumps([_row("invalid", {**source, "arr_response": "saved.json"}, None)]), encoding="utf-8")
    with pytest.raises(ValueError, match="not both"):
        module._read_events(path, allow_network=True)
    assert len(requests) == 1
