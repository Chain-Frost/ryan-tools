"""Focused wrapper checks; parsing and ARR network acceptance are separate gates."""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest

from ryan_library.functions.hydrology.rainfall_loss_screening import (
    LossSpec,
    PatternScreen,
    ScreeningStatus,
)

SCRIPT = Path(__file__).absolute().parents[3] / "ryan-scripts" / "hydrology-python" / "screen_rainfall_losses.py"


def _wrapper() -> Any:
    spec = importlib.util.spec_from_file_location("screen_rainfall_losses_wrapper", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_common_aeps_and_explicit_selection() -> None:
    m = _wrapper()
    original = ["12EY", "6EY", "63.2%", "50%", "20%", "0.2EY", "10%", "1%", "1 in 200"]
    assert m._aep_selection(original, [], "common") == ["63.2%", "50%", "20%", "10%", "1%", "1 in 200"]
    assert m._aep_selection(original, [], "all") == original
    assert m._aep_selection(original, ["12EY", "1 in 200"], "common") == ["12EY", "1 in 200"]
    with pytest.raises(ValueError, match="not in the source"):
        m._aep_selection(original, ["1 in 500"], "all")
    with pytest.raises(ValueError, match="duplicate"):
        m._aep_selection(original, ["20%", "20%"], "all")


def test_duration_filter_source_axis_and_explicit_choices() -> None:
    m = _wrapper()
    assert m._durations([1, 5, 60], [], arr=False) == [1, 5, 60]
    assert m._durations([1, 5, 60], [60], arr=False) == [60]
    with pytest.raises(ValueError, match="not in BoM"):
        m._durations([1, 5], [60], arr=False)
    with pytest.raises(ValueError, match="unique"):
        m._durations([], [60, 60], arr=True)


def test_loss_schedule_maps_aep_and_duration_exactly() -> None:
    m = _wrapper()
    losses = m._loss_schedules(
        {
            "20%": {"il_mm": 30, "cl_mm_per_hour": 5},
            "1%": {
                "60": {"il_mm": 20, "runoff_coefficient": 0.3},
                "1440": {"il_mm": 25, "cl_mm_per_hour": 2},
            },
        }
    )
    assert losses["20%"] == LossSpec(30, continuing_loss_mm_per_hour=5)
    assert isinstance(losses["1%"], dict)
    assert losses["1%"][60] == LossSpec(20, runoff_coefficient=0.3)
    assert losses["1%"][1440] == LossSpec(25, continuing_loss_mm_per_hour=2)
    with pytest.raises(ValueError, match="duration key"):
        m._loss_schedules({"1%": {"1.5": {"il_mm": 5, "cl_mm_per_hour": 1}}})


def test_bom_preburst_adjustment_is_not_doubled_and_overflow_fails() -> None:
    m = _wrapper()
    base = LossSpec(30, continuing_loss_mm_per_hour=5)
    adjusted, overrides = m._apply_bom_preburst(base, {"1%": LossSpec(20, runoff_coefficient=0.2)}, 10)
    assert adjusted == LossSpec(20, continuing_loss_mm_per_hour=5)
    assert overrides["1%"] == LossSpec(10, runoff_coefficient=0.2)
    with pytest.raises(ValueError, match="complete storm"):
        m._apply_bom_preburst(base, {}, 31)


def test_optional_loss_validation() -> None:
    m = _wrapper()
    assert m._default_loss(None, None, None) is None
    assert m._default_loss(30, 5, None) == LossSpec(30, continuing_loss_mm_per_hour=5)
    assert m._default_loss(30, None, 0.3) == LossSpec(30, runoff_coefficient=0.3)
    with pytest.raises(ValueError, match="INITIAL_LOSS_MM"):
        m._default_loss(None, 5, None)


def test_cli_help_and_explicit_aep_group(monkeypatch: pytest.MonkeyPatch) -> None:
    m = _wrapper()
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--source", "bom", "--aep", "20%", "1%", "--no-pause"])
    args: argparse.Namespace = m._arguments()
    assert args.source == "bom"
    assert args.aep == [["20%", "1%"]]
    assert args.no_pause is True
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--help"])
    with pytest.raises(SystemExit) as exc:
        m._arguments()
    assert exc.value.code == 0


def test_console_table_and_exclusive_csv(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    m = _wrapper()
    screens = [
        PatternScreen("20%", 60, None, "bom_ifd", None, ScreeningStatus.ZERO, 0, 0, "IL bound"),
        PatternScreen("10%", 60, "tp1", "bom_ifd", None, ScreeningStatus.POSITIVE, 1, 5, "positive"),
        PatternScreen("10%", 60, "tp2", "bom_ifd", None, ScreeningStatus.ZERO, 0, 0, "zero"),
        PatternScreen("1%", 60, None, "bom_ifd", None, ScreeningStatus.UNRESOLVED, None, None, "no losses"),
    ]
    depth = {
        ("20%", 60, None): (23, None, "IL 30; CL 5"),
        ("10%", 60, None): (37, None, "IL 30; CL 5"),
        ("1%", 60, None): (50, None, "missing"),
    }
    output = tmp_path / "screen.csv"
    m._print_report(screens, depth, "all", output)
    shown = capsys.readouterr().out
    assert "WIPE OUT" in shown
    assert "KEEP" in shown
    assert "UNRESOLVED" in shown
    assert "1 wiped out" in shown
    assert output.exists()
    with pytest.raises(FileExistsError):
        m._print_report(screens, depth, "all", output)


def test_cli_offline_and_no_offline_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    m = _wrapper()
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--source", "arr", "--offline", "--no-pause"])
    assert m._arguments().offline is True
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--source", "arr", "--no-offline", "--no-pause"])
    assert m._arguments().offline is False
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--source", "arr"])
    assert m._arguments().offline is None


def test_strict_offline_input_requirements(tmp_path: Path) -> None:
    m = _wrapper()
    response = tmp_path / "arr.json"
    response.write_text("{}", encoding="utf-8")
    point = tmp_path / "point_Increments.csv"
    point.write_text("placeholder", encoding="utf-8")
    areal = tmp_path / "areal_Increments.csv"
    areal.write_text("placeholder", encoding="utf-8")
    with pytest.raises(ValueError, match="--arr-response"):
        m._validate_local_sources(
            source="arr",
            offline=True,
            response_path=None,
            point_tp_path=point,
            areal_tp_path=areal,
            fetch_bom_tps=True,
        )
    with pytest.raises(ValueError, match="--point-tp"):
        m._validate_local_sources(
            source="arr",
            offline=True,
            response_path=response,
            point_tp_path=None,
            areal_tp_path=areal,
            fetch_bom_tps=True,
        )
    with pytest.raises(ValueError, match="existing readable"):
        m._validate_local_sources(
            source="arr",
            offline=True,
            response_path=response,
            point_tp_path=tmp_path / "missing.csv",
            areal_tp_path=areal,
            fetch_bom_tps=True,
        )
    m._validate_local_sources(
        source="arr",
        offline=True,
        response_path=response,
        point_tp_path=point,
        areal_tp_path=areal,
        fetch_bom_tps=True,
    )
    with pytest.raises(ValueError, match="ARR-only"):
        m._validate_local_sources(
            source="bom",
            offline=False,
            response_path=response,
            point_tp_path=point,
            areal_tp_path=None,
            fetch_bom_tps=False,
        )
    with pytest.raises(ValueError, match="--no-fetch-tps"):
        m._validate_local_sources(
            source="bom",
            offline=True,
            response_path=None,
            point_tp_path=None,
            areal_tp_path=None,
            fetch_bom_tps=True,
        )
    m._validate_local_sources(
        source="bom",
        offline=True,
        response_path=None,
        point_tp_path=None,
        areal_tp_path=None,
        fetch_bom_tps=False,
    )
    m._validate_local_sources(
        source="bom",
        offline=True,
        response_path=None,
        point_tp_path=point,
        areal_tp_path=None,
        fetch_bom_tps=True,
    )


def test_strict_offline_does_not_download_missing_areal_patterns() -> None:
    m = _wrapper()

    class Response:
        def layer(self, name: str) -> dict[str, str] | None:
            return {"url": "https://data.arr-software.org/areal.zip"} if name == "ArealTP" else None

    response = Response()
    with pytest.raises(ValueError, match="--areal-tp"):
        m._validate_offline_arr_patterns(response, offline=True, areal_tp_path=None)
    m._validate_offline_arr_patterns(response, offline=False, areal_tp_path=None)
    m._validate_offline_arr_patterns(response, offline=True, areal_tp_path=Path("areal.csv"))


def test_local_arr_response_precedes_network(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = pytest.importorskip("pytuflow.arr.api_client")
    m = _wrapper()
    data = tmp_path / "saved.json"
    data.write_text(
        '{"title":"Local ARR dataset","layers":{"ArealTP":{"url":"https://example.test/a.zip"}}}', encoding="utf-8"
    )

    def fail_fetch(_client: object, _config: object) -> None:
        message = "ARR API must not be queried when --arr-response is supplied"
        raise AssertionError(message)

    monkeypatch.setattr(api.ArrApiClient, "fetch", fail_fetch)
    loaded = m._arr_response(object(), data)
    assert loaded.title == "Local ARR dataset"
    assert loaded.layer("ArealTP")["url"] == "https://example.test/a.zip"


def test_offline_pytuflow_config_uses_local_json_and_temporal_patterns(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    api = pytest.importorskip("pytuflow.arr.api_client")
    engine_module = pytest.importorskip("pytuflow.arr.engine")
    patterns_module = pytest.importorskip("pytuflow.arr.temporal_patterns")
    m = _wrapper()
    response = tmp_path / "site.json"
    response.write_text("{}", encoding="utf-8")
    point = tmp_path / "point_Increments.csv"
    point.write_text("placeholder", encoding="utf-8")
    areal = tmp_path / "areal_Increments.csv"
    areal.write_text("placeholder", encoding="utf-8")
    config = m._arr_config(
        None,
        None,
        10.0,
        "offline site",
        ["1%"],
        [60],
        2030,
        "burst",
        "recommended",
        tmp_path,
        response,
        point_tp_path=point,
        areal_tp_path=areal,
    )
    assert config.response_json == str(response)
    assert config.site.latitude is None
    assert config.temporal_patterns.point_tp_csv == str(point)
    assert config.temporal_patterns.areal_tp_csv == str(areal)
    saved = api.ArrApiResponse(
        {
            "layers": {
                "PointTP": {"url": "https://example.test/point.zip"},
                "ArealTP": {"url": "https://example.test/areal.zip"},
            }
        }
    )
    calls: list[tuple[str | None, str | None, str | None, str | None]] = []

    def from_sources(
        _class: object,
        point_file: str | None,
        areal_file: str | None,
        point_url: str | None,
        areal_url: str | None,
        catchment_area: float | None = None,
    ) -> Any:
        assert catchment_area == 10.0
        calls.append((point_file, areal_file, point_url, areal_url))
        return object()

    monkeypatch.setattr(patterns_module.TemporalPatternSet, "from_sources", classmethod(from_sources))
    runner = engine_module.ArrEngine(config, saved)
    runner._tp_set_for_config()
    assert calls == [(str(point), str(areal), None, None)]


def test_offline_arr_config_rejects_missing_coordinates_only_in_live_mode(tmp_path: Path) -> None:
    pytest.importorskip("pytuflow.arr.config")
    errors = pytest.importorskip("pytuflow.arr.exceptions")
    m = _wrapper()
    response = tmp_path / "site.json"
    response.write_text("{}", encoding="utf-8")
    point = tmp_path / "point_Increments.csv"
    point.write_text("placeholder", encoding="utf-8")
    config = m._arr_config(
        None,
        None,
        0.0,
        "offline site",
        ["1%"],
        [60],
        2030,
        "burst",
        "recommended",
        tmp_path,
        response,
        point_tp_path=point,
    )
    assert config.site.latitude is None
    with pytest.raises(ValueError, match="both"):
        m._arr_config(
            -23.08632,
            None,
            0.0,
            "site",
            ["1%"],
            [60],
            2030,
            "burst",
            "recommended",
            tmp_path,
            response,
            point_tp_path=point,
        )
    with pytest.raises(errors.ArrConfigError, match="latitude"):
        m._arr_config(
            None,
            None,
            0.0,
            "live site",
            ["1%"],
            [60],
            2030,
            "burst",
            "recommended",
            tmp_path,
            None,
        )
