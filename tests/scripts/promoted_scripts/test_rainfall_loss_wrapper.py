"""Focused wrapper checks; parsing and ARR network acceptance are separate gates."""

from __future__ import annotations

import argparse
import csv
import importlib.util
import io
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from ryan_library.functions.hydrology.arr_user_climate_losses import ArrUserLossAdjustment
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
    assert m._aep_selection(original, [], "common") == ["50%", "20%", "10%", "1%", "1 in 200"]
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


def test_console_table_and_exclusive_csv(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    from rich.table import Table

    m = _wrapper()
    printed_tables: list[Table] = []
    original_print: Any = m.Console.print

    def capture_print(console: Any, *objects: Any, **kwargs: Any) -> None:
        printed_tables.extend(item for item in objects if isinstance(item, Table))
        original_print(console, *objects, **kwargs)

    monkeypatch.setattr(m.Console, "print", capture_print)
    screens = [
        PatternScreen("20%", 60, None, "bom_ifd", None, ScreeningStatus.ZERO, 0, 0, "IL bound"),
        PatternScreen("10%", 60, "tp1", "bom_ifd", None, ScreeningStatus.POSITIVE, 1, 5, "positive"),
        PatternScreen("10%", 60, "tp2", "bom_ifd", None, ScreeningStatus.ZERO, 0, 0, "zero"),
        PatternScreen("1%", 60, None, "bom_ifd", None, ScreeningStatus.UNRESOLVED, None, None, "no losses"),
    ]
    depth = {
        ("20%", 60, None): (23, 23, 1.0, None, "IL 30; CL 5"),
        ("10%", 60, None): (37, 33.3, 0.9, None, "IL 30; CL 5"),
        ("1%", 60, None): (50, 50, 1.0, None, "missing"),
    }
    output = tmp_path / "screen.csv"
    m._print_report(screens, depth, "all", output)
    shown = capsys.readouterr().out
    assert "DROP" in shown
    assert "KEEP" in shown
    assert "UNRESOLVED" in shown
    # Check grouping metadata, not renderer-specific table border glyph counts.
    assert [row.end_section for row in printed_tables[0].rows] == [True, True, False]
    assert "1 DROP" in shown
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
    # A saved ARR response is also allowed to provide BoM ARF coefficients.
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
        "main-burst-only",
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

    monkeypatch.setattr(patterns_module.TemporalPatternSet, "from_sources", cast(Any, classmethod(from_sources)))
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
        "main-burst-only",
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
            "main-burst-only",
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
            "main-burst-only",
            "recommended",
            tmp_path,
            None,
        )


def test_source_durations_and_reporting_modes(capsys: pytest.CaptureFixture[str]) -> None:
    m = _wrapper()

    class Response:
        def ifd_table(self, baseline: int) -> dict[str, object]:
            assert baseline == 2030
            return {"index": [30.0, 45, 60, 90, 120, 180], "columns": [63.2, 50, 20], "data": []}

    assert m._datahub_durations(Response(), 2030) == [30, 45, 60, 90, 120, 180]
    assert m._durations([30, 45, 60, 90, 120, 180], [], arr=True) == [30, 45, 60, 90, 120, 180]
    assert m._source_decimals([30.200, 24.50, 33]) == 1
    assert m._parse_climate_scenarios(["2050:SSP2", "2090:SSP5"]) == ((2050, "SSP2"), (2090, "SSP5"))
    with pytest.raises(ValueError, match="Unsupported climate"):
        m._parse_climate_scenarios(["2025:SSP2"])
    screens = [
        PatternScreen("20%", 60, None, "bom_ifd", None, ScreeningStatus.ZERO, 0, 0, "IL bound"),
        PatternScreen(
            "10%", 60, "point:Rangelands West:11:TP01", "bom_ifd", None, ScreeningStatus.POSITIVE, 2, 3, "positive", 1
        ),
    ]
    m._show_matrix(screens, enabled=True)
    output = capsys.readouterr().out
    assert "60" in output
    assert "DROP" in output
    assert "KEEP" in output
    assert "Rangelands West" in m._region_label(screens)


def test_arf_for_350_km2_uses_pytuflow_and_reports_frequent_unity() -> None:
    pytest.importorskip("pytuflow.arr.arf")
    m = _wrapper()
    params = {
        "a": 0.297,
        "b": 0.234,
        "c": 0.449,
        "d": 0.344,
        "e": 0.00142,
        "f": 0.216,
        "g": 0.129,
        "h": 0,
        "i": 0,
    }

    class Response:
        def layer(self, name: str, required: bool = False) -> dict[str, float]:
            assert name == "ARFParams"
            assert required
            return params

    factors = m._bom_arf_factors(
        area=350,
        durations=[120, 1440],
        aeps=["63.2%", "20%", "1%"],
        response=Response(),
        override=None,
    )
    assert factors["63.2%", 120] == pytest.approx(1.0)
    assert 0 < factors["20%", 120] < 1
    assert 0 < factors["1%", 1440] < 1
    asserted = m._bom_arf_factors(
        area=350,
        durations=[120],
        aeps=["63.2%", "20%"],
        response=None,
        override=0.8,
    )
    assert all(value == pytest.approx(0.8) for value in asserted.values())


def test_unknown_tp_column_hidden_when_all_resolved(
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    m = _wrapper()
    screens = [
        PatternScreen("20%", 60, None, "bom_ifd", None, ScreeningStatus.ZERO, 0, 0, "IL bound", 0),
        PatternScreen("10%", 60, "point:west:12:TP01", "bom_ifd", None, ScreeningStatus.POSITIVE, 1, 2, "rain", 1),
    ]
    totals = {
        ("20%", 60, None): (23, 23, 1.0, None, "IL 30; CL 5"),
        ("10%", 60, None): (37, 33.3, 0.9, None, "IL 30; CL 5"),
    }
    m._print_report(screens, totals, "all", None, source_decimal_places=1)
    shown = capsys.readouterr().out
    assert "Unknown TP" not in shown
    assert "DROP" in shown
    # Rich may omit rightmost headings at narrow CI terminal widths.
    csv_path = tmp_path / "audit.csv"
    m._print_report(screens, totals, "all", csv_path, source_decimal_places=1)
    contents = csv_path.read_text(encoding="utf-8")
    assert "unknown_tp" not in contents.splitlines()[0]
    assert "0.9000" in contents


def test_complete_storm_default_and_renamed_burst_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Do not silently interpret the former burst label as complete-storm mode."""
    m = _wrapper()
    assert m.STORM_MODE == "complete"
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--source", "arr", "--storm-mode", "main-burst-only"])
    assert m._arguments().storm_mode == "main-burst-only"
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--storm-mode", "complete"])
    assert m._arguments().storm_mode == "complete"
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--storm-mode", "burst"])
    with pytest.raises(SystemExit) as error:
        m._arguments()
    assert error.value.code == 2

    pytest.importorskip("pytuflow.arr.config")
    complete = m._arr_config(
        -23.08632, 119.2755, 350.0, "test site", ["1%"], [120], 2030, "complete", "recommended", tmp_path, None
    )
    main_only = m._arr_config(
        -23.08632, 119.2755, 350.0, "test site", ["1%"], [120], 2030, "main-burst-only", "recommended", tmp_path, None
    )
    assert complete.complete_storm is True
    assert main_only.complete_storm is False
    with pytest.raises(ValueError, match="STORM_MODE"):
        m._arr_config(
            -23.08632, 119.2755, 350.0, "test site", ["1%"], [120], 2030, "burst", "recommended", tmp_path, None
        )


def test_wrapper_defaults_to_full_increment_and_enables_point_tp_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m = _wrapper()
    assert m.LOSS_TIMING == "full_increment"
    assert m.ALLOW_POINT_TP_FALLBACK is True
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--no-point-tp-fallback"])
    assert m._arguments().point_tp_fallback is False
    monkeypatch.setattr(sys, "argv", [str(SCRIPT), "--point-tp-fallback"])
    assert m._arguments().point_tp_fallback is True


def test_offline_arr_can_suppress_areal_url_without_modifying_saved_source() -> None:
    api = pytest.importorskip("pytuflow.arr.api_client")
    m = _wrapper()
    original = api.ArrApiResponse(
        {
            "layers": {
                "ArealTP": {"url": "https://example.test/areal.zip"},
                "ARFParams": {"a": 0.2},
            },
            "title": "Original source",
        }
    )
    assert (
        m._validate_offline_arr_patterns(original, offline=True, areal_tp_path=None, allow_point_fallback=True) is True
    )
    adjusted = m._response_without_areal_tp(original)
    assert adjusted.layer("ArealTP") is None
    assert original.layer("ArealTP") == {"url": "https://example.test/areal.zip"}
    assert adjusted.layer("ARFParams") == {"a": 0.2}


def test_preflight_warning_groups_missing_duration_once(capsys: pytest.CaptureFixture[str]) -> None:
    m = _wrapper()
    admission = m.TemporalPatternAdmission(
        included_durations=(30, 60, 120),
        omitted_durations=(1, 2, 5),
        missing_bands={1: ("frequent", "rare"), 2: ("frequent",), 5: ("frequent", "intermediate")},
        point_fallback_durations=(30, 60),
    )
    m._announce_tp_admission(admission, area=350)
    # Rich can wrap warning text across terminal-width boundaries.
    captured = " ".join(capsys.readouterr().err.split())
    assert captured.count("Missing TPs for") == 1
    assert "1, 2, 5" in captured
    assert "NOT DROP" in captured
    assert captured.count("using point TPs instead") == 1


def test_bom_point_fallback_no_areal_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    m = _wrapper()
    tp_module = pytest.importorskip("pytuflow.arr.temporal_patterns")
    point = tmp_path / "point_Increments.csv"
    point.write_text("synthetic", encoding="utf-8")
    inputs: list[tuple[str, str | None, float | None]] = []

    def from_files(
        _klass: object,
        point_path: str,
        areal_path: str | None = None,
        catchment_area: float | None = None,
    ) -> object:
        inputs.append((point_path, areal_path, catchment_area))
        return object()

    monkeypatch.setattr(tp_module.TemporalPatternSet, "from_files", cast(Any, classmethod(from_files)))
    provider = m._bom_patterns(object(), None, None, 350.0, True, point, None, tmp_path, allow_point_fallback=True)
    assert provider is not None
    assert inputs == [(str(point), None, 350.0)]
    with pytest.raises(ValueError, match="fallback disabled"):
        m._bom_patterns(object(), None, None, 350.0, True, point, None, tmp_path, allow_point_fallback=False)


def test_bom_hybrid_point_download_preserves_explicit_local_areal_tp(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A local areal CSV must never be silently replaced by an online download."""
    tp_module = pytest.importorskip("pytuflow.arr.temporal_patterns")
    m = _wrapper()
    areal = tmp_path / "areal_Increments.csv"
    areal.write_text("authored local areal increments", encoding="utf-8")
    calls: list[tuple[str | None, str | None, str | None, str | None, float | None]] = []
    expected = object()

    class Response:
        def layer(self, name: str, required: bool = False) -> dict[str, str] | None:
            if name == "PointTP":
                assert required
                return {"url": "https://example.invalid/point.zip"}
            if name == "ArealTP":
                return {"url": "https://example.invalid/areal.zip"}
            raise AssertionError(name)

    def from_sources(
        _klass: object,
        point_path: str | None,
        areal_path: str | None,
        point_url: str | None,
        areal_url: str | None,
        catchment_area: float | None = None,
    ) -> object:
        calls.append((point_path, areal_path, point_url, areal_url, catchment_area))
        return expected

    monkeypatch.setattr(tp_module.TemporalPatternSet, "from_sources", cast(Any, classmethod(from_sources)))
    output = m._bom_patterns(object(), -23.08632, 119.2755, 350.0, True, None, areal, tmp_path, response=Response())
    assert output is expected
    assert calls == [
        (None, str(areal), "https://example.invalid/point.zip", "https://example.invalid/areal.zip", 350.0)
    ]


def test_excess_colour_depends_on_actual_value_not_display_rounding() -> None:
    """Exactly zero is grey; small nonzero rainfall excess stays undimmed."""
    m = _wrapper()
    true_zero = m._excess_cell(0.0, places=3)
    small_positive = m._excess_cell(0.0004, places=3)
    missing = m._excess_cell(None, places=3)
    assert true_zero.plain == "0.000"
    assert small_positive.plain == "<0.001"
    assert true_zero.style == "grey50"
    assert small_positive.style == "white"
    assert missing.plain == "—"
    assert missing.style == "grey50"


def test_provenance_explains_arf_unity_for_632_percent(
    capsys: pytest.CaptureFixture[str],
) -> None:
    m = _wrapper()
    screens = [
        PatternScreen(
            "63.2%",
            120,
            "point:Rangelands West:1:TP01",
            "arr_datahub",
            None,
            ScreeningStatus.POSITIVE,
            0.1,
            0.1,
            "positive",
            0.1,
        )
    ]
    m._print_provenance(
        latitude=-23.08632,
        longitude=119.2755,
        site="site",
        area=350,
        arf_source="PyTUFLOW ARR2019 Book 2 ARF, per AEP/duration",
        screens=screens,
        loss_timing="full_increment",
        climate="baseline only",
    )
    assert "ARF 1.0000" in " ".join(capsys.readouterr().out.split())


def test_compact_headers_aep_group_lines_and_preburst_ratio_csv(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from rich.table import Table

    m = _wrapper()
    screens = [
        PatternScreen("20%", 60, "tp01", "arr_datahub", None, ScreeningStatus.ZERO, 0, 0, "zero", 0),
        PatternScreen("20%", 90, "tp01", "arr_datahub", None, ScreeningStatus.ZERO, 0, 0, "zero", 0),
        PatternScreen(
            "10%", 60, "tp01", "arr_datahub", None, ScreeningStatus.POSITIVE, 0.000001, 0.000001, "positive", 0.000001
        ),
    ]
    totals = {
        ("20%", 60, None): (20, 17.5, 0.875, 0.3, "IL 50; CL 5"),
        ("20%", 90, None): (24, 21.0, 0.875, 0.36, "IL 50; CL 5"),
        ("10%", 60, None): (40, 35.5, 0.8875, None, "IL 50; CL 5"),
    }
    printed: list[Table] = []
    original_print: Any = m.Console.print

    def capture_print(console: Any, *objects: Any, **kwargs: Any) -> None:
        printed.extend(item for item in objects if isinstance(item, Table))
        original_print(console, *objects, **kwargs)

    monkeypatch.setattr(m.Console, "print", capture_print)
    output = tmp_path / "example.csv"
    m._print_report(screens, totals, "all", output, source_decimal_places=1)
    table = next(item for item in printed if "Design rainfall" in str(item.title))
    assert len(table.rows) == 3
    assert table.rows[0].end_section is False
    assert table.rows[1].end_section is True
    assert table.rows[2].end_section is False
    assert any("\n" in str(column.header) for column in table.columns)
    assert any(column.header == "PB\nratio" for column in table.columns)
    assert any(column.header == "Max\nbin mm" for column in table.columns)
    assert all(column.header != "CC" for column in table.columns)
    csv_text = output.read_text(encoding="utf-8")
    assert "preburst_ratio_fraction" in csv_text
    assert "0.015" in csv_text
    assert "1e-06" in csv_text


def test_arf_greyscale_and_decision_colours() -> None:
    m = _wrapper()
    assert m._arf_cell(1).style == "#ffffff"
    assert m._arf_cell(0).style == "#6e6e6e"
    assert m._arf_cell(0.5).style not in {"#ffffff", "#6e6e6e"}
    assert m._decision_cell(ScreeningStatus.ZERO).style == "bold green"
    assert m._decision_cell(ScreeningStatus.POSITIVE).style == "bold yellow"
    assert m._excess_cell(0).style == "grey50"
    assert m._excess_cell(0.00000001).style == "white"


def test_ladson_duration_area_interpolation_and_negative_floor_use_pytuflow() -> None:
    arf = pytest.importorskip("pytuflow.arr.arf")
    short = arf.arf_eqn241(10, 720, 0.01)
    params = (0.158, 0.276, 0.372, 0.315, 0.000141, 0.41, 0.15, 0.01, -0.0027)
    long_ = arf.arf_eqn242(10, 1440, 0.01, *params)
    expected_medium = (short + long_) / 2
    assert arf.arf_eqn243(1080, short, long_) == pytest.approx(expected_medium)
    assert arf.arf_eqn244(5, expected_medium) == pytest.approx(1 - 0.6614 * (1 - expected_medium) * (5**0.4 - 1))
    # Ladson's 1000 km² / 1 min / 1% example produces a negative raw
    # factor, which must be floored before it is used as rainfall.
    assert arf.arf_eqn241(1000, 1, 0.01) < 0
    factors = arf.arf_factors(
        1000,
        [1],
        ["1%"],
        dict(zip("abcdefghi", params, strict=True)),
        min_arf=0,
    )
    assert float(factors.loc[1, "1%"]) == 0


def test_arf_cautions_grouped_above_50_below_30_and_zero(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    pytest.importorskip("pytuflow.arr.arf")
    m = _wrapper()
    totals = {
        ("63.2%", 10, None): (10.8, 10.8, 1.0, None, "IL 50; CL 5"),
        ("50%", 10, None): (12.6, 5.84, 0.4633, None, "IL 50; CL 5"),
        ("50%", 20, None): (18.5, 11.0, 0.5947, None, "IL 50; CL 5"),
        ("1%", 60, None): (20.0, 0.0, 0.0, None, "IL 50; CL 5"),
    }
    flags = m._arf_policy_cautions(totals, area=350, override=None)
    assert flags["63.2%", 10, None] == ("AEP_GT_50_UNITY",)
    assert flags["50%", 10, None] == ("DURATION_LT_30_EXTRAPOLATED",)
    assert flags["1%", 60, None] == ("ZERO_ARF_REVIEW",)
    assert ("50%", 10, None) not in m._arf_policy_cautions(totals, area=350, override=0.8)
    screens = [
        PatternScreen(aep, duration, "tp01", "arr_datahub", None, ScreeningStatus.ZERO, 0, 0, "zero")
        for aep, duration, _ in totals
    ]
    m._print_provenance(
        latitude=-23.08632,
        longitude=119.2755,
        site="Test",
        area=350,
        arf_source="PyTUFLOW ARR2019 Book 2 ARF",
        screens=screens,
        loss_timing="full_increment",
        climate="baseline only",
        arf_cautions=flags,
    )
    shown = " ".join(capsys.readouterr().out.split())
    assert "63.2%" in shown
    assert "unity fallback" in shown
    assert "30-minute derivation limit" in shown
    assert "zero" in shown.lower()
    assert "ARF" in shown
    csv_path = tmp_path / "cautions.csv"
    m._print_report(screens, totals, "all", csv_path, arf_cautions=flags)
    rows = list(csv.DictReader(io.StringIO(csv_path.read_text(encoding="utf-8"))))
    keyed = {(row["aep"], int(row["duration_min"])): row for row in rows}
    assert keyed["50%", 10]["arf_cautions"] == "DURATION_LT_30_EXTRAPOLATED"
    assert keyed["63.2%", 10]["arf_cautions"] == "AEP_GT_50_UNITY"
    assert m._arf_cell(0.5, flagged=True).plain.endswith("*")


def test_arr_source_preburst_ratio_stays_constant_when_applied_depth_changes(
    tmp_path: Path,
) -> None:
    pytest.importorskip("pytuflow.arr.complete_storm")
    m = _wrapper()

    class Response:
        def layer(self, name: str, required: bool = False) -> dict[str, object] | None:
            if name == "RecPreburst":
                return None  # Pilbara: 'recommended' falls back to Preburst50.
            if name == "Preburst50":
                return {"index": [60], "columns": [1.0], "data": [[0.015]]}
            raise AssertionError((name, required))

    events = [
        SimpleNamespace(aep_name="1%", duration=60, cc_scenario=None, preburst=SimpleNamespace(depth=1.5)),
        SimpleNamespace(aep_name="1%", duration=60, cc_scenario="2050_SSP2", preburst=SimpleNamespace(depth=2.1)),
    ]
    ratios = m._arr_source_preburst_ratios(Response(), events, "recommended")
    assert ratios["1%", 60, None] == pytest.approx(0.015)
    assert ratios["1%", 60, "2050_SSP2"] == pytest.approx(0.015)
    screens = [
        PatternScreen("1%", 60, "tp01", "arr_datahub", scenario, ScreeningStatus.POSITIVE, 0.1, 0.1, "positive")
        for scenario in (None, "2050_SSP2")
    ]
    totals = {
        ("1%", 60, None): (100.0, 85.0, 0.85, 1.5, "IL 50; CL 5"),
        ("1%", 60, "2050_SSP2"): (140.0, 119.0, 0.85, 2.1, "IL 50; CL 5"),
    }
    output = tmp_path / "pb_source.csv"
    m._print_report(screens, totals, "all", output, source_preburst_ratios=ratios)
    rows = list(csv.DictReader(io.StringIO(output.read_text(encoding="utf-8"))))
    assert [row["preburst_mm"] for row in rows] == ["1.5", "2.1"]
    assert [row["preburst_ratio_fraction"] for row in rows] == ["0.015", "0.015"]
    assert all(row["preburst_ratio_basis"] == "arr_selected_source_ratio" for row in rows)
    # A suppressed/no-preburst event may still have a nonzero source ratio.
    events[0].preburst = None
    assert m._arr_source_preburst_ratios(Response(), events, "recommended")["1%", 60, None] == pytest.approx(0.015)


def test_native_climate_user_losses_reach_pytuflow_configuration(tmp_path: Path) -> None:
    """Use the existing native ARR user-loss path for a uniform storm IL/CL."""
    pytest.importorskip("pytuflow.arr.config")
    m = _wrapper()
    config = m._arr_config(
        -23.08632,
        119.2755,
        350.0,
        "native user losses",
        ["1%"],
        [60],
        2030,
        "complete",
        "recommended",
        tmp_path,
        None,
        user_losses=LossSpec(50.0, continuing_loss_mm_per_hour=5.0),
    )
    assert config.losses.user_initial_loss == 50.0
    assert config.losses.user_continuing_loss == 5.0
    assert config.losses.climate_change_method == "storm"
    with pytest.raises(ValueError, match="require IL/CL"):
        m._arr_config(
            -23.08632,
            119.2755,
            350.0,
            "invalid native coefficient",
            ["1%"],
            [60],
            2030,
            "complete",
            "recommended",
            tmp_path,
            None,
            user_losses=LossSpec(50.0, runoff_coefficient=0.2),
        )


def test_report_csv_retains_user_loss_reference_and_scenario_factors(tmp_path: Path) -> None:
    """Each row carries full-precision source loss year, factors and selected IL/CL."""
    m = _wrapper()
    base = LossSpec(30.0, continuing_loss_mm_per_hour=5.0)
    adjusted = LossSpec(24.0, continuing_loss_mm_per_hour=6.0)
    screens = [
        PatternScreen("1%", 60, "tp1", "arr_datahub", None, ScreeningStatus.ZERO, 0, 0, "zero"),
        PatternScreen("1%", 60, "tp1", "arr_datahub", "2050_SSP2", ScreeningStatus.POSITIVE, 14, 14, "positive"),
    ]
    totals = {
        ("1%", 60, None): (25.0, 25.0, 1.0, None, "IL 30; CL 5"),
        ("1%", 60, "2050_SSP2"): (40.0, 40.0, 1.0, None, "IL 24; CL 6"),
    }
    adjustments = {
        ("1%", 60, None): ArrUserLossAdjustment(base, base, 1990, 1.0, 1.0, "full_storm"),
        ("1%", 60, "2050_SSP2"): ArrUserLossAdjustment(base, adjusted, 1990, 0.8, 1.2, "full_storm"),
    }
    output = tmp_path / "scenario_audit.csv"
    m._print_report(screens, totals, "all", output, loss_adjustments=adjustments, source_kind="arr")
    with output.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 2
    assert [row["user_loss_reference_year"] for row in rows] == ["1990", "1990"]
    assert [row["user_base_il_mm"] for row in rows] == ["30", "30"]
    assert [row["user_il_multiplier"] for row in rows] == ["1", "0.8"]
    assert [row["user_cl_multiplier"] for row in rows] == ["1", "1.2"]
    assert [row["adjusted_user_il_mm"] for row in rows] == ["30", "24"]
    assert [row["adjusted_user_cl_mm_per_hour"] for row in rows] == ["5", "6"]
    assert [row["decision"] for row in rows] == ["DROP", "KEEP"]
    assert all(row["loss_source"] == "user_scenario_override" for row in rows)
    assert all(row["loss_adjustment_method"] == "full_storm" for row in rows)

    native_path = tmp_path / "native_omitted_pb_audit.csv"
    m._print_report(
        screens,
        totals,
        "all",
        native_path,
        loss_adjustments=adjustments,
        native_user_loss=True,
        native_reconciled_keys={("1%", 60, "2050_SSP2")},
        source_kind="arr",
    )
    with native_path.open(encoding="utf-8", newline="") as stream:
        native_rows = list(csv.DictReader(stream))
    assert [row["loss_source"] for row in native_rows] == [
        "user_pytuflow_native",
        "user_pytuflow_omitted_pb_reconciled",
    ]
    assert [row["loss_adjustment_method"] for row in native_rows] == [
        "pytuflow_native_storm",
        "native_storm_omitted_pb_reconciled",
    ]


def test_loss_reference_year_cli_is_independent_of_ifd_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    m = _wrapper()
    assert m.LOSS_REFERENCE_YEAR == 2030
    assert m.IFD_BASELINE_YEAR == 2030
    monkeypatch.setattr(
        sys,
        "argv",
        [str(SCRIPT), "--baseline-year", "2030", "--loss-reference-year", "1990"],
    )
    args = m._arguments()
    assert args.baseline_year == 2030
    assert args.loss_reference_year == 1990
