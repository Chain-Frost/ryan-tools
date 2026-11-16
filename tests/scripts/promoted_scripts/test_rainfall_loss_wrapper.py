"""Focused wrapper checks; parsing and ARR network acceptance are separate gates."""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path
from typing import Any, cast

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


def test_console_table_and_exclusive_csv(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    m = _wrapper()
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
    # One Rich horizontal separator between each adjacent AEP group.
    assert shown.count("├") == 2
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
    assert "bin mm" in shown
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
    captured = capsys.readouterr().err
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
    assert "ARF 1.0000" in capsys.readouterr().out



def test_compact_headers_aep_group_lines_and_preburst_ratio_csv(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> None:
    from rich.table import Table

    m = _wrapper()
    screens = [
        PatternScreen("20%", 60, "tp01", "arr_datahub", None, ScreeningStatus.ZERO, 0, 0, "zero", 0),
        PatternScreen("20%", 90, "tp01", "arr_datahub", None, ScreeningStatus.ZERO, 0, 0, "zero", 0),
        PatternScreen("10%", 60, "tp01", "arr_datahub", None, ScreeningStatus.POSITIVE, 0.000001, 0.000001, "positive", 0.000001),
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
    assert any("PB\nratio" == column.header for column in table.columns)
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
