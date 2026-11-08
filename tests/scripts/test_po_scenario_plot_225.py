"""WP10 #225: PO series identity, units, selections, cache provenance and safe exports."""

from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType
from typing import Any, ClassVar

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd
import pytest

SCRIPT = (
    Path(__file__).absolute().parents[2]
    / "ryan-scripts/TUFLOW-python/po_and_timeseries/plot_po_scenario_timeseries.py"
)


@pytest.fixture
def plotter(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    mpl.use("Agg", force=True)
    spec = importlib.util.spec_from_file_location("wp10_po_plot", SCRIPT)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "SHOW_PLOTS", False)
    monkeypatch.setattr(module, "EXPORT_PNG", False)
    monkeypatch.setattr(module, "PLOT_LOCATIONS", ["L1"])
    monkeypatch.setattr(module, "MEASUREMENT_TYPES", None)
    monkeypatch.setattr(module, "INTERNAL_NAME_FILTER", "R_")
    monkeypatch.setattr(module, "LOCATION_FILTER", None)
    return module


def sample_frame() -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for run, offsets in (("R_A", (0, 100)), ("R_B", (20, 110))):
        for measurement, y in (("Flow", offsets[0]), ("Water Level", offsets[1])):
            for hour, increment in (("0", 0), ("1", 1)):
                rows.append(
                    {
                        "internalName": run,
                        "Location": "L1",
                        "Type": measurement,
                        "Time": hour,
                        "Value": y + 10 * increment if measurement == "Flow" else y + increment,
                        "aep_text": "1%",
                        "aep_numeric": 1.0,
                        "tp_text": "TP01",
                        "duration_text": "60m",
                    }
                )
    return pd.DataFrame(rows)


def test_headless_series_preserve_measurement_and_full_run_id(
    plotter: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    drawn: list[tuple[Any, Any]] = []
    original = plt.subplots

    def capture(*args: Any, **kwargs: Any) -> tuple[Any, Any]:
        fig, ax = original(*args, **kwargs)
        drawn.append((fig, ax))
        return fig, ax

    monkeypatch.setattr(plotter.plt, "subplots", capture)
    plotter.plot_timeseries(sample_frame())
    assert len(drawn) == 2

    results = {ax.get_ylabel(): ax for _, ax in drawn}
    assert set(results) == {"Flow (m³/s)", "Water level (m)"}
    flow = results["Flow (m³/s)"]
    level = results["Water level (m)"]
    assert flow.get_xlabel() == "Simulation time (h)"
    assert len(flow.lines) == len(level.lines) == 2
    flow_values = {line.get_label().split("Run=")[1].split(" | ")[0]: list(line.get_ydata()) for line in flow.lines}
    assert flow_values == {"R_A": [0, 10], "R_B": [20, 30]}
    level_values = {line.get_label().split("Run=")[1].split(" | ")[0]: list(line.get_ydata()) for line in level.lines}
    assert level_values == {"R_A": [100, 101], "R_B": [110, 111]}
    for _, ax in drawn:
        for line in ax.lines:
            assert list(line.get_xdata()) == [0.0, 1.0]
            assert "AEP=1%" in line.get_label()
            assert "Type=" in line.get_label()


def test_filter_is_literal_and_does_not_restore_nonmatching_rows(
    plotter: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = sample_frame().assign(internalName=lambda df: df["internalName"].replace({"R_A": "A.1", "R_B": "A_1"}))
    monkeypatch.setattr(plotter, "INTERNAL_NAME_FILTER", "A.1")
    assert set(plotter.filter_internal_name(frame)["internalName"]) == {"A.1"}
    monkeypatch.setattr(plotter, "INTERNAL_NAME_FILTER", "missing")
    with pytest.raises(ValueError, match=r"INTERNAL_NAME_FILTER.*matched no"):
        plotter.filter_internal_name(frame)


def test_missing_filter_column_is_an_error(plotter: ModuleType) -> None:
    with pytest.raises(KeyError, match="internalName"):
        plotter.filter_internal_name(sample_frame().drop(columns=["internalName"]))
    plotter.LOCATION_FILTER = {"L1"}
    with pytest.raises(KeyError, match="Location"):
        plotter.filter_locations(sample_frame().drop(columns=["Location"]))


def test_unmatched_locations_and_measurements_fail(plotter: ModuleType) -> None:
    plotter.LOCATION_FILTER = {"not-here"}
    with pytest.raises(ValueError, match="LOCATION_FILTER"):
        plotter.select_po_dataframe(sample_frame())
    plotter.LOCATION_FILTER = None
    plotter.MEASUREMENT_TYPES = ["Velocity"]
    with pytest.raises(ValueError, match="MEASUREMENT_TYPES"):
        plotter.plot_timeseries(sample_frame())
    plotter.MEASUREMENT_TYPES = None
    plotter.PLOT_LOCATIONS = ["not-here"]
    with pytest.raises(ValueError, match="PLOT_LOCATIONS"):
        plotter.plot_timeseries(sample_frame())


def test_time_numeric_strings_are_hours_not_dates(plotter: ModuleType) -> None:
    frame = sample_frame()
    frame.loc[frame["Time"] == "0", "Time"] = "-0.5"
    prepared = plotter.prepare_plot_df(frame)
    assert set(prepared["Time"]) == {-0.5, 1.0}
    assert pd.api.types.is_numeric_dtype(prepared["Time"])
    assert plotter._measurement_ylabel("HU") == "Water level (m)"
    assert "units unspecified" in plotter._measurement_ylabel("Sediment")


@pytest.mark.parametrize("invalid", ["bad", "NaN", "inf", None])
def test_bad_time_fails_with_actionable_error(plotter: ModuleType, invalid: object) -> None:
    frame = sample_frame()
    frame.loc[0, "Time"] = invalid
    with pytest.raises(ValueError, match="Time must contain finite numeric"):
        plotter.prepare_plot_df(frame)


def test_missing_scenario_dimension_rejected(plotter: ModuleType) -> None:
    with pytest.raises(KeyError, match="scenario"):
        plotter.prepare_plot_df(sample_frame().drop(columns=["duration_text"]))


def cache_environment(
    plotter: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, df: pd.DataFrame
) -> Path:
    source = tmp_path / "R_A_PO.csv"
    source.write_text("source\n", encoding="utf-8")
    monkeypatch.setattr(plotter, "TUFLOW_RESULTS_ROOT", tmp_path)
    monkeypatch.setattr(plotter, "FILE_PATTERN", "*_PO.csv")
    monkeypatch.setattr(plotter, "PARQUET_PATH", tmp_path / "cache.parquet")
    monkeypatch.setattr(plotter, "LOCATION_FILTER", {"L1"})
    monkeypatch.setattr(plotter, "LOAD_FROM_PARQUET", False)
    monkeypatch.setattr(plotter, "REPLACE_PARQUET_CACHE", False)

    class Collection:
        processors: ClassVar[list[object]] = [object()]

        def po_combine(self) -> pd.DataFrame:
            return df.copy()

    monkeypatch.setattr(plotter, "process_files_sequentially", lambda _files: Collection())
    return source


def test_csv_and_verified_cache_select_the_same_rows(
    plotter: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyarrow")
    df = pd.concat([sample_frame(), sample_frame().assign(Location="L2")], ignore_index=True)
    cache_environment(plotter, tmp_path, monkeypatch, df)
    uncached = plotter.load_po_dataframe()
    assert len(uncached) == len(sample_frame())
    plotter.LOAD_FROM_PARQUET = True
    cached = plotter.load_po_dataframe()
    pd.testing.assert_frame_equal(uncached.reset_index(drop=True), cached.reset_index(drop=True), check_dtype=False)


def test_cache_rejects_old_schema_and_missing_provenance(
    plotter: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyarrow")
    cache_environment(plotter, tmp_path, monkeypatch, sample_frame())
    sample_frame().to_parquet(plotter.PARQUET_PATH, index=False)
    plotter.LOAD_FROM_PARQUET = True
    with pytest.raises(ValueError, match="provenance"):
        plotter.load_po_dataframe()


def test_cache_rejects_changed_source_and_ingest_config(
    plotter: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyarrow")
    source = cache_environment(plotter, tmp_path, monkeypatch, sample_frame())
    plotter.load_po_dataframe()
    original_cache = plotter.PARQUET_PATH.read_bytes()
    plotter.LOAD_FROM_PARQUET = True
    source.write_text("source changed\n", encoding="utf-8")
    with pytest.raises(ValueError, match="stale"):
        plotter.load_po_dataframe()
    source.write_text("source\n", encoding="utf-8")
    plotter.LOCATION_FILTER = {"L2"}
    with pytest.raises(ValueError, match="stale"):
        plotter.load_po_dataframe()
    assert plotter.PARQUET_PATH.read_bytes() == original_cache


def test_cache_requires_explicit_overwrite_and_preserves_on_write_failure(
    plotter: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyarrow")
    cache_environment(plotter, tmp_path, monkeypatch, sample_frame())
    plotter.load_po_dataframe()
    original_cache = plotter.PARQUET_PATH.read_bytes()
    with pytest.raises(FileExistsError, match="REPLACE_PARQUET_CACHE"):
        plotter.load_po_dataframe()
    assert plotter.PARQUET_PATH.read_bytes() == original_cache
    plotter.REPLACE_PARQUET_CACHE = True

    def failed_write(_df: pd.DataFrame, path: Path, **_kwargs: Any) -> None:
        path.write_bytes(b"incomplete parquet")
        msg = "simulated interrupted parquet write"
        raise OSError(msg)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", failed_write)
    with pytest.raises(OSError, match="interrupted"):
        plotter.load_po_dataframe()
    assert plotter.PARQUET_PATH.read_bytes() == original_cache


def test_headless_png_is_bounded_and_no_clobber(
    plotter: ModuleType, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(plotter, "MEASUREMENT_TYPES", ["Flow"])
    monkeypatch.setattr(plotter, "PNG_OUTPUT_DIR", tmp_path)
    monkeypatch.setattr(plotter, "EXPORT_PNG", True)
    plotter.plot_timeseries(sample_frame())
    files = list(tmp_path.glob("*.png"))
    assert len(files) == 1
    original = files[0].read_bytes()
    assert original.startswith(b"\x89PNG")
    with pytest.raises(FileExistsError, match="OVERWRITE_PNG"):
        plotter.plot_timeseries(sample_frame())
    assert files[0].read_bytes() == original
