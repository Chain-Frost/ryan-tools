"""Independent fixture-generator and safety checks for WP11 issue #229."""

from __future__ import annotations

import csv
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

SCRIPT_PATH = Path(__file__).parents[2] / "repo-scripts" / "benchmarks" / "benchmark_po_workload.py"


def load_script() -> ModuleType:
    spec = importlib.util.spec_from_file_location("benchmark_po_workload", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        message = f"Could not import {SCRIPT_PATH}"
        raise AssertionError(message)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # Dataclass type lookup needs its module registered.
    spec.loader.exec_module(module)
    return module


BENCH = load_script()


def test_full_spec_has_exact_unique_event_grid_and_endpoints() -> None:
    full = BENCH.workload("full")
    assert len(full.run_codes) == 300
    assert len(set(full.run_codes)) == 300
    assert full.timestamps == 2881
    assert full.observations(False) == 259_290_000
    assert full.observations(True) == 51_858_000
    assert full.observations(False) * 24 == 6_222_960_000
    assert full.observations(True) * 24 == 1_244_592_000
    assert full.run_codes[0] == "M11_01p_00060m_TP01"
    assert full.run_codes[-1] == "M11_20p_02880m_TP10"


def test_writer_preserves_header_time_signed_zero_and_independent_values(tmp_path: Path) -> None:
    path = tmp_path / "M11_01p_00120m_TP01_PO.csv"
    BENCH.write_po_csv(path, run_index=0, locations=2, final_minute=120)
    with path.open(newline="", encoding="utf-8") as stream:
        rows = list(csv.reader(stream))
    assert len(rows) == 123  # 2 headers + minutes 0 through 120 inclusive.
    assert rows[0] == ["", "Time", "H", "Q", "V", "H", "Q", "V"]
    assert rows[1] == ["", "Time", "L001", "L001", "L001", "L002", "L002", "L002"]
    assert float(rows[2][1]) == 0.0
    assert float(rows[2][3]) == -1.0
    assert float(rows[102][3]) == 0.0  # At 100 minutes, Q for L001 is exactly zero.
    assert float(rows[-1][1]) == 2.0
    assert float(rows[-1][2]) == pytest.approx(10.24)
    assert float(rows[-1][5]) == pytest.approx(10.34)
    assert float(rows[-1][6]) == pytest.approx(0.25)


def test_generation_refuses_to_replace_an_existing_fixture_directory(tmp_path: Path) -> None:
    spec = BENCH.workload("scaled")
    paths = BENCH.make_fixture(tmp_path / "input", spec)
    assert len(paths) == 2
    first_hash = paths[0].read_bytes()
    with pytest.raises(FileExistsError):
        BENCH.make_fixture(tmp_path / "input", spec)
    assert paths[0].read_bytes() == first_hash


def test_full_series_ram_gate_runs_before_parsing(tmp_path: Path) -> None:
    spec = BENCH.workload("scaled")
    with pytest.raises(ValueError, match="RAM budget"):
        BENCH.run_case(
            [tmp_path / "nonexistent_PO.csv"],
            spec,
            selected=False,
            summary_only=False,
            parallel=False,
            output_root=tmp_path,
            file_format="csv",
            workers=1,
            max_numeric_gib=0.000001,
        )


def test_selection_is_20_of_100_in_full_workload() -> None:
    spec = BENCH.workload("full")
    assert BENCH._selected_ids(spec, True) == [f"L{i:03d}" for i in range(1, 21)]
    assert len(BENCH._selected_ids(spec, False)) == 100
    assert BENCH.value_at(0, 0, 0, "Q") < 0
    assert BENCH.value_at(0, 0, 100, "Q") == 0
    with pytest.raises(ValueError, match="Unsupported"):
        BENCH.value_at(0, 0, 0, "Qx")


def test_export_readback_matches_source_values_and_exact_peak_times(tmp_path: Path) -> None:
    """Exercise the offline validator independently of the installed PO processor."""
    import pandas as pd

    spec = BENCH.Workload(["01p"], [60], [1], 2, 120, 1)
    sources = BENCH.make_fixture(tmp_path / "source", spec)
    reference = BENCH._source_reference(sources, spec)
    observations = []
    with sources[0].open(newline="", encoding="utf-8") as stream:
        for minute, row in enumerate(list(csv.reader(stream))[2:]):
            for location in range(2):
                for index, kind in enumerate(BENCH.VARIABLES):
                    value = float(row[2 + location * 3 + index])
                    observations.append(
                        {
                            "internalName": spec.run_codes[0],
                            "Location": f"L{location + 1:03d}",
                            "Type": kind,
                            "Time": minute / 60,
                            "Value": value,
                            "AbsValue": abs(value),
                        }
                    )
    frame = pd.DataFrame(observations)
    series_file = tmp_path / "series.csv"
    frame.to_csv(series_file, index=False)
    valid = BENCH._validate_export(series_file, reference, spec, selected=False, summary_only=False)
    assert valid["status"] == "PASS"
    assert valid["source_keys_checked"] > 0

    damaged = frame.copy()
    damaged.loc[0, "Value"] = 99.0
    damaged.to_csv(series_file, index=False)
    with pytest.raises(AssertionError, match="source Value"):
        BENCH._validate_export(series_file, reference, spec, selected=False, summary_only=False)

    summary = frame.loc[frame.groupby(["internalName", "Location", "Type"])["AbsValue"].idxmax()].copy()
    summary_file = tmp_path / "summary.csv"
    summary.to_csv(summary_file, index=False)
    valid_summary = BENCH._validate_export(summary_file, reference, spec, selected=False, summary_only=True)
    assert valid_summary["validated_rows"] == 6
    damaged_peak = summary.copy()
    damaged_peak.iloc[0, damaged_peak.columns.get_loc("Time")] += 1 / 60
    damaged_peak.to_csv(summary_file, index=False)
    with pytest.raises(AssertionError, match="peak/sample time"):
        BENCH._validate_export(summary_file, reference, spec, selected=False, summary_only=True)


def test_source_oracle_rejects_changed_headers_and_reserves_all_enabled_outputs(tmp_path: Path) -> None:
    spec = BENCH.Workload(["01p"], [60], [1], 2, 8, 1)
    sources = BENCH.make_fixture(tmp_path / "input", spec)
    original = sources[0].read_text(encoding="utf-8")
    sources[0].write_text(original.replace(",H,Q,V,", ",Q,H,V,", 1), encoding="utf-8")
    with pytest.raises(ValueError, match="header drift"):
        BENCH._source_reference(sources, spec)

    full = BENCH.workload("full")
    input_only = full.observations(False) * 21
    small_budget = BENCH._disk_reservation_bytes(full, 1.0)
    generous_budget = BENCH._disk_reservation_bytes(full, 64.0)
    assert small_budget > input_only
    assert generous_budget > small_budget
