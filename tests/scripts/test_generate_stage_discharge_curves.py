"""Regression tests for stage-discharge curves and all-or-nothing report publication."""

from __future__ import annotations

import csv
import importlib.util
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Protocol, cast

import numpy as np
import pytest

SCRIPT = (
    Path(__file__).absolute().parents[2]
    / "ryan-scripts"
    / "unsorted-python"
    / "generate_stage_discharge_curves.py"
)


class Reference(Protocol):
    identifier: str
    flow_line: str
    water_level_point: str


class CurveSelection(Protocol):
    source: Path
    maximum_water_level: float


class Peak(Protocol):
    source: Path
    value: float
    aep_label: str


class StageModule(Protocol):
    def read_reference(self, path: Path) -> list[Reference]: ...
    def select_curve_files(self, curve_folder: Path, references: list[Reference]) -> dict[str, CurveSelection]: ...
    def select_peak_flows(self, root: Path, references: list[Reference]) -> dict[str, list[Peak]]: ...
    def load_curve_data(
        self, source: Path, reference: Reference
    ) -> tuple[np.ndarray, np.ndarray, float, dict[str, float | int]]: ...
    def main(self, argv: Sequence[str] | None = None) -> int: ...


@pytest.fixture
def module() -> StageModule:
    spec = importlib.util.spec_from_file_location("stage_discharge_tested", SCRIPT)
    assert spec is not None and spec.loader is not None
    loaded = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = loaded
    spec.loader.exec_module(loaded)
    return cast("StageModule", loaded)


def make_reference(path: Path, rows: list[tuple[str, str, str]]) -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["ID", "Flow Line", "Water Level Point"])
        writer.writerows(rows)
    return path


def make_po(
    path: Path,
    flows: list[float],
    levels: list[float],
    *,
    flow_name: str = "FL_A",
    wl_name: str = "WL_A",
) -> Path:
    assert len(flows) == len(levels)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Source", "Time", "Flow", "Water Level", "Depth"])
        writer.writerow(["unit", "hours", flow_name, wl_name, wl_name])
        for hour, (flow, level) in enumerate(zip(flows, levels, strict=True)):
            writer.writerow(["", hour, flow, level, level - 100])
    return path


def make_pomm(path: Path, value: str, *, flow_name: str = "FL_A") -> Path:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["Source", "Statistic", "Flow"])
        writer.writerow(["unit", "name", flow_name])
        writer.writerow(["", "Maximum", value])
    return path


def fixture_inputs(tmp_path: Path) -> tuple[Path, Path]:
    root = tmp_path / "results"
    curve = root / "01.0p"
    curve.mkdir(parents=True)
    make_po(curve / "first_PO.csv", [0, 5, 10, 7], [100, 101, 102, 103])
    make_pomm(curve / "first_POMM.csv", "10")
    reference = make_reference(tmp_path / "references.csv", [("FG01", "FL_A", "WL_A")])
    return root, reference


def cli(root: Path, reference: Path, output: Path, *extra: str) -> list[str]:
    return [
        "--results-root",
        str(root),
        "--reference",
        str(reference),
        "--output-dir",
        str(output),
        *extra,
    ]


def test_rising_only_curve_preserves_measured_flow_range(module: StageModule, tmp_path: Path) -> None:
    po = make_po(tmp_path / "rising_PO.csv", [0, 5, 10, 15], [100, 101, 102, 103])
    reference_path = make_reference(tmp_path / "refs.csv", [("FG01", "FL_A", "WL_A")])
    ref = module.read_reference(reference_path)[0]
    flow, wl, _, stats = module.load_curve_data(po, ref)
    assert flow.tolist() == [0, 5, 10, 15]
    assert wl.tolist() == pytest.approx([100, 101, 102, 103])
    assert stats["envelope_points"] == 4
    assert stats["overlap_points"] == 1


def test_partially_receding_curve_uses_measured_union_no_extrapolation(
    module: StageModule, tmp_path: Path
) -> None:
    po = make_po(tmp_path / "partial_PO.csv", [0, 5, 10, 7], [100, 101, 102, 104])
    ref_path = make_reference(tmp_path / "refs.csv", [("FG01", "FL_A", "WL_A")])
    flow, wl, _, stats = module.load_curve_data(po, module.read_reference(ref_path)[0])
    assert flow.tolist() == [0, 5, 7, 10]
    assert wl.tolist() == pytest.approx([100, 101, 104, 102])
    assert stats["falling_points"] == 1
    assert stats["rising_points"] == 3
    assert stats["overlap_points"] == 2


def test_split_po_and_pomm_files_are_selected_per_location(module: StageModule, tmp_path: Path) -> None:
    curve = tmp_path / "results" / "01.0p"
    curve.mkdir(parents=True)
    make_po(curve / "A_PO.csv", [0, 2, 4], [100, 101, 102], flow_name="FL_A", wl_name="WL_A")
    make_po(curve / "B_PO.csv", [0, 3, 5], [100, 101, 103], flow_name="FL_B", wl_name="WL_B")
    make_pomm(curve / "A_POMM.csv", "4", flow_name="FL_A")
    make_pomm(curve / "B_POMM.csv", "5", flow_name="FL_B")
    reference = make_reference(
        tmp_path / "refs.csv", [("FG01", "FL_A", "WL_A"), ("FG02", "FL_B", "WL_B")]
    )
    refs = module.read_reference(reference)
    selections = module.select_curve_files(curve, refs)
    assert selections["FG01"].source.name == "A_PO.csv"
    assert selections["FG02"].source.name == "B_PO.csv"
    peaks = module.select_peak_flows(tmp_path / "results", refs)
    assert peaks["FG01"][0].value == 4
    assert peaks["FG02"][0].value == 5


def test_invalid_maxima_never_become_negative_infinity(module: StageModule, tmp_path: Path) -> None:
    curve = tmp_path / "results" / "01.0p"
    curve.mkdir(parents=True)
    make_pomm(curve / "A_POMM.csv", "NaN")
    refs = module.read_reference(make_reference(tmp_path / "refs.csv", [("FG01", "FL_A", "WL_A")]))
    with pytest.raises(RuntimeError, match="No finite POMM maximum"):
        module.select_peak_flows(tmp_path / "results", refs)
    make_pomm(curve / "B_POMM.csv", "12.5")
    assert module.select_peak_flows(tmp_path / "results", refs)["FG01"][0].value == 12.5


@pytest.mark.parametrize("unsafe", ["../escape", r"outside\name", "NUL", "FG01.", "FG:01", ".."])
def test_unsafe_reference_ids_rejected(module: StageModule, tmp_path: Path, unsafe: str) -> None:
    path = make_reference(tmp_path / "refs.csv", [(unsafe, "FL_A", "WL_A")])
    with pytest.raises(ValueError, match="Unsafe reference ID"):
        module.read_reference(path)


def test_case_colliding_reference_ids_rejected(module: StageModule, tmp_path: Path) -> None:
    path = make_reference(tmp_path / "refs.csv", [("FG01", "FL_A", "WL_A"), ("fg01", "FL_B", "WL_B")])
    with pytest.raises(ValueError, match="ignoring Windows filename case"):
        module.read_reference(path)


def test_dry_run_validates_without_creating_output(module: StageModule, tmp_path: Path) -> None:
    root, reference = fixture_inputs(tmp_path)
    output = tmp_path / "reports" / "Stage_Discharge"
    assert module.main(cli(root, reference, output, "--dry-run")) == 0
    assert not output.exists()
    assert not output.parent.exists()


def test_report_is_a_complete_dedicated_directory(module: StageModule, tmp_path: Path) -> None:
    root, reference = fixture_inputs(tmp_path)
    output = tmp_path / "reports" / "Stage_Discharge"
    assert module.main(cli(root, reference, output)) == 0
    assert (output / "FG01_Stage_Discharge_Curve.png").is_file()
    assert (output / "Stage_Discharge_Curve_Selection_Summary.csv").is_file()
    assert (output / "Stage_Discharge_AEP_Peak_Flows.csv").is_file()
    assert (output / "Stage_Discharge_Limb_Comparison.csv").is_file()
    assert (output / ".stage_discharge_report").is_file()
    assert not list(output.parent.glob(".*.staging-*"))
    assert module.main(cli(root, reference, tmp_path / "dry_report", "--dry-run")) == 0
    with pytest.raises(FileExistsError):
        module.main(cli(root, reference, output))


def test_failed_overwrite_preserves_existing_report(
    module: StageModule, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, reference = fixture_inputs(tmp_path)
    output = tmp_path / "Report"
    assert module.main(cli(root, reference, output)) == 0
    previous = {entry.name: entry.read_bytes() for entry in output.iterdir()}

    def fail_plot(*_args: object, **_kwargs: object) -> None:
        message = "simulated plot write failure"
        raise RuntimeError(message)

    # Module dynamically loaded from the standalone file: mutate its plotting callback.
    target = sys.modules["stage_discharge_tested"]
    monkeypatch.setattr(target, "plot_curve", fail_plot)
    with pytest.raises(RuntimeError, match="simulated plot write failure"):
        module.main(cli(root, reference, output, "--overwrite"))
    assert {entry.name: entry.read_bytes() for entry in output.iterdir()} == previous
    assert not list(tmp_path.glob(".*.staging-*"))


def test_overwrite_rejects_foreign_output_and_source_reference(
    module: StageModule, tmp_path: Path
) -> None:
    root, reference = fixture_inputs(tmp_path)
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    (foreign / "private.txt").write_text("preserve", encoding="utf-8")
    with pytest.raises(ValueError, match="not created by this script"):
        module.main(cli(root, reference, foreign, "--overwrite"))
    assert (foreign / "private.txt").read_text(encoding="utf-8") == "preserve"

    # A script-owned marker must not authorise replacing the input reference.
    with pytest.raises(ValueError, match="may not contain the reference"):
        module.main(cli(root, reference, reference.parent, "--overwrite"))


def test_valid_report_can_be_explicitly_replaced(module: StageModule, tmp_path: Path) -> None:
    root, reference = fixture_inputs(tmp_path)
    output = tmp_path / "Report"
    assert module.main(cli(root, reference, output)) == 0
    original = (output / "Stage_Discharge_AEP_Peak_Flows.csv").read_text(encoding="utf-8-sig")
    # New input: the entire report is rebuilt, then published as a directory.
    make_pomm(root / "01.0p" / "new_POMM.csv", "15")
    assert module.main(cli(root, reference, output, "--overwrite")) == 0
    updated = (output / "Stage_Discharge_AEP_Peak_Flows.csv").read_text(encoding="utf-8-sig")
    assert updated != original
    assert "15.0000" in updated
    assert not list(tmp_path.glob(".*.backup"))
