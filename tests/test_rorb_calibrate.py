"""Focused synthetic tests for standalone RORB calibration (no RORB licence required)."""

import csv
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "ryan-scripts" / "RORB-python"))
import rorb_calibrate as calibration  # noqa: E402


def test_parameter_ranges() -> None:
    assert [str(number) for number in calibration.values("15:20:5,25")] == ["15", "20", "25"]
    with pytest.raises(ValueError):
        calibration.values("0:5:3")
    with pytest.raises(ValueError):
        calibration.values("0:5:0")


def test_rmse_is_square_root_of_mse() -> None:
    metrics = calibration.score([1, 3], [3, 1], [0, 1])
    assert metrics["rmse"] == 2
    assert metrics["nse"] == -3


def test_parse_hydrographs(tmp_path: Path) -> None:
    output = tmp_path / "sample.out"
    output.write_text("junk\n Inc    Time    Hyd001   Hyd002\n 1  0.0  12.0  11.0\n 2  1.0  15.0  14.0\nend\n")
    data = calibration.parse_hydrographs(output)
    assert data["Hyd001"] == [12, 15]
    assert data["Hyd002"] == [11, 14]


def test_reject_ambiguous_hydrograph_tables(tmp_path: Path) -> None:
    output = tmp_path / "two.out"
    output.write_text("Inc Time Hyd001\n1 0 1\nEnd\nInc Time Hyd001\n1 0 1\n")
    with pytest.raises(ValueError, match="2 hydrograph tables"):
        calibration.parse_hydrographs(output)


def test_preflight_does_not_write(tmp_path: Path) -> None:
    catg, stm = tmp_path / "a.catg", tmp_path / "b.stm"
    catg.write_text("a")
    stm.write_text("b")
    out = tmp_path / "results"
    assert calibration.main(
        ["--catg", str(catg), "--stm", str(stm), "--rorb", sys.executable, "--output", str(out), "--dry-run"]
    ) == 0
    assert not out.exists()


def test_synthetic_rorb_execution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    catg, stm = tmp_path / "a.catg", tmp_path / "b.stm"
    catg.write_text("a")
    stm.write_text("b")

    def fake_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        work_dir = kwargs["cwd"]
        assert isinstance(work_dir, Path)
        (work_dir / "fake.out").write_text("Inc Time Hyd001 Hyd002\n1 0 1 2\n2 1 4 3\n")
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(calibration.subprocess, "run", fake_run)
    out = tmp_path / "results"
    assert calibration.main(
        [
            "--catg", str(catg), "--stm", str(stm), "--rorb", sys.executable,
            "--output", str(out), "--kc", "15", "--il", "10",
            "--continuing", "5", "--sim-column", "Hyd001", "--obs-column", "Hyd002",
        ]
    ) == 0
    results = next(out.glob("*/results.csv"))
    with results.open(newline="") as stream:
        records = list(csv.DictReader(stream))
    assert len(records) == 1
    assert records[0]["status"] == "success"
    assert records[0]["rmse"] == "1.0"
