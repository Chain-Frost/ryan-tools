"""Focused synthetic tests for standalone RORB calibration (no RORB licence required)."""

import csv
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "ryan-scripts" / "RORB-python"))
import rorb_calibrate as calibration


def test_parameter_ranges() -> None:
    assert [str(number) for number in calibration.values("15:20:5,25")] == ["15", "20", "25"]
    with pytest.raises(ValueError):
        calibration.values("0:5:3")
    with pytest.raises(ValueError):
        calibration.values("0:5:0")


@pytest.mark.parametrize(("model", "continuing"), [(1, "5"), (2, "0.35")])
def test_cmd_par_matches_calibration_format(model: int, continuing: str) -> None:
    case = calibration.Combination(Decimal(15), Decimal(10), Decimal(continuing))
    text = calibration.render_par(Path("catchment.catg"), Path("storm.stm"), case, Decimal("0.8"), model)
    assert text == (
        "# BEGIN\nCat file :catchment.catg\nStm file :storm.stm\nLumped kc:T\nVerbosity:3\n"
        f"Lossmodel:{model}\nNum ISA  :1\nISA 1    :15,0.8\nNum burst:1\nISA 1    :10,{continuing}\n# END\n"
    )


@pytest.mark.parametrize("flags", [["--dry-run"], ["--execute"]])
def test_invalid_runoff_coefficient_is_rejected_before_writes(tmp_path: Path, flags: list[str]) -> None:
    catg, stm = tmp_path / "model.catg", tmp_path / "event.stm"
    catg.touch()
    stm.touch()
    out = tmp_path / "results"
    args = [
        "--catg",
        str(catg),
        "--stm",
        str(stm),
        "--rorb",
        sys.executable,
        "--output",
        str(out),
        "--sim-column",
        "Hyd001",
        "--obs-column",
        "Hyd002",
        "--loss-model",
        "2",
        "--continuing",
        "0.5,1.01",
    ]
    assert calibration.main([*args, *flags]) == 1
    assert not out.exists()


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


@pytest.mark.parametrize(
    ("rows", "error"),
    [
        ("1 0 1 1\n2 1 ******** 5\n3 2 3 4\n", "Malformed hydrograph row"),
        ("1 0 1 1\n2 1 3\n3 2 3 4\n", "Malformed hydrograph row"),
        ("1 0 1 1\n2 1 3 4 extra\n3 2 3 4\n", "Malformed hydrograph row"),
        ("1 0 1 1\n2 1 3 nan\n3 2 3 4\n", "Non-finite hydrograph"),
        ("1 0 invalid 2\n2 1 3 4\n", "Malformed hydrograph row"),
    ],
)
def test_reject_malformed_numeric_hydrograph_rows(tmp_path: Path, rows: str, error: str) -> None:
    output = tmp_path / "bad.out"
    output.write_text("Inc Time Hyd001 Hyd002\n" + rows + "End of hydrograph\n")
    with pytest.raises(ValueError, match=error):
        calibration.parse_hydrographs(output)


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
    assert (
        calibration.main(
            ["--catg", str(catg), "--stm", str(stm), "--rorb", sys.executable, "--output", str(out), "--dry-run"]
        )
        == 0
    )
    assert not out.exists()


@pytest.mark.parametrize(
    ("configured_execute", "flags"),
    [(False, []), (True, ["--no-execute"]), (True, ["--dry-run"]), (False, ["--execute", "--dry-run"])],
)
def test_calibration_preflight_requires_execution_opt_in(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, configured_execute: bool, flags: list[str]
) -> None:
    catg, stm = tmp_path / "a.catg", tmp_path / "b.stm"
    catg.write_text("a")
    stm.write_text("b")
    out = tmp_path / "results"

    def prohibited_run(*_args: object, **_kwargs: object) -> subprocess.CompletedProcess[str]:
        pytest.fail("Preflight must never launch an external process")

    monkeypatch.setattr(calibration, "CATG_FILE", catg)
    monkeypatch.setattr(calibration, "STM_FILE", stm)
    monkeypatch.setattr(calibration, "RORB_EXECUTABLE", sys.executable)
    monkeypatch.setattr(calibration, "OUTPUT_PARENT", out)
    monkeypatch.setattr(calibration, "SIM_COLUMN", "Hyd001")
    monkeypatch.setattr(calibration, "OBS_COLUMN", "Hyd002")
    monkeypatch.setattr(calibration, "EXECUTE", configured_execute)
    monkeypatch.setattr(calibration.subprocess, "run", prohibited_run)

    assert calibration.main(flags) == 0
    assert not out.exists()


def test_editable_settings_and_explicit_cli_overrides(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    default_catg = tmp_path / "default.catg"
    cli_catg = tmp_path / "override.catg"
    default_report = tmp_path / "inspect.out"
    cli_report = tmp_path / "score.out"
    cli_csv = tmp_path / "observed.csv"
    monkeypatch.setattr(calibration, "CATG_FILE", default_catg)
    monkeypatch.setattr(calibration, "INSPECT_OUTPUT", default_report)
    monkeypatch.setattr(calibration, "OBS_COLUMN", "Hyd002")
    monkeypatch.setattr(calibration, "KC_VALUES", "12")
    monkeypatch.setattr(calibration, "PLOTS", True)
    monkeypatch.setattr(calibration, "HMA", True)
    monkeypatch.setattr(calibration, "EXECUTE", True)
    monkeypatch.setattr(calibration, "DRY_RUN", True)

    defaults = calibration.resolve_arguments(calibration.parser(), [])
    assert defaults.catg == default_catg
    assert defaults.inspect_output == default_report
    assert defaults.obs_column == "Hyd002"
    assert defaults.kc == "12"
    assert defaults.plots
    assert defaults.hma
    assert defaults.execute
    assert defaults.dry_run

    override = calibration.resolve_arguments(
        calibration.parser(),
        [
            "--catg",
            str(cli_catg),
            "--score-output",
            str(cli_report),
            "--observed-csv",
            str(cli_csv),
            "--kc",
            "15",
            "--hma-window",
            "0",
            "--no-plots",
            "--no-hma",
            "--no-execute",
            "--no-dry-run",
        ],
    )
    assert override.catg == cli_catg
    assert override.inspect_output is None
    assert override.score_output == cli_report
    assert override.obs_column is None
    assert override.observed_csv == cli_csv
    assert override.kc == "15"
    assert override.hma_window == 0
    assert not override.plots
    assert not override.hma
    assert not override.execute
    assert not override.dry_run

    live = calibration.resolve_arguments(calibration.parser(), ["--calibrate"])
    assert live.inspect_output is None
    assert live.score_output is None


def test_synthetic_rorb_execution(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    catg, stm = tmp_path / "a.catg", tmp_path / "b.stm"
    catg_bytes, stm_bytes = b"catchment\r\nfixed padding   \r\n", b"storm\r\n1,2,-99\r\n"
    catg.write_bytes(catg_bytes)
    stm.write_bytes(stm_bytes)

    def fake_run(*_args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        work_dir = kwargs["cwd"]
        assert isinstance(work_dir, Path)
        par_lines = (work_dir / "calibration.par").read_text().splitlines()
        catchment_locator, storm_locator = Path(par_lines[1][10:]), Path(par_lines[2][10:])
        assert not catchment_locator.is_absolute()
        assert not storm_locator.is_absolute()
        staged_catchment, staged_storm = work_dir / catchment_locator, work_dir / storm_locator
        assert staged_catchment.read_bytes() == catg_bytes
        assert staged_storm.read_bytes() == stm_bytes
        # Match the observed native rule: output follows the catchment, not cwd.
        staged_catchment.with_suffix(".out").write_text("Inc Time Hyd001 Hyd002\n1 0 1 2\n2 1 4 3\n")
        return subprocess.CompletedProcess(args=["RORB_CMD"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(calibration.subprocess, "run", fake_run)
    out = tmp_path / "results"
    assert (
        calibration.main(
            [
                "--catg",
                str(catg),
                "--stm",
                str(stm),
                "--rorb",
                sys.executable,
                "--output",
                str(out),
                "--kc",
                "15",
                "--il",
                "10",
                "--continuing",
                "5",
                "--sim-column",
                "Hyd001",
                "--obs-column",
                "Hyd002",
                "--execute",
            ]
        )
        == 0
    )
    results = next(out.glob("*/results.csv"))
    with results.open(newline="") as stream:
        records = list(csv.DictReader(stream))
    assert len(records) == 1
    assert records[0]["status"] == "success"
    assert records[0]["rmse"] == "1.0"
    assert Path(records[0]["output_report"]).parent == Path(records[0]["directory"])
    assert catg.read_bytes() == catg_bytes
    assert stm.read_bytes() == stm_bytes
    assert not catg.with_suffix(".out").exists()


def test_score_archived_output_without_rorb(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Archived reports can be checked even when Windows denies RORB_CMD launch."""
    archived = tmp_path / "historic.out"
    archived.write_text("Inc Time Hyd001 Hyd002\n1 0 1 2\n2 1 4 3\n")

    def prohibited_run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        message = "Offline scoring must not launch RORB_CMD"
        raise AssertionError(message)

    monkeypatch.setattr(calibration.subprocess, "run", prohibited_run)
    root = tmp_path / "analysis"
    assert (
        calibration.main(
            [
                "--score-output",
                str(archived),
                "--sim-column",
                "Hyd001",
                "--obs-column",
                "Hyd002",
                "--rorb",
                "missing_RORB_CMD.exe",
                "--output",
                str(root),
                "--no-pause",
            ]
        )
        == 0
    )
    scores = next(root.glob("archived_*/metrics.csv"))
    with scores.open(newline="") as stream:
        record = next(csv.DictReader(stream))
    assert float(record["rmse"]) == 1
    assert float(record["nse"]) == -3
    assert record["source_file"] == str(archived)


def test_archived_score_with_measured_flow_csv(tmp_path: Path) -> None:
    archived = tmp_path / "historic.out"
    archived.write_text("Inc Time Hyd001 Hyd002\n1 0 1 80\n2 1 4 90\n")
    observed = tmp_path / "gauge.csv"
    observed.write_text("Time,Flow\n0,2\n1,3\n")
    assert (
        calibration.main(
            [
                "--score-output",
                str(archived),
                "--sim-column",
                "Hyd001",
                "--observed-csv",
                str(observed),
                "--output",
                str(tmp_path / "analysis"),
            ]
        )
        == 0
    )


def test_archived_score_requires_explicit_reference(tmp_path: Path) -> None:
    archived = tmp_path / "historic.out"
    archived.write_text("Inc Time Hyd001 Hyd002\n1 0 1 2\n2 1 3 4\n")
    output = tmp_path / "analysis"
    assert calibration.main(["--score-output", str(archived), "--sim-column", "Hyd001", "--output", str(output)]) == 1
    assert not output.exists()


def test_archived_csv_time_mismatch_rejected(tmp_path: Path) -> None:
    archived = tmp_path / "historic.out"
    archived.write_text("Inc Time Hyd001 Hyd002\n1 0 1 2\n2 1 4 3\n")
    observed = tmp_path / "gauge.csv"
    observed.write_text("Time,Flow\n0,2\n1.5,3\n")
    assert (
        calibration.main(
            [
                "--score-output",
                str(archived),
                "--sim-column",
                "Hyd001",
                "--observed-csv",
                str(observed),
                "--output",
                str(tmp_path / "analysis"),
            ]
        )
        == 1
    )
    assert not (tmp_path / "analysis").exists()


def test_archived_score_dry_run_has_no_filesystem_side_effects(tmp_path: Path) -> None:
    archived = tmp_path / "historic.out"
    archived.write_text("Inc Time Hyd001 Hyd002\n1 0 1 2\n2 1 4 3\n")
    target = tmp_path / "analysis"
    assert (
        calibration.main(
            [
                "--score-output",
                str(archived),
                "--sim-column",
                "Hyd001",
                "--obs-column",
                "Hyd002",
                "--dry-run",
                "--plots",
                "--output",
                str(target),
            ]
        )
        == 0
    )
    assert not target.exists()


def test_malformed_archived_output_does_not_create_results(tmp_path: Path) -> None:
    archived = tmp_path / "bad.out"
    archived.write_text("Inc Time Hyd001 Hyd002\n1 0 10 10\n2 1 ******** 100\n3 2 12 20\nEnd\n")
    target = tmp_path / "analysis"
    assert (
        calibration.main(
            [
                "--score-output",
                str(archived),
                "--sim-column",
                "Hyd001",
                "--obs-column",
                "Hyd002",
                "--output",
                str(target),
            ]
        )
        == 1
    )
    assert not target.exists()


def test_malformed_live_output_marks_run_failed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    catg, stm = tmp_path / "a.catg", tmp_path / "b.stm"
    catg.write_text("a")
    stm.write_text("b")

    def fake_run(*_args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        work_dir = kwargs["cwd"]
        assert isinstance(work_dir, Path)
        (work_dir / "fake.out").write_text("Inc Time Hyd001 Hyd002\n1 0 10 10\n2 1 ******** 100\n3 2 12 20\n")
        return subprocess.CompletedProcess(args=["RORB_CMD"], returncode=0, stdout="", stderr="")

    monkeypatch.setattr(calibration.subprocess, "run", fake_run)
    root = tmp_path / "calibration"
    assert (
        calibration.main(
            [
                "--catg",
                str(catg),
                "--stm",
                str(stm),
                "--rorb",
                sys.executable,
                "--output",
                str(root),
                "--kc",
                "15",
                "--il",
                "10",
                "--continuing",
                "5",
                "--sim-column",
                "Hyd001",
                "--obs-column",
                "Hyd002",
                "--execute",
            ]
        )
        == 1
    )
    with next(root.glob("*/results.csv")).open(newline="") as stream:
        record = next(csv.DictReader(stream))
    assert record["status"] == "failed"
    assert "Malformed hydrograph row" in record["error"]
    assert not record["rmse"]
