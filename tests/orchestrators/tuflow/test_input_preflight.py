"""Focused no-TUFLOW-executable tests for the scenario-aware input preflight."""

from __future__ import annotations

import sqlite3
import sys
import types
from pathlib import Path

import pandas as pd
import pytest

from ryan_library.functions.tuflow.run_flags import extract_run_flags, read_run_commands
from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_simulation_inputs


def test_extract_flags_from_launcher_arguments() -> None:
    args = ["TUFLOW.exe", "-b", "-pu0", "-e1", "01.0p ", "-e2", "00720m", "-s1", "EXG", "model.tcf"]
    assert extract_run_flags(args) == {"e1": "01.0p", "e2": "00720m", "s1": "EXG"}
    with pytest.raises(ValueError, match="Conflicting"):
        extract_run_flags(["-e1", "01.0p", "-e1", "20.0p"])


def test_read_exported_run_commands(tmp_path: Path) -> None:
    commands = tmp_path / "run_tuflow_simulations_commands.txt"
    commands.write_text(
        'set "TCF=model_~e1~.tcf"\n'
        'START /LOW /WAIT "" "%TUFLOW_EXE%" -b -e1 01.0p -s1 EXG "%TCF%"\n'
        'START /LOW /WAIT "" "%TUFLOW_EXE%" -b -e1 20.0p -s1 DEV "%TCF%"\n'
        "Pause\n",
        encoding="utf-8",
    )
    assert read_run_commands(commands) == [
        {"e1": "01.0p", "s1": "EXG"},
        {"e1": "20.0p", "s1": "DEV"},
    ]


def test_missing_event_specific_bc_source_blocks_all_affected_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.write_text("TCF placeholder", encoding="utf-8")
    bc = tmp_path / "bc_dbase.csv"
    bc.write_text("Name,Source\nA,inflow.csv\n", encoding="utf-8")
    dem = tmp_path / "dem.tif"
    dem.touch()
    (tmp_path / "hydro_01.0p.csv").touch()

    class FakeInput:
        parent = types.SimpleNamespace(fpath=tcf)
        has_missing_files = False

        def __init__(self, path: Path) -> None:
            self.files = [path]

        def __str__(self) -> str:
            return "Read GRID Zpts == dem.tif"

    class FakeDatabase:
        fpath = bc

        def __init__(self, event: str) -> None:
            self.df = pd.DataFrame({"Source": [f"hydro_{event}.csv"]}, index=["catchment"])

    class FakeContext:
        def __init__(self, event: str) -> None:
            self.event = event

        def find_input(self, lhs: str | None = None) -> list[FakeInput]:
            return [] if lhs else [FakeInput(dem)]

        def bc_dbase(self) -> FakeDatabase:
            return FakeDatabase(self.event)

    class FakeTCF:
        def __init__(self, path: str) -> None:
            assert path == str(tcf.absolute())

        def context(self, flags: str) -> FakeContext:
            return FakeContext("01.0p" if "-e1 01.0p" in flags else "20.0p")

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    runs = [
        RunRequest(index=1, flags={"e1": "01.0p"}),
        RunRequest(index=2, flags={"e1": "20.0p"}),
        RunRequest(index=3, flags={"e1": "20.0p", "s1": "DEV"}),
    ]
    report = check_simulation_inputs(tcf, runs)
    assert report.run_count == 3
    assert report.affected_runs == 2
    assert not report.passed
    assert {issue.run_index for issue in report.issues} == {2, 3}
    assert all(issue.status == "missing" for issue in report.issues)
    assert all(issue.reference.endswith("hydro_20.0p.csv") for issue in report.issues)
    output = report.write_csv(tmp_path / "report.csv")
    assert "hydro_20.0p.csv" in output.read_text(encoding="utf-8-sig")


def test_unresolved_context_fails_closed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.touch()

    class BrokenTCF:
        def __init__(self, path: str) -> None:
            pass

        def context(self, _flags: str) -> None:
            message = "Unknown scenario"
            raise ValueError(message)

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=BrokenTCF))
    report = check_simulation_inputs(tcf, [RunRequest(index=1, flags={"s1": "BAD"})])
    assert not report.passed
    assert report.issues[0].status == "unresolved"


def test_missing_geopackage_layer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    gpkg = tmp_path / "gis.gpkg"
    with sqlite3.connect(gpkg) as db:
        db.execute("CREATE TABLE gpkg_contents(table_name TEXT)")
        db.execute("INSERT INTO gpkg_contents VALUES ('existing_layer')")

    class GpkgPath:
        dbpath = gpkg
        lyrname = "missing_layer"

    class Input:
        files = (GpkgPath(),)
        has_missing_files = False
        parent = types.SimpleNamespace(fpath=tcf)

        def __str__(self) -> str:
            return "Read GIS Code == gis.gpkg >> missing_layer"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [] if lhs else [Input()]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, path: str) -> None:
            pass

        def context(self, _flags: str) -> Context:
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    run = RunRequest(index=1, flags={"e1": "01.0p"})
    assert check_simulation_inputs(tcf, [run]).issues[0].status == "missing"
    assert check_simulation_inputs(tcf, [run], check_gpkg_layers=False).passed


def test_output_directories_are_not_required_inputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Neither an absent output directory nor a directory on disk is a missing input."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    output_path = tmp_path / "results" / "future_run"

    class OutputInput:
        lhs = "Output Folder"
        files = (output_path,)
        parent = types.SimpleNamespace(fpath=tcf)
        has_missing_files = True

        def __str__(self) -> str:
            return "Output Folder == ..\\results\\future_run"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[OutputInput]:
            return [] if lhs else [OutputInput()]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, path: str) -> None:
            pass

        def context(self, _flags: str) -> Context:
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "01.0p"})])
    assert report.passed
    assert report.checked_paths == set()


def test_bc_event_source_assignment_is_not_treated_as_a_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """BC Event Source maps event variables; its pipe-delimited value is not a path."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()

    class EventSourceInput:
        lhs = "BC Event Source"
        files = (tmp_path / "05.00p | 05.00p",)
        parent = types.SimpleNamespace(fpath=tcf)
        has_missing_files = True

        def __str__(self) -> str:
            return "BC Event Source == 05.00p | 05.00p"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[EventSourceInput]:
            return [] if lhs else [EventSourceInput()]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, path: str) -> None:
            pass

        def context(self, _flags: str) -> Context:
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "05.00p"})])

    assert report.passed
    assert report.checked_paths == set()


def test_identical_files_are_collected_before_one_network_scan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """500 model contexts sharing eight files require one parent listing."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    inputs = tuple(tmp_path / f"input_{index}.csv" for index in range(8))
    for file in inputs[:-1]:
        file.touch()

    class Input:
        lhs = "Read GIS"
        parent = types.SimpleNamespace(fpath=tcf)
        has_missing_files = False
        files = inputs

        def __str__(self) -> str:
            return "Read GIS == shared files"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [] if lhs else [Input()]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, path: str) -> None:
            pass

        def context(self, _flags: str) -> Context:
            return Context()

    from ryan_library.functions.tuflow import dependency_index

    real_scan = dependency_index.os.scandir
    scanned: list[Path] = []

    def recording_scan(path: Path) -> object:
        scanned.append(Path(path))
        return real_scan(path)

    monkeypatch.setattr(dependency_index.os, "scandir", recording_scan)
    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))

    runs = [RunRequest(index=i, flags={"e1": f"TP{i:03d}"}) for i in range(1, 501)]
    report = check_simulation_inputs(tcf, runs)
    assert not report.passed
    assert report.run_count == 500
    assert len(report.checked_paths) == 8
    assert report.directories_scanned == 1
    assert report.individually_checked == 0
    assert scanned == [tmp_path]
    assert len(report.issues) == 500
    assert report.affected_runs == 500
    assert {issue.reference for issue in report.issues} == {str(inputs[-1])}


def test_missing_input_in_dense_directory_maps_to_only_affected_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Shared files are scanned once; an event-specific missing source affects only its run."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    shared = tuple(tmp_path / f"common_{index}.tif" for index in range(8))
    for file in shared:
        file.touch()

    class Input:
        lhs = "Read GRID"
        parent = types.SimpleNamespace(fpath=tcf)
        has_missing_files = False

        def __init__(self, event: str) -> None:
            self.files = (*shared, tmp_path / f"rain_{event}.csv")

        def __str__(self) -> str:
            return "Read GRID == shared and rainfall files"

    class Context:
        def __init__(self, event: str) -> None:
            self.event = event

        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [] if lhs else [Input(self.event)]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, path: str) -> None:
            pass

        def context(self, flags: str) -> Context:
            return Context("EXG" if "EXG" in flags else "DEV")

    (tmp_path / "rain_EXG.csv").touch()
    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    report = check_simulation_inputs(
        tcf,
        [RunRequest(1, {"s1": "EXG"}), RunRequest(2, {"s1": "DEV"})],
    )
    assert report.directories_scanned == 1
    assert report.affected_runs == 1
    assert report.issues[0].run_index == 2
    assert report.issues[0].reference.endswith("rain_DEV.csv")
