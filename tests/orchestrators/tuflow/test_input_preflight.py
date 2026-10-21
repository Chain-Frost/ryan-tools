"""Focused no-TUFLOW-executable tests for the scenario-aware input preflight."""

from __future__ import annotations

import inspect
import os
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


def test_bc_event_source_assignment_is_not_treated_as_a_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_deferred_collection_never_accesses_eager_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Repeated deferred contexts describe files without materialising any input."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    source = tmp_path / "rainfall.csv"
    source.touch()
    calls: list[str] = []

    class DeferredInput:
        lhs = "Read File"

        @property
        def files(self) -> None:
            message = "Deferred input.files must never be accessed"
            raise AssertionError(message)

        @property
        def file_references(self) -> tuple[types.SimpleNamespace, ...]:
            return (
                types.SimpleNamespace(
                    path_pattern=source,
                    base_directory=tmp_path,
                    origin="Read File",
                    layer_pattern=None,
                    unresolved_variables=(),
                    requires_attribute_scan=False,
                    requires_database_scan=False,
                ),
            )

        def __str__(self) -> str:
            return "Read File == rainfall.csv"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[DeferredInput]:
            return [] if lhs else [DeferredInput()]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            calls.append(flags)
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    runs = [RunRequest(i, {"e1": f"TP{i:03d}"}) for i in range(1, 441)]
    report = check_simulation_inputs(tcf, runs)
    assert report.passed
    assert report.run_count == 440
    assert len(calls) == 440
    assert report.checked_paths == {str(source)}


def test_deferred_wildcards_are_expanded_once_and_missing_patterns_fail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    for name in ("rain_01.csv", "rain_02.csv"):
        (tmp_path / name).touch()
    pattern = tmp_path / "rain_*.csv"

    class DeferredInput:
        lhs = "Read File"

        @property
        def files(self) -> None:
            message = "No eager resolution"
            raise AssertionError(message)

        def __init__(self, pattern_: Path) -> None:
            self.file_references = (
                types.SimpleNamespace(
                    path_pattern=pattern_,
                    base_directory=tmp_path,
                    origin="Read File",
                    layer_pattern=None,
                    unresolved_variables=(),
                    requires_attribute_scan=False,
                    requires_database_scan=False,
                ),
            )

        def __str__(self) -> str:
            return "Read File == rain_*.csv"

    class Context:
        def __init__(self, pattern_: Path) -> None:
            self.pattern = pattern_

        def find_input(self, lhs: str | None = None) -> list[DeferredInput]:
            return [] if lhs else [DeferredInput(self.pattern)]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context(pattern if "EXG" in flags else tmp_path / "absent_*.csv")

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    report = check_simulation_inputs(tcf, [RunRequest(1, {"s1": "EXG"}), RunRequest(2, {"s1": "DEV"})])
    assert report.run_count == 2
    assert report.affected_runs == 1
    assert report.issues[0].run_index == 2
    assert report.issues[0].status == "missing"
    assert report.directories_scanned == 1
    assert report.checked_paths == {str(tmp_path / "rain_01.csv"), str(tmp_path / "rain_02.csv")}


def test_deferred_geopackage_layer_pattern_and_attribute_guard(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    gpkg = tmp_path / "model.gpkg"
    with sqlite3.connect(gpkg) as db:
        db.execute("CREATE TABLE gpkg_contents(table_name TEXT)")
        db.execute("INSERT INTO gpkg_contents VALUES ('2d_code_001')")

    class Input:
        lhs = "Read GIS Code"

        def __init__(self, *, scan_attributes: bool = False) -> None:
            self.file_references = (
                types.SimpleNamespace(
                    path_pattern=gpkg,
                    base_directory=tmp_path,
                    origin="Read GIS Code",
                    layer_pattern="2d_code_*",
                    unresolved_variables=(),
                    requires_attribute_scan=scan_attributes,
                    requires_database_scan=False,
                ),
            )

        @property
        def files(self) -> None:
            message = "No eager GIS attribute inspection"
            raise AssertionError(message)

        def __str__(self) -> str:
            return "Read GIS Code == model.gpkg >> 2d_code_*"

    class Context:
        def __init__(self, scan: bool) -> None:
            self.scan = scan

        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [] if lhs else [Input(scan_attributes=self.scan)]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context("DEV" in flags)

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    report = check_simulation_inputs(tcf, [RunRequest(1, {"s1": "EXG"})])
    assert report.passed
    guarded = check_simulation_inputs(tcf, [RunRequest(2, {"s1": "DEV"})])
    assert not guarded.passed
    assert guarded.issues[0].status == "unresolved"
    assert "attribute" in guarded.issues[0].detail.lower()


def test_deferred_unresolved_variables_are_never_probed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.touch()

    class Input:
        lhs = "Read File"
        file_references = (
            types.SimpleNamespace(
                path_pattern=tmp_path / "rain_<<MISSING>>.csv",
                base_directory=tmp_path,
                origin="Read File",
                layer_pattern=None,
                unresolved_variables=("<<MISSING>>",),
                requires_attribute_scan=False,
                requires_database_scan=False,
            ),
        )

        @property
        def files(self) -> None:
            message = "Never access input.files"
            raise AssertionError(message)

        def __str__(self) -> str:
            return "Read File == rain_<<MISSING>>.csv"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [] if lhs else [Input()]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, flags: str, *, file_resolution: str | None = None) -> Context:
            assert flags
            assert file_resolution == "deferred"
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "01.0p"})])
    assert not report.passed
    assert report.issues[0].status == "unresolved"
    assert report.checked_paths == set()


@pytest.mark.skipif(
    os.environ.get("RYAN_TEST_DEFERRED_PYTUFLOW") != "1",
    reason="The real fork integration test requires the pinned deferred PyTUFLOW checkout",
)
def test_real_pytuflow_deferred_context_smoke(tmp_path: Path) -> None:
    """Exercise the fork's actual context/reference API, not only protocol fakes."""
    from pytuflow import TCF

    assert "file_resolution" in inspect.signature(TCF.context).parameters
    tcf = tmp_path / "model.tcf"
    tcf.write_text("Read GIS Code == missing_code.shp\n", encoding="utf-8")

    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "01.0p"})])
    assert not report.passed
    assert any(issue.status == "missing" and "missing_code.shp" in issue.reference for issue in report.issues)


def test_deferred_bc_sources_and_materials_are_resolved_without_eager_accessor(  # noqa: C901 - contained protocol fakes
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One event-specific source is missing; numeric materials introduce no secondary file."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    bc = tmp_path / "bc_dbase.csv"
    mat = tmp_path / "materials.csv"
    for path in (bc, mat, tmp_path / "hydro_01.csv"):
        path.touch()

    class Input:
        def __init__(self, name: str, path: Path) -> None:
            self.lhs = name
            self.file_references = (
                types.SimpleNamespace(
                    path_pattern=path,
                    base_directory=tmp_path,
                    origin=name,
                    layer_pattern=None,
                    unresolved_variables=(),
                    requires_attribute_scan=False,
                    requires_database_scan=True,
                ),
            )

        @property
        def files(self) -> None:
            message = "Deferred input.files must not be materialised"
            raise AssertionError(message)

        def __str__(self) -> str:
            return self.lhs

    class Context:
        def __init__(self, event: str) -> None:
            self.event = event

        def find_input(self, lhs: str | None = None) -> list[Input]:
            inputs = [Input("BC Database", bc), Input("Read Materials File", mat)]
            return [item for item in inputs if item.lhs.casefold() == lhs.casefold()] if lhs else inputs

        def bc_dbase(self) -> None:
            message = "No eager BC database accessor in deferred mode"
            raise AssertionError(message)

        def deferred_database_sources(self) -> tuple[types.SimpleNamespace, ...]:
            return (
                types.SimpleNamespace(
                    database_path=bc,
                    database_kind="bc",
                    entry_name="catchment",
                    path_pattern=tmp_path / f"hydro_{self.event}.csv",
                ),
            )

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context("01" if "01.0p" in flags else "20")

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    runs = [RunRequest(1, {"e1": "01.0p"}), RunRequest(2, {"e1": "20.0p"})]
    report = check_simulation_inputs(tcf, runs)
    assert report.affected_runs == 1
    assert len(report.issues) == 1
    assert report.issues[0].run_index == 2
    assert report.issues[0].status == "missing"
    assert len(report.checked_paths) == 4


def test_deferred_legacy_fork_without_database_api_fails_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A partially updated PyTUFLOW must never certify a BC database."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    bc = tmp_path / "bc_dbase.csv"
    bc.touch()

    class Input:
        lhs = "BC Database"
        file_references = (
            types.SimpleNamespace(
                path_pattern=bc,
                base_directory=tmp_path,
                origin="BC Database",
                layer_pattern=None,
                unresolved_variables=(),
                requires_attribute_scan=False,
                requires_database_scan=True,
            ),
        )

        def __str__(self) -> str:
            return self.lhs

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [Input()] if lhs is None or lhs.casefold() == "bc database" else []

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, _flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "01.0p"})])
    assert not report.passed
    assert any(issue.status == "unresolved" for issue in report.issues)


@pytest.mark.skipif(
    os.environ.get("RYAN_TEST_DEFERRED_PYTUFLOW") != "1",
    reason="The real fork database test requires a pinned deferred PyTUFLOW checkout",
)
def test_real_pytuflow_event_sources_and_materials(tmp_path: Path) -> None:
    """Verify actual BC event substitution and numeric-only materials end to end."""
    from pytuflow import TCF

    assert hasattr(TCF, "context")
    tcf = tmp_path / "model.tcf"
    (tmp_path / "events.tef").write_text(
        "Define Event == Q100\n"
        "    BC Event Source == _event_ | 100yr\n"
        "End Define\n"
        "Define Event == QPMF\n"
        "    BC Event Source == _event_ | PMFyr\n"
        "End Define\n",
        encoding="utf-8",
    )
    (tmp_path / "bc_dbase.csv").write_text(
        "Name,Source,Column 1,Column 2,Add Col 1,Mult Col 2,Add Col 2,Column 3,Column 4\n"
        "FC01,_event__001.csv,time,value,,,,,\n",
        encoding="utf-8",
    )
    (tmp_path / "materials.csv").write_text(
        "Material ID,Manning's n,Rainfall Losses,Land Use Hazard ID,SRF,Fraction Impervious\n1,0.03,0,0,0,0\n",
        encoding="utf-8",
    )
    tcf.write_text(
        "Event File == events.tef\nBC Database == bc_dbase.csv\nRead Materials File == materials.csv | 0.8\n",
        encoding="utf-8",
    )
    (tmp_path / "100yr_001.csv").touch()
    (tmp_path / "PMFyr_001.csv").touch()
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "Q100"}), RunRequest(2, {"e1": "QPMF"})])
    assert report.passed, report.issues
    assert len(report.checked_paths) == 5


def test_literal_brackets_in_parent_folder_do_not_trigger_wildcard_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Windows-legal bracket characters in an ancestor are literal model paths."""
    folder = tmp_path / "model[stage]01"
    folder.mkdir()
    tcf = folder / "model.tcf"
    tcf.touch()
    data = folder / "rainfall.csv"
    data.touch()

    class Input:
        lhs = "Read File"
        file_references = (
            types.SimpleNamespace(
                path_pattern=data,
                base_directory=folder,
                origin="Read File",
                layer_pattern=None,
                unresolved_variables=(),
                requires_attribute_scan=False,
                requires_database_scan=False,
            ),
        )

        def __str__(self) -> str:
            return "Read File == rainfall.csv"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [] if lhs else [Input()]

        def deferred_database_sources(self) -> tuple[types.SimpleNamespace, ...]:
            return ()

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, _flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "Q100"})])
    assert report.passed
    assert report.directories_scanned == 0
    assert report.checked_paths == {str(data)}


def test_output_variable_assignments_are_not_input_dependencies(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Assignments with path-like RHS remain out of the required-file graph."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    source = tmp_path / "real_input.csv"
    source.touch()

    class Input:
        def __init__(self, lhs: str, path: Path) -> None:
            self.lhs = lhs
            self.files = (path,)
            self.has_missing_files = not path.exists()

        def __str__(self) -> str:
            return self.lhs

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            if lhs:
                return []
            return [
                Input("Set Variable OutputBase", tmp_path / "uncreated_results"),
                Input("Set Variable OutputRoot", tmp_path / "other_uncreated_results"),
                Input("Set Variable XMDFBase", tmp_path / "uncreated.xmdf"),
                Input("Set Variable InputRaster", tmp_path / "not_directly_read.tif"),
                Input("Read File", source),
            ]

        def bc_dbase(self) -> None:
            return None

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, _flags: str) -> Context:
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "Q100"})])
    assert report.passed, report.issues
    assert report.checked_paths == {str(source)}


@pytest.mark.skipif(
    os.environ.get("RYAN_TEST_DEFERRED_PYTUFLOW") != "1",
    reason="The nested output-variable regression requires the updated fork",
)
def test_real_deferred_output_root_variables_do_not_block_preflight(tmp_path: Path) -> None:
    """Resolve real nested output variables without treating destinations as source files."""
    tcf = tmp_path / "model.tcf"
    (tmp_path / "2d_code.shp").touch()
    tcf.write_text(
        "Set Variable OutputBase == results\n"
        "Set Variable OutputRoot == ..\\<<OutputBase>>\n"
        "Set Variable XMDFBase == exports/report.xmdf\n"
        "Output Folder == <<OutputRoot>>\n"
        "Read GIS Code == 2d_code.shp\n",
        encoding="utf-8",
    )
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "Q100"})])
    assert report.passed, report.issues
    assert report.checked_paths == {str(tmp_path / "2d_code.shp")}


@pytest.mark.skipif(
    os.environ.get("RYAN_TEST_DEFERRED_PYTUFLOW") != "1",
    reason="Nested control-file coverage needs the pinned deferred fork",
)
def test_real_deferred_nested_control_dependencies_are_not_silently_omitted(tmp_path: Path) -> None:
    """Complete nested TGC/TBC/ECF file coverage is a required preflight contract."""
    tcf = tmp_path / "model.tcf"
    children = ("geometry.tgc", "boundary.tbc", "pipes.ecf")
    sources = ("2d_code.shp", "terrain.tif", "2d_bc.shp", "1d_code.shp")
    for source in sources:
        if source != "terrain.tif":
            (tmp_path / source).touch()
    # The raster-metadata stage must inspect a *valid* input, not a
    # zero-byte placeholder with a .tif extension.
    import numpy as np
    import rasterio
    from rasterio.transform import from_origin

    with rasterio.open(
        tmp_path / "terrain.tif",
        "w",
        driver="GTiff",
        width=1,
        height=1,
        count=1,
        dtype="float32",
        transform=from_origin(0, 1, 1, 1),
    ) as dataset:
        dataset.write(np.ones((1, 1), dtype=np.float32), 1)
    (tmp_path / children[0]).write_text(
        "Read GIS Code == 2d_code.shp\nRead GRID Zpts == terrain.tif\n",
        encoding="utf-8",
    )
    (tmp_path / children[1]).write_text("Read GIS Code == 2d_bc.shp\n", encoding="utf-8")
    (tmp_path / children[2]).write_text("Read GIS Code == 1d_code.shp\n", encoding="utf-8")
    tcf.write_text(
        "Geometry Control File == geometry.tgc\nBC Control File == boundary.tbc\nESTRY Control File == pipes.ecf\n",
        encoding="utf-8",
    )
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "Q100"})])
    assert report.passed, report.issues
    expected_paths = {str(tmp_path / name) for name in (*children, *sources)}
    assert report.checked_paths == expected_paths


def test_deferred_network_attribute_files_are_scanned_once_for_440_runs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only one GIS attribute database open is needed for all 440 contexts."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    gpkg = tmp_path / "1d_nwk_example.gpkg"
    with sqlite3.connect(gpkg) as db:
        db.execute("CREATE TABLE gpkg_geometry_columns(table_name TEXT, column_name TEXT, geometry_type_name TEXT)")
        db.execute("CREATE TABLE gpkg_contents(table_name TEXT)")
        db.execute("INSERT INTO gpkg_geometry_columns VALUES ('1d_nwk_example','geom','LINESTRING')")
        db.execute("INSERT INTO gpkg_contents VALUES ('1d_nwk_example')")
        attrs = ", ".join(f"attr_{index} TEXT" for index in range(11))
        db.execute(f'CREATE TABLE "1d_nwk_example" (fid INTEGER PRIMARY KEY, geom BLOB, {attrs})')
        db.execute(
            'INSERT INTO "1d_nwk_example" (attr_1, attr_10) VALUES (?, ?)',
            ("M", "operation.csv | section.csv"),
        )
    for file in ("operation.csv", "section.csv"):
        (tmp_path / file).touch()

    class Input:
        lhs = "Read GIS Network"
        file_references = (
            types.SimpleNamespace(
                path_pattern=gpkg,
                base_directory=tmp_path,
                origin="Read GIS Network",
                layer_pattern="1d_nwk_example",
                unresolved_variables=(),
                requires_attribute_scan=True,
                requires_database_scan=False,
            ),
        )

        @property
        def files(self) -> None:
            message = "Never call eager input.files during deferred inspection"
            raise AssertionError(message)

        def __str__(self) -> str:
            return "Read GIS Network == 1d_nwk_example.gpkg"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [] if lhs else [Input()]

        def deferred_database_sources(self) -> tuple[types.SimpleNamespace, ...]:
            return ()

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, _flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    runs = [RunRequest(index, {"e1": f"TP{index:03d}"}) for index in range(1, 441)]
    report = check_simulation_inputs(tcf, runs)
    assert report.passed, report.issues
    assert report.run_count == 440
    assert report.attribute_databases_scanned == 1
    assert report.affected_runs == 0
    assert report.checked_paths == {
        str(gpkg),
        str(tmp_path / "operation.csv"),
        str(tmp_path / "section.csv"),
    }

    (tmp_path / "section.csv").unlink()
    missing = check_simulation_inputs(tcf, runs[:3])
    assert not missing.passed
    assert missing.attribute_databases_scanned == 1
    assert missing.affected_runs == 3
    assert len(missing.issues) == 3
    assert all("section.csv" in issue.reference for issue in missing.issues)


def test_deferred_q_curve_ids_resolve_through_depth_discharge_without_false_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Repeated Q channel identifiers yield one cached CSV lookup, not fake paths."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    gpkg = tmp_path / "1d_nwk_q.gpkg"
    depth_database = tmp_path / "depth_discharge.csv"
    (tmp_path / "curve_source.csv").touch()
    depth_database.write_text(
        "Name,Source,Column 1,Column 2\nKerb_A,curve_source.csv,Depth,Flow\n",
        encoding="utf-8",
    )
    with sqlite3.connect(gpkg) as db:
        db.execute("CREATE TABLE gpkg_geometry_columns(table_name TEXT, column_name TEXT, geometry_type_name TEXT)")
        db.execute("CREATE TABLE gpkg_contents(table_name TEXT)")
        db.execute("INSERT INTO gpkg_geometry_columns VALUES ('1d_nwk_q', 'geom', 'LINESTRING')")
        db.execute("INSERT INTO gpkg_contents VALUES ('1d_nwk_q')")
        fields = ", ".join(f"attr_{index} TEXT" for index in range(11))
        db.execute(f'CREATE TABLE "1d_nwk_q" (fid INTEGER PRIMARY KEY, geom BLOB, {fields})')
        db.execute('INSERT INTO "1d_nwk_q" (attr_1, attr_10) VALUES (?, ?)', ("Q", "Kerb_A"))

    class Input:
        def __init__(self, lhs: str, source: Path, layer: str | None, kind: str | None) -> None:
            self.lhs = lhs
            self.file_references = (
                types.SimpleNamespace(
                    path_pattern=source,
                    base_directory=tmp_path,
                    origin=lhs,
                    layer_pattern=layer,
                    unresolved_variables=(),
                    requires_attribute_scan=layer is not None,
                    requires_database_scan=kind is not None,
                    database_kind=kind,
                ),
            )

        @property
        def files(self) -> None:
            message = "Never inspect eager .files in deferred mode"
            raise AssertionError(message)

        def __str__(self) -> str:
            return self.lhs

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            inputs = [
                Input("Read GIS Network", gpkg, "1d_nwk_q", None),
                Input("Depth Discharge Database", depth_database, None, "depth_discharge"),
            ]
            return [item for item in inputs if item.lhs.casefold() == lhs.casefold()] if lhs else inputs

        def deferred_database_sources(self) -> tuple[types.SimpleNamespace, ...]:
            return ()

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, _flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    runs = [RunRequest(index, {"e1": str(index)}) for index in range(1, 4)]
    report = check_simulation_inputs(tcf, runs)
    assert report.passed, report.issues
    assert report.attribute_databases_scanned == 1
    assert report.checked_paths == {str(gpkg), str(depth_database), str(tmp_path / "curve_source.csv")}
    assert all("Kerb_A" not in item for item in report.checked_paths)

    depth_database.write_text("Name,Source\nOther,other.csv\n", encoding="utf-8")
    missing = check_simulation_inputs(tcf, runs)
    assert not missing.passed
    assert missing.affected_runs == 3
    assert all("has no entry" in issue.detail for issue in missing.issues)
    assert not any("Kerb_A" in item for item in missing.checked_paths)


@pytest.mark.skipif(
    os.environ.get("RYAN_TEST_DEFERRED_PYTUFLOW") != "1",
    reason="Real deferred material/soils parity requires pinned PyTUFLOW fork",
)
def test_real_deferred_materials_and_soils_are_primary_inputs(tmp_path: Path) -> None:
    """Recognised inline tables keep their primary files and no invented paths."""
    tcf = tmp_path / "model.tcf"
    mat = tmp_path / "materials.csv"
    soils = tmp_path / "soils.tsoilf"
    tcf.write_text(
        "Read Materials File == materials.csv | 0.8\nRead Soil File == soils.tsoilf\n",
        encoding="utf-8",
    )
    mat.write_text(
        'Material ID,Manning\'s n,Rainfall Losses\n1,"0.05, 0.1, 0.1, 0.05",0\n',
        encoding="utf-8",
    )
    soils.write_text("1,ILCL,12,1.2\n", encoding="utf-8")
    report = check_simulation_inputs(
        tcf,
        [RunRequest(1, {"e1": "Q100"}), RunRequest(2, {"e1": "Q20"})],
    )
    assert report.passed, report.issues
    assert report.checked_paths == {str(mat), str(soils)}

    soils.write_text("1,CUSTOM,parameters\n", encoding="utf-8")
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "Q100"})])
    assert not report.passed
    assert any(issue.status == "unresolved" for issue in report.issues)


def test_deferred_shapefile_network_attribute_sources_checked_once_per_run_group(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Shapefile network dependencies use the same batched checks as GeoPackages."""
    import shapefile

    tcf = tmp_path / "model.tcf"
    tcf.touch()
    source = tmp_path / "1d_nwk_test.shp"
    with shapefile.Writer(str(source), shapeType=shapefile.POLYLINE) as writer:
        for field_index in range(11):
            writer.field(f"attr_{field_index}", "C", size=90)
        values = [""] * 11
        values[1], values[10] = "QO", "Gate_A | hydrograph.csv"
        writer.null()
        writer.record(*values)
    linked = tmp_path / "hydrograph.csv"
    linked.touch()

    class Input:
        lhs = "Read GIS Network"
        file_references = (
            types.SimpleNamespace(
                path_pattern=source,
                base_directory=tmp_path,
                origin="Read GIS Network",
                layer_pattern=None,
                unresolved_variables=(),
                requires_attribute_scan=True,
                requires_database_scan=False,
            ),
        )

        @property
        def files(self) -> None:
            msg = "Deferred input must not use eager .files"
            raise AssertionError(msg)

        def __str__(self) -> str:
            return "Read GIS Network == 1d_nwk_test.shp"

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [] if lhs else [Input()]

        def deferred_database_sources(self) -> tuple[types.SimpleNamespace, ...]:
            return ()

    class FakeTCF:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, _flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=FakeTCF))
    runs = [RunRequest(index, {"e1": str(index)}) for index in range(1, 25)]
    report = check_simulation_inputs(tcf, runs)
    assert report.passed, report.issues
    assert report.attribute_databases_scanned == 1
    assert report.checked_paths == {str(source), str(linked)}

    linked.unlink()
    failed = check_simulation_inputs(tcf, runs[:3])
    assert not failed.passed
    assert failed.attribute_databases_scanned == 1
    assert failed.affected_runs == 3
    assert len(failed.issues) == 3
    assert all("hydrograph.csv" in issue.reference for issue in failed.issues)


def test_typed_rainfall_ad_bc_and_gis_table_links_bulk_inventory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A supported upstream typed reference creates a real checked dependency."""
    import numpy as np
    import rasterio
    import shapefile
    from rasterio.transform import from_origin

    tcf = tmp_path / "model.tcf"
    tcf.touch()
    rainfall_index = tmp_path / "rainfall.csv"
    rainfall_index.write_text("Time,Rainfall_File\n0,rainfall.tif\n", encoding="utf-8")
    rainfall = tmp_path / "rainfall.tif"
    with rasterio.open(
        rainfall,
        "w",
        driver="GTiff",
        height=1,
        width=1,
        count=1,
        dtype="float32",
        transform=from_origin(1, 2, 1, 1),
    ) as dataset:
        dataset.write(np.ones((1, 1), dtype=np.float32), 1)
    ad_database = tmp_path / "ad_bc.csv"
    ad_database.write_text("Name,Source\nB,ad_hydro.csv\n", encoding="utf-8")
    hydrograph = tmp_path / "ad_hydro.csv"
    hydrograph.touch()
    xs = tmp_path / "1d_xs_example.shp"
    with shapefile.Writer(str(xs), shapeType=shapefile.POLYLINE) as writer:
        writer.field("source", "C", size=100)
        writer.null()
        writer.record("section.csv")
    section = tmp_path / "section.csv"
    section.touch()

    class Input:
        def __init__(self, lhs: str, path: Path, *, kind: str, gis: bool = False) -> None:
            self.lhs = lhs
            self.file_references = (
                types.SimpleNamespace(
                    path_pattern=path,
                    base_directory=tmp_path,
                    origin=lhs,
                    layer_pattern=None,
                    unresolved_variables=(),
                    requires_database_scan=kind in {"rainfall", "ad_bc"},
                    requires_attribute_scan=gis,
                    database_kind=kind,
                    requires_table_link_scan=gis,
                    requires_raster_metadata_scan=False,
                ),
            )

        @property
        def files(self) -> None:
            msg = "Deferred mode must never inspect eager files"
            raise AssertionError(msg)

        def __str__(self) -> str:
            return self.lhs

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            inputs = [
                Input("Read Grid RF", rainfall_index, kind="rainfall"),
                Input("AD BC Database", ad_database, kind="ad_bc"),
                Input("Read GIS Table Links", xs, kind="gis_table_links", gis=True),
            ]
            return [item for item in inputs if item.lhs.casefold() == lhs.casefold()] if lhs else inputs

        def deferred_database_sources(self) -> tuple[types.SimpleNamespace, ...]:
            return (
                types.SimpleNamespace(
                    database_path=rainfall_index,
                    database_kind="rainfall",
                    entry_name="0",
                    path_pattern=rainfall,
                    owning_command="Read Grid RF",
                    owner_control_file=tcf,
                    layer_pattern=None,
                    requires_raster_metadata_scan=True,
                ),
                types.SimpleNamespace(
                    database_path=ad_database,
                    database_kind="ad_bc",
                    entry_name="B",
                    path_pattern=hydrograph,
                    owning_command="AD BC Database",
                    owner_control_file=tcf,
                    layer_pattern=None,
                    requires_raster_metadata_scan=False,
                ),
            )

    class Model:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, _flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=Model))
    runs = [RunRequest(1, {"e1": "A"}), RunRequest(2, {"e1": "B"})]
    report = check_simulation_inputs(tcf, runs)
    assert report.passed, report.issues
    assert report.raster_files_scanned == 1
    assert report.attribute_databases_scanned == 1
    assert report.checked_paths == {
        str(rainfall_index),
        str(rainfall),
        str(ad_database),
        str(hydrograph),
        str(xs),
        str(section),
    }

    hydrograph.unlink()
    failed = check_simulation_inputs(tcf, runs)
    assert not failed.passed
    assert failed.affected_runs == 2
    assert sum("ad_hydro.csv" in issue.reference for issue in failed.issues) == 2


def test_unknown_deferred_database_kind_remains_unresolved(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """An unimplemented valid database role cannot silently pass."""
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    unknown = tmp_path / "other.csv"
    unknown.touch()

    class Input:
        lhs = "Unknown Database"
        file_references = (
            types.SimpleNamespace(
                path_pattern=unknown,
                base_directory=tmp_path,
                origin="Unknown Database",
                layer_pattern=None,
                unresolved_variables=(),
                requires_database_scan=True,
                requires_attribute_scan=False,
                database_kind="unknown",
            ),
        )

        def __str__(self) -> str:
            return self.lhs

    class Context:
        def find_input(self, lhs: str | None = None) -> list[Input]:
            return [] if lhs else [Input()]

        def deferred_database_sources(self) -> tuple[object, ...]:
            return ()

    class Model:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, _flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            return Context()

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=Model))
    report = check_simulation_inputs(tcf, [RunRequest(1, {"e1": "A"})])
    assert not report.passed
    assert any("Unsupported deferred database materialisation" in issue.detail for issue in report.issues)
