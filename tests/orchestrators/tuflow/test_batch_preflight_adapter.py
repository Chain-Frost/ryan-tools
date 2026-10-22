"""Opt-in batch preflight adapter tests; no TUFLOW executable or fork installation."""

from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow.batch_preflight_adapter import BatchModelView
from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_simulation_inputs


def _ref(path: Path, kind: str = "", *, origin: str = "Read GIS Code") -> types.SimpleNamespace:
    return types.SimpleNamespace(
        path_pattern=path,
        base_directory=path.parent,
        layer_pattern=None,
        origin=origin,
        unresolved_variables=(),
        requires_attribute_scan=False,
        requires_database_scan=bool(kind),
        requires_table_link_scan=False,
        requires_raster_metadata_scan=False,
        database_kind=kind or None,
    )


def _model(tmp_path: Path) -> type:
    bc = tmp_path / "bc.csv"
    bc.touch()
    shared = tmp_path / "common.shp"
    shared.touch()
    (tmp_path / "hydro_01.csv").touch()

    class Input:
        def __init__(self, name: str, reference: object) -> None:
            self.lhs = name
            self.file_references = (reference,)

        def __str__(self) -> str:
            return self.lhs

    class Context:
        def __init__(self, event: str) -> None:
            self.event = event

        def find_input(self, lhs: str | None = None) -> list[Input]:
            candidates = [
                Input("Read GIS Code", _ref(shared)),
                Input("BC Database", _ref(bc, "bc", origin="BC Database")),
            ]
            return [item for item in candidates if item.lhs.casefold() == lhs.casefold()] if lhs else candidates

        def deferred_database_sources(self) -> tuple[object, ...]:
            return (
                types.SimpleNamespace(
                    input_uuid="db-uuid",
                    database_path=bc,
                    database_kind="bc",
                    entry_name="Catchment",
                    path_pattern=tmp_path / f"hydro_{self.event}.csv",
                    owner_control_file=tmp_path / "model.tcf",
                    layer_pattern=None,
                    requires_raster_metadata_scan=False,
                ),
            )

    class Model:
        def __init__(self, _path: str) -> None:
            self.collected = 0
            self.individual = 0

        def context(self, flags: str, *, file_resolution: str | None = None) -> Context:
            assert file_resolution == "deferred"
            self.individual += 1
            return Context("01" if "01.0p" in flags else "20")

        def collect_batch_dependencies(self, flags: tuple[str, ...]) -> types.SimpleNamespace:
            self.collected += 1
            cases = []
            for index, flag in enumerate(flags):
                context = Context("01" if "01.0p" in flag else "20")
                inputs = context.find_input()
                entries = tuple(
                    types.SimpleNamespace(
                        input_uuid=f"input-{j}" if j == 0 else "db-uuid",
                        command_index=j,
                        owner_control_file=tmp_path / "model.tcf",
                        line_number=j + 1,
                        reference=reference,
                    )
                    for j, item in enumerate(inputs)
                    for reference in item.file_references
                )
                cases.append(types.SimpleNamespace(
                    index=index,
                    flags=flag,
                    error=None,
                    file_references=tuple(row.reference for row in entries),
                    input_references=entries,
                    database_sources=context.deferred_database_sources(),
                    database_activations=(types.SimpleNamespace(
                        input_uuid="db-uuid", database_kind="bc",
                    ),),
                ))
            return types.SimpleNamespace(runs=tuple(cases))
    return Model


def test_batch_matches_individual_including_missing_event_specific_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.touch()
    model = _model(tmp_path)
    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=model))
    runs = (RunRequest(31, {"e1": "01.0p"}), RunRequest(52, {"e1": "20.0p"}))
    regular = check_simulation_inputs(tcf, runs, context_mode="individual")
    batch = check_simulation_inputs(tcf, runs, context_mode="batch")
    assert not regular.passed and not batch.passed
    assert regular.checked_paths == batch.checked_paths
    assert set(regular.issues) == set(batch.issues)
    assert batch.affected_runs == 1
    assert batch.issues[0].run_index == 52
    assert batch.issues[0].reference.endswith("hydro_20.csv")


def test_batch_rejects_unavailable_api_without_silent_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.touch()

    class LegacyModel:
        def __init__(self, _path: str) -> None:
            pass

        def context(self, _flags: str, *, file_resolution: str | None = None) -> object:
            raise AssertionError("Individual context must not run for requested batch")

    monkeypatch.setitem(sys.modules, "pytuflow", types.SimpleNamespace(TCF=LegacyModel))
    with pytest.raises(ValueError, match="requires collect_batch_dependencies"):
        check_simulation_inputs(tcf, (RunRequest(1, {"e1": "A"}),), context_mode="batch")


def test_batch_rejects_truncated_source_projection() -> None:
    flags = ("-e1 A",)
    class Model:
        def collect_batch_dependencies(self, _flags: tuple[str, ...]) -> types.SimpleNamespace:
            return types.SimpleNamespace(runs=(types.SimpleNamespace(
                index=0, flags=flags[0], error=None,
                file_references=("required-but-omitted",),
                input_references=(), database_sources=(), database_activations=(),
            ),))
    view = BatchModelView(Model(), flags)
    with pytest.raises(ValueError, match="Incomplete batch input ownership"):
        view.context(flags[0], file_resolution="deferred")


def test_batch_rejects_wrong_source_activation_and_duplicate_bc() -> None:
    flag = "-s1 EXG"
    row = types.SimpleNamespace(
        input_uuid="dbA", command_index=0,
        reference=types.SimpleNamespace(origin="BC Database"),
    )
    source = types.SimpleNamespace(input_uuid="unknown", database_kind="bc")
    cases = [
        types.SimpleNamespace(
            index=0, flags=flag, error=None,
            file_references=(row.reference,), input_references=(row,),
            database_sources=(source,),
            database_activations=(types.SimpleNamespace(input_uuid="dbA", database_kind="bc"),),
        ),
        types.SimpleNamespace(
            index=0, flags=flag, error=None,
            file_references=(row.reference,), input_references=(row,),
            database_sources=(),
            database_activations=(
                types.SimpleNamespace(input_uuid="dbA", database_kind="bc"),
                types.SimpleNamespace(input_uuid="dbB", database_kind="bc"),
            ),
        ),
    ]
    for case, expected in zip(cases, ("not owned", "Ambiguous batch bc"), strict=True):
        class Model:
            def collect_batch_dependencies(self, _flags: tuple[str, ...]) -> types.SimpleNamespace:
                return types.SimpleNamespace(runs=(case,))
        with pytest.raises(ValueError, match=expected):
            BatchModelView(Model(), (flag,)).context(flag, file_resolution="deferred")


def test_batch_rejects_context_index_or_flag_mismatch() -> None:
    class Model:
        def collect_batch_dependencies(self, flags: tuple[str, ...]) -> types.SimpleNamespace:
            return types.SimpleNamespace(runs=(types.SimpleNamespace(
                index=99, flags=flags[0], error=None,
                file_references=(), input_references=(),
                database_sources=(), database_activations=(),
            ),))
    with pytest.raises(ValueError, match="index or flags mismatch"):
        BatchModelView(Model(), ("-e1 A",)).context("-e1 A", file_resolution="deferred")


@pytest.mark.skipif(
    os.environ.get("RYAN_TEST_BATCH_PYTUFLOW") != "1",
    reason="Requires isolated PyTUFLOW #5 batch fork",
)
def test_real_fork_batch_and_individual_have_identical_preflight(tmp_path: Path) -> None:
    """Exercise real parsed TCF contexts without invoking TUFLOW or opening GIS."""
    source = tmp_path / "shared.tcf"
    source.write_text(
        "IF Scenario == DEV\\n"
        "    Read GIS Code == code_DEV.shp\\n"
        "ELSE\\n"
        "    Read GIS Code == code_EXG.shp\\n"
        "END IF\\n",
        encoding="utf-8",
    )
    (tmp_path / "code_DEV.shp").touch()
    runs = (
        RunRequest(10, {"s1": "DEV", "e1": "TP01"}),
        RunRequest(20, {"s1": "EXG", "e1": "TP01"}),
        RunRequest(30, {"s1": "DEV", "e1": "TP02"}),
    )
    individual = check_simulation_inputs(source, runs, context_mode="individual")
    batch = check_simulation_inputs(source, runs, context_mode="batch")
    assert batch.checked_paths == individual.checked_paths
    assert set(batch.issues) == set(individual.issues)
    assert batch.affected_runs == individual.affected_runs == 1
    assert {item.run_index for item in batch.issues} == {20}
