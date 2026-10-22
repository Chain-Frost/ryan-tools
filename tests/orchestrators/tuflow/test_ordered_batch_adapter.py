"""PyTUFLOW PR #6 ordered batch contract and per-activation consumer regressions."""

from __future__ import annotations

import types
from pathlib import Path

import pytest

from ryan_library.orchestrators.tuflow.batch_preflight_adapter import BatchModelView
from ryan_library.orchestrators.tuflow.input_preflight import (
    PreflightReport,
    RunRequest,
    _collect_references,  # pyright: ignore[reportPrivateUsage] - exercise the production collector
)


def _command(index: int, uuid: str, name: str, *, tcf: Path) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        command_index=index,
        input_uuid=uuid,
        command_name=name,
        command_text=f"{name} == value_{index}",
        owner_control_file=tcf,
        line_number=index + 1,
    )


def _reference(path: Path, *, origin: str, database: bool = False) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        path_pattern=path,
        base_directory=path.parent,
        layer_pattern=None,
        origin=origin,
        unresolved_variables=(),
        requires_attribute_scan=False,
        requires_database_scan=database,
        requires_table_link_scan=False,
        requires_raster_metadata_scan=False,
        database_kind="bc" if database else None,
    )


def _source(uuid: str, name: str, path: Path, *, database_path: Path, tcf: Path) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        input_uuid=uuid,
        database_kind="bc",
        entry_name=name,
        path_pattern=path,
        database_path=database_path,
        owner_control_file=tcf,
        requires_raster_metadata_scan=False,
        layer_pattern=None,
    )


def _case(
    flags: str,
    commands: tuple[types.SimpleNamespace, ...],
    reference_pairs: tuple[tuple[int, object], ...],
    *,
    activations: tuple[object, ...] = (),
    sources: tuple[object, ...] = (),
) -> types.SimpleNamespace:
    records = tuple(
        types.SimpleNamespace(
            command_index=ordinal,
            input_uuid=commands[ordinal].input_uuid,
            owner_control_file=commands[ordinal].owner_control_file,
            line_number=commands[ordinal].line_number,
            reference=reference,
        )
        for ordinal, reference in reference_pairs
    )
    return types.SimpleNamespace(
        index=0,
        flags=flags,
        error=None,
        file_references=tuple(item.reference for item in records),
        input_references=records,
        ordered_inputs=commands,
        database_sources=sources,
        database_activations=activations,
    )


def _view(case: types.SimpleNamespace) -> BatchModelView:
    class Model:
        def collect_batch_dependencies(self, _flags: tuple[str, ...]) -> types.SimpleNamespace:
            return types.SimpleNamespace(runs=(case,))

    return BatchModelView(Model(), (case.flags,))


def test_ordered_batch_retains_file_free_commands_and_correct_owner(tmp_path: Path) -> None:
    tcf = tmp_path / "model.tcf"
    commands = (
        _command(0, "db-a", "BC Database", tcf=tcf),
        _command(1, "variable", "Set Variable OutputRoot", tcf=tcf),
        _command(2, "gis", "Read GIS BC", tcf=tcf),
        _command(3, "output", "Output Folder", tcf=tcf),
    )
    bc = _reference(tmp_path / "bc.csv", origin="BC Database", database=True)
    gis = _reference(tmp_path / "boundary.shp", origin="Read GIS BC")
    activation = types.SimpleNamespace(input_uuid="db-a", sequence=0, database_kind="bc", database_path=bc.path_pattern)
    ctx = _view(_case("-e1 TP01", commands, ((0, bc), (2, gis)), activations=(activation,))).context(
        "-e1 TP01", file_resolution="deferred"
    )

    inputs = ctx.find_input()
    assert [inp.uuid for inp in inputs] == ["db-a", "variable", "gis", "output"]
    assert [inp.lhs for inp in inputs] == [item.command_name for item in commands]
    assert [len(inp.file_references) for inp in inputs] == [1, 0, 1, 0]
    assert [inp.uuid for inp in ctx.find_input(lhs="BC Database")] == ["db-a"]
    assert [inp.uuid for inp in ctx.find_input(lhs="Read GIS BC")] == ["gis"]
    assert ctx.deferred_database_activations() == (activation,)


def test_two_bc_declarations_select_sources_by_original_command_order(tmp_path: Path) -> None:
    tcf = tmp_path / "model.tcf"
    commands = (
        _command(0, "db-a", "BC Database", tcf=tcf),
        _command(1, "gis-a", "Read GIS BC", tcf=tcf),
        _command(2, "variable", "Set Variable OutputRoot", tcf=tcf),
        _command(3, "db-b", "BC Database", tcf=tcf),
        _command(4, "gis-b", "Read GIS BC", tcf=tcf),
    )
    a_path, b_path = tmp_path / "a.csv", tmp_path / "b.csv"
    refs = (
        (0, _reference(a_path, origin="BC Database", database=True)),
        (1, _reference(tmp_path / "boundary_a.shp", origin="Read GIS BC")),
        (3, _reference(b_path, origin="BC Database", database=True)),
        (4, _reference(tmp_path / "boundary_b.shp", origin="Read GIS BC")),
    )
    activations = (
        types.SimpleNamespace(input_uuid="db-a", sequence=0, database_kind="bc", database_path=a_path),
        types.SimpleNamespace(input_uuid="db-b", sequence=3, database_kind="bc", database_path=b_path),
    )
    sources = (
        _source("db-a", "A", tmp_path / "a_source.csv", database_path=a_path, tcf=tcf),
        _source("db-b", "B", tmp_path / "b_source.csv", database_path=b_path, tcf=tcf),
    )
    flag = "-e1 TP01"
    view = _view(_case(flag, commands, refs, activations=activations, sources=sources))
    ctx = view.context(flag, file_resolution="deferred")
    assert ctx.deferred_database_sources_for_activation("db-a") == (sources[0],)
    assert ctx.deferred_database_sources_for_activation("db-b") == (sources[1],)
    with pytest.raises(ValueError, match="Ambiguous"):
        ctx.deferred_database_sources()
    with pytest.raises(ValueError, match="Unknown batch database activation"):
        ctx.deferred_database_sources_for_activation("not-active")

    # Execute the real common collector, not just the adapter API. Neither
    # GIS geometry nor physical files are inspected during this stage.
    view = _view(_case(flag, commands, refs, activations=activations, sources=sources))
    run = RunRequest(42, {"e1": "TP01"})
    assert run.arguments == flag
    report = PreflightReport(run_count=1)
    collected, _ = _collect_references(
        tcf, (run,), view, report  # pyright: ignore[reportArgumentType] - read-only batch view mirrors collector context
    )
    assert not report.issues
    bc_sources = {ref.path.name: ref.bc_activation_uuid for ref in collected if ref.bc_activation_uuid}
    assert bc_sources == {"boundary_a.shp": "db-a", "boundary_b.shp": "db-b"}
    assert report.bc_activation_sources[(42, "db-a")] == {"a": tmp_path / "a_source.csv"}
    assert report.bc_activation_sources[(42, "db-b")] == {"b": tmp_path / "b_source.csv"}


@pytest.mark.parametrize("defect", ["ordinal", "duplicate", "missing", "reference_owner", "activation_sequence"])
def test_ordered_batch_malformed_lineage_fails_closed(tmp_path: Path, defect: str) -> None:
    tcf = tmp_path / "model.tcf"
    commands = [_command(0, "db-a", "BC Database", tcf=tcf), _command(1, "gis", "Read GIS BC", tcf=tcf)]
    bc = _reference(tmp_path / "a.csv", origin="BC Database", database=True)
    activation = types.SimpleNamespace(input_uuid="db-a", sequence=0, database_kind="bc", database_path=bc.path_pattern)
    case = _case("-e1 TP01", tuple(commands), ((0, bc),), activations=(activation,))
    if defect == "ordinal":
        commands[1].command_index = 10
    elif defect == "duplicate":
        commands[1].input_uuid = "db-a"
    elif defect == "missing":
        case.ordered_inputs = ()
    elif defect == "reference_owner":
        case.input_references[0].line_number = 42
    elif defect == "activation_sequence":
        activation.sequence = 1
    with pytest.raises(ValueError, match=r"lineage|missing from ordered|provenance|ordinal/UUID"):
        _view(case).context(case.flags, file_resolution="deferred")


def test_legacy_batch_without_ordered_inputs_remains_single_database_compatible(tmp_path: Path) -> None:
    bc = _reference(tmp_path / "bc.csv", origin="BC Database", database=True)
    case = _case(
        "-e1 TP01",
        (_command(0, "db", "BC Database", tcf=tmp_path / "model.tcf"),),
        ((0, bc),),
        activations=(
            types.SimpleNamespace(input_uuid="db", sequence=0, database_kind="bc", database_path=bc.path_pattern),
        ),
    )
    del case.ordered_inputs
    # Historic projections did not include owner/line metadata in every test
    # double and reconstruct a reference-bearing command only.
    context = _view(case).context(case.flags, file_resolution="deferred")
    assert len(context.find_input()) == 1
    assert context.deferred_database_sources() == ()
