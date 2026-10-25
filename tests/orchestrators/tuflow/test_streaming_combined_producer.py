"""Exact opt-in combined producer contracts; synthetic files, never a solver."""

from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import fields, is_dataclass
from pathlib import Path
from typing import Protocol, cast

import pytest

from ryan_library.orchestrators.tuflow.input_preflight import RunRequest, check_simulation_inputs
from ryan_library.orchestrators.tuflow.streaming_io_probe import probe_batch_io_overlap

pytestmark = pytest.mark.skipif(
    os.environ.get("RYAN_TEST_STREAM_IO_PYTUFLOW") != "1", reason="Requires exact combined PyTUFLOW producer"
)


class _Command(Protocol):
    input_uuid: str
    command_index: int


class _Activation(Protocol):
    input_uuid: str
    sequence: int


class _Run(Protocol):
    index: int
    flags: str
    error: str | None
    database_activations: tuple[_Activation, ...]
    ordered_inputs: tuple[_Command, ...]


class _Result(Protocol):
    passed: bool
    runs: tuple[_Run, ...]


class _Producer(Protocol):
    def collect_batch_dependencies(
        self,
        flags: tuple[str, ...],
        *,
        on_run: Callable[[object], None] | None = None,
    ) -> _Result: ...


def canonical(value: object) -> object:
    """Compare complete public fields, while fresh UUIDs are checked within each run."""
    if is_dataclass(value) and not isinstance(value, type):
        return tuple(
            (field.name, canonical(cast(object, getattr(value, field.name))))
            for field in fields(value)
            if "uuid" not in field.name
        )
    if isinstance(value, tuple):
        return tuple(canonical(item) for item in cast(tuple[object, ...], value))
    return value


def test_combined_callback_retains_complete_activations_references_and_failures(tmp_path: Path) -> None:
    from pytuflow import TCF

    (tmp_path / "bc.csv").write_text(
        "Name,Source,Column 1,Column 2,Add Col 1,Mult Col 2,Add Col 2,Column 3,Column 4\n"
        "FC01,flow.csv,time,value,,,,,\n",
        encoding="utf-8",
    )
    (tmp_path / "flow.csv").write_text("time,value\n0,1\n", encoding="utf-8")
    tcf = tmp_path / "model.tcf"
    tcf.write_text(
        "BC Database == bc.csv\nIF Scenario == DEV\nRead GIS Code == code_DEV.shp\n"
        "ELSE\nRead GIS Code == code_EXG.shp\nEND IF\n",
        encoding="utf-8",
    )
    (tmp_path / "code_DEV.shp").touch()
    flags = ("-s1 DEV", "-s1 EXG", "-s1 DEV")
    producer = cast(_Producer, TCF(tcf))
    plain = producer.collect_batch_dependencies(flags)
    assert plain.passed
    observed: list[object] = []
    returned: list[_Run] = []

    class Observed:
        def collect_batch_dependencies(
            self,
            flags: tuple[str, ...],
            *,
            on_run: Callable[[object], None],
        ) -> _Result:
            def completed(run: object) -> None:
                observed.append(run)
                on_run(run)

            result = producer.collect_batch_dependencies(flags, on_run=completed)
            returned.extend(result.runs)
            return result

    view, summary = probe_batch_io_overlap(Observed(), flags, max_pending=1)
    assert len(observed) == len(returned) == 3
    assert all(callback is run for callback, run in zip(observed, returned, strict=True))
    assert canonical(tuple(returned)) == canonical(plain.runs)
    assert [run.flags for run in returned] == list(flags)
    assert all(run.database_activations for run in returned)
    for run in returned:
        owners = {item.input_uuid: item.command_index for item in run.ordered_inputs}
        assert all(owners[item.input_uuid] == item.sequence for item in run.database_activations)
        view.context(run.flags, file_resolution="deferred")
    view.assert_consumed()
    assert summary.peak_outstanding <= 1
    # Speculative missing metadata cannot become an authoritative successful check.
    requests = [RunRequest(i, {"s1": scenario}) for i, scenario in enumerate(("DEV", "EXG", "DEV"), 1)]
    individual = check_simulation_inputs(tcf, requests, context_mode="individual")
    batch = check_simulation_inputs(tcf, requests, context_mode="batch")
    assert batch.checked_paths == individual.checked_paths
    assert set(batch.issues) == set(individual.issues)
    assert not batch.passed
    assert {issue.run_index for issue in batch.issues} == {2}


@pytest.mark.parametrize("fail_at", [0, 1, 2])
def test_combined_callback_exceptions_abort_without_duplicate_notifications(tmp_path: Path, fail_at: int) -> None:
    from pytuflow import TCF

    tcf = tmp_path / "model.tcf"
    tcf.write_text("Read GIS Code == literal.shp\n", encoding="utf-8")
    producer = cast(_Producer, TCF(tcf))
    seen: list[object] = []

    def callback(run: object) -> None:
        seen.append(run)
        if len(seen) == fail_at + 1:
            message = "consumer failure"
            raise ValueError(message)

    with pytest.raises(ValueError, match="consumer failure"):
        producer.collect_batch_dependencies(("-s1 DEV", "-s1 EXG", "-s1 DEV"), on_run=callback)
    assert len(seen) == fail_at + 1
