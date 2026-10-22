"""Opt-in adapter from PyTUFLOW batch projections to the existing preflight collector.

The adapter is deliberately *not* a TUFLOW parser: it reconstructs only the
read-only input/context interface used by the existing individual-run collector.
Syntactic references, database rows, command ownership, and errors originate
from PyTUFLOW. Physical inspection stays in input_preflight.py.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol, cast


class _InputReference(Protocol):
    input_uuid: str
    command_index: int
    reference: object


class _Activation(Protocol):
    input_uuid: str
    database_kind: str


class _Source(Protocol):
    input_uuid: str
    database_kind: str


class _Case(Protocol):
    index: int
    flags: str
    error: str | None
    input_references: tuple[_InputReference, ...]
    file_references: tuple[object, ...]
    database_sources: tuple[_Source, ...]
    database_activations: tuple[_Activation, ...]


class _Result(Protocol):
    runs: tuple[_Case, ...]


class _BatchCollector(Protocol):
    def collect_batch_dependencies(self, flags: Sequence[str]) -> _Result: ...


@dataclass(frozen=True, slots=True)
class _InputView:
    """The original input identity and its immutable references."""

    lhs: str
    origin: str
    file_references: tuple[object, ...]

    def __str__(self) -> str:
        return self.origin


class _ContextView:
    def __init__(self, case: _Case) -> None:
        if case.error is not None:
            raise ValueError(f"PyTUFLOW batch context failed: {case.error}")

        # A projection without per-command ownership cannot provide safe
        # database activation ordering. Refuse old/incomplete batch APIs.
        records = case.input_references
        if tuple(record.reference for record in records) != case.file_references:
            raise ValueError("Incomplete batch input ownership or missing references")

        groups: dict[tuple[int, str], list[object]] = {}
        for record in records:
            if not record.input_uuid or record.command_index < 0:
                raise ValueError("Invalid batch source command identity")
            key = (record.command_index, record.input_uuid)
            groups.setdefault(key, []).append(record.reference)

        inputs: list[_InputView] = []
        for key in sorted(groups):
            refs = tuple(groups[key])
            first = refs[0]
            origin = cast(str, getattr(first, "origin", ""))
            if not origin:
                raise ValueError("Batch file reference has no originating command")
            inputs.append(_InputView(origin, origin, refs))
        self._inputs = tuple(inputs)

        activations = case.database_activations
        activation_map: dict[str, _Activation] = {}
        kinds: dict[str, int] = {}
        for activation in activations:
            uuid = activation.input_uuid
            if not uuid or uuid in activation_map:
                raise ValueError("Missing or duplicate batch database activation UUID")
            activation_map[uuid] = activation
            kinds[activation.database_kind] = kinds.get(activation.database_kind, 0) + 1

        # The individual deferred source accessor fails closed on ambiguous
        # stateful database roles. Until feature-level GIS row selection
        # exists, do not union multiple BC declarations into a false pass.
        for kind, count in kinds.items():
            if count > 1 and kind not in {"rainfall", "materials"}:
                raise ValueError(f"Ambiguous batch {kind} database activations: {count}")

        for source in case.database_sources:
            owner = activation_map.get(source.input_uuid)
            if owner is None or owner.database_kind != source.database_kind:
                raise ValueError("Batch database source is not owned by its activation")
        self._sources = case.database_sources

    def find_input(self, lhs: str | None = None) -> list[_InputView]:
        if lhs is None:
            return list(self._inputs)
        query = lhs.strip().casefold()
        return [inp for inp in self._inputs if inp.lhs.strip().casefold() == query]

    def deferred_database_sources(self) -> tuple[_Source, ...]:
        return self._sources


class BatchModelView:
    """Single-use replay of an exact, ordered batch against existing collector."""

    def __init__(self, model: object, flags: Sequence[str]) -> None:
        batch = getattr(model, "collect_batch_dependencies", None)
        if not callable(batch):
            raise ValueError(
                "PyTUFLOW batch mode requires collect_batch_dependencies(); "
                "install the tested fork in an isolated environment"
            )
        output = cast(_Result, batch(tuple(flags)))
        if len(output.runs) != len(flags):
            raise ValueError("PyTUFLOW batch returned a different number of runs")
        self._cases = output.runs
        self._flags = tuple(flags)
        self._next = 0

    def context(self, flags: str, *, file_resolution: str | None = None) -> _ContextView:
        if file_resolution != "deferred":
            raise ValueError("Batch projection only supports deferred input collection")
        if self._next >= len(self._cases):
            raise ValueError("Batch collector requested more contexts than supplied")
        index = self._next
        self._next += 1
        case = self._cases[index]
        if case.index != index or flags != self._flags[index] or case.flags != flags:
            raise ValueError("PyTUFLOW batch context index or flags mismatch")
        return _ContextView(case)

    def assert_consumed(self) -> None:
        """Detect an incomplete collector run rather than silently passing."""
        if self._next != len(self._cases):
            raise ValueError("Batch collector did not inspect every proposed run")
