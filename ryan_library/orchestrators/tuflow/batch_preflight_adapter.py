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
    owner_control_file: object
    line_number: int
    reference: object


class _OrderedInput(Protocol):
    input_uuid: str
    command_index: int
    owner_control_file: object
    line_number: int
    command_name: str
    command_text: str


class _Activation(Protocol):
    input_uuid: str
    database_kind: str
    sequence: int
    database_path: object


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
    # Introduced in PyTUFLOW PR #6; older fork batches omit this field.
    ordered_inputs: tuple[_OrderedInput, ...]


class _Result(Protocol):
    runs: tuple[_Case, ...]


@dataclass(frozen=True, slots=True)
class _InputView:
    """The original input identity and its immutable references."""

    lhs: str
    origin: str
    file_references: tuple[object, ...]
    uuid: str = ""

    def __str__(self) -> str:
        return self.origin


def _validate_active_sources(
    activations: tuple[_Activation, ...],
    sources: tuple[_Source, ...],
    *,
    ordered_inputs: tuple[_OrderedInput, ...] | None,
) -> tuple[_Source, ...]:
    """Reject orphan rows, invalid declaration order and unsafe BC aggregation."""
    activation_map: dict[str, _Activation] = {}
    kinds: dict[str, int] = {}
    by_uuid = {item.input_uuid: item for item in ordered_inputs} if ordered_inputs is not None else {}
    for activation in activations:
        uuid = activation.input_uuid
        if not uuid or uuid in activation_map:
            message = "Missing or duplicate batch database activation UUID"
            raise ValueError(message)
        if ordered_inputs is not None:
            command = by_uuid.get(uuid)
            if command is None or activation.sequence != command.command_index:
                message = "Batch database activation ordinal/UUID mismatch"
                raise ValueError(message)
        activation_map[uuid] = activation
        kinds[activation.database_kind] = kinds.get(activation.database_kind, 0) + 1

    # Before ordered-input lineage was available, a multi-BC projection could
    # silently lose the declaration that had no file references. Require PR #6
    # descriptors before exposing this opt-in feature to the common collector.
    for kind, count in kinds.items():
        if count > 1 and kind not in {"rainfall", "materials"} and not (kind == "bc" and ordered_inputs is not None):
            message = f"Ambiguous batch {kind} database activations: {count}"
            raise ValueError(message)

    for source in sources:
        owner = activation_map.get(source.input_uuid)
        if owner is None or owner.database_kind != source.database_kind:
            message = "Batch database source is not owned by its activation"
            raise ValueError(message)
    return sources


class _ContextView:
    def __init__(self, case: _Case) -> None:  # noqa: C901 - fail-closed lineage and provenance checks
        if case.error is not None:
            message = f"PyTUFLOW batch context failed: {case.error}"
            raise ValueError(message)

        records = case.input_references
        if tuple(record.reference for record in records) != case.file_references:
            message = "Incomplete batch input ownership or missing references"
            raise ValueError(message)

        groups: dict[tuple[int, str], list[_InputReference]] = {}
        for record in records:
            if not record.input_uuid or record.command_index < 0:
                message = "Invalid batch source command identity"
                raise ValueError(message)
            key = (record.command_index, record.input_uuid)
            groups.setdefault(key, []).append(record)

        # Old cached-batch builds only expose commands with file references.
        # Keep that validated compatibility path for single-BC projects while
        # the new ordered-input API is not yet part of the installed release.
        ordered_inputs = getattr(case, "ordered_inputs", None)
        inputs: list[_InputView] = []
        if ordered_inputs is None:
            for key in sorted(groups):
                refs = tuple(item.reference for item in groups[key])
                origin = cast(str, getattr(refs[0], "origin", ""))
                if not origin:
                    message = "Batch file reference has no originating command"
                    raise ValueError(message)
                inputs.append(_InputView(origin, origin, refs, key[1]))
        else:
            # Every active input occupies exactly one original ordinal, even
            # settings, variables and BC declarations with zero references.
            ordered_inputs = tuple(ordered_inputs)
            seen_uuids: set[str] = set()
            for ordinal, item in enumerate(ordered_inputs):
                if (
                    item.command_index != ordinal
                    or not item.input_uuid
                    or item.input_uuid in seen_uuids
                    or not item.command_name.strip()
                    or not item.command_text.strip()
                ):
                    message = "Invalid or incomplete batch ordered-input lineage"
                    raise ValueError(message)
                seen_uuids.add(item.input_uuid)
                key = (ordinal, item.input_uuid)
                owned = groups.pop(key, ())
                for record in owned:
                    if record.owner_control_file != item.owner_control_file or record.line_number != item.line_number:
                        message = "Batch reference command/provenance mismatch"
                        raise ValueError(message)
                inputs.append(
                    _InputView(
                        item.command_name,
                        item.command_text,
                        tuple(record.reference for record in owned),
                        item.input_uuid,
                    )
                )
            if groups:
                message = "Batch references include commands missing from ordered inputs"
                raise ValueError(message)
        self._inputs = tuple(inputs)
        self._activations = tuple(case.database_activations)
        self._sources = _validate_active_sources(
            self._activations, case.database_sources, ordered_inputs=ordered_inputs
        )
        self._activation_ids = frozenset(item.input_uuid for item in self._activations)

    def find_input(self, lhs: str | None = None) -> list[_InputView]:
        if lhs is None:
            return list(self._inputs)
        query = lhs.strip().casefold()
        return [inp for inp in self._inputs if inp.lhs.strip().casefold() == query]

    def deferred_database_activations(self) -> tuple[_Activation, ...]:
        return self._activations

    def deferred_database_sources_for_activation(self, input_uuid: str) -> tuple[_Source, ...]:
        if input_uuid not in self._activation_ids:
            message = f"Unknown batch database activation UUID: {input_uuid}"
            raise ValueError(message)
        return tuple(source for source in self._sources if source.input_uuid == input_uuid)

    def deferred_database_sources(self) -> tuple[_Source, ...]:
        # The existing collector calls per-activation accessors when multiple
        # BC declarations exist. Never present a union as one active BC table.
        if sum(item.database_kind == "bc" for item in self._activations) > 1:
            message = "Ambiguous batch bc database aggregation"
            raise ValueError(message)
        return self._sources

class BatchModelView:
    """Single-use replay of an exact, ordered batch against existing collector."""

    def __init__(self, model: object, flags: Sequence[str]) -> None:
        batch = getattr(model, "collect_batch_dependencies", None)
        if not callable(batch):
            message = (
                "PyTUFLOW batch mode requires collect_batch_dependencies(); "
                "install the tested fork in an isolated environment"
            )
            raise ValueError(message)
        output = cast(_Result, batch(tuple(flags)))
        if len(output.runs) != len(flags):
            message = "PyTUFLOW batch returned a different number of runs"
            raise ValueError(message)
        self._cases = output.runs
        self._flags = tuple(flags)
        self._next = 0

    def context(self, flags: str, *, file_resolution: str | None = None) -> _ContextView:
        if file_resolution != "deferred":
            message = "Batch projection only supports deferred input collection"
            raise ValueError(message)
        if self._next >= len(self._cases):
            message = "Batch collector requested more contexts than supplied"
            raise ValueError(message)
        index = self._next
        self._next += 1
        case = self._cases[index]
        if case.index != index or flags != self._flags[index] or case.flags != flags:
            message = "PyTUFLOW batch context index or flags mismatch"
            raise ValueError(message)
        return _ContextView(case)

    def assert_consumed(self) -> None:
        """Detect an incomplete collector run rather than silently passing."""
        if self._next != len(self._cases):
            message = "Batch collector did not inspect every proposed run"
            raise ValueError(message)
