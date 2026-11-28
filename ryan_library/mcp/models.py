"""Typed models for MCP workflow discovery."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, cast


class CapabilityProfile(StrEnum):
    """Cumulative workflow exposure profiles."""

    CORE = "core"
    ANALYSIS = "analysis"
    CREATE = "create"
    PRIVILEGED = "privileged"


class MutationKind(StrEnum):
    """Filesystem or process effect associated with a workflow."""

    READ_ONLY = "read_only"
    CREATES_OUTPUTS = "creates_outputs"
    CREATES_OR_REPLACES = "creates_or_replaces"
    IN_PLACE = "in_place"
    DELETES = "deletes"
    EXECUTES_EXTERNAL = "executes_external"


class ExecutionKind(StrEnum):
    """Supported ways to invoke a catalogued workflow."""

    MODULE = "module"
    SCRIPT = "script"
    CONSOLE_SCRIPT = "console_script"


_PROFILE_RANK: dict[CapabilityProfile, int] = {
    CapabilityProfile.CORE: 0,
    CapabilityProfile.ANALYSIS: 1,
    CapabilityProfile.CREATE: 2,
    CapabilityProfile.PRIVILEGED: 3,
}


def _empty_metadata() -> dict[str, Any]:
    """Return a precisely typed empty metadata mapping for dataclass defaults."""
    return {}


def profile_allows(*, configured: CapabilityProfile, required: CapabilityProfile) -> bool:
    """Return whether a configured profile includes a required profile."""
    return _PROFILE_RANK[configured] >= _PROFILE_RANK[required]


@dataclass(frozen=True, slots=True)
class ExecutionTarget:
    """Explicit invocation target for a workflow."""

    kind: ExecutionKind
    value: str

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> ExecutionTarget:
        """Read exactly one explicit execution target from catalogue data."""
        candidates: list[tuple[ExecutionKind, str]] = []
        for kind in ExecutionKind:
            value: object | None = raw.get(kind.value)
            if value is None:
                continue
            if not isinstance(value, str) or not value.strip():
                msg = f"Workflow field {kind.value!r} must be a non-empty string."
                raise ValueError(msg)
            if kind is ExecutionKind.SCRIPT:
                # Workflow scripts must stay beneath the installed/copied
                # ryan-scripts tree on both POSIX and Windows. Reject traversal
                # in the catalogue itself, before resolving any checkout path.
                segments = value.split("/")
                if (
                    len(segments) < 2
                    or segments[0] != "ryan-scripts"
                    or any(segment in {"", ".", ".."} for segment in segments)
                    or "\\" in value
                ):
                    msg = f"Workflow script target must be a relative ryan-scripts path: {value!r}"
                    raise ValueError(msg)
            candidates.append((kind, value))

        if len(candidates) != 1:
            target_fields: str = ", ".join(repr(kind.value) for kind in ExecutionKind)
            msg = f"Workflow must define exactly one execution target from: {target_fields}."
            raise ValueError(msg)
        kind, value = candidates[0]
        return cls(kind=kind, value=value)


def normalize_workflow_keywords(raw: object, *, workflow_id: str, allow_tuple: bool = False) -> tuple[str, ...]:
    """Validate and normalize curated search terms from either catalogue."""
    if not isinstance(raw, list) and not (allow_tuple and isinstance(raw, tuple)):
        msg = (
            f"Workflow {workflow_id!r} field 'keywords' must be an array of non-empty strings; "
            "omit the field or use [] when no keywords are needed."
        )
        raise ValueError(msg)
    values = cast("list[object] | tuple[object, ...]", raw)
    normalized: list[str] = []
    seen: set[str] = set()
    for index, value in enumerate(values):
        if not isinstance(value, str) or not value.strip():
            msg = f"Workflow {workflow_id!r} field 'keywords' item {index} must be a non-empty string."
            raise ValueError(msg)
        term = value.strip()
        folded = term.casefold()
        if folded not in seen:
            normalized.append(term)
            seen.add(folded)
    return tuple(normalized)


@dataclass(frozen=True, slots=True)
class WorkflowSpec:
    """A discoverable CLI workflow with an explicit execution target."""

    workflow_id: str
    title: str
    domain: str
    purpose: str
    execution: ExecutionTarget
    profile: CapabilityProfile
    mutation: MutationKind
    lifecycle: str = "maintained"
    requires_explicit_approval: bool = False
    headless_arguments: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=_empty_metadata)
    keywords: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        """Normalize direct model construction as well as JSON catalogue loading."""
        object.__setattr__(
            self,
            "keywords",
            normalize_workflow_keywords(self.keywords, workflow_id=self.workflow_id, allow_tuple=True),
        )

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> WorkflowSpec:
        """Validate and construct a workflow specification from JSON data."""

        def required_string(key: str) -> str:
            value: object | None = raw.get(key)
            if not isinstance(value, str) or not value.strip():
                msg = f"Workflow field {key!r} must be a non-empty string."
                raise ValueError(msg)
            return value

        raw_headless_value: object = raw.get("headless_arguments", [])
        if not isinstance(raw_headless_value, list):
            msg = "Workflow field 'headless_arguments' must be a list of strings."
            raise ValueError(msg)
        raw_headless: list[object] = cast("list[object]", raw_headless_value)
        if not all(isinstance(item, str) for item in raw_headless):
            msg = "Workflow field 'headless_arguments' must be a list of strings."
            raise ValueError(msg)
        headless_arguments: tuple[str, ...] = tuple(cast("str", item) for item in raw_headless)

        known_keys: set[str] = {
            "id",
            "title",
            "domain",
            "purpose",
            *(kind.value for kind in ExecutionKind),
            "profile",
            "mutation",
            "lifecycle",
            "requires_explicit_approval",
            "headless_arguments",
            "keywords",
        }
        metadata: dict[str, Any] = {}
        for key, value in raw.items():
            if key not in known_keys:
                metadata[key] = value

        return cls(
            workflow_id=required_string("id"),
            title=required_string("title"),
            domain=required_string("domain"),
            purpose=required_string("purpose"),
            execution=ExecutionTarget.from_mapping(raw),
            profile=CapabilityProfile(required_string("profile")),
            mutation=MutationKind(required_string("mutation")),
            lifecycle=str(raw.get("lifecycle", "maintained")),
            requires_explicit_approval=bool(raw.get("requires_explicit_approval", False)),
            headless_arguments=headless_arguments,
            keywords=normalize_workflow_keywords(raw.get("keywords", []), workflow_id=required_string("id")),
            metadata=metadata,
        )

    def summary(self, *, available: bool) -> dict[str, Any]:
        """Return the compact representation used by workflow listing."""
        return {
            "id": self.workflow_id,
            "title": self.title,
            "domain": self.domain,
            "purpose": self.purpose,
            "profile": self.profile.value,
            "mutation": self.mutation.value,
            "lifecycle": self.lifecycle,
            "requires_explicit_approval": self.requires_explicit_approval,
            "execution_kind": self.execution.kind.value,
            "execution_target": self.execution.value,
            "available": available,
        }
