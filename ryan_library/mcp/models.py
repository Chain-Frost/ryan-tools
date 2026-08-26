"""Typed models for MCP workflow discovery."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Mapping, cast


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


_PROFILE_RANK: dict[CapabilityProfile, int] = {
    CapabilityProfile.CORE: 0,
    CapabilityProfile.ANALYSIS: 1,
    CapabilityProfile.CREATE: 2,
    CapabilityProfile.PRIVILEGED: 3,
}


def profile_allows(*, configured: CapabilityProfile, required: CapabilityProfile) -> bool:
    """Return whether a configured profile includes a required profile."""
    return _PROFILE_RANK[configured] >= _PROFILE_RANK[required]


@dataclass(frozen=True, slots=True)
class WorkflowSpec:
    """A discoverable CLI workflow backed by a repository script."""

    workflow_id: str
    title: str
    domain: str
    purpose: str
    script: str
    profile: CapabilityProfile
    mutation: MutationKind
    lifecycle: str = "maintained"
    requires_explicit_approval: bool = False
    headless_arguments: tuple[str, ...] = ()
    metadata: dict[str, Any] = field(default_factory=lambda: {})

    @classmethod
    def from_mapping(cls, raw: Mapping[str, object]) -> WorkflowSpec:
        """Validate and construct a workflow specification from JSON data."""

        def required_string(key: str) -> str:
            value: object | None = raw.get(key)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Workflow field {key!r} must be a non-empty string.")
            return value

        raw_headless_value: object = raw.get("headless_arguments", [])
        if not isinstance(raw_headless_value, list):
            raise ValueError("Workflow field 'headless_arguments' must be a list of strings.")
        raw_headless: list[object] = cast(list[object], raw_headless_value)
        if not all(isinstance(item, str) for item in raw_headless):
            raise ValueError("Workflow field 'headless_arguments' must be a list of strings.")
        headless_arguments: tuple[str, ...] = tuple(cast(str, item) for item in raw_headless)

        known_keys: set[str] = {
            "id",
            "title",
            "domain",
            "purpose",
            "script",
            "profile",
            "mutation",
            "lifecycle",
            "requires_explicit_approval",
            "headless_arguments",
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
            script=required_string("script"),
            profile=CapabilityProfile(required_string("profile")),
            mutation=MutationKind(required_string("mutation")),
            lifecycle=str(raw.get("lifecycle", "maintained")),
            requires_explicit_approval=bool(raw.get("requires_explicit_approval", False)),
            headless_arguments=headless_arguments,
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
            "available": available,
        }
