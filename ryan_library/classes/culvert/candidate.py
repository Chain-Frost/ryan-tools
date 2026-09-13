"""Typed culvert design candidate definitions."""

from dataclasses import dataclass

from .crossing import CrossingDefinition


@dataclass(frozen=True, slots=True)
class DesignCandidate:
    """One named crossing configuration considered by a design search."""

    name: str
    crossing: CrossingDefinition

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            msg = "name must be nonempty text."
            raise ValueError(msg)
        object.__setattr__(self, "name", name)
