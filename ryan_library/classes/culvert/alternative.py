"""Typed alternative crossing definitions."""

from dataclasses import dataclass

from .crossing import CrossingDefinition


@dataclass(frozen=True, slots=True)
class Alternative:
    """A named alternative crossing configuration."""

    name: str
    crossing: CrossingDefinition
    source: str | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            msg = "name must be nonempty text."
            raise ValueError(msg)
        object.__setattr__(self, "name", name)
        source = None if self.source is None else self.source.strip()
        object.__setattr__(self, "source", source or None)
        object.__setattr__(self, "notes", self.notes.strip())
