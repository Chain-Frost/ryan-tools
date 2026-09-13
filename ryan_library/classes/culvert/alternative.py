"""Typed alternative crossing definitions."""

from dataclasses import dataclass

from .crossing import CrossingDefinition


@dataclass(frozen=True, slots=True)
class Alternative:
    """A named alternative crossing configuration."""

    name: str
    crossing: CrossingDefinition

    def __post_init__(self) -> None:
        name = self.name.strip()
        if not name:
            raise ValueError("name must be nonempty text.")
        object.__setattr__(self, "name", name)
