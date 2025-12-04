"""Central registry for column descriptions used across exports."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar


@dataclass(frozen=True, slots=True)
class ColumnDefinition:
    """Describe the intent of a DataFrame column."""

    name: str
    description: str
    value_type: str | None = None


BaseDefinitions = Mapping[str, ColumnDefinition]
SheetSpecificDefinitions = Mapping[str, BaseDefinitions]

_DEFAULT_DEFINITIONS_PATH: Path = Path(__file__).with_name("column_definitions.json")


def _freeze_base_definitions(definitions: Mapping[str, ColumnDefinition]) -> BaseDefinitions:
    return MappingProxyType(mapping=dict(definitions))


def _freeze_sheet_specific_definitions(
    definitions: Mapping[str, Mapping[str, ColumnDefinition]],
) -> SheetSpecificDefinitions:
    return MappingProxyType(
        mapping={sheet: _freeze_base_definitions(definitions=defs) for sheet, defs in definitions.items()}
    )


def _to_column_definition(name: str, payload: Mapping[str, Any]) -> ColumnDefinition:
    description = payload.get("description")
    if description is None:
        msg: str = f"Missing description for column definition '{name}' in {_DEFAULT_DEFINITIONS_PATH.name}"
        raise ValueError(msg)

    value_type = payload.get("value_type")
    return ColumnDefinition(
        name=name,
        description=str(description),
        value_type=str(value_type) if value_type is not None else None,
    )


def _load_default_definitions() -> tuple[BaseDefinitions, SheetSpecificDefinitions]:
    data = json.loads(_DEFAULT_DEFINITIONS_PATH.read_text(encoding="utf-8"))
    base_definitions: dict[Any, ColumnDefinition] = {
        name: _to_column_definition(name=name, payload=payload)
        for name, payload in data.get("base_definitions", {}).items()
    }
    sheet_specific_definitions = {
        sheet_name: {name: _to_column_definition(name=name, payload=payload) for name, payload in definitions.items()}
        for sheet_name, definitions in data.get("sheet_specific_definitions", {}).items()
    }
    return _freeze_base_definitions(definitions=base_definitions), _freeze_sheet_specific_definitions(
        definitions=sheet_specific_definitions
    )


DEFAULT_BASE_DEFINITIONS, DEFAULT_SHEET_SPECIFIC_DEFINITIONS = _load_default_definitions()


class ColumnMetadataRegistry:
    """Registry providing consistent column descriptions across exports."""

    _INSTANCE: ClassVar["ColumnMetadataRegistry"] | None = None

    def __init__(
        self,
        base_definitions: BaseDefinitions | None = None,
        sheet_specific: SheetSpecificDefinitions | None = None,
    ) -> None:
        self._base_definitions: BaseDefinitions = (
            _freeze_base_definitions(definitions=base_definitions)
            if base_definitions is not None
            else DEFAULT_BASE_DEFINITIONS
        )
        self._sheet_specific: SheetSpecificDefinitions = (
            _freeze_sheet_specific_definitions(definitions=sheet_specific)
            if sheet_specific is not None
            else DEFAULT_SHEET_SPECIFIC_DEFINITIONS
        )

    def definition_for(self, column_name: str, sheet_name: str | None = None) -> ColumnDefinition:
        """Return a :class:`ColumnDefinition` for ``column_name``.

        Sheet-specific definitions override base definitions. If no definition
        exists a placeholder entry is returned so that missing descriptions are
        easy to spot in the exported workbook.
        """

        if sheet_name is not None and sheet_name in self._sheet_specific:
            sheet_def: BaseDefinitions = self._sheet_specific[sheet_name]
            if column_name in sheet_def:
                return sheet_def[column_name]

        if column_name in self._base_definitions:
            return self._base_definitions[column_name]

        return ColumnDefinition(
            name=column_name,
            description=f"TODO: add description for '{column_name}'.",
            value_type=None,
        )

    def iter_definitions(self, column_names: Iterable[str], sheet_name: str | None = None) -> list[ColumnDefinition]:
        """Return definitions for ``column_names`` preserving order."""

        return [self.definition_for(column_name=col, sheet_name=sheet_name) for col in column_names]

    @classmethod
    def default(cls) -> "ColumnMetadataRegistry":
        """Return the default registry instance."""

        if cls._INSTANCE is None:
            cls._INSTANCE = cls(
                base_definitions=DEFAULT_BASE_DEFINITIONS,
                sheet_specific=DEFAULT_SHEET_SPECIFIC_DEFINITIONS,
            )
        return cls._INSTANCE


__all__: list[str] = ["ColumnDefinition", "ColumnMetadataRegistry"]
