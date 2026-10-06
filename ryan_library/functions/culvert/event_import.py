"""Import externally supplied event targets without performing hydrology."""

import csv
from pathlib import Path

from ...classes.culvert.event import EventDefinition

_FIELDS: set[str] = {
    "name",
    "aep_percent",
    "discharge_m3s",
    "target_headwater_elevation_m",
    "tailwater_elevation_m",
    "source",
    "notes",
}


def _optional_float(row: dict[str, str], field: str, row_number: int) -> float | None:
    text = row.get(field, "").strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError as exc:
        msg = f"CSV row {row_number}, field {field}: expected a number, received {text!r}."
        raise ValueError(msg) from exc


def load_event_csv(path: Path) -> tuple[EventDefinition, ...]:
    """Load event targets in source row order with row-specific validation."""
    source = Path(path)
    with source.open(encoding="utf-8-sig", newline="") as stream:
        reader: csv.DictReader[str] = csv.DictReader(stream)
        if reader.fieldnames is None:
            msg = "Event CSV requires a header row."
            raise ValueError(msg)
        unknown: list[str] = sorted(set(reader.fieldnames) - _FIELDS)
        if unknown:
            msg = f"Event CSV contains unknown column(s): {', '.join(unknown)}."
            raise ValueError(msg)
        events: list[EventDefinition] = []
        for row_number, row in enumerate(reader, start=2):
            try:
                events.append(
                    EventDefinition(
                        name=row.get("name") or None,
                        aep_percent=_optional_float(row, "aep_percent", row_number),
                        discharge_m3s=_optional_float(row, "discharge_m3s", row_number),
                        target_headwater_elevation_m=_optional_float(
                            row,
                            "target_headwater_elevation_m",
                            row_number,
                        ),
                        tailwater_elevation_m=_optional_float(row, "tailwater_elevation_m", row_number),
                        source=row.get("source") or None,
                        notes=row.get("notes", ""),
                    )
                )
            except ValueError as exc:
                msg = f"Invalid event CSV row {row_number}: {exc}"
                raise ValueError(msg) from exc
    if not events:
        msg = "Event CSV must contain at least one data row."
        raise ValueError(msg)
    return tuple(events)
