# ryan-scripts\TUFLOW-python\generate_2d_po_label_list.py
"""Utility to convert a 2d_po GeoPackage into a Python list of PO labels.

Example:
    python generate_2d_po_label_list.py path/to/2d_po_file.gpkg
"""


import argparse
import sqlite3
from dataclasses import dataclass
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

# ------------------------------------------------------------------------------
# Configuration section
# ------------------------------------------------------------------------------
# If you frequently run this script against the same GeoPackage you can hardcode
# the file path below (either as a Path instance or a raw string). Leave it as
# None to rely exclusively on argparse. When both a hardcoded path and a CLI
# argument are supplied, the CLI argument wins.
HARDCODED_GPKG_PATH: Path | str | None = r"E:\Library\Automation\ryan-tools\2d_po_PilaruWest_01_L.gpkg"


@dataclass(slots=True)
class RuntimeOptions:
    """Container for the fully-resolved runtime options."""

    gpkg_path: Path
    layer: str | None


def parse_and_resolve_options(argv: Sequence[str] | None = None) -> RuntimeOptions:
    """Parse CLI arguments (if provided) and merge them with the hardcoded defaults."""

    parser = argparse.ArgumentParser(
        description=(
            "Read a 2d_po GeoPackage and emit a Python list of PO labels, " "including inline comments when provided."
        )
    )
    parser.add_argument(
        "gpkg_path",
        nargs="?",
        type=Path,
        help="Path to the 2d_po GeoPackage (e.g., 2d_po_Example_L.gpkg).",
    )
    parser.add_argument(
        "--layer",
        help=(
            "Optional layer/table name inside the GeoPackage. "
            "If omitted the script auto-detects the first 2d_po layer."
        ),
    )
    args: argparse.Namespace = parser.parse_args(argv)

    gpkg_candidate: Path | str | None
    if args.gpkg_path is not None:
        gpkg_candidate = args.gpkg_path
    else:
        gpkg_candidate = HARDCODED_GPKG_PATH

    if gpkg_candidate is None:
        parser.error("No GeoPackage supplied. Provide a path argument or set HARDCODED_GPKG_PATH.")

    gpkg_path: Path = Path(gpkg_candidate).expanduser().resolve()
    return RuntimeOptions(gpkg_path=gpkg_path, layer=args.layer)


def main() -> None:
    """Entry point used by both CLI execution and IDE run configurations."""

    options: RuntimeOptions = parse_and_resolve_options()
    if not options.gpkg_path.exists():
        raise SystemExit(f"GeoPackage not found: {options.gpkg_path}")

    try:
        entries: list[tuple[str, str | None]] = load_label_entries(
            gpkg_path=options.gpkg_path, preferred_layer=options.layer
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    output: str = format_as_python_list(entries)
    output_path: Path = options.gpkg_path.with_suffix(".txt")
    write_output_file(output_path=output_path, contents=output)
    print(output)
    print(f"\nList written to {output_path}")


def load_label_entries(gpkg_path: Path, preferred_layer: str | None = None) -> list[tuple[str, str | None]]:
    """Open the GeoPackage, determine the relevant layer, and read label/comment pairs."""

    # Connect directly to the GeoPackage; it's just a SQLite database under the hood.
    with sqlite3.connect(database=gpkg_path.as_posix()) as connection:
        cursor: sqlite3.Cursor = connection.cursor()
        table_name, column_lookup = resolve_table_and_columns(cursor=cursor, preferred_layer=preferred_layer)

        label_column: str | None = column_lookup.get("label")
        if label_column is None:
            raise ValueError(f'Table "{table_name}" does not contain a "Label" column.')
        comment_column: str | None = column_lookup.get("comment")
        order_column: str = column_lookup.get("fid", label_column)

        quoted_table: str = quote_identifier(table_name)
        quoted_label: str = quote_identifier(label_column)
        quoted_comment: str = quote_identifier(comment_column) if comment_column is not None else "NULL"
        quoted_order: str = quote_identifier(order_column)

        # Build a read-only query that orders the labels by fid (or label fallback).
        sql: str = (
            f"SELECT {quoted_label} AS label_value, "
            f"{quoted_comment} AS comment_value "
            f"FROM {quoted_table} ORDER BY {quoted_order};"
        )
        rows: list[Any] = cursor.execute(sql).fetchall()

    entries: list[tuple[str, str | None]] = []
    for label_value, comment_value in rows:
        if label_value is None:
            continue

        # Clean up whitespace so the output list reads nicely.
        label_text: str = str(label_value).strip()
        if not label_text:
            continue

        comment_text: str | None
        if comment_value is None:
            comment_text = None
        else:
            comment_text = sanitize_comment(str(comment_value))

        entries.append((label_text, comment_text))

    if not entries:
        raise ValueError(f'No label values found in table "{table_name}".')
    return entries


def resolve_table_and_columns(cursor: sqlite3.Cursor, preferred_layer: str | None) -> tuple[str, dict[str, str]]:
    """Pick a layer that exposes a Label column and collect its schema metadata."""

    tables_to_check: list[str] = build_table_priority_list(cursor=cursor, preferred_layer=preferred_layer)
    for table in tables_to_check:
        column_lookup: dict[str, str] = get_column_lookup(cursor=cursor, table_name=table)
        if column_lookup.get("label"):
            return table, column_lookup
    raise ValueError("Could not locate a layer with a 'Label' column.")


def build_table_priority_list(cursor: sqlite3.Cursor, preferred_layer: str | None) -> list[str]:
    """Return candidate layers, prioritizing those that look like 2d_po layers."""

    layer_rows: list[Any] = cursor.execute(
        "SELECT table_name FROM gpkg_contents WHERE data_type = 'features';"
    ).fetchall()
    discovered_layers: list[Any] = [row[0] for row in layer_rows]

    if preferred_layer:
        return [preferred_layer]

    def sort_key(name: str) -> tuple[int, str]:
        lowered: str = name.lower()
        return (0 if lowered.startswith("2d_po") else 1, lowered)

    return sorted(discovered_layers, key=sort_key)


def get_column_lookup(cursor: sqlite3.Cursor, table_name: str) -> dict[str, str]:
    """Fetch column names for the table and normalize them for case-insensitive use."""

    quoted_table: str = quote_identifier(table_name)
    pragma_rows: list[Any] = cursor.execute(f"PRAGMA table_info({quoted_table});").fetchall()
    return {row[1].lower(): row[1] for row in pragma_rows}


def sanitize_comment(comment: str) -> str | None:
    """Normalize whitespace so comments become single-line Python-friendly strings."""

    normalized: str = comment.replace("\r\n", "\n").replace("\r", " ").replace("\n", " ").strip()
    return normalized or None


def format_as_python_list(entries: Iterable[tuple[str, str | None]]) -> str:
    """Format the label/comment pairs as a copy/paste-friendly Python list literal."""

    lines: list[str] = ["["]
    for label, comment in entries:
        # Represent labels with repr() so any embedded quotes are automatically escaped.
        line: str = f"    {repr(label)},"
        if comment:
            line += f"  # {comment}"
        lines.append(line)
    lines.append("]")
    return "\n".join(lines)


def quote_identifier(identifier: str) -> str:
    """Quote SQLite identifiers to avoid syntax errors or SQL injection surprises."""

    escaped = identifier.replace('"', '""')
    return f'"{escaped}"'


def write_output_file(*, output_path: Path, contents: str) -> None:
    """Persist the rendered list next to the source GeoPackage for reuse."""

    output_path.write_text(f"{contents}\n", encoding="utf-8")


if __name__ == "__main__":
    main()
