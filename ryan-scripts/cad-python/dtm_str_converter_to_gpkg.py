# ryan-scripts\misc-python\dtm_str_converter_to_gpkg.py
# Updated 2026-05-25
"""Convert supported Surpac STR and DTM files to GIS-friendly outputs.

What this script does:
- Converts a Surpac STR file to point and linestring GeoPackage layers.
- Converts a matching Surpac DTM file to a triangulation GeoPackage layer.
- Automatically looks for a DTM with the same name as the STR file, for example
  ``file1.str`` -> ``file1.dtm``.
- Supports ASCII STR files and the validated big-endian binary STR variant.
- Supports ASCII DTM files and the validated big-endian binary DTM variant.

Default behavior:
- GeoPackage output is enabled.
- Excel output is disabled. Add ``--excel`` if tabular XLSX files are needed.
- DTM conversion is automatic when a same-basename DTM exists next to the STR.

Common command-line examples:
    python dtm_str_converter_v3.py --str "path/to/file.str"
    python dtm_str_converter_v3.py --str "path/to/file.str" --excel
    python dtm_str_converter_v3.py --str "path/to/file.str" --dtm "path/to/file.dtm"
    python dtm_str_converter_v3.py --str "path/to/file.str" --no-dtm
    python dtm_str_converter_v3.py --str "path/to/file.str" --output-dir "path/to/outputs"

For frequent local use, edit the DEFAULT_* constants below. Command-line
arguments override those defaults.
"""

from __future__ import annotations

import argparse
import math
import struct
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point, Polygon

SCRIPT_DIR: Path = Path(__file__).resolve().parent

# Default settings. CLI arguments can override these values.
DEFAULT_BASE_DIR: Path = SCRIPT_DIR
DEFAULT_DTM_FILE: Path | None = None
DEFAULT_STR_FILE: Path = Path("bhsite2403.str")
DEFAULT_OUTPUT_DIR: Path = Path(".")
DEFAULT_CRS: str = "EPSG:28351"
DEFAULT_EXPORT_GEOPACKAGE: bool = True
DEFAULT_EXPORT_EXCEL: bool = False
DEFAULT_PROGRESS_INTERVAL: int = 10_000
DEFAULT_VERBOSE: bool = True
DEFAULT_PAUSE_ON_COMPLETE: bool = False
DEFAULT_AUTO_DTM: bool = True
DEFAULT_ENCODING: str = "ascii"
ASCII_CONTROL_BYTES: set[int] = {9, 10, 13}
BINARY_STR_PREFIX: bytes = b"\0" * 5
BINARY_STR_END_MARKER: bytes = b"END\0"
BINARY_STR_FIXED_RECORD_SIZE: int = 28
BINARY_STR_MIN_RECORD_SIZE: int = BINARY_STR_FIXED_RECORD_SIZE + 1
BINARY_DTM_PREFIX: bytes = b"\0" * 33 + BINARY_STR_END_MARKER
BINARY_DTM_FINAL_MARKER: bytes = b"\xff" * 8
BINARY_DTM_BLOCK_HEADER: struct.Struct = struct.Struct(">iiiBi")
BINARY_DTM_SUBMESH_HEADER: struct.Struct = struct.Struct(">iBi")
BINARY_DTM_TRIANGLE_FIELDS: struct.Struct = struct.Struct(">8i")
BINARY_DTM_EMBEDDED_POINT_RECORD: struct.Struct = struct.Struct(">iBddd")
BINARY_DTM_REQUIRED_METADATA_KEYS: set[str] = {"neighbours", "validated", "algorithm"}
BINARY_DTM_OPTIONAL_METADATA_KEYS: set[str] = {"closed", "direction"}

DTM_COLUMNS: list[str] = [
    "triangle_number",
    "vertex1",
    "vertex2",
    "vertex3",
    "neighbour1",
    "neighbour2",
    "neighbour3",
]

GEOPACKAGE_OUTPUT_SUFFIXES: dict[str, str] = {
    "str_linestrings": "str_linestrings.gpkg",
    "str_points": "str_points.gpkg",
    "dtm_triangulation": "dtm_triangulation.gpkg",
}

EXCEL_OUTPUT_SUFFIXES: dict[str, str] = {
    "str_linestrings": "str_linestrings.xlsx",
    "str_points": "str_points.xlsx",
    "dtm_df": "dtm_df.xlsx",
}


@dataclass(frozen=True)
class ConverterConfig:
    base_dir: Path = DEFAULT_BASE_DIR
    dtm_file_path: Path | None = DEFAULT_DTM_FILE
    str_file_path: Path = DEFAULT_STR_FILE
    output_dir: Path = DEFAULT_OUTPUT_DIR
    crs: str = DEFAULT_CRS
    export_geopackage: bool = DEFAULT_EXPORT_GEOPACKAGE
    export_excel: bool = DEFAULT_EXPORT_EXCEL
    progress_interval: int = DEFAULT_PROGRESS_INTERVAL
    verbose: bool = DEFAULT_VERBOSE
    pause_on_complete: bool = DEFAULT_PAUSE_ON_COMPLETE
    auto_dtm: bool = DEFAULT_AUTO_DTM


class ConverterError(Exception):
    """Base exception for expected conversion failures."""


class ConverterInputError(ConverterError):
    """Raised when a configured input cannot be read as supported text."""


class ConverterParseError(ConverterError):
    """Raised when a supported input file cannot be parsed."""


@dataclass(frozen=True)
class BinaryStrRecord:
    """One point or segment-break record from a binary Surpac STR file."""

    string_number: int
    y: float
    x: float
    z: float
    description: str


@dataclass(frozen=True)
class BinaryDtmTriangle:
    """One triangle and its topology from a binary Surpac DTM block."""

    string_number: int
    mesh_number: int
    triangle_number: int
    vertex1: int
    vertex2: int
    vertex3: int
    neighbour1: int
    neighbour2: int
    neighbour3: int


def resolve_path(path: Path, base_dir: Path) -> Path:
    """Resolve relative paths against the configured base directory."""
    return path if path.is_absolute() else base_dir / path


def resolve_optional_path(path: Path | None, base_dir: Path) -> Path | None:
    return None if path is None else resolve_path(path, base_dir)


def resolve_crs_definition(crs: str, base_dir: Path) -> str:
    """Return a literal CRS definition or load one from an explicitly named PRJ file."""
    if "\n" in crs or "\r" in crs or not crs.lower().endswith(".prj"):
        return crs

    crs_path: Path = resolve_path(Path(crs), base_dir)
    try:
        return crs_path.read_text(encoding=DEFAULT_ENCODING)
    except FileNotFoundError as exc:
        raise ConverterInputError(f"CRS PRJ file not found: {crs_path}") from exc
    except (OSError, UnicodeDecodeError) as exc:
        raise ConverterInputError(f"Could not read CRS PRJ file: {crs_path}") from exc


def find_matching_dtm_file(str_file_path: Path) -> Path | None:
    exact_candidate: Path = str_file_path.with_suffix(".dtm")
    if exact_candidate.exists():
        return exact_candidate

    uppercase_candidate: Path = str_file_path.with_suffix(".DTM")
    if uppercase_candidate.exists():
        return uppercase_candidate

    for sibling_path in str_file_path.parent.glob(f"{str_file_path.stem}.*"):
        if sibling_path.suffix.lower() == ".dtm":
            return sibling_path

    return None


def get_dtm_file_path(
    config: ConverterConfig,
    str_file_path: Path,
    base_dir: Path,
) -> Path | None:
    configured_dtm_file_path: Path | None = resolve_optional_path(
        config.dtm_file_path,
        base_dir,
    )
    if configured_dtm_file_path is not None:
        return configured_dtm_file_path

    if not config.auto_dtm:
        return None

    return find_matching_dtm_file(str_file_path)


def build_output_name(source_stem: str, output_suffix: str) -> str:
    return f"{source_stem}_{output_suffix}"


def get_output_source_stem(
    name: str,
    str_file_path: Path,
    dtm_file_path: Path | None,
) -> str:
    if name.startswith("dtm_") and dtm_file_path is not None:
        return dtm_file_path.stem

    return str_file_path.stem


def read_input_bytes(file_path: Path, file_description: str) -> bytes:
    try:
        raw_data: bytes = file_path.read_bytes()
    except FileNotFoundError as exc:
        raise ConverterInputError(f"{file_description} file not found: {file_path}") from exc
    except OSError as exc:
        raise ConverterInputError(f"Could not read {file_description} file: {file_path}") from exc

    if not raw_data:
        raise ConverterParseError(f"{file_description} file is empty: {file_path}")

    return raw_data


def decode_ascii_lines(raw_data: bytes, file_path: Path, file_description: str) -> list[str]:
    """Validate and decode an already-read ASCII Surpac input."""

    unsupported_control_bytes: list[int] = [byte for byte in raw_data if byte < 32 and byte not in ASCII_CONTROL_BYTES]
    if unsupported_control_bytes:
        raise ConverterInputError(f"{file_description} file appears to be binary, not ASCII text: {file_path}")

    try:
        text: str = raw_data.decode(DEFAULT_ENCODING)
    except UnicodeDecodeError as exc:
        raise ConverterInputError(f"{file_description} file is not supported ASCII text: {file_path}") from exc

    return text.splitlines()


def read_ascii_lines(file_path: Path, file_description: str) -> list[str]:
    return decode_ascii_lines(read_input_bytes(file_path, file_description), file_path, file_description)


def read_ascii_dtm_data(raw_data: bytes, dtm_file_path: Path) -> pd.DataFrame:
    """Parse an ASCII Surpac DTM payload."""
    lines: list[str] = decode_ascii_lines(raw_data, dtm_file_path, "DTM")

    try:
        start_index = next(i for i, line in enumerate(lines) if "TRISOLATION" in line.upper())
    except StopIteration as exc:
        raise ConverterParseError(f"No TRISOLATION section found in DTM file: {dtm_file_path}") from exc

    data: list[list[str]] = []
    for line in lines[start_index + 1 :]:
        split_line: list[str] = [part.rstrip(",") for part in line.strip().split()]
        if len(split_line) >= len(DTM_COLUMNS):
            data.append(split_line[: len(DTM_COLUMNS)])

    if not data:
        raise ConverterParseError(f"No triangle rows found in DTM file: {dtm_file_path}")

    try:
        return pd.DataFrame(data, columns=DTM_COLUMNS).astype(int)
    except (TypeError, ValueError) as exc:
        raise ConverterParseError(f"Could not parse triangle rows in DTM file: {dtm_file_path}") from exc


def parse_binary_dtm_metadata(raw_data: bytes, start: int, end: int, dtm_file_path: Path) -> dict[str, str]:
    """Decode and validate one observed binary DTM block metadata string."""
    try:
        metadata_text: str = raw_data[start:end].decode(DEFAULT_ENCODING)
    except UnicodeDecodeError as exc:
        invalid_offset: int = start + exc.start
        raise ConverterParseError(
            f"Binary DTM metadata is not ASCII at byte offset {invalid_offset}: {dtm_file_path}"
        ) from exc

    metadata: dict[str, str] = {}
    for item in metadata_text.split(","):
        key, separator, value = item.partition("=")
        if not separator or not key or not value:
            raise ConverterParseError(f"Malformed binary DTM metadata at byte offset {start}: {dtm_file_path}")
        metadata[key] = value

    metadata_keys: set[str] = set(metadata)
    if (
        not BINARY_DTM_REQUIRED_METADATA_KEYS.issubset(metadata_keys)
        or not metadata_keys.issubset(BINARY_DTM_REQUIRED_METADATA_KEYS | BINARY_DTM_OPTIONAL_METADATA_KEYS)
        or metadata["neighbours"] != "yes"
    ):
        raise ConverterParseError(f"Unsupported binary DTM metadata at byte offset {start}: {dtm_file_path}")
    return metadata


def parse_binary_dtm_prefix(raw_data: bytes, header_end: int, dtm_file_path: Path) -> int:
    """Validate an observed direct or embedded-coordinate prefix and return the first block offset."""
    direct_prefix_end: int = header_end + len(BINARY_DTM_PREFIX)
    if raw_data[header_end:direct_prefix_end] == BINARY_DTM_PREFIX:
        return direct_prefix_end

    end_marker_position: int = raw_data.find(BINARY_STR_END_MARKER, header_end)
    preamble: bytes = raw_data[header_end:end_marker_position] if end_marker_position >= 0 else b""
    embedded_start_size: int = 4
    embedded_end_size: int = 58
    embedded_payload_size: int = len(preamble) - embedded_start_size - embedded_end_size
    if (
        end_marker_position < 0
        or not preamble.startswith(b"\0" * embedded_start_size)
        or not preamble.endswith(b"\0" * embedded_end_size)
        or embedded_payload_size <= 0
        or embedded_payload_size % BINARY_DTM_EMBEDDED_POINT_RECORD.size != 0
    ):
        raise ConverterParseError(f"Unexpected binary DTM prefix at byte offset {header_end}: {dtm_file_path}")

    position: int = header_end + embedded_start_size
    embedded_end: int = end_marker_position - embedded_end_size
    while position < embedded_end:
        reserved, record_type, x, y, z = BINARY_DTM_EMBEDDED_POINT_RECORD.unpack_from(raw_data, position)
        if reserved != 0 or record_type != 1 or not all(math.isfinite(value) for value in (x, y, z)):
            raise ConverterParseError(f"Malformed binary DTM embedded point at byte offset {position}: {dtm_file_path}")
        position += BINARY_DTM_EMBEDDED_POINT_RECORD.size

    return end_marker_position + len(BINARY_STR_END_MARKER)


def validate_binary_dtm_topology(
    block_records: list[tuple[int, BinaryDtmTriangle]],
    dtm_file_path: Path,
) -> None:
    """Validate neighbour ranges, shared edges and reciprocal references for one block."""
    record_count: int = len(block_records)
    for record_offset, record in block_records:
        vertices: tuple[int, int, int] = (record.vertex1, record.vertex2, record.vertex3)
        edges: tuple[frozenset[int], frozenset[int], frozenset[int]] = (
            frozenset((vertices[0], vertices[1])),
            frozenset((vertices[1], vertices[2])),
            frozenset((vertices[2], vertices[0])),
        )
        neighbours: tuple[int, int, int] = (record.neighbour1, record.neighbour2, record.neighbour3)

        for edge, neighbour_number in zip(edges, neighbours):
            if neighbour_number in {-1, 0}:
                continue
            if not 1 <= neighbour_number <= record_count:
                raise ConverterParseError(
                    f"Binary DTM neighbour is out of range at byte offset {record_offset}: {dtm_file_path}"
                )

            neighbour: BinaryDtmTriangle = block_records[neighbour_number - 1][1]
            neighbour_vertices: frozenset[int] = frozenset((neighbour.vertex1, neighbour.vertex2, neighbour.vertex3))
            neighbour_neighbours: tuple[int, int, int] = (
                neighbour.neighbour1,
                neighbour.neighbour2,
                neighbour.neighbour3,
            )
            if not edge.issubset(neighbour_vertices) or record.triangle_number not in neighbour_neighbours:
                raise ConverterParseError(
                    f"Inconsistent binary DTM neighbour topology at byte offset {record_offset}: {dtm_file_path}"
                )


def parse_binary_dtm_records(raw_data: bytes, dtm_file_path: Path) -> list[BinaryDtmTriangle]:
    """Parse the validated block-based, big-endian binary DTM variant."""
    try:
        first_line_end: int = raw_data.index(b"\n") + 1
    except ValueError as exc:
        raise ConverterParseError(f"Binary DTM expected an ASCII header at byte offset 0: {dtm_file_path}") from exc

    try:
        raw_data[:first_line_end].rstrip(b"\r\n").decode(DEFAULT_ENCODING)
    except UnicodeDecodeError as exc:
        raise ConverterParseError(
            f"Binary DTM header is not ASCII at byte offset {exc.start}: {dtm_file_path}"
        ) from exc

    position: int = parse_binary_dtm_prefix(raw_data, first_line_end, dtm_file_path)

    if not raw_data.endswith(BINARY_DTM_FINAL_MARKER):
        raise ConverterParseError(
            f"Binary DTM is missing the final marker at byte offset {len(raw_data)}: {dtm_file_path}"
        )
    payload_end: int = len(raw_data) - len(BINARY_DTM_FINAL_MARKER)

    records: list[BinaryDtmTriangle] = []
    current_string_number: int | None = None
    previous_mesh_number: int = 0
    while position < payload_end:
        block_start: int = position
        if payload_end - position < BINARY_DTM_SUBMESH_HEADER.size + 1:
            raise ConverterParseError(f"Truncated binary DTM block at byte offset {block_start}: {dtm_file_path}")

        header_marker: int = struct.unpack_from(">i", raw_data, position)[0]
        if header_marker == 1:
            if payload_end - position < BINARY_DTM_BLOCK_HEADER.size + 1:
                raise ConverterParseError(f"Truncated binary DTM block at byte offset {block_start}: {dtm_file_path}")
            block_flag, string_number, reserved, record_type, mesh_number = BINARY_DTM_BLOCK_HEADER.unpack_from(
                raw_data, position
            )
            if (block_flag, reserved, record_type) != (1, 0, 2) or string_number <= 0 or mesh_number <= 0:
                raise ConverterParseError(
                    f"Unsupported binary DTM block header at byte offset {block_start}: {dtm_file_path}"
                )
            current_string_number = string_number
            previous_mesh_number = mesh_number
            position += BINARY_DTM_BLOCK_HEADER.size
        elif header_marker == 0 and current_string_number is not None:
            reserved, record_type, mesh_number = BINARY_DTM_SUBMESH_HEADER.unpack_from(raw_data, position)
            if reserved != 0 or record_type != 2 or mesh_number <= previous_mesh_number:
                raise ConverterParseError(
                    f"Unsupported binary DTM submesh header at byte offset {block_start}: {dtm_file_path}"
                )
            string_number = current_string_number
            previous_mesh_number = mesh_number
            position += BINARY_DTM_SUBMESH_HEADER.size
        else:
            raise ConverterParseError(
                f"Unsupported binary DTM block header at byte offset {block_start}: {dtm_file_path}"
            )

        metadata_start: int = position
        metadata_end: int = raw_data.find(b"\0", metadata_start, payload_end)
        if metadata_end < 0:
            raise ConverterParseError(
                f"Unterminated binary DTM metadata at byte offset {metadata_start}: {dtm_file_path}"
            )
        parse_binary_dtm_metadata(raw_data, metadata_start, metadata_end, dtm_file_path)
        position = metadata_end + 1

        block_records: list[tuple[int, BinaryDtmTriangle]] = []
        expected_triangle_number: int = 1
        while position < payload_end and raw_data[position : position + 4] == b"\0\0\0\3":
            record_offset: int = position
            if payload_end - position < BINARY_DTM_TRIANGLE_FIELDS.size:
                raise ConverterParseError(
                    f"Truncated binary DTM triangle at byte offset {record_offset}: {dtm_file_path}"
                )
            (
                vertex_count,
                triangle_number,
                vertex1,
                vertex2,
                vertex3,
                neighbour1,
                neighbour2,
                neighbour3,
            ) = BINARY_DTM_TRIANGLE_FIELDS.unpack_from(raw_data, position)
            if vertex_count != 3 or triangle_number != expected_triangle_number:
                raise ConverterParseError(
                    f"Malformed binary DTM triangle at byte offset {record_offset}: {dtm_file_path}"
                )
            if min(vertex1, vertex2, vertex3) <= 0 or len({vertex1, vertex2, vertex3}) != 3:
                raise ConverterParseError(
                    f"Invalid binary DTM vertices at byte offset {record_offset}: {dtm_file_path}"
                )

            record = BinaryDtmTriangle(
                string_number,
                mesh_number,
                triangle_number,
                vertex1,
                vertex2,
                vertex3,
                neighbour1,
                neighbour2,
                neighbour3,
            )
            block_records.append((record_offset, record))
            fields_end: int = position + BINARY_DTM_TRIANGLE_FIELDS.size
            omitted_terminator_before_submesh: bool = False
            if payload_end - fields_end >= BINARY_DTM_SUBMESH_HEADER.size:
                next_reserved, next_record_type, _ = BINARY_DTM_SUBMESH_HEADER.unpack_from(raw_data, fields_end)
                omitted_terminator_before_submesh = next_reserved == 0 and next_record_type == 2

            if omitted_terminator_before_submesh:
                position = fields_end
            elif fields_end >= payload_end or raw_data[fields_end] != 0:
                raise ConverterParseError(
                    f"Malformed binary DTM triangle at byte offset {record_offset}: {dtm_file_path}"
                )
            else:
                position = fields_end + 1
            expected_triangle_number += 1

        if not block_records:
            raise ConverterParseError(
                f"Binary DTM block contains no triangles at byte offset {block_start}: {dtm_file_path}"
            )
        validate_binary_dtm_topology(block_records, dtm_file_path)
        records.extend(record for _, record in block_records)

    return records


def binary_dtm_records_to_dataframe(records: list[BinaryDtmTriangle]) -> pd.DataFrame:
    """Convert binary DTM records to the established triangle schema plus block string."""
    rows: list[list[int]] = [
        [
            record.string_number,
            record.mesh_number,
            record.triangle_number,
            record.vertex1,
            record.vertex2,
            record.vertex3,
            record.neighbour1,
            record.neighbour2,
            record.neighbour3,
        ]
        for record in records
    ]
    return pd.DataFrame(rows, columns=["string", "mesh", *DTM_COLUMNS])


def read_dtm_file_with_format(dtm_file_path: Path) -> tuple[pd.DataFrame, str]:
    """Read either supported DTM representation and report the detected format."""
    raw_data: bytes = read_input_bytes(dtm_file_path, "DTM")
    has_binary_control_bytes: bool = any(byte < 32 and byte not in ASCII_CONTROL_BYTES for byte in raw_data)
    if has_binary_control_bytes:
        records: list[BinaryDtmTriangle] = parse_binary_dtm_records(raw_data, dtm_file_path)
        return binary_dtm_records_to_dataframe(records), "binary DTM"

    return read_ascii_dtm_data(raw_data, dtm_file_path), "ASCII DTM"


def read_dtm_file(dtm_file_path: Path) -> pd.DataFrame:
    """Read a supported Surpac DTM file into triangle and topology columns."""
    dataframe, _ = read_dtm_file_with_format(dtm_file_path)
    return dataframe


def read_ascii_str_data(raw_data: bytes, str_file_path: Path) -> pd.DataFrame:
    """Parse an ASCII Surpac STR payload using the established row semantics."""
    data: list[list[Any]] = []
    data_line_numbers: list[int] = []
    current_group: int = 0
    point_counter: int = -1
    max_description_columns: int = 0

    lines: list[str] = decode_ascii_lines(raw_data, str_file_path, "STR")
    if len(lines) < 2:
        raise ConverterParseError(f"STR file does not contain point rows: {str_file_path}")

    for line_number, line in enumerate(lines[1:], start=2):
        parts: list[str] = [part.strip() for part in line.strip().split(",")]
        point_counter += 1

        if not parts or parts[0] == "0":
            current_group += 1
            continue

        if len(parts) < 4:
            continue

        data.append([point_counter, current_group, *parts])
        data_line_numbers.append(line_number)
        max_description_columns = max(max_description_columns, len(parts) - 4)

    if not data:
        raise ConverterParseError(f"No valid point rows found in STR file: {str_file_path}")

    column_names: list[str] = ["point_number", "group", "string", "y", "x", "z"] + [
        f"d{i}" for i in range(1, max_description_columns + 1)
    ]

    df_str: pd.DataFrame = pd.DataFrame(data, columns=column_names)
    df_str[["string", "x", "y", "z"]] = df_str[["string", "x", "y", "z"]].apply(
        pd.to_numeric,
        errors="coerce",
    )
    invalid_numeric_rows: pd.Series = df_str[["string", "x", "y", "z"]].isna().any(axis=1)
    if invalid_numeric_rows.any():
        invalid_lines: list[int] = [data_line_numbers[index] for index in df_str.index[invalid_numeric_rows].tolist()]
        raise ConverterParseError(f"Could not parse numeric STR values on line(s) {invalid_lines}: {str_file_path}")

    return df_str[df_str["string"] != 0]


def parse_binary_str_records(raw_data: bytes, str_file_path: Path) -> tuple[list[str], list[BinaryStrRecord]]:
    """Parse the validated big-endian binary STR variant without guessing alternatives."""
    line_ends: list[int] = []
    position: int = 0
    for _ in range(2):
        try:
            position = raw_data.index(b"\n", position) + 1
        except ValueError as exc:
            raise ConverterParseError(
                f"Binary STR expected two ASCII header lines at byte offset {position}: {str_file_path}"
            ) from exc
        line_ends.append(position)

    header_ranges: tuple[tuple[int, int], tuple[int, int]] = (
        (0, line_ends[0]),
        (line_ends[0], line_ends[1]),
    )
    headers: list[str] = []
    for header_start, header_end in header_ranges:
        try:
            headers.append(raw_data[header_start:header_end].rstrip(b"\r\n").decode(DEFAULT_ENCODING))
        except UnicodeDecodeError as exc:
            invalid_offset: int = header_start + exc.start
            raise ConverterParseError(
                f"Binary STR header is not ASCII at byte offset {invalid_offset}: {str_file_path}"
            ) from exc

    if raw_data[position : position + len(BINARY_STR_PREFIX)] != BINARY_STR_PREFIX:
        raise ConverterParseError(f"Unexpected binary STR prefix at byte offset {position}: {str_file_path}")
    position += len(BINARY_STR_PREFIX)

    if not raw_data.endswith(BINARY_STR_END_MARKER):
        raise ConverterParseError(
            f"Binary STR is missing the final END marker at byte offset {len(raw_data)}: {str_file_path}"
        )
    records_end: int = len(raw_data) - len(BINARY_STR_END_MARKER)

    records: list[BinaryStrRecord] = []
    while position < records_end:
        record_start: int = position
        remaining_bytes: int = records_end - position
        if remaining_bytes == BINARY_STR_FIXED_RECORD_SIZE:
            string_number = struct.unpack_from(">i", raw_data, position)[0]
            y, x, z = struct.unpack_from(">ddd", raw_data, position + 4)
            if string_number == 0 and y == 0.0 and x == 0.0 and z == 0.0:
                # Observed files omit the description NUL on their final break record.
                records.append(BinaryStrRecord(0, 0.0, 0.0, 0.0, ""))
                position = records_end
                continue
        if records_end - position < BINARY_STR_MIN_RECORD_SIZE:
            raise ConverterParseError(f"Truncated binary STR record at byte offset {record_start}: {str_file_path}")

        string_number: int = struct.unpack_from(">i", raw_data, position)[0]
        position += 4
        y, x, z = struct.unpack_from(">ddd", raw_data, position)
        position += 24

        description_start: int = position
        description_end: int = raw_data.find(b"\0", position, records_end)
        if description_end < 0:
            raise ConverterParseError(
                f"Unterminated binary STR description at byte offset {description_start}: {str_file_path}"
            )
        try:
            description: str = raw_data[description_start:description_end].decode(DEFAULT_ENCODING)
        except UnicodeDecodeError as exc:
            invalid_offset = description_start + exc.start
            raise ConverterParseError(
                f"Binary STR description is not ASCII at byte offset {invalid_offset}: {str_file_path}"
            ) from exc
        position = description_end + 1

        if string_number < 0:
            raise ConverterParseError(
                f"Negative binary STR string number at byte offset {record_start}: {str_file_path}"
            )
        if not all(math.isfinite(value) for value in (y, x, z)):
            raise ConverterParseError(
                f"Non-finite binary STR coordinate at byte offset {record_start}: {str_file_path}"
            )
        if string_number == 0 and (y != 0.0 or x != 0.0 or z != 0.0 or description):
            raise ConverterParseError(
                f"Malformed binary STR segment break at byte offset {record_start}: {str_file_path}"
            )

        records.append(BinaryStrRecord(string_number, y, x, z, description))

    return headers, records


def binary_str_records_to_dataframe(
    headers: list[str],
    records: list[BinaryStrRecord],
    str_file_path: Path,
) -> pd.DataFrame:
    """Convert binary records to the same schema and numbering used by ASCII STR."""
    data: list[list[Any]] = []
    second_header_parts: list[str] = [part.strip() for part in headers[1].strip().split(",")]
    current_group: int = 1 if not second_header_parts or second_header_parts[0] == "0" else 0
    max_description_columns: int = 0

    # ASCII header record 2 consumes point number zero before the first point.
    for point_number, record in enumerate(records, start=1):
        if record.string_number == 0:
            current_group += 1
            continue

        descriptions: list[str] = record.description.split(",") if record.description else []
        data.append(
            [
                point_number,
                current_group,
                record.string_number,
                record.y,
                record.x,
                record.z,
                *descriptions,
            ]
        )
        max_description_columns = max(max_description_columns, len(descriptions))

    if not data:
        raise ConverterParseError(f"No valid point rows found in STR file: {str_file_path}")

    column_names: list[str] = ["point_number", "group", "string", "y", "x", "z"] + [
        f"d{i}" for i in range(1, max_description_columns + 1)
    ]
    return pd.DataFrame(data, columns=column_names)


def read_str_file_with_format(str_file_path: Path) -> tuple[pd.DataFrame, str]:
    """Read either supported STR representation and report the detected format."""
    raw_data: bytes = read_input_bytes(str_file_path, "STR")
    has_binary_control_bytes: bool = any(byte < 32 and byte not in ASCII_CONTROL_BYTES for byte in raw_data)
    if has_binary_control_bytes:
        headers, records = parse_binary_str_records(raw_data, str_file_path)
        return binary_str_records_to_dataframe(headers, records, str_file_path), "binary STR"

    return read_ascii_str_data(raw_data, str_file_path), "ASCII STR"


def read_str_file(str_file_path: Path) -> pd.DataFrame:
    """Read a supported Surpac STR file into generated point and group IDs."""
    dataframe, _ = read_str_file_with_format(str_file_path)
    return dataframe


def create_points_gdf(df_str: pd.DataFrame, crs: str) -> gpd.GeoDataFrame:
    geometry: list[Point] = [Point(x, y, z) for x, y, z in zip(df_str["x"], df_str["y"], df_str["z"])]
    return gpd.GeoDataFrame(df_str, geometry=geometry, crs=crs)


def generate_linestrings(gdf_points: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    rows: list[dict[str, Any]] = []

    for group, values in gdf_points.groupby("group"):
        if len(values) > 1:
            rows.append(
                {
                    "geometry": LineString(values.geometry.tolist()),
                    "group": group,
                }
            )

    return gpd.GeoDataFrame(rows, columns=["geometry", "group"], crs=gdf_points.crs)


def create_polygons(
    df_dtm: pd.DataFrame,
    gdf_points: gpd.GeoDataFrame,
    progress_interval: int = DEFAULT_PROGRESS_INTERVAL,
) -> gpd.GeoDataFrame:
    point_geom_map: dict[int, Point] = {
        int(point_number): cast(Point, geometry)
        for point_number, geometry in zip(gdf_points["point_number"], gdf_points.geometry)
    }

    polygons: list[Polygon] = []
    dtm_rows: list[dict[str, Any]] = []

    for progress_counter, (_, row) in enumerate(df_dtm.iterrows(), start=1):
        vertex_ids: list[int] = [
            int(row["vertex1"]),
            int(row["vertex2"]),
            int(row["vertex3"]),
        ]
        vertices: list[Point | None] = [point_geom_map.get(vertex_id) for vertex_id in vertex_ids]

        if not all(vertex is not None and vertex.has_z for vertex in vertices):
            print("Missing 3D point for triangle " f"{row['triangle_number']} with vertices {vertex_ids}")
            continue

        valid_vertices: list[Point] = [vertex for vertex in vertices if vertex is not None]
        polygon = Polygon([(vertex.x, vertex.y, vertex.z) for vertex in valid_vertices])
        polygons.append(polygon)
        dtm_rows.append({str(column): value for column, value in row.to_dict().items()})

        if progress_interval > 0 and progress_counter % progress_interval == 0:
            print(f"Processed {progress_counter} triangles")

    return gpd.GeoDataFrame(dtm_rows, geometry=polygons, crs=gdf_points.crs)


def export_to_geopackage(gdf: gpd.GeoDataFrame, output_path: Path) -> None:
    print(f"Exporting {output_path}")
    # Fiona preserves WKT2 derived CRSs that cannot be represented as WKT1_GDAL.
    gdf.to_file(output_path, driver="GPKG", engine="fiona")  # pyright: ignore[reportUnknownMemberType]


def export_to_excel(df: pd.DataFrame, output_path: Path) -> None:
    print(f"Exporting {output_path}")
    df.to_excel(output_path, index=False)  # pyright: ignore[reportUnknownMemberType]


def print_preview(title: str, df: pd.DataFrame) -> None:
    print(f"{title}:")
    print(df.head())
    print("")


def run_conversion(config: ConverterConfig) -> None:
    base_dir: Path = config.base_dir.resolve()
    str_file_path: Path = resolve_path(config.str_file_path, base_dir)
    dtm_file_path: Path | None = get_dtm_file_path(config, str_file_path, base_dir)
    output_dir: Path = resolve_path(config.output_dir, base_dir)
    crs_definition: str = resolve_crs_definition(config.crs, base_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if config.verbose:
        print(f"Base directory: {base_dir}")
        print(f"DTM input: {dtm_file_path if dtm_file_path is not None else 'none'}")
        print(f"STR input: {str_file_path}")
        print(f"Output directory: {output_dir}")
        print("")

    df_str, str_format = read_str_file_with_format(str_file_path)
    if config.verbose:
        print(f"Detected STR format: {str_format}")
        print_preview("Processed STR DataFrame", df_str)
        print("STR groups:")
        print(df_str["group"].unique())
        print("")

    gdf_points: gpd.GeoDataFrame = create_points_gdf(df_str, crs_definition)
    if config.verbose:
        print_preview("Points GeoDataFrame", gdf_points)

    gdf_linestrings: gpd.GeoDataFrame = generate_linestrings(gdf_points)
    if config.verbose:
        print_preview("LineStrings GeoDataFrame", gdf_linestrings)

    geodataframes: dict[str, gpd.GeoDataFrame] = {
        "str_linestrings": gdf_linestrings,
        "str_points": gdf_points,
    }
    dataframes: dict[str, pd.DataFrame] = {
        "str_linestrings": gdf_linestrings.drop(columns="geometry", errors="ignore"),
        "str_points": gdf_points.drop(columns="geometry", errors="ignore"),
    }

    if dtm_file_path is not None:
        df_dtm, dtm_format = read_dtm_file_with_format(dtm_file_path)
        if config.verbose:
            print(f"Detected DTM format: {dtm_format}")
            print_preview("DTM DataFrame", df_dtm)

        gdf_triangles: gpd.GeoDataFrame = create_polygons(
            df_dtm,
            gdf_points,
            progress_interval=config.progress_interval,
        )
        if config.verbose:
            print_preview("Triangulation GeoDataFrame", gdf_triangles)

        geodataframes["dtm_triangulation"] = gdf_triangles
        dataframes["dtm_df"] = df_dtm

    if config.export_geopackage:
        for name, output_suffix in GEOPACKAGE_OUTPUT_SUFFIXES.items():
            if name in geodataframes:
                source_stem: str = get_output_source_stem(
                    name,
                    str_file_path,
                    dtm_file_path,
                )
                output_name: str = build_output_name(source_stem, output_suffix)
                export_to_geopackage(geodataframes[name], output_dir / output_name)

    if config.export_excel:
        for name, output_suffix in EXCEL_OUTPUT_SUFFIXES.items():
            if name in dataframes:
                source_stem: str = get_output_source_stem(
                    name,
                    str_file_path,
                    dtm_file_path,
                )
                output_name: str = build_output_name(source_stem, output_suffix)
                export_to_excel(dataframes[name], output_dir / output_name)

    print("Script completed. The data has been processed and saved.")

    if config.pause_on_complete:
        input("Press Enter to exit...")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert Surpac DTM and STR files to GeoPackage and Excel outputs.",
    )
    parser.add_argument(
        "--base-dir",
        type=Path,
        default=DEFAULT_BASE_DIR,
        help="Base directory used to resolve relative input and output paths.",
    )
    parser.add_argument(
        "--dtm",
        dest="dtm_file_path",
        type=Path,
        default=DEFAULT_DTM_FILE,
        help="Optional input DTM file path. Requires a matching STR file.",
    )
    parser.add_argument(
        "--no-dtm",
        dest="disable_dtm",
        action="store_true",
        help="Disable DTM processing and convert only the STR file.",
    )
    parser.add_argument(
        "--auto-dtm",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_AUTO_DTM,
        help="Automatically convert a same-basename DTM next to the STR file.",
    )
    parser.add_argument(
        "--str",
        dest="str_file_path",
        type=Path,
        default=DEFAULT_STR_FILE,
        help="Input STR file path.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory where output files will be written.",
    )
    parser.add_argument(
        "--crs",
        default=DEFAULT_CRS,
        help="Output CRS as an authority code, WKT string, or absolute/relative .prj file path.",
    )
    parser.add_argument(
        "--gpkg",
        dest="export_geopackage",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_EXPORT_GEOPACKAGE,
        help="Enable or disable GeoPackage exports.",
    )
    parser.add_argument(
        "--excel",
        dest="export_excel",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_EXPORT_EXCEL,
        help="Enable or disable Excel exports. Disabled by default.",
    )
    parser.add_argument(
        "--progress-interval",
        type=int,
        default=DEFAULT_PROGRESS_INTERVAL,
        help="Triangle progress print interval. Use 0 to disable progress messages.",
    )
    parser.add_argument(
        "--verbose",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_VERBOSE,
        help="Enable or disable DataFrame preview output.",
    )
    parser.add_argument(
        "--pause",
        action=argparse.BooleanOptionalAction,
        default=DEFAULT_PAUSE_ON_COMPLETE,
        help="Pause for keyboard input before exiting.",
    )
    return parser.parse_args(argv)


def config_from_args(args: argparse.Namespace) -> ConverterConfig:
    return ConverterConfig(
        base_dir=args.base_dir,
        dtm_file_path=None if args.disable_dtm else args.dtm_file_path,
        str_file_path=args.str_file_path,
        output_dir=args.output_dir,
        crs=args.crs,
        export_geopackage=args.export_geopackage,
        export_excel=args.export_excel,
        progress_interval=args.progress_interval,
        verbose=args.verbose,
        pause_on_complete=args.pause,
        auto_dtm=args.auto_dtm and not args.disable_dtm,
    )


def main(argv: Sequence[str] | None = None) -> int:
    config: ConverterConfig = config_from_args(parse_args(argv))
    try:
        run_conversion(config)
    except ConverterError as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
