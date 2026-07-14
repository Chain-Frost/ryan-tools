"""Extract DXF polyface mesh vertices and faces to Parquet.

This intentionally streams the DXF text file and does not use ezdxf, because
large CAD exports can be multiple gigabytes and expensive to materialize.
"""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path
from typing import Iterable, TextIO

import pyarrow as pa
import pyarrow.parquet as pq

DxfPairs = list[tuple[str, str]]


VERTEX_SCHEMA = pa.schema(
    [
        ("mesh_id", pa.int32()),
        ("mesh_handle", pa.string()),
        ("layer", pa.string()),
        ("vertex_index", pa.int32()),
        ("x", pa.float64()),
        ("y", pa.float64()),
        ("z", pa.float64()),
    ]
)

FACE_SCHEMA = pa.schema(
    [
        ("mesh_id", pa.int32()),
        ("mesh_handle", pa.string()),
        ("layer", pa.string()),
        ("face_index", pa.int32()),
        ("v1", pa.int32()),
        ("v2", pa.int32()),
        ("v3", pa.int32()),
        ("v4", pa.int32()),
    ]
)


def dxf_entities(file_obj: TextIO) -> Iterable[tuple[str, DxfPairs]]:
    """Yield entities from the ENTITIES section as ``(entity_type, pairs)``."""
    section: str | None = None
    entity_type: str | None = None
    pairs: DxfPairs = []

    while True:
        code_line = file_obj.readline()
        if not code_line:
            if entity_type is not None:
                yield entity_type, pairs
            return

        value_line = file_obj.readline()
        if not value_line:
            if entity_type is not None:
                yield entity_type, pairs
            return

        code = code_line.strip()
        value = value_line.strip()

        if code == "0" and value == "SECTION":
            if entity_type is not None:
                yield entity_type, pairs
                entity_type = None
                pairs = []

            section_code = file_obj.readline()
            section_value = file_obj.readline()
            if section_code and section_value and section_code.strip() == "2":
                section = section_value.strip()
            continue

        if code == "0" and value == "ENDSEC":
            if entity_type is not None:
                yield entity_type, pairs
                entity_type = None
                pairs = []
            section = None
            continue

        if section != "ENTITIES":
            continue

        if code == "0":
            if entity_type is not None:
                yield entity_type, pairs
            entity_type = value
            pairs = []
            continue

        if entity_type is not None:
            pairs.append((code, value))


def last_value(pairs: DxfPairs, code: str, default: str = "") -> str:
    """Return the last value for a DXF group code."""
    value = default
    for pair_code, pair_value in pairs:
        if pair_code == code:
            value = pair_value
    return value


def all_values(pairs: DxfPairs, code: str) -> list[str]:
    """Return all values for a DXF group code."""
    return [value for pair_code, value in pairs if pair_code == code]


def int_value(pairs: DxfPairs, code: str, default: int = 0) -> int:
    value = last_value(pairs, code)
    if not value:
        return default
    return int(value)


def float_value(pairs: DxfPairs, code: str) -> float:
    return float(last_value(pairs, code))


class ParquetBatchWriter:
    """Small row-buffer wrapper around ``pyarrow.parquet.ParquetWriter``."""

    def __init__(self, path: Path, schema: pa.Schema, batch_size: int) -> None:
        self.path = path
        self.schema = schema
        self.batch_size = batch_size
        self.rows: dict[str, list[object]] = {field.name: [] for field in schema}
        self.writer: pq.ParquetWriter | None = None
        self.row_count = 0

    def append(self, row: dict[str, object]) -> None:
        for field in self.schema:
            self.rows[field.name].append(row[field.name])
        self.row_count += 1

        if len(next(iter(self.rows.values()))) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.rows or not next(iter(self.rows.values())):
            return

        table = pa.Table.from_pydict(self.rows, schema=self.schema)
        if self.writer is None:
            self.writer = pq.ParquetWriter(
                self.path,
                self.schema,
                compression="zstd",
                use_dictionary=True,
                write_statistics=True,
            )
        self.writer.write_table(table)  # pyright: ignore[reportUnknownMemberType]
        self.rows = {field.name: [] for field in self.schema}

    def close(self) -> None:
        self.flush()
        if self.writer is not None:
            self.writer.close()


def is_polyface_mesh(pairs: DxfPairs) -> bool:
    return "AcDbPolyFaceMesh" in all_values(pairs, "100")


def is_coordinate_vertex(pairs: DxfPairs) -> bool:
    return "AcDbPolyFaceMeshVertex" in all_values(pairs, "100")


def is_face_record(pairs: DxfPairs) -> bool:
    return "AcDbFaceRecord" in all_values(pairs, "100")


def extract_polyface_meshes(input_path: Path, output_dir: Path, prefix: str, batch_size: int) -> dict[str, object]:
    vertices_path = output_dir / f"{prefix}_polyface_vertices.parquet"
    faces_path = output_dir / f"{prefix}_polyface_faces.parquet"
    summary_path = output_dir / f"{prefix}_polyface_summary.json"

    vertex_writer = ParquetBatchWriter(vertices_path, VERTEX_SCHEMA, batch_size)
    face_writer = ParquetBatchWriter(faces_path, FACE_SCHEMA, batch_size)

    mesh_id = 0
    active_mesh: dict[str, object] | None = None
    vertex_index = 0
    face_index = 0
    layer_counts: Counter[str] = Counter()
    entity_counts: Counter[str] = Counter()
    started_at = time.perf_counter()

    with input_path.open("r", encoding="cp1252", errors="replace", buffering=8 * 1024 * 1024) as file_obj:
        for entity_type, pairs in dxf_entities(file_obj):
            entity_counts[entity_type] += 1

            if entity_type == "POLYLINE":
                active_mesh = None
                if not is_polyface_mesh(pairs):
                    continue

                mesh_id += 1
                layer = last_value(pairs, "8", "")
                active_mesh = {
                    "mesh_id": mesh_id,
                    "mesh_handle": last_value(pairs, "5", ""),
                    "layer": layer,
                    "declared_vertices": int_value(pairs, "71"),
                    "declared_faces": int_value(pairs, "72"),
                }
                vertex_index = 0
                face_index = 0
                layer_counts[layer] += 1
                continue

            if active_mesh is None:
                continue

            if entity_type == "SEQEND":
                active_mesh = None
                continue

            if entity_type != "VERTEX":
                continue

            if is_face_record(pairs):
                face_index += 1
                face_writer.append(
                    {
                        "mesh_id": active_mesh["mesh_id"],
                        "mesh_handle": active_mesh["mesh_handle"],
                        "layer": active_mesh["layer"],
                        "face_index": face_index,
                        "v1": int_value(pairs, "71"),
                        "v2": int_value(pairs, "72"),
                        "v3": int_value(pairs, "73"),
                        "v4": int_value(pairs, "74"),
                    }
                )
                continue

            if is_coordinate_vertex(pairs):
                vertex_index += 1
                vertex_writer.append(
                    {
                        "mesh_id": active_mesh["mesh_id"],
                        "mesh_handle": active_mesh["mesh_handle"],
                        "layer": active_mesh["layer"],
                        "vertex_index": vertex_index,
                        "x": float_value(pairs, "10"),
                        "y": float_value(pairs, "20"),
                        "z": float_value(pairs, "30"),
                    }
                )

    vertex_writer.close()
    face_writer.close()

    summary: dict[str, object] = {
        "input": str(input_path),
        "input_bytes": input_path.stat().st_size,
        "vertices": str(vertices_path),
        "faces": str(faces_path),
        "mesh_count": mesh_id,
        "vertex_rows": vertex_writer.row_count,
        "face_rows": face_writer.row_count,
        "polyface_layers": dict(layer_counts.most_common()),
        "entity_counts": dict(entity_counts.most_common()),
        "elapsed_seconds": round(time.perf_counter() - started_at, 1),
    }
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    summary["summary"] = str(summary_path)
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="Input ASCII DXF path")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output folder. Defaults to the input file folder.",
    )
    parser.add_argument(
        "--prefix",
        default=None,
        help="Output filename prefix. Defaults to the input stem.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=250_000,
        help="Rows per Parquet write batch.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_path = args.input.resolve()
    output_dir = (args.output_dir or input_path.parent).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = args.prefix or input_path.stem

    summary = extract_polyface_meshes(
        input_path=input_path,
        output_dir=output_dir,
        prefix=prefix,
        batch_size=args.batch_size,
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
