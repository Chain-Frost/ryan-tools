"""Extract DXF mesh vertices and triangles to Parquet and optional XYZ text.

Run ``python dwg-to-points.py --help`` for the complete CLI. A typical call is
``python dwg-to-points.py model.dxf --xyz-output model.xyz``. Raw vertices and
triangulated faces are written to separate Parquet files, while the optional XYZ
file contains deduplicated coordinates at the requested precision.

Existing outputs are protected unless ``--force`` is supplied. This reads DXF,
not binary DWG; export or convert a DWG before running the tool.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Iterator

import ezdxf
from ezdxf.entities import DXFGraphic
import pyarrow as pa
import pyarrow.parquet as pq

Vec3 = tuple[float, float, float]
Triangle = tuple[Vec3, Vec3, Vec3]

VERTEX_SCHEMA = pa.schema(
    [
        ("entity_id", pa.int64()),
        ("entity_type", pa.string()),
        ("handle", pa.string()),
        ("layer", pa.string()),
        ("vertex_id", pa.int64()),
        ("x", pa.float64()),
        ("y", pa.float64()),
        ("z", pa.float64()),
    ]
)

TRIANGLE_SCHEMA = pa.schema(
    [
        ("entity_id", pa.int64()),
        ("entity_type", pa.string()),
        ("handle", pa.string()),
        ("layer", pa.string()),
        ("triangle_id", pa.int64()),
        ("x0", pa.float64()),
        ("y0", pa.float64()),
        ("z0", pa.float64()),
        ("x1", pa.float64()),
        ("y1", pa.float64()),
        ("z1", pa.float64()),
        ("x2", pa.float64()),
        ("y2", pa.float64()),
        ("z2", pa.float64()),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read a DXF (or a DWG that has been exported to DXF) and collect every " "mesh vertex and triangle face."
        )
    )
    parser.add_argument("source", type=Path, help="DXF file that contains the mesh geometry.")
    parser.add_argument(
        "-o",
        "--xyz-output",
        type=Path,
        help="Optional deduplicated XYZ output file. If omitted, no XYZ text file is written.",
    )
    parser.add_argument(
        "--vertices-parquet",
        type=Path,
        help="Raw vertex Parquet output. Defaults to '<source>.vertices.parquet'.",
    )
    parser.add_argument(
        "--triangles-parquet",
        type=Path,
        help="Triangulated face Parquet output. Defaults to '<source>.triangles.parquet'.",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=6,
        help="Number of decimal places to use for deduplicating optional XYZ coordinates.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100_000,
        help="Rows per Parquet write batch.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output file if it already exists.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    source = args.source.expanduser().resolve()
    if not source.exists():
        raise SystemExit(f"Source '{source}' does not exist.")
    if source.suffix.lower() == ".dwg":
        raise SystemExit("DWG files cannot be read directly. Export the drawing to DXF and retry.")
    if args.precision < 0:
        raise SystemExit("Precision must be zero or positive.")
    if args.batch_size < 1:
        raise SystemExit("Batch size must be at least 1.")

    try:
        doc = ezdxf.readfile(source)
    except (OSError, ezdxf.DXFStructureError) as exc:
        raise SystemExit(f"Unable to read '{source}': {exc}") from exc

    vertices_path = args.vertices_parquet or source.with_suffix(".vertices.parquet")
    triangles_path = args.triangles_parquet or source.with_suffix(".triangles.parquet")
    output_paths = [vertices_path, triangles_path]
    if args.xyz_output:
        output_paths.append(args.xyz_output)
    for output_path in output_paths:
        if output_path.exists() and not args.force:
            raise SystemExit(f"Output '{output_path}' already exists; use '--force' to overwrite.")

    modelspace = doc.modelspace()
    vertex_count = write_vertices_parquet(vertices_path, modelspace, args.batch_size)
    triangle_count = write_triangles_parquet(triangles_path, modelspace, args.batch_size)
    print(f"Wrote {vertex_count} raw vertices to '{vertices_path}'.")
    print(f"Wrote {triangle_count} triangles to '{triangles_path}'.")

    if args.xyz_output:
        unique_points = collect_unique_vertices(modelspace, args.precision)
        write_xyz(args.xyz_output, unique_points.values(), args.precision)
        print(f"Wrote {len(unique_points)} unique vertices to '{args.xyz_output}'.")


def write_vertices_parquet(path: Path, modelspace: Iterable[DXFGraphic], batch_size: int) -> int:
    total = 0
    batch: list[dict[str, object]] = []
    with ParquetBatchWriter(path, VERTEX_SCHEMA) as writer:
        for entity_id, entity in enumerate(modelspace):
            for vertex_id, point in enumerate(iterate_entity_vertices(entity)):
                x, y, z = point
                batch.append(
                    {
                        "entity_id": entity_id,
                        "entity_type": entity.dxftype(),
                        "handle": entity.dxf.handle,
                        "layer": entity.dxf.get("layer", ""),
                        "vertex_id": vertex_id,
                        "x": x,
                        "y": y,
                        "z": z,
                    }
                )
                if len(batch) >= batch_size:
                    writer.write(batch)
                    total += len(batch)
                    batch.clear()
        if batch:
            writer.write(batch)
            total += len(batch)
    return total


def write_triangles_parquet(path: Path, modelspace: Iterable[DXFGraphic], batch_size: int) -> int:
    total = 0
    batch: list[dict[str, object]] = []
    with ParquetBatchWriter(path, TRIANGLE_SCHEMA) as writer:
        for entity_id, entity in enumerate(modelspace):
            for triangle_id, triangle in enumerate(iterate_entity_triangles(entity)):
                (x0, y0, z0), (x1, y1, z1), (x2, y2, z2) = triangle
                batch.append(
                    {
                        "entity_id": entity_id,
                        "entity_type": entity.dxftype(),
                        "handle": entity.dxf.handle,
                        "layer": entity.dxf.get("layer", ""),
                        "triangle_id": triangle_id,
                        "x0": x0,
                        "y0": y0,
                        "z0": z0,
                        "x1": x1,
                        "y1": y1,
                        "z1": z1,
                        "x2": x2,
                        "y2": y2,
                        "z2": z2,
                    }
                )
                if len(batch) >= batch_size:
                    writer.write(batch)
                    total += len(batch)
                    batch.clear()
        if batch:
            writer.write(batch)
            total += len(batch)
    return total


class ParquetBatchWriter:
    def __init__(self, path: Path, schema: pa.Schema) -> None:
        self.path = path
        self.schema = schema
        self.writer: pq.ParquetWriter | None = None

    def __enter__(self) -> "ParquetBatchWriter":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self.writer is not None:
            self.writer.close()

    def write(self, rows: list[dict[str, object]]) -> None:
        table = pa.Table.from_pylist(rows, schema=self.schema)
        if self.writer is None:
            self.writer = pq.ParquetWriter(self.path, self.schema, compression="zstd")
        self.writer.write_table(table)


def collect_unique_vertices(modelspace: Iterable[DXFGraphic], precision: int) -> dict[Vec3, Vec3]:
    unique: dict[Vec3, Vec3] = {}
    for entity in modelspace:
        for point in iterate_entity_vertices(entity):
            normalized = normalize(point, precision)
            if normalized not in unique:
                unique[normalized] = point
    return unique


def iterate_entity_vertices(entity: DXFGraphic) -> Iterator[Vec3]:
    dxftype = entity.dxftype()
    if dxftype == "POINT":
        location = _to_vec3(entity.dxf.location)
        if location:
            yield location
        return
    if dxftype == "LINE":
        for attr in ("start", "end"):
            location = _to_vec3(getattr(entity.dxf, attr, None))
            if location:
                yield location
        return
    if dxftype == "3DFACE":
        for attr in ("vtx0", "vtx1", "vtx2", "vtx3"):
            location = _to_vec3(entity.dxf.get(attr))
            if location:
                yield location
        return

    if dxftype == "MESH":
        for vertex in getattr(entity, "vertices", ()):
            point = _to_vec3(vertex)
            if point:
                yield point
        return

    virtual_entities = getattr(entity, "virtual_entities", None)
    if virtual_entities:
        for sub_entity in virtual_entities():
            yield from iterate_entity_vertices(sub_entity)

    for attr in ("vertices_in_wcs", "points_in_wcs", "vertices"):
        attribute = getattr(entity, attr, None)
        if attribute is None:
            continue
        iterable = attribute() if callable(attribute) else attribute
        if not iterable:
            continue
        for item in iterable:
            point = _to_vec3(item)
            if point:
                yield point
        return


def iterate_entity_triangles(entity: DXFGraphic) -> Iterator[Triangle]:
    dxftype = entity.dxftype()
    if dxftype == "3DFACE":
        points = [
            point for attr in ("vtx0", "vtx1", "vtx2", "vtx3") if (point := _to_vec3(entity.dxf.get(attr))) is not None
        ]
        yield from triangulate_points(points)
        return

    if dxftype == "MESH":
        vertices = [_to_vec3(vertex) for vertex in getattr(entity, "vertices", ())]
        for face in getattr(entity, "faces", ()):
            indices = tuple(int(index) for index in face)
            points = [vertices[index] for index in indices if 0 <= index < len(vertices)]
            yield from triangulate_points([point for point in points if point is not None])
        return

    virtual_entities = getattr(entity, "virtual_entities", None)
    if virtual_entities:
        for sub_entity in virtual_entities():
            yield from iterate_entity_triangles(sub_entity)


def triangulate_points(points: list[Vec3]) -> Iterator[Triangle]:
    if len(points) < 3:
        return
    if len(points) == 4 and points[2] == points[3]:
        points = points[:3]
    origin = points[0]
    for index in range(1, len(points) - 1):
        yield (origin, points[index], points[index + 1])


def normalize(point: Vec3, precision: int) -> Vec3:
    return tuple(round(coord, precision) for coord in point)


def _to_vec3(value) -> Vec3 | None:
    if value is None:
        return None
    if isinstance(value, DXFGraphic):
        return None
    if hasattr(value, "dxf"):
        location = getattr(value.dxf, "location", None)
        if location is not None:
            return _tuple_from_iterable(location)
    if hasattr(value, "__iter__"):
        return _tuple_from_iterable(value)
    if hasattr(value, "x") and hasattr(value, "y"):
        return (float(value.x), float(value.y), float(getattr(value, "z", 0.0)))
    return None


def _tuple_from_iterable(iterable) -> Vec3 | None:
    coords = tuple(float(coord) for coord in iterable)
    if not coords:
        return None
    if len(coords) == 1:
        return (coords[0], 0.0, 0.0)
    if len(coords) == 2:
        return (coords[0], coords[1], 0.0)
    return (coords[0], coords[1], coords[2])


def write_xyz(path: Path, points: Iterable[Vec3], precision: int) -> None:
    format_string = f"{{:.{precision}f}} {{:.{precision}f}} {{:.{precision}f}}"
    with path.open("w", encoding="utf-8") as handle:
        for x, y, z in points:
            handle.write(format_string.format(x, y, z))
            handle.write("\n")


if __name__ == "__main__":
    main()
