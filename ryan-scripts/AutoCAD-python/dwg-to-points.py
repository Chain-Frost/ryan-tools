"""Extract vertex coordinates from a DXF so they can be written to an XYZ file."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Iterator

import ezdxf
from ezdxf.entities.dxfentity import DXFGraphic

Vec3 = tuple[float, float, float]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read a DXF (or a DWG that has been exported to DXF) and collect every "
            "vertex so it can be written to XYZ."
        )
    )
    parser.add_argument("source", type=Path, help="DXF file that contains the mesh geometry.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        help="XYZ output file (defaults to the source filename with an .xyz suffix).",
    )
    parser.add_argument(
        "--precision",
        type=int,
        default=6,
        help="Number of decimal places to use for deduplicating coordinates.",
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
        raise SystemExit(
            "DWG files cannot be read directly. Export the drawing to DXF and retry."
        )
    if args.precision < 0:
        raise SystemExit("Precision must be zero or positive.")

    try:
        doc = ezdxf.readfile(source)
    except (OSError, ezdxf.DXFStructureError) as exc:
        raise SystemExit(f"Unable to read '{source}': {exc}") from exc

    unique_points = collect_unique_vertices(doc.modelspace(), args.precision)
    output_path = args.output or source.with_suffix(".xyz")
    if output_path.exists() and not args.force:
        raise SystemExit(
            f"Output '{output_path}' already exists; use '--force' to overwrite."
        )

    write_xyz(output_path, unique_points.values(), args.precision)
    print(f"Wrote {len(unique_points)} unique vertices to '{output_path}'.")


def collect_unique_vertices(
    modelspace: Iterable[DXFGraphic], precision: int
) -> dict[Vec3, Vec3]:
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
