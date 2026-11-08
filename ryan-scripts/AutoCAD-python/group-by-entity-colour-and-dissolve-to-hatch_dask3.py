#!/usr/bin/env python3
"""Dissolve colour-grouped DXF polygons into hatches using Dask workers.

Edit the configuration constants and project paths/filenames in :func:`main`,
then run ``python group-by-entity-colour-and-dissolve-to-hatch_dask3.py``. Large
colour groups are divided into chunks, dissolved by local Dask workers, merged
again, and written as solid hatches to a new DXF.

Tune ``N_WORKERS``, ``MEMORY_LIMIT``, ``CHUNK_SIZE``, and ``TOLERANCE`` for the
machine and drawing. Validate colour assignment and dissolved boundaries in CAD.
"""

# External optional keyword signatures are incomplete; exceptions stay on individual calls.

import os
from collections.abc import Callable, Iterable, Sequence
from datetime import datetime
from typing import Protocol, cast

from dask.distributed import Client
from ezdxf.document import Drawing
from ezdxf.entities.dxfgfx import DXFGraphic
from ezdxf.entities.lwpolyline import LWPolyline
from ezdxf.filemanagement import (
    new,
    readfile,  # pyright: ignore[reportUnknownVariableType] - external optional encoding lacks an annotation
)
from ezdxf.layouts.layout import Modelspace
from shapely.geometry import GeometryCollection, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry
from shapely.ops import unary_union

# ── CONFIG ────────────────────────────────────────────────────────────────
TOLERANCE = 0.05  # buffer tolerance
PROGRESS_INTERVAL = 10000  # print progress every N entities
CHUNK_SIZE = 10_000  # dissolve in blocks of 10k polygons
N_WORKERS = 4  # number of Dask workers
MEMORY_LIMIT = "5GB"  # per-worker memory limit
# ─────────────────────────────────────────────────────────────────────────


class DissolveClient(Protocol):
    """The synchronous Dask calls used by this batch workflow."""

    def scatter(self, data: list[list[tuple[float, ...]]]) -> object: ...
    def submit(self, function: Callable[..., tuple[str, list[Polygon]]], *args: object) -> object: ...
    def gather(self, futures: Sequence[object]) -> list[tuple[str, list[Polygon]]]: ...


def main() -> None:
    # ── paths & filenames ─────────────────────────────────────────────────
    os.chdir(
        r"\\bge-resources.com\Resources\Atrium\SGT\RP20013.001 MCPHEE PROJECT - RH\60 CADD\63 Civil\12D\Output\250702 - MSA Heatmap"
    )
    # input_file = "XR-HEATMAP_SAMPLE.dxf"
    # output_file = "XR-HEATMAP_SAMPLE_corrected_colours.dxf"
    input_file = "XR-HEATMAP.dxf"
    output_file = "XR-HEATMAP_grouped_and_dissolved_hatched_clipped_dask.dxf"
    polygon_export = "export_polygons_corrected_colours.dxf"
    # ─────────────────────────────────────────────────────────────────────

    print(f"[{now()}] Reading '{input_file}'…")
    doc: Drawing = readfile(input_file)
    msp: Modelspace = doc.modelspace()

    # 1) Relayer & set layer colours (store both tc & ic)
    print(f"[{now()}] Re-layering entities by color…")
    colour_map: dict[str, tuple[int | None, int]] = {}  # layer -> (tc, ic)
    idx = 0
    for idx, e in enumerate(msp, start=1):
        tc = e.get_dxf_attrib("true_color", None)
        ic = e.get_dxf_attrib("color", None)
        lname = make_layer_name(tc, ic)
        if not doc.layers.has_entry(lname):
            if tc is not None:
                layer = doc.layers.add(name=lname)  # pyright: ignore[reportUnknownMemberType]
                layer.dxf.true_color = tc
            else:
                doc.layers.add(name=lname, dxfattribs={"color": ic})  # pyright: ignore[reportUnknownMemberType]
        colour_map[lname] = (tc, ic)
        e.dxf.layer = lname
        if idx % PROGRESS_INTERVAL == 0:
            print(f"[{now()}]   • {idx} entities re-layered…")
    print(f"[{now()}]    → {idx} entities → {len(colour_map)} layers.")

    # 2) Extract raw coords per layer
    print(f"[{now()}] Extracting raw polygons per layer…")
    layer_coords: dict[str, list[list[tuple[float, ...]]]] = {}
    for idx, e in enumerate(msp, start=1):
        layer = e.dxf.layer
        for geom in extract_polygons(e):
            if isinstance(geom, Polygon):
                layer_coords.setdefault(layer, []).append(list(geom.exterior.coords))
            elif hasattr(geom, "geoms"):
                for part in cast("MultiPolygon | GeometryCollection", geom).geoms:
                    if isinstance(part, Polygon):
                        layer_coords.setdefault(layer, []).append(list(part.exterior.coords))
        if idx % PROGRESS_INTERVAL == 0:
            print(f"[{now()}]   • Scanned {idx} entities…")

    # 3) Start Dask
    print(f"[{now()}] Starting Dask client…")
    client = cast("DissolveClient", Client(n_workers=N_WORKERS, threads_per_worker=1, memory_limit=MEMORY_LIMIT))

    # 4) Dissolve per layer in CHUNK_SIZE-ring chunks
    dissolved_map: dict[str, list[Polygon]] = {}
    for layer, coords in layer_coords.items():
        total = len(coords)
        print(f"[{now()}] → Layer {layer!r}: {total} rings → chunking…")
        futures: list[object] = []
        for i in range(0, total, CHUNK_SIZE):
            chunk = coords[i : i + CHUNK_SIZE]
            f_chunk = client.scatter(chunk)
            futures.append(client.submit(dissolve_chunk, layer, f_chunk, TOLERANCE))

        chunk_results = client.gather(futures)
        partial = [poly for (_, polys) in chunk_results for poly in polys]

        # snap topology with zero buffer
        if partial:
            merged = cast("BaseGeometry", unary_union(partial).buffer(0))
            final_polys = [merged] if isinstance(merged, Polygon) else list(cast("MultiPolygon", merged).geoms)
        else:
            final_polys = []

        print(f"[{now()}]    → Layer {layer!r} → {len(final_polys)} regions")
        dissolved_map[layer] = final_polys

    # 5) Export raw polygons
    print(f"[{now()}] Exporting polygons to '{polygon_export}'…")
    poly_doc = new(dxfversion=doc.dxfversion)
    poly_msp = poly_doc.modelspace()
    for lname, polys in dissolved_map.items():
        tc, ic = colour_map[lname]
        if not poly_doc.layers.has_entry(lname):
            if tc is not None:
                lyr = poly_doc.layers.add(name=lname)  # pyright: ignore[reportUnknownMemberType]
                lyr.dxf.true_color = tc
            else:
                poly_doc.layers.add(name=lname, dxfattribs={"color": ic})  # pyright: ignore[reportUnknownMemberType]
        for poly in polys:
            coord_list = list(poly.exterior.coords)
            poly_msp.add_lwpolyline(coord_list, close=True, dxfattribs={"layer": lname})  # pyright: ignore[reportUnknownMemberType]
            for hole in poly.interiors:
                poly_msp.add_lwpolyline(list(hole.coords), close=True, dxfattribs={"layer": lname})  # pyright: ignore[reportUnknownMemberType]
    poly_doc.saveas(polygon_export)  # pyright: ignore[reportUnknownMemberType]

    # 6) Remove originals
    print(f"[{now()}] Removing original solids/polylines…")
    for lname in colour_map:
        for e in list(cast("Iterable[DXFGraphic]", msp.query(f'*[layer=="{lname}"]'))):
            msp.delete_entity(e)

    # 7) Hatch & outline, filtering to real Polygons
    print(f"[{now()}] Generating clipped hatches…")
    all_polys = [poly for polys in dissolved_map.values() for poly in polys]
    global_union = unary_union(all_polys)

    for lname, polys in dissolved_map.items():
        tc, ic = colour_map[lname]
        other = global_union.difference(unary_union(polys))
        for poly in polys:
            clipped = poly.difference(other)
            regions: list[Polygon] = []
            if isinstance(clipped, Polygon):
                regions = [clipped]
            elif hasattr(clipped, "geoms"):
                for part in cast("MultiPolygon | GeometryCollection", clipped).geoms:
                    if isinstance(part, Polygon):
                        regions.append(part)

            for region in regions:
                coords_ext = list(region.exterior.coords)
                # 7.1) solid hatch
                hatch = msp.add_hatch(dxfattribs={"layer": lname, "solid_fill": 1})  # pyright: ignore[reportUnknownMemberType]
                hatch.dxf.pattern_name = "SOLID"
                hatch.paths.add_polyline_path(coords_ext, is_closed=True)
                for hole in region.interiors:
                    hatch.paths.add_polyline_path(list(hole.coords), is_closed=True)
                # 7.2) outline
                pl = msp.add_lwpolyline(coords_ext, close=True, dxfattribs={"layer": lname})  # pyright: ignore[reportUnknownMemberType]
                # apply colours
                if tc is not None:
                    hatch.dxf.true_color = tc
                    pl.dxf.true_color = tc
                else:
                    hatch.dxf.color = ic
                    pl.dxf.color = ic

    # 8) Save final DXF
    print(f"[{now()}] Saving '{output_file}'…")
    doc.saveas(output_file)  # pyright: ignore[reportUnknownMemberType]
    print(f"[{now()}] Done.")


def now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def rgb_from_int(color: int) -> tuple[int, int, int]:
    return ((color >> 16) & 0xFF, (color >> 8) & 0xFF, color & 0xFF)


def make_layer_name(tc: int | None, ic: int) -> str:
    if tc is not None:
        r, g, b = rgb_from_int(tc)
        return f"T_{r:03d}_{g:03d}_{b:03d}"
    return f"I_{ic:03d}"


def extract_polygons(e: DXFGraphic) -> list[BaseGeometry]:
    """Return 2D Shapely polygons for closed LWPOLYLINE, SOLID, 3DFACE entities."""
    et = e.dxftype()
    pts: list[tuple[float, float]] = []
    if et == "LWPOLYLINE" and cast("LWPolyline", e).closed:
        pts = [(x, y) for x, y, *_ in cast("LWPolyline", e).get_points()]
    elif et in ("SOLID", "3DFACE"):
        v0 = e.get_dxf_attrib("vtx0", None)
        v1 = e.get_dxf_attrib("vtx1", None)
        v2 = e.get_dxf_attrib("vtx2", None)
        v3 = e.get_dxf_attrib("vtx3", None)
        if not (v0 and v1 and v2):
            return []
        p0, p1, p2 = (v0.x, v0.y), (v1.x, v1.y), (v2.x, v2.y)
        if v3 and (v3.x, v3.y) != p2:
            pts = [p0, p1, (v3.x, v3.y), p2]
        else:
            pts = [p0, p1, p2]
    if len(pts) < 3:
        return []
    poly: BaseGeometry = Polygon(pts)
    if not poly.is_valid:
        poly = poly.buffer(0)
    return [poly] if not poly.is_empty else []


def dissolve_chunk(layer: str, coords_chunk: list[list[tuple[float, ...]]], tol: float) -> tuple[str, list[Polygon]]:
    raws = [Polygon(ring).buffer(tol) for ring in coords_chunk]
    if raws:
        merged = cast("BaseGeometry", unary_union(raws).buffer(-tol))
        if isinstance(merged, Polygon):
            return layer, [merged]
        return layer, list(cast("MultiPolygon", merged).geoms)
    return layer, []


if __name__ == "__main__":
    main()
