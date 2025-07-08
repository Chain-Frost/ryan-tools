#!/usr/bin/env python3
import os
import ezdxf
from ezdxf.document import Drawing
from ezdxf.entities.dxfgfx import DXFGraphic
from ezdxf.layouts.layout import Modelspace
from shapely.geometry import Polygon
from shapely.ops import unary_union
from datetime import datetime

# tolerance for merging “point-touchers” (in drawing units)
TOLERANCE = 0.05
# how many entities between progress messages
PROGRESS_INTERVAL = 10000


def main() -> None:
    # ── adjust this path & filenames ────────────────────────────────────────
    os.chdir(
        r"\\bge-resources.com\Resources\Atrium\SGT\RP20013.001 MCPHEE PROJECT - RH"
        r"\60 CADD\63 Civil\12D\Output\250702 - MSA Heatmap"
    )
    input_file = "XR-HEATMAP.dxf"
    output_file = "XR-HEATMAP_grouped_and_dissolved_hatched_clipped.dxf"
    # ────────────────────────────────────────────────────────────────────────

    print(f"[{now()}] Reading '{input_file}'…")
    doc: Drawing = ezdxf.readfile(input_file)
    msp: Modelspace = doc.modelspace()

    # 1) Re-layer by true_color / index color
    print(f"[{now()}] Re-layering entities by color…")
    colour_map: dict[str, int] = {}
    for idx, e in enumerate(msp, start=1):
        tc = e.get_dxf_attrib("true_color", None)
        ic = e.get_dxf_attrib("color", None)
        lname = make_layer_name(tc, ic)
        if lname not in doc.layers:
            doc.layers.add(name=lname)
        colour_map[lname] = tc if tc is not None else ic
        e.dxf.layer = lname
        if idx % PROGRESS_INTERVAL == 0:
            print(f"[{now()}]   • {idx} entities re-layered so far…")
    print(f"[{now()}]    → {idx} entities → {len(colour_map)} layers.")

    # 2) Dissolve to shapely per layer (in memory)
    print(f"[{now()}] Computing dissolved regions per layer…")
    dissolved_map: dict[str, list[Polygon]] = {}
    for layer in colour_map:
        raws: list[Polygon] = []
        count = 0
        for e in msp.query(f'*[layer=="{layer}"]'):
            count += 1
            for poly in extract_polygons(e):
                raws.append(poly.buffer(TOLERANCE))
            if count % PROGRESS_INTERVAL == 0:
                print(f"[{now()}]   • Scanned {count} entities in {layer}…")
        if raws:
            merged = unary_union(raws).buffer(-TOLERANCE)
            polys = [merged] if isinstance(merged, Polygon) else list(merged.geoms)
        else:
            polys = []
        dissolved_map[layer] = polys
        print(f"[{now()}]    → {layer}: {len(polys)} dissolved region(s)")

    # 3) Remove original entities
    print(f"[{now()}] Removing original solids/polylines…")
    for layer in colour_map:
        for e in list(msp.query(f'*[layer=="{layer}"]')):
            msp.delete_entity(e)

    # 4) Hatch & outline, clipped by adjacent colors
    print(f"[{now()}] Generating clipped hatches…")
    all_polys = [poly for polys in dissolved_map.values() for poly in polys]
    global_union = unary_union(all_polys)

    for layer, polys in dissolved_map.items():
        other_union = global_union.difference(unary_union(polys))
        cv = colour_map[layer]
        for poly in polys:
            clipped = poly.difference(other_union)
            # collect only Polygon parts
            regions: list[Polygon] = []
            if isinstance(clipped, Polygon):
                regions = [clipped]
            elif hasattr(clipped, "geoms"):
                for part in clipped.geoms:
                    if isinstance(part, Polygon):
                        regions.append(part)
            for region in regions:
                coords_ext = list(region.exterior.coords)

                # 1) true solid‐fill hatch
                hatch = msp.add_hatch(
                    dxfattribs={
                        "layer": layer,
                        "solid_fill": 1,
                    }
                )
                hatch.dxf.pattern_name = "SOLID"
                hatch.paths.add_polyline_path(coords_ext, is_closed=True)
                for hole in region.interiors:
                    hatch.paths.add_polyline_path(list(hole.coords), is_closed=True)

                # **set the hatch fill color to the layer color**
                if 1 <= cv <= 256:
                    hatch.dxf.color = cv
                else:
                    hatch.dxf.true_color = cv

                # 2) outline polyline
                pl = msp.add_lwpolyline(coords_ext, close=True, dxfattribs={"layer": layer})
                if 1 <= cv <= 256:
                    pl.dxf.color = cv
                else:
                    pl.dxf.true_color = cv

    # 5) Save
    print(f"[{now()}] Saving '{output_file}'…")
    doc.saveas(filename=output_file)
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


def extract_polygons(e: DXFGraphic) -> list[Polygon]:
    et = e.dxftype()
    pts: list[tuple[float, float]] = []
    if et == "LWPOLYLINE" and e.closed:
        pts = [(x, y) for x, y, *_ in e.get_points()]
    elif et in ("SOLID", "3DFACE"):
        v0 = e.get_dxf_attrib("vtx0", None)
        v1 = e.get_dxf_attrib("vtx1", None)
        v2 = e.get_dxf_attrib("vtx2", None)
        v3 = e.get_dxf_attrib("vtx3", None)
        if not (v0 and v1 and v2):
            return []
        p0, p1, p2 = (v0.x, v0.y), (v1.x, v1.y), (v2.x, v2.y)
        if v3 and (v3.x, v3.y) != p2:
            p3 = (v3.x, v3.y)
            pts = [p0, p1, p3, p2]
        else:
            pts = [p0, p1, p2]
    if len(pts) < 3:
        return []
    poly = Polygon(pts)
    if not poly.is_valid:
        poly = poly.buffer(0)
    return [poly] if not poly.is_empty else []


if __name__ == "__main__":
    main()
