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
# emit a status every N entities
PROGRESS_INTERVAL = 10000


def main() -> None:
    # ── adjust this path & filenames ────────────────────────────────────────
    os.chdir(
        r"\\bge-resources.com\Resources\Atrium\SGT\RP20013.001 MCPHEE PROJECT - RH"
        r"\60 CADD\63 Civil\12D\Output\250702 - MSA Heatmap"
    )
    input_file = "XR-HEATMAP_SAMPLE.dxf"
    output_file = "XR-HEATMAP_SAMPLE_grouped_and_dissolved9.dxf"
    # ────────────────────────────────────────────────────────────────────────

    print(f"[{now()}] Reading '{input_file}'…")
    doc: Drawing = ezdxf.readfile(input_file)
    msp: Modelspace = doc.modelspace()

    # 1) Re-layer by true_color / index color
    print(f"[{now()}] Re-layering entities by color…")
    colour_map: dict[str, int] = {}
    layer_counts: dict[str, int] = {}
    for idx, e in enumerate(msp, start=1):
        old_layer = e.get_dxf_attrib("layer", "0")
        tc = e.get_dxf_attrib("true_color", None)
        ic = e.get_dxf_attrib("color", None)
        lname = make_layer_name(tc, ic)

        if lname not in doc.layers:
            doc.layers.add(name=lname)
        colour_map[lname] = tc if tc is not None else ic
        e.dxf.layer = lname

        # debug-print first few mappings
        if idx <= 5:
            print(f"[{now()}]   #{idx}: {e.dxftype()}  {old_layer!r} → {lname!r}")

        layer_counts[lname] = layer_counts.get(lname, 0) + 1
        if idx % PROGRESS_INTERVAL == 0:
            print(f"[{now()}]   • {idx} entities re-layered so far…")

    print(f"[{now()}] Re-layering complete: {idx} entities → {len(colour_map)} layers.")
    zeros = [L for L, c in layer_counts.items() if c == 0]
    print(f"[{now()}]   • Non-empty layers: {len(layer_counts)-len(zeros)}; Empty layers: {len(zeros)}")
    if zeros:
        print(f"[{now()}]   • Layers with 0 assignments (shouldn’t happen!): {zeros[:10]}…")

    # 2) Dissolve each color‐grouped layer
    for layer in colour_map:
        print(f"[{now()}] Dissolving layer '{layer}'…")
        dissolve_layer(msp, layer, colour_map)
        print(f"[{now()}]   → Finished layer '{layer}'.")

    # 3) Save
    print(f"[{now()}] Saving '{output_file}'…")
    doc.saveas(filename=output_file)
    print(f"[{now()}] Done.")


def now() -> str:
    """Return current time as HH:MM:SS."""
    return datetime.now().strftime("%H:%M:%S")


def rgb_from_int(color: int) -> tuple[int, int, int]:
    return ((color >> 16) & 0xFF, (color >> 8) & 0xFF, color & 0xFF)


def make_layer_name(tc: int | None, ic: int) -> str:
    """Create a layer name based on true_color or index color."""
    if tc is not None:
        r, g, b = rgb_from_int(tc)
        return f"T_{r:03d}_{g:03d}_{b:03d}"
    else:
        return f"I_{ic:03d}"


def extract_polygons(e: DXFGraphic) -> list[Polygon]:
    """
    Return a list of 2D Shapely Polygons from a DXFGraphic.
    Supports:
      - Closed LWPOLYLINE
      - SOLID       (vtx0 → vtx1 → vtx3 → vtx2)
      - 3DFACE      (vtx0 → vtx1 → vtx3 → vtx2, or triangle if no vtx3)
    """
    et = e.dxftype()
    pts: list[tuple[float, float]] = []

    if et == "LWPOLYLINE" and e.closed:
        pts = [(x, y) for x, y, *_ in e.get_points()]

    elif et in ("SOLID", "3DFACE"):
        # fetch the four possible vertices
        v0 = e.get_dxf_attrib("vtx0", None)
        v1 = e.get_dxf_attrib("vtx1", None)
        v2 = e.get_dxf_attrib("vtx2", None)
        v3 = e.get_dxf_attrib("vtx3", None)
        if not (v0 and v1 and v2):
            return []
        p0 = (v0.x, v0.y)
        p1 = (v1.x, v1.y)
        p2 = (v2.x, v2.y)
        if v3 and (v3.x, v3.y) != p2:
            p3 = (v3.x, v3.y)
            # follow AutoCAD SOLID winding: v0 → v1 → v3 → v2
            pts = [p0, p1, p3, p2]
        else:
            # triangle
            pts = [p0, p1, p2]

    # skip anything else
    if len(pts) < 3:
        return []

    poly = Polygon(pts)
    if not poly.is_valid:
        # minimal self-intersection fix
        poly = poly.buffer(0)
    return [poly] if not poly.is_empty else []


def dissolve_layer(msp: Modelspace, layer_name: str, colour_map: dict[str, int]) -> None:
    """
    Collect all polygons on 'layer_name', buffer & union to dissolve,
    delete originals, and re-add as single or multiple LWPOLYLINEs.
    """
    raws: list[Polygon] = []
    handles_to_delete: list[str] = []
    count = 0

    for e in msp.query(f'*[layer=="{layer_name}"]'):
        count += 1
        for poly in extract_polygons(e):
            raws.append(poly.buffer(distance=TOLERANCE))
            handles_to_delete.append(e.dxf.handle)

        if count % PROGRESS_INTERVAL == 0:
            print(f"[{now()}]   • {count} entities scanned in '{layer_name}'…")

    if not raws:
        print(f"[{now()}]   • No polygons found on '{layer_name}', skipping.")
        return

    merged = unary_union(raws).buffer(distance=-TOLERANCE)
    final_polys = [merged] if isinstance(merged, Polygon) else list(merged.geoms)

    # delete the raw entities
    for h in handles_to_delete:
        msp.delete_entity(msp.entitydb[h])

    # re-add dissolved shapes
    for poly in final_polys:
        coords = list(poly.exterior.coords)
        new_pl = msp.add_lwpolyline(coords, close=True, dxfattribs={"layer": layer_name})
        orig = colour_map[layer_name]
        if 1 <= orig <= 256:
            new_pl.dxf.color = orig
        else:
            new_pl.dxf.true_color = orig

    print(f"[{now()}]   • Dissolved to {len(final_polys)} polygon(s) on '{layer_name}'.")


if __name__ == "__main__":
    main()
