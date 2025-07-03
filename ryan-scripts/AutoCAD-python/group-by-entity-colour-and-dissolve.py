"""
group_by_color_and_dissolve.py

Scan a DXF for index- and true-colour values, regroup entities so that each unique
colour lives on its own layer (with a name that encodes the colour), dissolve
contiguous closed polylines per layer, and write out a DXF with each entity
still carrying its original colour.
"""

import os
import ezdxf
from ezdxf.document import Drawing
from ezdxf.entities.dxfgfx import DXFGraphic
from ezdxf.layouts.layout import Modelspace
from shapely.geometry import Polygon
from shapely.ops import unary_union


def main() -> None:
    os.chdir(
        r"\\bge-resources.com\Resources\Atrium\SGT\RP20013.001 MCPHEE PROJECT - RH\60 CADD\63 Civil\12D\Output\250702 - MSA Heatmap"
    )
    # INPUT_DXF = "XR-HEATMAP.dxf"
    INPUT_DXF = "XR-HEATMAP_SAMPLE.dxf"
    # OUTPUT_DXF = "XR-HEATMAP_grouped_and_dissolved.dxf"
    OUTPUT_DXF = "XR-HEATMAP_SAMPLE_grouped_and_dissolved2.dxf"
    PROGRESS_INTERVAL = 10000  # print status every N entities

    group_and_dissolve_by_color(
        input_file=INPUT_DXF,
        output_file=OUTPUT_DXF,
        progress_interval=PROGRESS_INTERVAL,
    )


def rgb_from_int(color: int) -> tuple[int, int, int]:
    """Convert a 0xRRGGBB integer into an (R, G, B) tuple."""
    return ((color >> 16) & 0xFF, (color >> 8) & 0xFF, color & 0xFF)


def make_layer_name(true_color: int | None, index_color: int) -> str:
    """
    Return a deconstructable layer name:
      - If true_color is given: T_RRR_GGG_BBB
      - Else:                   I_n
    """
    if true_color is not None:
        r, g, b = rgb_from_int(true_color)
        return f"T_{r:03d}_{g:03d}_{b:03d}"
    else:
        return f"I_{index_color}"


def group_and_dissolve_by_color(input_file: str, output_file: str, progress_interval: int = 1000) -> None:
    print(f"Loading DXF '{input_file}'...")
    doc: Drawing = ezdxf.readfile(input_file)  # type: ignore
    msp: Modelspace = doc.modelspace()

    total = len(msp)
    print(f"Found {total} entities in modelspace.")
    layer_map: dict[str, dict[str, int]] = {}
    processed = 0

    # 1) Reassign every entity to its colour‐encoded layer
    for e in msp:
        processed += 1
        if processed % progress_interval == 0:
            print(f"  → Processed {processed}/{total} entities...")

        tc = e.get_dxf_attrib("true_color", None)
        ic = e.dxf.color  # always present (0 = by layer)

        layer_name = make_layer_name(tc, ic)
        if layer_name not in doc.layers:
            doc.layers.add(name=layer_name)
            print(f"  + Created layer '{layer_name}'")

        # remember the original colour so we can restore it later
        layer_map.setdefault(layer_name, {})[e.dxf.handle] = tc if tc is not None else ic

        # move entity onto its new layer
        e.dxf.layer = layer_name

    print(f"Finished grouping into {len(layer_map)} layers.")

    # 2) On each layer, dissolve contiguous closed polylines
    print("Dissolving contiguous shapes per layer…")

    # Tolerance in drawing units; pick something > half the largest gap you want to close
    TOLERANCE = 0.1

    for layer_name, colours in layer_map.items():
        raw_polys = []
        handles_to_delete = []

        # 1) collect & round your closed LWPOLYLINEs
        for e in msp.query(f'LWPolyline[layer=="{layer_name}"]'):
            if not e.closed:
                continue
            pts = [(round(x, 4), round(y, 4)) for x, y, *rest in e.get_points()]
            poly = Polygon(pts)
            if not poly.is_valid or poly.is_empty:
                # “clean” geometry
                poly = poly.buffer(0)
            if poly.is_empty:
                continue
            raw_polys.append(poly)
            handles_to_delete.append(e.dxf.handle)

        if not raw_polys:
            continue

        # 2) grow each polygon so they overlap at vertices
        grown = [poly.buffer(TOLERANCE) for poly in raw_polys]

        # 3) union the grown shapes
        merged = unary_union(grown)

        # 4) shrink back by the same amount
        dissolved = merged.buffer(-TOLERANCE)

        # 5) extract final Polygon list
        final_polys = (
            [dissolved]
            if isinstance(dissolved, Polygon)
            else [p for p in dissolved.geoms if isinstance(p, Polygon) and not p.is_empty]
        )

        # 6) delete the originals
        for h in handles_to_delete:
            msp.delete_entity(msp.entitydb[h])

        # 7) re‐draw and restore colours
        for poly in final_polys:
            coords = list(poly.exterior.coords)
            new_pl = msp.add_lwpolyline(coords, close=True, dxfattribs={"layer": layer_name})
            original = next(iter(colours.values()))
            if isinstance(original, int) and 1 <= original <= 256:
                new_pl.dxf.color = original
            else:
                new_pl.dxf.true_color = original

    print("Dissolve pass complete.")

    # 3) Save result
    print("Saving output…")
    doc.saveas(output_file)
    print(f"Done: '{output_file}'")


if __name__ == "__main__":
    main()
