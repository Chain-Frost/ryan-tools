# ryan-scripts\AutoCAD-python.py
"""
group_by_color.py
Scan a DXF for true-color (RGB) values and regroup entities so
that each unique color lives on its own layer, with progress reports.
"""

import ezdxf
import os

from ezdxf.document import Drawing
from ezdxf.layouts.layout import Modelspace


def main() -> None:
    os.chdir(
        r"\\bge-resources.com\Resources\Atrium\SGT\RP20013.001 MCPHEE PROJECT - RH\60 CADD\63 Civil\12D\Output\250702 - MSA Heatmap"
    )
    INPUT_DXF = "XR-HEATMAP.dxf"
    OUTPUT_DXF = "XR-HEATMAP.dxf_grouped_by_colour.dxf"
    PROGRESS_INTERVAL = 10000  # print status every N entities
    # ─────────────────────────────────────────────────────────────────────────
    group_by_true_color(input_file=INPUT_DXF, output_file=OUTPUT_DXF, progress_interval=PROGRESS_INTERVAL)


def rgb_from_int(color: int) -> tuple[int, int, int]:
    """Convert a 0xRRGGBB integer into an (R, G, B) tuple."""
    return ((color >> 16) & 0xFF, (color >> 8) & 0xFF, color & 0xFF)


def group_by_true_color(input_file: str, output_file: str, progress_interval: int) -> None:
    print(f"Loading DXF '{input_file}'...")
    # Load the drawing
    doc: Drawing = ezdxf.readfile(input_file)
    msp: Modelspace = doc.modelspace()

    total = len(msp)
    print(f"Found {total} entities in modelspace.")
    # Map from RGB tuple to layer name
    color_layer_map: dict[tuple[int, int, int], str] = {}
    processed = 0

    for entity in msp:
        processed += 1
        if processed % progress_interval == 0:
            print(f"  → Processed {processed}/{total} entities...")
        # Grab true_color (group code 420); skip if not present
        true_color = entity.get_dxf_attrib("true_color", None)
        if true_color is None:
            continue

        rgb: tuple[int, int, int] = rgb_from_int(true_color)

        # If first time seeing this RGB, create a new layer
        if rgb not in color_layer_map:
            base_name: str = f"COL_{rgb[0]:03d}{rgb[1]:03d}{rgb[2]:03d}"
            layer_name: str = base_name
            suffix = 1
            # avoid collision with existing layers
            while layer_name in doc.layers:
                layer_name = f"{base_name}_{suffix}"
                suffix += 1
            doc.layers.add(name=layer_name)
            color_layer_map[rgb] = layer_name
            print(f"  + Created layer '{layer_name}' for color {rgb}")

        # Reassign entity to its color layer
        entity.dxf.layer = color_layer_map[rgb]

    # Save result
    print(f"Finished processing {processed} entities. Created {len(color_layer_map)} layers.")
    doc.saveas(filename=output_file)
    print(f"Grouped-by-color DXF written to: {output_file}")


if __name__ == "__main__":
    main()
