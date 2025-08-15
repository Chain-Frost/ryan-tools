"""
Process .arch_d files into:
1) CSV (all points with attributes)
2) GPKG (3D LineStrings per object, with attributes)
3) DXF (3D polylines, metres, no closure)

Requires:
    pip install fiona ezdxf
"""

# ---------- USER SETTINGS ----------
from pathlib import Path
import os

# Path to the .arch_d file
INPUT_FILE = Path(
    r"P:\BGER\PER\RP23067.003 MCPHEE SUPPORT SERVICES - ATLAS\7 DOCUMENT CONTROL\2 RECIEVED DATA\1 CLIENT\250808 mar arch_d file\mod-3d-mar-option_02_250611.arch_d"
)

# EPSG code for vector outputs (e.g. 28355 = GDA94 / MGA Zone 55)
OUTPUT_EPSG = 28351

# Output GPKG layer name
GPKG_LAYER = "arch_d_lines"
# -----------------------------------

import re
import csv
from typing import Any
import fiona
from fiona.crs import CRS
import ezdxf


# ---------- Helpers ----------


def parse_point_line(line: str) -> tuple[float, float, float]:
    """Extract X, Y, Z coordinates from a 'Point'/'Origi'/'Final' line."""
    parts = line.strip().split()
    # Example: "Point:      0   X   Y   Z   0.000000POINT_1"
    return float(parts[2]), float(parts[3]), float(parts[4])


def clean_layer_name(name: str) -> str:
    """Sanitise DXF layer name."""
    name = re.sub(r'[<>/":;?*|=]', "_", name)
    name = name.strip()[:50]
    return name if name else "DEFAULT"


# ---------- Main Parsing ----------


def process_file(input_path: Path) -> tuple[list[list[Any]], list[dict[str, Any]]]:
    """
    Parse .arch_d into:
      - cleaned_data: rows for CSV
      - line_features: list of features carrying geometry + attributes
        geometry is a GeoJSON dict with 3D coordinates.
    """
    with input_path.open("r", encoding="utf-8", errors="replace") as file:
        lines = list(file)

    cleaned_data: list[list[Any]] = [
        ["objectID", "Layer", "HEDType", "HED", "Graphic", "Named", "PointNum", "X", "Y", "Z"]
    ]
    line_features: list[dict[str, Any]] = []

    object_id = -1
    current_layer = ""
    hed_type = ""
    hed_value = ""
    graphic = ""
    named = ""
    point_num = 0
    current_points_2d: list[tuple[float, float]] = []
    current_points_3d: list[tuple[float, float, float]] = []

    def flush_geometry() -> None:
        """Emit current feature (if any) preserving Z."""
        if current_points_3d:
            line_features.append(
                {
                    "geometry": {
                        "type": "LineString",
                        "coordinates": current_points_3d,  # 3D tuples
                    },
                    "properties": {
                        "objectID": object_id,
                        "Layer": current_layer,
                        "HEDType": hed_type,
                        "HED": hed_value,
                        "Graphic": graphic,
                        "Named": named,
                        # Keep raw coords as well for DXF writer
                        "coords_2d": current_points_2d,
                        "coords_3d": current_points_3d,
                    },
                }
            )

    for line in lines:
        keyword = line.split(":", 1)[0]

        if keyword == "Layer":
            # Keep only the layer name token; drop "Created layer ..." tail
            current_layer = line.split(":", 1)[1].strip().split()[0]

        elif keyword == "POLHED":
            flush_geometry()
            current_points_2d = []
            current_points_3d = []
            hed_value = line.split(":", 1)[1].strip()
            hed_type = "POLHED"
            point_num = 0
            object_id += 1

        elif keyword == "ARWHED":
            flush_geometry()
            current_points_2d = []
            current_points_3d = []
            hed_value = line.split(":", 1)[1].strip()
            hed_type = "ARWHED"
            point_num = 0
            object_id += 1

        elif keyword == "Graphic":
            graphic = line.split(":", 1)[1].strip()

        elif keyword == "Named":
            named = line.split(":", 1)[1].strip()

        elif keyword in {"Point", "Origi", "Final"}:
            x, y, z = parse_point_line(line)
            cleaned_data.append([object_id, current_layer, hed_type, hed_value, graphic, named, point_num, x, y, z])
            current_points_2d.append((x, y))
            current_points_3d.append((x, y, z))
            point_num += 1

        # ignore other lines (FMT_4, Attr_len, Attr_tem, etc.)

    flush_geometry()
    return cleaned_data, line_features


# ---------- Outputs ----------


def save_to_csv(data: list[list[Any]], output_path: Path) -> None:
    """Write cleaned point data to CSV."""
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)


def save_to_gpkg(features: list[dict[str, Any]], output_path: Path, layer_name: str, epsg: int) -> None:
    """
    Write 3D LineString features to GeoPackage.
    Use schema geometry "LineString" and pass 3D coords; GPKG will store Z.
    """
    schema = {
        "geometry": "LineString",  # Use plain LineString; coords carry Z
        "properties": {
            "objectID": "int",
            "Layer": "str",
            "HEDType": "str",
            "HED": "str",
            "Graphic": "str",
            "Named": "str",
        },
    }

    # Overwrite file if it exists (mode="w" recreates the GPKG)
    with fiona.open(
        output_path,
        mode="w",
        driver="GPKG",
        layer=layer_name,
        crs=CRS.from_epsg(epsg),
        schema=schema,
        encoding="utf-8",
        # Helpful options; not required for Z, but good defaults:
        layer_creation_options=["GEOMETRY_NAME=geom", "SPATIAL_INDEX=YES"],
    ) as gpkg:
        for feature in features:
            coords3d = feature["properties"]["coords_3d"]
            gpkg.write(
                {
                    "geometry": {
                        "type": "LineString",
                        "coordinates": coords3d,  # 3D tuples -> Z preserved
                    },
                    "properties": {
                        "objectID": feature["properties"]["objectID"],
                        "Layer": feature["properties"]["Layer"],
                        "HEDType": feature["properties"]["HEDType"],
                        "HED": feature["properties"]["HED"],
                        "Graphic": feature["properties"]["Graphic"],
                        "Named": feature["properties"]["Named"],
                    },
                }
            )


def save_to_dxf(features: list[dict[str, Any]], output_path: Path) -> None:
    """Write 3D polylines to DXF (units in metres, no closure)."""
    doc = ezdxf.new(setup=True)
    doc.units = ezdxf.units.M
    msp = doc.modelspace()

    for feat in features:
        layer_name = clean_layer_name(feat["properties"]["Layer"])
        if layer_name not in doc.layers:
            doc.layers.add(layer_name)
        coords_xyz = feat["properties"]["coords_3d"]
        msp.add_polyline3d(coords_xyz, dxfattribs={"layer": layer_name})

    doc.saveas(output_path)


# ---------- Runner ----------


def main() -> None:
    os.chdir(INPUT_FILE.parent)  # Run relative to the input file folder

    cleaned_points, line_features = process_file(INPUT_FILE)

    save_to_csv(cleaned_points, INPUT_FILE.with_suffix(".csv"))
    save_to_gpkg(line_features, INPUT_FILE.with_suffix(".gpkg"), layer_name=GPKG_LAYER, epsg=OUTPUT_EPSG)
    save_to_dxf(line_features, INPUT_FILE.with_suffix(".dxf"))

    print(f"CSV saved to:   {INPUT_FILE.with_suffix('.csv')}")
    print(f"GPKG saved to:  {INPUT_FILE.with_suffix('.gpkg')} (layer: {GPKG_LAYER})")
    print(f"DXF saved to:   {INPUT_FILE.with_suffix('.dxf')}")


if __name__ == "__main__":
    main()
