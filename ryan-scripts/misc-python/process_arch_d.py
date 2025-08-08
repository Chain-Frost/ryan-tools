import os
import re
import csv
from pathlib import Path
from typing import Any
import shapely.geometry as geom
import fiona
from fiona.crs import CRS
import ezdxf


# ---------- Parsing Helpers ----------


def parse_point_line(line: str) -> tuple[float, float, float]:
    """Extract X, Y, Z coordinates from a line."""
    parts = line.strip().split()
    return float(parts[2]), float(parts[3]), float(parts[4])


def clean_layer_name(name: str) -> str:
    """Sanitise layer name so it is valid in DXF."""
    # Remove illegal DXF characters
    name = re.sub(r'[<>/":;?*|=]', "_", name)
    # Trim and limit length
    name = name.strip()[:50]
    # Fallback if empty
    return name if name else "DEFAULT"


# ---------- Main Parsing Function ----------


def process_file(input_path: Path) -> tuple[list[list[Any]], list[dict[str, Any]]]:
    """Parse .arch_d file into cleaned data and LineString features."""
    with input_path.open("r", encoding="utf-8", errors="replace") as file:
        lines = list(file)

    cleaned_data = [["objectID", "Layer", "HEDType", "HED", "Graphic", "Named", "PointNum", "X", "Y", "Z"]]
    line_features: list[dict[str, Any]] = []

    object_id = -1
    current_layer = ""
    hed_type = ""
    hed_value = ""
    graphic = ""
    named = ""
    point_num = 0
    current_points: list[tuple[float, float]] = []

    def flush_geometry():
        """Store current geometry as LineString feature."""
        if current_points:
            line_features.append(
                {
                    "geometry": geom.LineString(current_points).__geo_interface__,
                    "properties": {
                        "objectID": object_id,
                        "Layer": current_layer,
                        "HEDType": hed_type,
                        "HED": hed_value,
                        "Graphic": graphic,
                        "Named": named,
                        "coords": current_points,
                    },
                }
            )

    for line in lines:
        keyword = line.split(":", 1)[0]

        if keyword == "Layer":
            # Take only the first word after colon to avoid "Created layer ..." text
            current_layer = line.split(":", 1)[1].strip().split()[0]

        elif keyword == "POLHED":
            flush_geometry()
            current_points = []
            hed_value = line.split(":", 1)[1].strip()
            hed_type = "POLHED"
            point_num = 0
            object_id += 1

        elif keyword == "ARWHED":
            flush_geometry()
            current_points = []
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
            current_points.append((x, y))
            point_num += 1

    flush_geometry()
    return cleaned_data, line_features


# ---------- Output Functions ----------


def save_to_csv(data: list[list[Any]], output_path: Path) -> None:
    """Write cleaned point data to CSV."""
    with output_path.open("w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(data)


def save_to_shapefile(features: list[dict[str, Any]], output_path: Path, epsg: int) -> None:
    """Write LineString geometries to a shapefile."""
    schema = {
        "geometry": "LineString",
        "properties": {
            "objectID": "int",
            "Layer": "str",
            "HEDType": "str",
            "HED": "str",
            "Graphic": "str",
            "Named": "str",
        },
    }
    with fiona.open(
        output_path, "w", driver="ESRI Shapefile", crs=CRS.from_epsg(epsg), schema=schema, encoding="utf-8"
    ) as shp:
        for feature in features:
            shp.write(
                {
                    "geometry": feature["geometry"],
                    "properties": {k: feature["properties"][k] for k in schema["properties"]},
                }
            )


def save_to_dxf(features: list[dict[str, Any]], output_path: Path) -> None:
    """Write LineString geometries to DXF (units in metres)."""
    doc = ezdxf.new(setup=True)
    doc.units = ezdxf.units.M  # Set units to metres
    msp = doc.modelspace()

    for feat in features:
        raw_layer = feat["properties"]["Layer"]
        layer_name = clean_layer_name(raw_layer)
        if layer_name not in doc.layers:
            doc.layers.add(layer_name)
        coords = feat["properties"]["coords"]
        msp.add_lwpolyline(coords, dxfattribs={"layer": layer_name})

    doc.saveas(output_path)


# ---------- Main Entry Point ----------


def main() -> None:
    input_file = Path(
        r"P:\BGER\PER\RP23067.003 MCPHEE SUPPORT SERVICES - ATLAS\7 DOCUMENT CONTROL\2 RECIEVED DATA\1 CLIENT\250808 mar arch_d file\mod-3d-mar-option_02_250611.arch_d"
    )
    os.chdir(input_file.parent)  # Change working dir to file's location

    cleaned_points, line_features = process_file(input_file)

    save_to_csv(cleaned_points, input_file.with_suffix(".csv"))
    save_to_shapefile(line_features, input_file.with_suffix(".shp"), epsg=28351)  # GDA94 / MGA Zone 55
    save_to_dxf(line_features, input_file.with_suffix(".dxf"))

    print(f"CSV saved to: {input_file.with_suffix('.csv')}")
    print(f"Shapefile saved to: {input_file.with_suffix('.shp')}")
    print(f"DXF saved to: {input_file.with_suffix('.dxf')}")


if __name__ == "__main__":
    main()
