# generate_culvert_polygons.py
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon, LineString
from loguru import logger
from ryan_library.functions.process_12D_culverts import get_combined_df_from_csv
import os
from shapely import affinity
from math import atan2, degrees
from pathlib import Path
from ryan_library.functions.loguru_helpers import (
    LoguruMultiprocessingLogger,
    setup_logger,
)

"""In progress, it works but not properly polished.
Need to adjust the placement, and also make some more of 
it a library for use in TUFLOW element generation"""

# Configure logging

setup_logger(console_log_level="DEBUG")


def main():
    # Path to the combined_culverts.csv file
    # csv_path = os.path.join(os.getcwd(), "combined_culverts.csv")  # Adjust if necessary
    csv_path = Path(r"Q:\BGER\PER\RP20180.317 WYLOO CREEK CROSSING PFS - FMG\TUFLOW_Wyloo\model\gis\culverts\241219")
    # Load combined_df
    combined_df = get_combined_df_from_csv(csv_path)

    if combined_df.empty:
        logger.error("Combined DataFrame is empty. Exiting script.")
        return

    # Generate geometries
    polygons_gdf, lines_gdf = generate_geometries(combined_df)

    # Define output Shapefile paths
    output_polygons_shp = os.path.join(os.getcwd(), "culvert_polygons2.shp")
    output_lines_shp = os.path.join(os.getcwd(), "culvert_lines2.shp")

    # Export to Shapefiles
    try:
        # Write polygons to Shapefile
        polygons_gdf.to_file(output_polygons_shp, driver="ESRI Shapefile")
        logger.info(f"Polygons GeoDataFrame saved to '{output_polygons_shp}'.")
    except Exception as e:
        logger.error(f"Error saving polygons Shapefile: {e}")

    try:
        # Write lines to Shapefile
        lines_gdf.to_file(output_lines_shp, driver="ESRI Shapefile")
        logger.info(f"Lines GeoDataFrame saved to '{output_lines_shp}'.")
    except Exception as e:
        logger.error(f"Error saving lines Shapefile: {e}")


def create_rectangle(width, length):
    """
    Creates a rectangle centered at (0,0).

    Args:
        width (float): Width of the rectangle.
        length (float): Length of the rectangle.

    Returns:
        Polygon: A shapely Polygon object representing the rectangle.
    """
    half_width = width / 2
    half_length = length / 2
    return Polygon(
        [
            (-half_length, -half_width),
            (-half_length, half_width),
            (half_length, half_width),
            (half_length, -half_width),
        ]
    )


def rotate_and_translate(polygon, rotation_angle, x, y):
    """
    Rotates and translates a polygon.

    Args:
        polygon (Polygon): The shapely Polygon to transform.
        rotation_angle (float): The rotation angle in degrees.
        x (float): The X-coordinate for translation.
        y (float): The Y-coordinate for translation.

    Returns:
        Polygon: The transformed shapely Polygon.
    """
    # Rotate the polygon around (0,0). Negative sign because Shapely rotates counter-clockwise.
    rotated = affinity.rotate(polygon, -rotation_angle, origin=(0, 0), use_radians=False)
    # Translate the polygon to (x, y)
    translated = affinity.translate(rotated, xoff=x, yoff=y)
    return translated


def create_linestring(us_x, us_y, ds_x, ds_y):
    """
    Creates a LineString from upstream to downstream coordinates.

    Args:
        us_x (float): Upstream X-coordinate.
        us_y (float): Upstream Y-coordinate.
        ds_x (float): Downstream X-coordinate.
        ds_y (float): Downstream Y-coordinate.

    Returns:
        LineString: A shapely LineString object.
    """
    return LineString([(us_x, us_y), (ds_x, ds_y)])


def generate_geometries(combined_df):
    """
    Generates inlet and outlet polygons and culvert lines based on combined_df.

    Args:
        combined_df (pd.DataFrame): Combined DataFrame with culvert information.

    Returns:
        tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]: (polygons_gdf, lines_gdf)
    """
    polygons = []
    polygon_attributes = []

    lines = []
    line_attributes = []

    for idx, row in combined_df.iterrows():
        name = row["Name"]
        us_x = row["US_X"]
        us_y = row["US_Y"]
        ds_x = row["DS_X"]
        ds_y = row["DS_Y"]

        # Validate coordinates
        if pd.isna(us_x) or pd.isna(us_y) or pd.isna(ds_x) or pd.isna(ds_y):
            logger.warning(f"Missing coordinates for culvert '{name}'. Skipping geometry creation.")
            continue

        # Compute delta_x and delta_y
        delta_x = ds_x - us_x
        delta_y = ds_y - us_y

        # Compute bearing in degrees
        bearing_rad = atan2(delta_x, delta_y)
        bearing_deg = (degrees(bearing_rad) + 360) % 360

        # Get skew angle
        skew_angle = row["Angle_Degrees"]
        if pd.isna(skew_angle):
            skew_angle = 0

        # Compute rotation angle
        rotation_angle = (bearing_deg + skew_angle) % 360

        # Create inlet rectangle (2m x 3m) at upstream
        inlet_rect = create_rectangle(width=2, length=3)
        inlet_rect_transformed = rotate_and_translate(inlet_rect, rotation_angle, us_x, us_y)

        # Create outlet rectangle (5m x 10m) at downstream
        outlet_rect = create_rectangle(width=5, length=10)
        outlet_rect_transformed = rotate_and_translate(outlet_rect, rotation_angle, ds_x, ds_y)

        # Append inlet polygon and attributes
        polygons.append(inlet_rect_transformed)
        polygon_attributes.append(
            {
                "Name": name[:10],  # Truncate to 10 characters
                "Type": "Inlet",
                "Angle": row.get("Angle", "")[:10],  # Truncate if necessary
                "Angle_Degrees": row["Angle_Degrees"],
                "US_X": us_x,
                "US_Y": us_y,
                "DS_X": ds_x,
                "DS_Y": ds_y,
                "Invert_US": row["Invert US"],
                "Invert_DS": row["Invert DS"],
                "Diameter": row["Diameter"],
                "Width": row["Width"],
                "Number_of_Pipes": row["Number of Pipes"],
                "Separation": row["Separation"],
                "Pipe_Type": row.get("Pipe Type", "")[:10],  # Truncate to 10 characters
                "Direction": row.get("Direction", ""),  # Truncate if necessary
            }
        )

        # Append outlet polygon and attributes
        polygons.append(outlet_rect_transformed)
        polygon_attributes.append(
            {
                "Name": name[:10],  # Truncate to 10 characters
                "Type": "Outlet",
                "Angle": row.get("Angle", "")[:10],  # Truncate if necessary
                "Angle_Degrees": row["Angle_Degrees"],
                "US_X": us_x,
                "US_Y": us_y,
                "DS_X": ds_x,
                "DS_Y": ds_y,
                "Invert_US": row["Invert US"],
                "Invert_DS": row["Invert DS"],
                "Diameter": row["Diameter"],
                "Width": row["Width"],
                "Number_of_Pipes": row["Number of Pipes"],
                "Separation": row["Separation"],
                "Pipe_Type": row.get("Pipe Type", "")[:10],  # Truncate to 10 characters
                "Direction": row.get("Direction", ""),  # Truncate if necessary
            }
        )

        # Create LineString
        line = create_linestring(us_x, us_y, ds_x, ds_y)
        lines.append(line)
        line_attributes.append(
            {
                "Name": name[:10],  # Truncate to 10 characters
                "Angle": row.get("Angle", "")[:10],  # Truncate if necessary
                "Angle_Degrees": row["Angle_Degrees"],
                "US_X": us_x,
                "US_Y": us_y,
                "DS_X": ds_x,
                "DS_Y": ds_y,
                "Invert_US": row["Invert US"],
                "Invert_DS": row["Invert DS"],
                "Diameter": row["Diameter"],
                "Width": row["Width"],
                "Number_of_Pipes": row["Number of Pipes"],
                "Separation": row["Separation"],
                "Pipe_Type": row.get("Pipe Type", "")[:10],  # Truncate to 10 characters
                "Direction": row.get("Direction", ""),  # Truncate if necessary
            }
        )

        logger.info(f"Generated geometries for culvert '{name}'.")

    # Create GeoDataFrames
    polygons_gdf = gpd.GeoDataFrame(polygon_attributes, geometry=polygons, crs="EPSG:28350")
    lines_gdf = gpd.GeoDataFrame(line_attributes, geometry=lines, crs="EPSG:28350")

    return polygons_gdf, lines_gdf


if __name__ == "__main__":
    main()
