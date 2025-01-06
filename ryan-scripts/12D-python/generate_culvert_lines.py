# generate_culvert_lines.py
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
from loguru import logger
from ryan_library.functions.process_12D_culverts import get_combined_df_from_csv
from pathlib import Path


def main() -> None:
    # Define the current working directory
    # cwd: Path = Path.cwd()
    cwd = Path(
        r"Q:\BGER\PER\RP20180.317 WYLOO CREEK CROSSING PFS - FMG\TUFLOW_Wyloo\model\gis\culverts\241219"
    )

    # Path to the combined_culverts.csv file
    csv_path: Path = cwd / "minor crossings 241219 for 12D.csv"  # Adjust if necessary

    # Load combined_df
    combined_df = get_combined_df_from_csv(csv_path)

    if combined_df.empty:
        logger.error("Combined DataFrame is empty. Exiting script.")
        return

    # Generate culvert lines
    lines_gdf: gpd.GeoDataFrame = generate_lines(combined_df)

    if lines_gdf.empty:
        logger.error("No culvert lines were generated. Exiting script.")
        return

    # Define output GeoPackage file path
    output_gpkg: Path = cwd / "culvert_geometries.gpkg"

    # Export to GeoPackage
    try:
        # Write lines to GeoPackage
        lines_gdf.to_file(output_gpkg, layer="culvert_lines", driver="GPKG")
        logger.info(
            f"Lines GeoDataFrame saved to '{output_gpkg}' in layer 'culvert_lines'."
        )
    except Exception as e:
        logger.error(f"Error saving to GeoPackage: {e}")


def create_linestring(us_x: float, us_y: float, ds_x: float, ds_y: float) -> LineString:
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


def generate_lines(combined_df: pd.DataFrame) -> gpd.GeoDataFrame:
    """
    Generates culvert LineStrings based on combined_df.

    Args:
        combined_df (pd.DataFrame): Combined DataFrame with culvert information.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame containing culvert lines with attributes.
    """
    # Drop rows with missing coordinates
    valid_df = combined_df.dropna(subset=["US_X", "US_Y", "DS_X", "DS_Y"])

    if valid_df.empty:
        logger.warning("No valid culvert entries with complete coordinates found.")
        return gpd.GeoDataFrame()

    # Create LineStrings
    geometry = [
        create_linestring(row.US_X, row.US_Y, row.DS_X, row.DS_Y)
        for _, row in valid_df.iterrows()
    ]

    # Select relevant attributes
    attributes = valid_df[
        [
            "Name",
            "Angle",
            "Angle_Degrees",
            "US_X",
            "US_Y",
            "DS_X",
            "DS_Y",
            "Invert US",
            "Invert DS",
            "Diameter",
            "Width",
            "Number of Pipes",
            "Separation",
            "Pipe Type",
            "Direction",
        ]
    ].copy()

    # Assign geometry
    lines_gdf = gpd.GeoDataFrame(attributes, geometry=geometry, crs="EPSG:28350")

    logger.info(f"Generated {len(lines_gdf)} LineStrings for culverts.")

    return lines_gdf


if __name__ == "__main__":
    main()
