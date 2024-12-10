from typing import Tuple, List, Any, Union

import os
import geopandas as gpd
import fiona
from geopandas import GeoDataFrame
from pandas import Series


def get_schemas(shapefile_path: str) -> dict[str, Any]:
    with fiona.open(fp=shapefile_path, mode="r") as src:
        return src.schema or {}


def generate_trd_files(
    output_dir: str, gdf_1d_nwk: GeoDataFrame, gdf_2d_bc: GeoDataFrame
) -> None:
    base_string_1d_nwk = "Read GIS Network ==  "
    base_string_2d_bc = "Read GIS BC ==  "
    path_string: str = os.path.join(output_dir, "splits")

    # Generating 1d_nwk.trd file
    output_file_path = os.path.join(output_dir, "1d_nwk_data.trd")
    with open(output_file_path, "w") as file_1d_nwk:
        unique_ids = gdf_1d_nwk["ID"].unique()
        for unique_id in unique_ids:
            file_name: str = f"1d_nwk_{unique_id}.shp"
            full_path: str = os.path.join(path_string, file_name)
            file_1d_nwk.write(base_string_1d_nwk + full_path + "\n")
    print("1d_nwk_data.trd generated!")

    # Generating 2d_bc.trd file
    trimmed_unique_ids = set(
        uid[:-2] if uid.endswith("_U") or uid.endswith("_D") else uid
        for uid in gdf_2d_bc["Name"].unique()
    )

    output_file_path = os.path.join(output_dir, "2d_bc_data.trd")
    with open(output_file_path, "w") as file_2d_bc:
        for unique_id in trimmed_unique_ids:
            file_name = f"2d_bc_{unique_id}.shp"
            full_path = os.path.join(path_string, file_name)
            file_2d_bc.write(base_string_2d_bc + full_path + "\n")
    print("2d_bc_data.trd generated!")
    print("TRD files exported!")


def load_shapefiles(nwk_path: str, bc_path: str) -> tuple[GeoDataFrame, GeoDataFrame]:
    print("Loading shapefiles into GeoDataFrames...")
    gdf_1d_nwk: GeoDataFrame = gpd.read_file(filename=nwk_path)
    gdf_2d_bc: GeoDataFrame = gpd.read_file(filename=bc_path)
    return gdf_1d_nwk, gdf_2d_bc


def save_subset_files(
    gdf: GeoDataFrame,
    unique_ids: List[Union[str, int]],
    prefix: str,
    filter_condition: Any,
    output_dir: str,
    schema: dict[str, Any],
) -> int:
    matched_groups = 0
    print(f"Splitting {prefix} file and saving subsets...")

    for unique_id in unique_ids:
        subset: GeoDataFrame = filter_condition(gdf, unique_id)
        if not subset.empty:
            file_name: str = f"{prefix}_{unique_id}.shp"
            full_path: str = os.path.join(output_dir, file_name)
            subset.to_file(
                filename=full_path,
                schema=schema,
                driver="ESRI Shapefile",
                engine="fiona",
            )
            print(f"Saved: {full_path}")
            matched_groups += 1

    return matched_groups


def filter_condition_1d_nwk(gdf, unique_id):
    return gdf[gdf["ID"] == unique_id]


def filter_condition_2d_bc(gdf, unique_id):
    return gdf[gdf["Name"].str.contains(f"{unique_id}_U|{unique_id}_D", na=False)]


def save_subsets(
    output_dir: str,
    gdf_1d_nwk: GeoDataFrame,
    gdf_2d_bc: GeoDataFrame,
    nwk_schema: dict[str, Any],
    bc_schema: dict[str, Any],
) -> Tuple[int, int, int]:
    unique_ids = gdf_1d_nwk["ID"].unique()
    trimmed_unique_ids = set(
        uid[:-2] if uid.endswith("_U") or uid.endswith("_D") else uid
        for uid in gdf_2d_bc["Name"].unique()
    )
    total_2d_bc_groups = len(trimmed_unique_ids)

    nwk_groups_count = save_subset_files(
        gdf_1d_nwk,
        unique_ids,
        "1d_nwk",
        filter_condition_1d_nwk,
        output_dir,
        nwk_schema,
    )
    matched_2d_bc_groups = save_subset_files(
        gdf_2d_bc, unique_ids, "2d_bc", filter_condition_2d_bc, output_dir, bc_schema
    )

    return nwk_groups_count, matched_2d_bc_groups, total_2d_bc_groups


def main() -> None:
    working_dir = r"Q:\BGER\PER\RP20180.317 WYLOO CREEK CROSSING PFS - FMG\TUFLOW_Wyloo\model\gis\culverts\241209"
    os.chdir(working_dir)

    nwk_path = "1d_nwk_multi_241209_001_L.shp"
    bc_path = "2d_bc_multi_241209_001_L.shp"
    output_dir = "splits"

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    gdf_1d_nwk, gdf_2d_bc = load_shapefiles(nwk_path, bc_path)
    nwk_schema: dict[str, Any] = get_schemas(nwk_path)
    bc_schema: dict[str, Any] = get_schemas(bc_path)
    nwk_groups_count, matched_2d_bc_groups, total_2d_bc_groups = save_subsets(
        output_dir, gdf_1d_nwk, gdf_2d_bc, nwk_schema, bc_schema
    )
    generate_trd_files(output_dir, gdf_1d_nwk, gdf_2d_bc)

    # Reporting the counts to the user
    print(f"\nNumber of 1d_nwk groups found: {nwk_groups_count}")
    print(f"Number of 2d_bc groups matched with 1d_nwk: {matched_2d_bc_groups}")
    print(f"Total number of 2d_bc groups found: {total_2d_bc_groups}")

    if (
        nwk_groups_count != matched_2d_bc_groups
        or nwk_groups_count != total_2d_bc_groups
    ):
        print("Warning: Discrepancy detected in group counts!")


if __name__ == "__main__":
    main()
