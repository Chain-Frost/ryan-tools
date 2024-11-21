import os
import shutil
import subprocess
from concurrent.futures import ThreadPoolExecutor
import sqlite3
from pprint import pprint


def get_file_mappings():  # -> dict[str, dict[str, Any]]:
    raster_exts: list[str] = ["flt", "tif"]
    vector_exts: list[str] = ["shp", "gpkg"]

    mapping_dict = {
        "d_Max": {
            "exts": raster_exts,
            "qml": r"\\bgersgtnas05\Waterways\Library\Style\depth_for_legend_max2m.qml",
        },
        "h_Max": {
            "exts": raster_exts,
            "qml": r"\\bgersgtnas05\Waterways\Library\Style\hmax.qml",
        },
        "V_Max": {
            "exts": raster_exts,
            "qml": r"\\bgersgtnas05\Waterways\Library\Style\velocities scour protection mrwa.qml",
        },
        "DEM_Z": {
            "exts": raster_exts,
            "qml": r"\\bgersgtnas05\Waterways\Library\Style\hillshade.qml",
        },
        "1d_ccA_L": {
            "exts": vector_exts,
            "qml": r"\\bgersgtnas05\Waterways\Library\Style\1d_ccA.qml",
        },
        "DIFF_P2-P1": {
            "exts": raster_exts,
            "qml": r"\\bgersgtnas05\Waterways\Library\Style\Depth Diff GOOOD.qml",
        },
        "Results1D": {
            "exts": vector_exts,  # Updated to include 'gpkg'
            "layer_name": "1d_ccA_L",  # Specify the layer name within the GeoPackage
            "qml": r"\\bgersgtnas05\Waterways\Library\Style\1d_ccA.qml",
        },
    }
    mapping_dict["d_HR_Max"] = mapping_dict["d_Max"]
    mapping_dict["h_HR_Max"] = mapping_dict["h_Max"]
    mapping_dict["DEM_Z_HR"] = mapping_dict["DEM_Z"]

    return mapping_dict


def process_data(filename, ext, current_path, copy_from):
    filename_without_ext, source_ext = os.path.splitext(filename)
    source_ext = source_ext.lstrip(".")
    new_filename = f"{filename_without_ext}.{ext}"
    full_path = os.path.join(current_path, filename)
    print(f"    {source_ext} >> {ext}  {full_path}")
    shutil.copy(copy_from, os.path.join(current_path, new_filename))


def process_gpkg(filename, layer_name, gpkg_name, current_path, copy_from):
    pass
    full_path = os.path.join(current_path, filename)
    print(f"    GeoPackage: {full_path}")

    conn = sqlite3.connect(full_path)
    cursor = conn.cursor()

    cursor.execute(
        f'SELECT styleName, styleQML FROM layer_styles WHERE f_table_name = "{layer_name}";'
    )
    styles = cursor.fetchall()

    for style in styles:
        style_name, style_qml = style
        print(f"        Applying style {style_name} to layer {layer_name}")

        # Logic to apply styling to the specific layer goes here
        # You might use geopandas or fiona to apply the QML style to the layer

    conn.close()


def tree_process(current_path, mappings):
    # print(f'Processing folder: {current_path}')
    with ThreadPoolExecutor() as executor:
        # Specifically, it defaults to os.cpu_count() * 5
        # with ThreadPoolExecutor(max_workers=10) as executor:
        # if you want to manually set a limit.
        for filename in os.listdir(current_path):
            for key, value in mappings.items():
                for ext in value["exts"]:
                    if filename.lower().endswith(f"{key.lower()}.{ext.lower()}"):
                        if ext.lower() == "gpkg":
                            executor.submit(
                                process_gpkg,
                                filename,
                                value["layer_name"],
                                value.get("gpkg", ""),
                                current_path,
                                value["qml"],
                            )
                        else:
                            executor.submit(
                                process_data,
                                filename,
                                "qml",
                                current_path,
                                value["qml"],
                            )

        for filename in os.listdir(current_path):
            if os.path.isdir(os.path.join(current_path, filename)):
                executor.submit(
                    tree_process, os.path.join(current_path, filename), mappings
                )


if __name__ == "__main__":
    mappings = get_file_mappings()
    os.chdir(os.path.dirname(os.path.abspath(__file__)))
    current_path = os.getcwd()
    print(current_path)
    print()
    pprint(mappings)
    print()    
    tree_process(current_path, mappings)
    subprocess.call("pause", shell=True)  # wait for exit
