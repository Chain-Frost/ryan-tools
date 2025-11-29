import csv
import re
import os
from qgis.core import (  # type: ignore
    QgsProject,
    QgsMapThemeCollection,
    QgsLayerTreeLayer,
    QgsLayerTreeModel,
)

# Configuration parameters for easy editing
base_theme_name = "base"  # Name of the base theme
layer_list_file = "layer_list.txt"  # Text file with the list of layers
overwrite_existing = True
# Set to True to overwrite existing themes, False to raise an exception
ignore_errors = True  # Set to True to ignore errors and collect problem layers, False to raise an exception
output_csv_file = "themes_output.csv"
# CSV file to output theme names and additional columns
theme_prefix = ""  # Prefix to add to all generated themes
working_directory = ""
# Optional working directory, leave empty to use current directory


def main(
    base_theme_name,
    layer_list_file,
    overwrite_existing,
    ignore_errors,
    theme_prefix,
    working_directory,
):
    if working_directory:
        os.chdir(working_directory)
        print(f"Changed working directory to: {working_directory}")

    print("Starting theme creation process...")
    themes = create_new_themes(
        base_theme_name,
        layer_list_file,
        overwrite_existing,
        ignore_errors,
        theme_prefix,
    )
    print(f"Created {len(themes)} themes. Generating CSV output...")
    generate_csv_output(themes, output_csv_file, theme_prefix)
    print(f"CSV output generated: {output_csv_file}")
    print("Theme creation process completed.")


def check_layer_ambiguities(layer_name):
    project = QgsProject.instance()
    layers = project.mapLayersByName(layer_name)

    if not layers:
        return None, f"Layer '{layer_name}' not found in the project."

    if len(layers) > 1:
        return (
            None,
            f"Ambiguous layer name '{layer_name}' found. Please resolve ambiguity.",
        )

    return layers[0], None


def create_map_theme_record(base_layer_ids, layer, all_layer_ids):
    project = QgsProject.instance()
    root = project.layerTreeRoot()

    # Set all layers to invisible initially
    for layer_id in all_layer_ids:
        layer_tree_layer = root.findLayer(layer_id)
        if layer_tree_layer:
            layer_tree_layer.setItemVisibilityChecked(False)

    # Set the base theme layers to visible
    for layer_id in base_layer_ids:
        base_layer_tree_layer = root.findLayer(layer_id)
        if base_layer_tree_layer:
            base_layer_tree_layer.setItemVisibilityChecked(True)

    # Set the specified layer to visible and ensure containing groups are also visible
    layer_tree_layer = root.findLayer(layer.id())
    if layer_tree_layer:
        layer_tree_layer.setItemVisibilityChecked(True)
        parent = layer_tree_layer.parent()
        while parent and parent != root:
            parent.setItemVisibilityChecked(True)
            parent = parent.parent()
    else:
        raise Exception(f"Layer tree layer for '{layer.name()}' not found.")

    # Create the theme from the current state
    map_theme_record = QgsMapThemeCollection.createThemeFromCurrentState(root, QgsLayerTreeModel(root))

    return map_theme_record


def insert_map_theme(theme_name, theme_record, overwrite=False):
    project = QgsProject.instance()
    map_theme_collection = project.mapThemeCollection()

    if map_theme_collection.hasMapTheme(theme_name):
        if overwrite:
            map_theme_collection.removeMapTheme(theme_name)
        else:
            raise Exception(f"Theme '{theme_name}' already exists.")

    map_theme_collection.insert(theme_name, theme_record)


def create_new_themes(
    base_theme_name,
    layer_list_file,
    overwrite=False,
    ignore_errors=False,
    theme_prefix="",
):
    project = QgsProject.instance()
    map_theme_collection = project.mapThemeCollection()

    if not map_theme_collection.hasMapTheme(base_theme_name):
        raise Exception(f"Base theme '{base_theme_name}' not found.")

    base_layer_ids = map_theme_collection.mapThemeVisibleLayerIds(base_theme_name)
    all_layers = project.mapLayers()
    all_layer_ids = [layer.id() for layer in all_layers.values()]

    with open(layer_list_file, "r") as file:
        layer_names = [line.strip() for line in file]

    problem_layers = []
    themes = []

    for layer_name in layer_names:
        layer, error = check_layer_ambiguities(layer_name)
        if error:
            if ignore_errors:
                problem_layers.append(error)
                print(f"skip: {layer_name}")
                continue
            else:
                raise Exception(error)

        try:
            theme_record = create_map_theme_record(base_layer_ids, layer, all_layer_ids)
            theme_name_with_prefix = f"{theme_prefix}{layer_name}"
            insert_map_theme(theme_name_with_prefix, theme_record, overwrite)
            themes.append(theme_name_with_prefix)
            print(f"{theme_name_with_prefix}: {layer_name}")
        except Exception as e:
            print(f"skip: {layer_name} - {str(e)}")
            if ignore_errors:
                problem_layers.append(layer_name)
            else:
                raise e

    if problem_layers:
        print("Problematic layers:")
        for problem in problem_layers:
            print(problem)

    return themes


def check_string_duration(string: str) -> str:
    pattern = r"(?:[_+]|^)(\d{3,5}[mM])(?:[_+]|$)"
    match = re.search(pattern, string, re.IGNORECASE)
    if match:
        return match.group(0).replace("_", "").replace("m", "")
    else:
        raise ValueError(f"Duration pattern not found in the string: {string}")


def check_string_aep(string: str) -> str:
    pattern = r"(?:[_+]|^)(\d{2}\.\d{1,2}p)(?:[_+]|$)"
    match = re.search(pattern, string, re.IGNORECASE)
    if match:
        return match.group(0).replace("_", "").replace("p", "")
    else:
        raise ValueError(f"AEP pattern not found in the string: {string}")


def check_result_type(string: str) -> str:
    result_types = [
        "V_Max",
        "d_Max",
        "h_Max",
        "d_HR_Max",
        "h_HR_Max",
        "dt_Min",
        "TMax_h",
        "DEM_Z",
        "DEM_Z_HR",
    ]
    for result_type in result_types:
        if result_type in string:
            return result_type
    return "Unknown"


def generate_csv_output(themes, output_csv_file, theme_prefix):
    with open(output_csv_file, "w", newline="") as csvfile:
        fieldnames = ["theme_name", "AEP", "duration", "result_type", "DIFF"] + [
            f"part_{i+1}" for i in range(max(len(t.split("_")) for t in themes))
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for theme_name in themes:
            row = {"theme_name": theme_name}
            try:
                row["AEP"] = check_string_aep(theme_name)
            except ValueError:
                row["AEP"] = "N/A"
            try:
                row["duration"] = check_string_duration(theme_name)
            except ValueError:
                row["duration"] = "N/A"
            row["result_type"] = check_result_type(theme_name)
            row["DIFF"] = 1 if "DIFF" in theme_name.split("_") else 0

            parts = re.split(r"[_+]", theme_name)
            for i, part in enumerate(parts):
                row[f"part_{i+1}"] = part

            writer.writerow(row)


# Main execution
main(
    base_theme_name,
    layer_list_file,
    overwrite_existing,
    ignore_errors,
    theme_prefix,
    working_directory,
)
