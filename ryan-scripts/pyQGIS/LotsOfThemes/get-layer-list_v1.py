from qgis.core import QgsProject  # type: ignore

group_name = "results"  # Replace with your group name
output_file = "layer_list.txt"


def export_layer_list(group_name, output_file):
    project = QgsProject.instance()
    root = project.layerTreeRoot()
    group = root.findGroup(group_name)

    if not group:
        raise Exception(f"Group '{group_name}' not found.")

    layers = group.findLayers()
    layer_names = sorted([layer.layer().name() for layer in layers])

    with open(output_file, "w") as file:
        for name in layer_names:
            file.write(name + "\n")


export_layer_list(group_name, output_file)
print("Complete")
