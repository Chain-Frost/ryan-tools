root = QgsProject.instance().layerTreeRoot()
project = QgsProject.instance()

group_nodes_to_remove: list[QgsLayerTreeGroup] = []
layer_ids_to_remove: list[str] = []


def collect_layer_ids_in_subtree(node: QgsLayerTreeNode) -> list[str]:
    layer_ids: list[str] = []

    if isinstance(node, QgsLayerTreeLayer):
        layer_ids.append(node.layerId())
    elif isinstance(node, QgsLayerTreeGroup):
        for child in node.children():
            layer_ids.extend(collect_layer_ids_in_subtree(child))

    return layer_ids


def walk_group(group: QgsLayerTreeGroup) -> None:
    for child in list(group.children()):
        if isinstance(child, QgsLayerTreeGroup):
            if not child.itemVisibilityChecked():
                group_nodes_to_remove.append(child)
                layer_ids_to_remove.extend(collect_layer_ids_in_subtree(child))
            else:
                walk_group(child)

        elif isinstance(child, QgsLayerTreeLayer):
            if not child.itemVisibilityChecked():
                layer_ids_to_remove.append(child.layerId())


walk_group(root)

# Remove layer objects from the project first
layer_ids_to_remove = list(dict.fromkeys(layer_ids_to_remove))
project.removeMapLayers(layer_ids_to_remove)

# Then remove the unchecked group nodes themselves
for group_node in group_nodes_to_remove:
    parent = group_node.parent()
    if parent is not None:
        parent.removeChildNode(group_node)

print(f"Removed {len(layer_ids_to_remove)} layer(s) and {len(group_nodes_to_remove)} group(s).")