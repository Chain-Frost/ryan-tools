from collections import defaultdict
import os
from pathlib import Path

import processing
from qgis.core import (
    Qgis,
    QgsProcessing,
    QgsProject,
    QgsProviderRegistry,
    QgsVectorFileWriter,
    QgsVectorLayer,
)


project = QgsProject.instance()
transform_context = project.transformContext()

# Groups all loaded line layers by their physical DXF file.
dxf_layers: dict[str, list[QgsVectorLayer]] = defaultdict(list)
dxf_paths: dict[str, Path] = {}

# Prevent duplicate exports where the same DXF sublayer has been loaded twice.
seen_layer_sources: set[str] = set()


def get_physical_path(layer: QgsVectorLayer) -> Path:
    """Extract the physical file path from a QGIS layer source URI."""
    decoded_uri = QgsProviderRegistry.instance().decodeUri(
        layer.providerType(),
        layer.source(),
    )

    path = decoded_uri.get("path")

    if not path:
        # Fallback for sources such as:
        # file.dxf|layername=entities|geometrytype=LineString
        path = layer.source().split("|", maxsplit=1)[0]

    return Path(path)


for layer in project.mapLayers().values():
    if not isinstance(layer, QgsVectorLayer):
        continue

    if not layer.isValid():
        continue

    dxf_path = get_physical_path(layer)

    if dxf_path.suffix.casefold() != ".dxf":
        continue

    # Ignore point, polygon and non-spatial DXF layers.
    if layer.geometryType() != Qgis.GeometryType.Line:
        continue

    source_key = layer.source().casefold()

    if source_key in seen_layer_sources:
        continue

    seen_layer_sources.add(source_key)

    path_key = os.path.normcase(os.path.normpath(str(dxf_path)))

    dxf_layers[path_key].append(layer)
    dxf_paths[path_key] = dxf_path


if not dxf_layers:
    raise RuntimeError(
        "No loaded DXF layers with line geometry were found."
    )


successful: list[Path] = []
failed: list[str] = []


for path_key, line_layers in dxf_layers.items():
    dxf_path = dxf_paths[path_key]

    output_path = dxf_path.with_suffix(".gpkg")
    output_layer_name = dxf_path.stem

    try:
        # Normally each DXF has one loaded line layer. If multiple line
        # sublayers exist, combine them into one output layer.
        if len(line_layers) == 1:
            export_layer = line_layers[0]
        else:
            first_crs = line_layers[0].crs()

            merge_result = processing.run(
                "native:mergevectorlayers",
                {
                    "LAYERS": line_layers,
                    "CRS": first_crs if first_crs.isValid() else None,
                    "OUTPUT": QgsProcessing.TEMPORARY_OUTPUT,
                },
            )

            export_layer = merge_result["OUTPUT"]

        options = QgsVectorFileWriter.SaveVectorOptions()
        options.driverName = "GPKG"
        options.layerName = output_layer_name
        options.fileEncoding = "UTF-8"
        options.actionOnExistingFile = (
            QgsVectorFileWriter.CreateOrOverwriteFile
        )

        result = QgsVectorFileWriter.writeAsVectorFormatV3(
            export_layer,
            str(output_path),
            transform_context,
            options,
        )

        error_code = result[0]
        error_message = result[1] if len(result) > 1 else ""

        if error_code == QgsVectorFileWriter.NoError:
            successful.append(output_path)
            print(f"Created: {output_path}")
        else:
            failed.append(f"{dxf_path}: {error_message}")
            print(f"FAILED: {dxf_path}")
            print(f"        {error_message}")

    except Exception as exception:
        failed.append(f"{dxf_path}: {exception}")
        print(f"FAILED: {dxf_path}")
        print(f"        {exception}")


print()
print("=" * 70)
print(f"GeoPackages created: {len(successful)}")
print(f"Failures:             {len(failed)}")

if failed:
    print()
    print("Failures:")
    for failure in failed:
        print(f"  - {failure}")