from __future__ import annotations

from pathlib import Path
import re

from qgis.core import (
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsProject,
    QgsVectorFileWriter,
    QgsVectorLayer,
)


OUTPUT_DIR = Path(r"C:\temp\qgis_shp_export")
TARGET_EPSG = "EPSG:28351"


def safe_layer_stem(name: str) -> str:
    """
    Convert a layer name into a filesystem-safe shapefile stem.
    """
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", name).strip()
    cleaned = re.sub(r"\s+", "_", cleaned)
    cleaned = cleaned.rstrip(". ")
    return cleaned or "layer"


def unique_path(folder: Path, stem: str) -> Path:
    """
    Return a unique .shp output path.
    """
    candidate = folder / f"{stem}.shp"
    i = 1
    while candidate.exists():
        candidate = folder / f"{stem}_{i}.shp"
        i += 1
    return candidate


def is_memory_layer(layer: QgsVectorLayer) -> bool:
    """
    Return True for memory provider layers.
    """
    return layer.providerType().lower() == "memory"


def is_spatial_vector_layer(layer: QgsVectorLayer) -> bool:
    """
    Return True if the layer is a valid spatial vector layer.
    """
    if not layer.isValid():
        return False
    return layer.geometryType() != -1


def sanitize_field_names(layer: QgsVectorLayer) -> list[str]:
    """
    Build shapefile-safe field names:
    - max 10 chars
    - letters/digits/underscore only
    - must be unique
    - cannot start with a digit
    """
    used: set[str] = set()
    out: list[str] = []

    for field in layer.fields():
        original = field.name().strip()

        # Replace invalid chars with underscore
        name = re.sub(r"[^A-Za-z0-9_]", "_", original)

        # Collapse repeated underscores
        name = re.sub(r"_+", "_", name).strip("_")

        # Shapefile field names should not start with a digit
        if not name:
            name = "field"
        if name[0].isdigit():
            name = f"f_{name}"

        # Keep within 10 chars and make unique
        base = name[:10] or "field"
        candidate = base
        suffix = 1

        while candidate.lower() in used:
            suffix_text = str(suffix)
            candidate = f"{base[:10 - len(suffix_text)]}{suffix_text}"
            suffix += 1

        used.add(candidate.lower())
        out.append(candidate)

    return out


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    project = QgsProject.instance()
    transform_context = project.transformContext()
    target_crs = QgsCoordinateReferenceSystem(TARGET_EPSG)

    if not target_crs.isValid():
        raise ValueError(f"Invalid target CRS: {TARGET_EPSG}")

    exported = 0
    skipped = 0
    failed = 0

    print(f"Export folder: {OUTPUT_DIR}")
    print(f"Target CRS:    {TARGET_EPSG}")
    print("-" * 100)

    for layer in project.mapLayers().values():
        if not isinstance(layer, QgsVectorLayer):
            continue

        layer_name = layer.name()

        if not is_spatial_vector_layer(layer):
            print(f"SKIP {layer_name} (invalid or non-spatial)")
            skipped += 1
            continue

        if is_memory_layer(layer):
            print(f"SKIP {layer_name} (memory layer)")
            skipped += 1
            continue

        source_crs = layer.crs()
        source_authid = source_crs.authid() or "unknown"

        if not source_crs.isValid():
            print(f"SKIP {layer_name} (invalid source CRS)")
            skipped += 1
            continue

        out_path = unique_path(OUTPUT_DIR, safe_layer_stem(layer_name))
        field_names = sanitize_field_names(layer)

        options = QgsVectorFileWriter.SaveVectorOptions()
        options.driverName = "ESRI Shapefile"
        options.fileEncoding = "UTF-8"
        options.attributesExportNames = field_names
        options.destCRS = target_crs

        # Explicit transform: this is the important part for reprojection
        options.ct = QgsCoordinateTransform(source_crs, target_crs, project)

        error_code, error_message, new_filename, _new_layer = (
            QgsVectorFileWriter.writeAsVectorFormatV3(
                layer,
                str(out_path),
                transform_context,
                options,
            )
        )

        if error_code == QgsVectorFileWriter.NoError:
            written_name = Path(new_filename).name if new_filename else out_path.name
            reproj_note = (
                "reprojected"
                if source_crs != target_crs
                else "already in target CRS"
            )
            print(
                f"OK   {layer_name} -> {written_name} "
                f"({source_authid} -> {TARGET_EPSG}, {reproj_note})"
            )
            exported += 1
        else:
            print(f"FAIL {layer_name}: {error_message}")
            failed += 1

    print("-" * 100)
    print(f"Exported: {exported}")
    print(f"Skipped:  {skipped}")
    print(f"Failed:   {failed}")
    print(f"Output:   {OUTPUT_DIR}")


main()