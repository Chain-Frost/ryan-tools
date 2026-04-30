from __future__ import annotations

from pathlib import Path
import re
import shutil

from qgis.core import (
    QgsLayerDefinition,
    QgsMapLayer,
    QgsProject,
    QgsProviderRegistry,
)


# -----------------------------
# USER SETTINGS
# -----------------------------
THEME_NAME = "Fig 1-1 Proposed Infrastructure"  # change this
OUT_DIR = Path(r"C:\temp\theme_export")  # change this
OVERWRITE = False


def safe_name(name: str) -> str:
    name = re.sub(r'[<>:"/\\|?*]+', "_", name)
    name = re.sub(r"\s+", "_", name).strip("_")
    return name or "layer"


def next_free_path(path: Path) -> Path:
    if OVERWRITE or not path.exists():
        return path

    stem = path.stem
    suffix = path.suffix
    parent = path.parent

    i = 1
    while True:
        candidate = parent / f"{stem}_{i}{suffix}"
        if not candidate.exists():
            return candidate
        i += 1


def copy_file_with_sidecars(src: Path, dst_dir: Path) -> list[Path]:
    """
    Copy a file-based dataset and common sidecars.
    For shapefiles/raster sidecars, copies files sharing the same stem.
    For other single-file sources, copies just the file.
    """
    copied: list[Path] = []

    if not src.exists():
        return copied

    suffix = src.suffix.lower()

    # Shapefile and similar sidecar-heavy formats
    if suffix in {".shp", ".tab", ".mif", ".mid"}:
        for p in src.parent.glob(f"{src.stem}.*"):
            if p.is_file():
                target = next_free_path(dst_dir / p.name)
                shutil.copy2(p, target)
                copied.append(target)
        return copied

    # Common raster sidecars / aux files
    if suffix in {".tif", ".tiff", ".img", ".asc", ".bil"}:
        patterns = [
            src.name,
            f"{src.name}.aux.xml",
            f"{src.stem}.ovr",
            f"{src.name}.ovr",
            f"{src.stem}.tfw",
            f"{src.stem}.tifw",
            f"{src.stem}.prj",
            f"{src.stem}.xml",
        ]
        seen: set[Path] = set()
        for name in patterns:
            p = src.parent / name
            if p.exists() and p.is_file() and p not in seen:
                target = next_free_path(dst_dir / p.name)
                shutil.copy2(p, target)
                copied.append(target)
                seen.add(p)
        return copied

    # Generic single-file copy
    target = next_free_path(dst_dir / src.name)
    shutil.copy2(src, target)
    copied.append(target)
    return copied


def copy_directory_dataset(src_dir: Path, dst_dir: Path) -> Path | None:
    """
    Copy a directory-based dataset, e.g. FileGDB.
    """
    if not src_dir.exists() or not src_dir.is_dir():
        return None

    target = next_free_path(dst_dir / src_dir.name)
    shutil.copytree(src_dir, target)
    return target


def save_qml(layer: QgsMapLayer, qml_path: Path) -> tuple[bool, str]:
    qml_path = next_free_path(qml_path)

    try:
        result = layer.saveNamedStyle(str(qml_path))
        ok = qml_path.exists()

        if isinstance(result, tuple):
            msg = " | ".join(str(x) for x in result)
        else:
            msg = str(result)

        return ok, msg

    except Exception as exc:
        return False, str(exc)


def export_qlr_for_layer(layer: QgsMapLayer, qlr_path: Path) -> tuple[bool, str]:
    root = QgsProject.instance().layerTreeRoot()
    node = root.findLayer(layer.id())
    if node is None:
        return False, "Layer tree node not found"

    error_message = ""
    ok = QgsLayerDefinition.exportLayerDefinition(str(qlr_path), [node], error_message)
    return ok, error_message


def classify_layer_source(layer: QgsMapLayer) -> tuple[str, Path | None]:
    """
    Returns:
      ("file", path)      -> copy file dataset
      ("directory", path) -> copy directory dataset
      ("remote", None)    -> export QLR
      ("unknown", None)   -> export QLR
    """
    provider = layer.providerType() or ""
    uri = layer.source() or ""

    # Memory / virtual / obvious non-file cases
    if provider in {"memory", "virtual", "wms", "arcgismapserver", "arcgisfeatureserver", "vectortile", "postgres", "mssql", "oracle"}:
        return "remote", None

    parts = QgsProviderRegistry.instance().decodeUri(provider, uri) or {}

    path_value = parts.get("path")
    url_value = parts.get("url")

    if url_value and not path_value:
        return "remote", None

    if path_value:
        path = Path(path_value)
        if path.exists():
            if path.is_file():
                return "file", path
            if path.is_dir():
                return "directory", path

    # Fallback: sometimes layer.source() itself is a plain path
    raw_path = Path(uri)
    if raw_path.exists():
        if raw_path.is_file():
            return "file", raw_path
        if raw_path.is_dir():
            return "directory", raw_path

    return "unknown", None


def main() -> None:
    project = QgsProject.instance()
    themes = project.mapThemeCollection()

    if THEME_NAME not in themes.mapThemes():
        raise ValueError(f"Theme not found: {THEME_NAME}. Available: {themes.mapThemes()}")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    layers = themes.mapThemeVisibleLayers(THEME_NAME)

    print(f"Theme: {THEME_NAME}")
    print(f"Output folder: {OUT_DIR}")
    print(f"Visible layers: {len(layers)}")
    print("-" * 80)

    for layer in layers:
        layer_name = layer.name()
        base = safe_name(layer_name)

        print(f"Layer: {layer_name}")
        print(f"  Provider: {layer.providerType()}")
        print(f"  Source:   {layer.source()}")

        source_kind, source_path = classify_layer_source(layer)

        if source_kind == "file" and source_path is not None:
            copied = copy_file_with_sidecars(source_path, OUT_DIR)
            if copied:
                print("  Copied:")
                for p in copied:
                    print(f"    {p}")
                qml_path = next_free_path(OUT_DIR / f"{base}.qml")
                ok, msg = save_qml(layer, qml_path)
                print(f"  QML: {'OK' if ok else 'FAILED'} - {qml_path} {msg}")
            else:
                print("  Copy failed or source missing")
                qlr_path = next_free_path(OUT_DIR / f"{base}.qlr")
                ok, msg = export_qlr_for_layer(layer, qlr_path)
                print(f"  QLR fallback: {'OK' if ok else 'FAILED'} - {qlr_path} {msg}")

        elif source_kind == "directory" and source_path is not None:
            copied_dir = copy_directory_dataset(source_path, OUT_DIR)
            if copied_dir is not None:
                print(f"  Copied directory dataset: {copied_dir}")
                qml_path = next_free_path(OUT_DIR / f"{base}.qml")
                ok, msg = save_qml(layer, qml_path)
                print(f"  QML: {'OK' if ok else 'FAILED'} - {qml_path} {msg}")
            else:
                print("  Directory copy failed")
                qlr_path = next_free_path(OUT_DIR / f"{base}.qlr")
                ok, msg = export_qlr_for_layer(layer, qlr_path)
                print(f"  QLR fallback: {'OK' if ok else 'FAILED'} - {qlr_path} {msg}")

        else:
            qlr_path = next_free_path(OUT_DIR / f"{base}.qlr")
            ok, msg = export_qlr_for_layer(layer, qlr_path)
            print(f"  Exported QLR: {'OK' if ok else 'FAILED'} - {qlr_path} {msg}")

        print()

    print("Done.")


main()