# Run in the QGIS Python Console

from __future__ import annotations

from pathlib import Path
from qgis.core import QgsProject, QgsProviderRegistry

# EPSG codes to hide
EXCLUDE_EPSG: set[int] = {28350, 4283}


def _epsg_code(layer) -> int | None:
    crs = layer.crs()
    auth = (crs.authid() or "").strip()  # e.g. "EPSG:7855"
    if auth.upper().startswith("EPSG:"):
        try:
            return int(auth.split(":", 1)[1])
        except ValueError:
            return None
    return None


def _crs_label(layer) -> str:
    crs = layer.crs()
    auth = (crs.authid() or "").strip()
    if auth:
        return auth
    desc = (crs.description() or "").strip()
    return desc if desc else "<unknown>"


def _source_filename(layer) -> str:
    """
    Try to get the underlying file name for the layer.
    Falls back to the layer name if it cannot be resolved.
    """
    provider = layer.providerType()
    uri = layer.dataProvider().dataSourceUri()

    # Try provider metadata decode (works for ogr/gdal etc.)
    md = QgsProviderRegistry.instance().providerMetadata(provider)
    if md is not None and hasattr(md, "decodeUri"):
        try:
            parts = md.decodeUri(uri)
            for key in ("path", "url", "file"):
                if key in parts and parts[key]:
                    return Path(parts[key]).name
        except Exception:
            pass

    # Fallback: strip QGIS options from URI and take last path component
    base = uri.split("|", 1)[0].split("?", 1)[0]
    name = Path(base).name
    return name if name else layer.name()


def list_current_layers_crs(
    exclude_epsg: set[int] = EXCLUDE_EPSG,
) -> None:
    """
    List file name, layer name, and CRS for layers in the current QGIS project,
    skipping those whose CRS EPSG code is in exclude_epsg.
    """
    project = QgsProject.instance()

    for layer in project.mapLayers().values():
        code = _epsg_code(layer)
        if code is not None and code in exclude_epsg:
            continue

        file_name = _source_filename(layer)
        crs_label = _crs_label(layer)
        print(f"{file_name}\t{layer.name()}\t{crs_label}")


# --- Usage ---
# Just run:
list_current_layers_crs()
# Or with a different exclusion list:
# list_current_layers_crs(exclude_epsg={28350, 4283, 7844})
