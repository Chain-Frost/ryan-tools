"""Reusable GDAL vector-format metadata, inspection, and translation helpers."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from typing import Any, Literal, NamedTuple

from osgeo import gdal  # type: ignore

VectorFormat = Literal["fgb", "geojson", "gpkg", "shp", "sqlite", "dxf", "kml"]


class VectorFormatSpec(NamedTuple):
    """GDAL driver metadata for a supported vector output format."""

    driver: str
    extension: str
    supports_multiple_layers: bool


VECTOR_FORMATS: dict[VectorFormat, VectorFormatSpec] = {
    "fgb": VectorFormatSpec(driver="FlatGeobuf", extension=".fgb", supports_multiple_layers=False),
    "geojson": VectorFormatSpec(driver="GeoJSON", extension=".geojson", supports_multiple_layers=False),
    "gpkg": VectorFormatSpec(driver="GPKG", extension=".gpkg", supports_multiple_layers=True),
    "shp": VectorFormatSpec(driver="ESRI Shapefile", extension=".shp", supports_multiple_layers=False),
    "sqlite": VectorFormatSpec(driver="SQLite", extension=".sqlite", supports_multiple_layers=True),
    "dxf": VectorFormatSpec(driver="DXF", extension=".dxf", supports_multiple_layers=True),
    "kml": VectorFormatSpec(driver="KML", extension=".kml", supports_multiple_layers=True),
}


def resolve_vector_format(value: str) -> tuple[VectorFormat, VectorFormatSpec]:
    """Normalize a format name or extension and return its canonical metadata."""
    normalized = value.lower().lstrip(".")
    if normalized not in VECTOR_FORMATS:
        supported = ", ".join(VECTOR_FORMATS)
        raise ValueError(f"Unsupported vector format '{value}'. Choose from: {supported}")
    return normalized, VECTOR_FORMATS[normalized]


def require_vector_driver(vector_format: str) -> tuple[VectorFormat, VectorFormatSpec]:
    """Resolve a vector format and verify that its GDAL driver is available."""
    normalized, spec = resolve_vector_format(vector_format)
    with gdal.ExceptionMgr():  # type: ignore
        if gdal.GetDriverByName(spec.driver) is None:  # type: ignore
            raise RuntimeError(f"The active GDAL installation does not provide the '{spec.driver}' driver")
    return normalized, spec


def get_vector_layer_names(source: str | Path) -> list[str]:
    """Return vector layer names in source order."""
    source_path = Path(source).resolve()
    with gdal.ExceptionMgr():  # type: ignore
        source_dataset = gdal.OpenEx(str(source_path), gdal.OF_VECTOR)  # type: ignore
        if source_dataset is None:
            raise RuntimeError(f"GDAL could not open vector dataset {source_path}")
        try:
            layer_names: list[str] = []
            for index in range(int(source_dataset.GetLayerCount())):  # type: ignore
                layer = source_dataset.GetLayerByIndex(index)  # type: ignore
                if layer is None:
                    raise RuntimeError(f"GDAL could not read layer index {index} from {source_path}")
                layer_names.append(str(layer.GetName()))  # type: ignore
            return layer_names
        finally:
            source_dataset = None


def get_unique_attribute_values(source: str | Path, layer_name: str, attribute_name: str) -> list[str]:
    """Return a list of unique values for an attribute in a vector layer."""
    source_path = Path(source).resolve()
    with gdal.ExceptionMgr():  # type: ignore
        source_dataset = gdal.OpenEx(str(source_path), gdal.OF_VECTOR)  # type: ignore
        if source_dataset is None:
            raise RuntimeError(f"GDAL could not open vector dataset {source_path}")
        try:
            layer = source_dataset.GetLayerByName(layer_name)  # type: ignore
            if layer is None:
                raise RuntimeError(f"GDAL could not read layer '{layer_name}' from {source_path}")
            
            # Use executeSQL to get distinct values
            sql = f'SELECT DISTINCT "{attribute_name}" FROM "{layer_name}"'
            result_layer = source_dataset.ExecuteSQL(sql) # type: ignore
            
            if result_layer is None:
                raise RuntimeError(f"Failed to execute SQL or attribute '{attribute_name}' does not exist.")
            
            values: list[str] = []
            for feature in result_layer:  # type: ignore
                val = feature.GetFieldAsString(0) # type: ignore
                if val:
                    values.append(val)
                    
            source_dataset.ReleaseResultSet(result_layer) # type: ignore
            return sorted(values)
        finally:
            source_dataset = None


def translate_vector_dataset(
    source: str | Path,
    output: str | Path,
    *,
    vector_format: str,
    layer_name: str | None = None,
    src_srs: str | None = None,
    dst_srs: str | None = None,
    where: str | None = None,
) -> tuple[Path, ...]:
    """Translate one vector layer or a complete dataset and atomically publish its files.

    Existing outputs are never overwritten. Multi-file formats such as Shapefile
    are built together in a temporary directory before their component files are
    moved into the destination directory.
    """
    source_path = Path(source).resolve()
    output_path = Path(output).resolve()
    _normalized, spec = require_vector_driver(vector_format)
    if not source_path.exists():
        raise FileNotFoundError(f"Source vector dataset does not exist: {source_path}")
    if output_path.exists():
        raise FileExistsError(f"Output already exists: {output_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_directory = Path(tempfile.mkdtemp(prefix=f".{output_path.stem}.converting-", dir=output_path.parent))
    temporary_output = temporary_directory / output_path.name
    published_files: list[Path] = []

    try:
        options: Any = gdal.VectorTranslateOptions(  # type: ignore
            format=spec.driver,
            layers=[layer_name] if layer_name is not None else None,
            srcSRS=src_srs,
            dstSRS=dst_srs,
            where=where,
        )
        with gdal.ExceptionMgr():  # type: ignore
            output_dataset = gdal.VectorTranslate(  # type: ignore
                str(temporary_output),
                str(source_path),
                options=options,
            )
            if output_dataset is None:
                raise RuntimeError("GDAL did not create an output dataset")
            output_dataset.FlushCache()  # type: ignore
            output_dataset = None

        generated_files = [path for path in temporary_directory.iterdir() if path.is_file()]
        if not generated_files:
            raise RuntimeError("GDAL reported success but produced no output files")

        for generated_file in generated_files:
            published_file = output_path.parent / generated_file.name
            generated_file.replace(published_file)
            published_files.append(published_file)
        if not output_path.is_file():
            raise RuntimeError(f"GDAL did not produce the expected primary output {output_path}")
        return tuple(published_files)
    except Exception as exc:
        cleanup_failures: list[str] = []
        for published_file in published_files:
            try:
                published_file.unlink(missing_ok=True)
            except OSError:
                cleanup_failures.append(str(published_file))
        if cleanup_failures:
            raise RuntimeError(f"{exc}; could not remove partial outputs: {', '.join(cleanup_failures)}") from exc
        raise
    finally:
        shutil.rmtree(temporary_directory, ignore_errors=True)
