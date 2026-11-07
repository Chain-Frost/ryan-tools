"""Convert DXF or DWG drawings to geometry-separated GeoPackages.

Each input produces a same-named ``.gpkg`` beside the drawing by default. The
GeoPackage contains ``areas``, ``lines``, and ``points`` layers. CAD attributes,
the source dataset layer, source feature ID, geometry-part number, Z values, and
OGR style strings are retained where available.

Dependencies and environment:
- Python 3.14 with the GDAL/OGR Python bindings.
- DXF is read directly by GDAL.
- A DWG version unsupported by GDAL can be exported to a temporary AutoCAD 2013
  DXF when ``--autocad-export`` is deliberately enabled. This requires an
  installed AutoCAD or DWG TrueView ``accoreconsole.exe``.
- CAD readers do not provide reliable CRS metadata. Coordinates are preserved
  unchanged; ``--crs`` assigns metadata only and does not transform them.

Examples:
    python cad_to_gpkg.py "D:\\CAD\\services.dxf"
    python cad_to_gpkg.py "D:\\CAD\\services.dwg" --autocad-export
    python cad_to_gpkg.py drawing.dxf --crs EPSG:7850 --overwrite

Existing outputs are never replaced unless ``--overwrite`` is supplied. Each
conversion is built and validated in a temporary file before publication.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path

SCRIPT_VERSION = "2026-11-04.1"

# CLI arguments override these editable settings. Relative paths use the
# current working directory. None for OUTPUT_DIRECTORY writes beside each CAD.
INPUT_PATHS: tuple[Path, ...] = ()
OUTPUT_DIRECTORY: Path | None = None
ASSIGNED_CRS: str | None = None
CLOSED_LINES_AS_AREAS = True
OVERWRITE = False

# DWG fallback launches an installed Autodesk process and is disabled until the
# user explicitly opts in here or with --autocad-export.
AUTOCAD_EXPORT = False
AUTOCAD_CONSOLE: Path | None = None
AUTOCAD_TIMEOUT_SECONDS = 300

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
from contextlib import contextmanager
from typing import Any

from osgeo import gdal, ogr, osr

SUPPORTED_SUFFIXES = frozenset({".dxf", ".dwg"})
OUTPUT_LAYER_NAMES = {0: "points", 1: "lines", 2: "areas"}
RESERVED_FIELD_NAMES = frozenset({"cad_source_layer", "cad_source_fid", "cad_part", "cad_style"})
LIST_FIELD_TYPES = frozenset({ogr.OFTIntegerList, ogr.OFTInteger64List, ogr.OFTRealList, ogr.OFTStringList})
GPKG_FIELD_TYPES = frozenset(
    {
        ogr.OFTInteger,
        ogr.OFTInteger64,
        ogr.OFTReal,
        ogr.OFTString,
        ogr.OFTBinary,
        ogr.OFTDate,
        ogr.OFTTime,
        ogr.OFTDateTime,
    }
)

gdal.DontUseExceptions()
ogr.DontUseExceptions()


class ConversionError(RuntimeError):
    """Raised for an expected CAD conversion failure."""


@dataclass(frozen=True)
class ConversionOptions:
    """Validated operational settings for one invocation."""

    input_paths: tuple[Path, ...]
    output_directory: Path | None
    assigned_crs: str | None
    closed_lines_as_areas: bool
    overwrite: bool
    autocad_export: bool
    autocad_console: Path | None
    autocad_timeout_seconds: int


@dataclass(frozen=True)
class FieldSpec:
    """One output attribute field shared by all geometry layers."""

    name: str
    field_type: int
    field_subtype: int
    width: int
    precision: int


@dataclass(frozen=True)
class ConversionResult:
    """Summary of one successfully published GeoPackage."""

    source: Path
    output: Path
    counts: dict[str, int]


@contextmanager
def _gdal_config_option(name: str, value: str) -> Iterator[None]:
    previous_value: str | None = gdal.GetConfigOption(name)
    gdal.SetConfigOption(name, value)
    try:
        yield
    finally:
        gdal.SetConfigOption(name, previous_value)


def _absolute_from(path: Path, base_directory: Path) -> Path:
    candidate = path if path.is_absolute() else base_directory / path
    return candidate.absolute()


def _validate_options(  # noqa: C901 - Keep related input and output safeguards together.
    *,
    input_paths: Sequence[Path],
    output_directory: Path | None,
    assigned_crs: str | None,
    closed_lines_as_areas: bool,
    overwrite: bool,
    autocad_export: bool,
    autocad_console: Path | None,
    autocad_timeout_seconds: int,
) -> ConversionOptions:
    base_directory = Path.cwd().absolute()
    absolute_inputs = tuple(_absolute_from(Path(path), base_directory) for path in input_paths)
    if not absolute_inputs:
        message = "At least one .dxf or .dwg input is required."
        raise ConversionError(message)
    if autocad_timeout_seconds <= 0:
        message = f"AUTOCAD_TIMEOUT_SECONDS must be greater than zero, not {autocad_timeout_seconds}."
        raise ConversionError(message)

    for input_path in absolute_inputs:
        if input_path.suffix.lower() not in SUPPORTED_SUFFIXES:
            message = f"Unsupported input extension for {input_path}; expected .dxf or .dwg."
            raise ConversionError(message)
        if not input_path.is_file():
            message = f"Input file does not exist: {input_path}"
            raise ConversionError(message)

    absolute_output_directory = (
        None if output_directory is None else _absolute_from(Path(output_directory), base_directory)
    )
    if absolute_output_directory is not None and not absolute_output_directory.is_dir():
        message = f"Output directory does not exist: {absolute_output_directory}"
        raise ConversionError(message)

    absolute_autocad_console = (
        None if autocad_console is None else _absolute_from(Path(autocad_console), base_directory)
    )
    if absolute_autocad_console is not None and not absolute_autocad_console.is_file():
        message = f"AutoCAD console does not exist: {absolute_autocad_console}"
        raise ConversionError(message)

    normalized_crs = assigned_crs.strip() if assigned_crs is not None else None
    if normalized_crs == "":
        message = "ASSIGNED_CRS cannot be blank; use None for no assigned CRS or a value such as EPSG:7850."
        raise ConversionError(message)
    if normalized_crs is not None:
        _spatial_reference(normalized_crs)

    outputs = tuple(
        (absolute_output_directory or source.parent) / source.with_suffix(".gpkg").name for source in absolute_inputs
    )
    duplicate_outputs = {path for path in outputs if outputs.count(path) > 1}
    if duplicate_outputs:
        formatted = ", ".join(str(path) for path in sorted(duplicate_outputs))
        message = f"Multiple inputs would write the same output: {formatted}"
        raise ConversionError(message)
    if not overwrite:
        existing_outputs = [path for path in outputs if path.exists()]
        if existing_outputs:
            formatted = ", ".join(str(path) for path in existing_outputs)
            message = f"Output already exists; use --overwrite to replace it: {formatted}"
            raise ConversionError(message)

    return ConversionOptions(
        input_paths=absolute_inputs,
        output_directory=absolute_output_directory,
        assigned_crs=normalized_crs,
        closed_lines_as_areas=closed_lines_as_areas,
        overwrite=overwrite,
        autocad_export=autocad_export,
        autocad_console=absolute_autocad_console,
        autocad_timeout_seconds=autocad_timeout_seconds,
    )


def _spatial_reference(crs: str) -> osr.SpatialReference:
    spatial_reference = osr.SpatialReference()
    if spatial_reference.SetFromUserInput(crs) != ogr.OGRERR_NONE:
        message = f"Could not parse CRS {crs!r}; use a value such as EPSG:7850."
        raise ConversionError(message)
    spatial_reference.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return spatial_reference


def _open_vector(path: Path, *, closed_lines_as_areas: bool) -> gdal.Dataset | None:
    option_value = "TRUE" if closed_lines_as_areas else "FALSE"
    gdal.ErrorReset()
    with _gdal_config_option("DXF_CLOSED_LINE_AS_POLYGON", option_value), _gdal_config_option(
        "DWG_CLOSED_LINE_AS_POLYGON", option_value
    ):
        gdal.PushErrorHandler("CPLQuietErrorHandler")
        try:
            dataset = gdal.OpenEx(str(path), gdal.OF_VECTOR | gdal.OF_READONLY)
        finally:
            gdal.PopErrorHandler()
    return dataset


def _find_autocad_console(configured_path: Path | None) -> Path:
    if configured_path is not None:
        return configured_path

    autocad_candidates: list[Path] = []
    trueview_candidates: list[Path] = []
    for environment_name in ("ProgramFiles", "ProgramFiles(x86)"):
        program_files = os.environ.get(environment_name)
        if not program_files:
            continue
        autodesk_directory = Path(program_files) / "Autodesk"
        autocad_candidates.extend(autodesk_directory.glob("AutoCAD */accoreconsole.exe"))
        trueview_candidates.extend(autodesk_directory.glob("DWG TrueView */accoreconsole.exe"))
        trueview_candidates.extend(autodesk_directory.glob("DWG TrueView * - */accoreconsole.exe"))

    # Full AutoCAD has a per-user writable profile and is more reliable for
    # headless export than TrueView's machine-wide configuration.
    existing_autocad = sorted(
        (candidate.absolute() for candidate in autocad_candidates if candidate.is_file()), reverse=True
    )
    existing_trueview = sorted(
        (candidate.absolute() for candidate in trueview_candidates if candidate.is_file()), reverse=True
    )
    existing_candidates = [*existing_autocad, *existing_trueview]
    if not existing_candidates:
        message = (
            "No AutoCAD accoreconsole.exe was found. Install AutoCAD/DWG TrueView or provide "
            "--autocad-console, then retry with --autocad-export."
        )
        raise ConversionError(message)
    return existing_candidates[0]


def _decode_process_output(value: bytes) -> str:
    if not value:
        return ""
    encoding = "utf-16-le" if b"\0" in value[:100] else "utf-8"
    return value.decode(encoding, errors="replace").lstrip("\ufeff")


def _export_dwg_to_dxf(
    source: Path,
    destination: Path,
    *,
    autocad_console: Path | None,
    timeout_seconds: int,
) -> None:
    console_path = _find_autocad_console(autocad_console)
    script_path = destination.with_suffix(".scr")
    autocad_destination = destination.as_posix().replace('"', '\\"')
    script_path.write_text(
        '(setvar "FILEDIA" 0)\n'
        f'(command "_.SAVEAS" "_DXF" "_Version" "2013" "16" "{autocad_destination}")\n',
        encoding="ascii",
    )
    command = [str(console_path), "/i", str(source), "/s", str(script_path), "/l", "en-US"]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        message = f"AutoCAD export timed out after {timeout_seconds} seconds: {source}"
        raise ConversionError(message) from exc
    except OSError as exc:
        message = f"Could not launch AutoCAD console {console_path}: {exc}"
        raise ConversionError(message) from exc

    if completed.returncode != 0 or not destination.is_file() or destination.stat().st_size == 0:
        decoded_output = (_decode_process_output(completed.stdout), _decode_process_output(completed.stderr))
        diagnostic = "\n".join(part.strip() for part in decoded_output if part.strip())
        if len(diagnostic) > 4_000:
            diagnostic = diagnostic[-4_000:]
        message = f"AutoCAD could not export {source} to DXF (exit {completed.returncode})."
        if diagnostic:
            message = f"{message}\n{diagnostic}"
        raise ConversionError(message)


def _source_field_name(field_name: str) -> str:
    return f"source_{field_name}" if field_name.casefold() in RESERVED_FIELD_NAMES else field_name


def _compatible_field_type(field_type: int) -> int:
    if field_type in LIST_FIELD_TYPES or field_type not in GPKG_FIELD_TYPES:
        return ogr.OFTString
    return field_type


def _collect_field_specs(dataset: gdal.Dataset) -> list[FieldSpec]:
    by_name: dict[str, FieldSpec] = {}
    for layer_index in range(dataset.GetLayerCount()):
        layer = dataset.GetLayerByIndex(layer_index)
        definition = layer.GetLayerDefn()
        for field_index in range(definition.GetFieldCount()):
            source_definition = definition.GetFieldDefn(field_index)
            name = _source_field_name(source_definition.GetName())
            key = name.casefold()
            field_type = _compatible_field_type(source_definition.GetType())
            existing = by_name.get(key)
            if existing is not None and existing.field_type != field_type:
                by_name[key] = FieldSpec(existing.name, ogr.OFTString, ogr.OFSTNone, 0, 0)
                continue
            if existing is None:
                subtype = source_definition.GetSubType() if field_type == source_definition.GetType() else ogr.OFSTNone
                by_name[key] = FieldSpec(
                    name=name,
                    field_type=field_type,
                    field_subtype=subtype,
                    width=source_definition.GetWidth(),
                    precision=source_definition.GetPrecision(),
                )
    return list(by_name.values())


def _create_output_layer(
    dataset: gdal.Dataset,
    name: str,
    spatial_reference: osr.SpatialReference | None,
    field_specs: Sequence[FieldSpec],
) -> ogr.Layer:
    layer = dataset.CreateLayer(name, spatial_reference, ogr.wkbUnknown, options=["SPATIAL_INDEX=YES"])
    if layer is None:
        message = f"Could not create output layer {name!r}."
        raise ConversionError(message)

    standard_fields = (
        ogr.FieldDefn("cad_source_layer", ogr.OFTString),
        ogr.FieldDefn("cad_source_fid", ogr.OFTInteger64),
        ogr.FieldDefn("cad_part", ogr.OFTInteger),
        ogr.FieldDefn("cad_style", ogr.OFTString),
    )
    for definition in standard_fields:
        if layer.CreateField(definition) != ogr.OGRERR_NONE:
            message = f"Could not create field {definition.GetName()!r} in layer {name!r}."
            raise ConversionError(message)
    for spec in field_specs:
        definition = ogr.FieldDefn(spec.name, spec.field_type)
        definition.SetSubType(spec.field_subtype)
        definition.SetWidth(spec.width)
        definition.SetPrecision(spec.precision)
        if layer.CreateField(definition) != ogr.OGRERR_NONE:
            message = f"Could not create field {spec.name!r} in layer {name!r}."
            raise ConversionError(message)
    return layer


def _iter_geometry_parts(geometry: ogr.Geometry) -> Iterator[tuple[int, ogr.Geometry]]:
    flat_type = ogr.GT_Flatten(geometry.GetGeometryType())
    if flat_type == ogr.wkbGeometryCollection:
        for index in range(geometry.GetGeometryCount()):
            child = geometry.GetGeometryRef(index)
            if child is not None:
                yield from _iter_geometry_parts(child)
        return

    dimension = geometry.GetDimension()
    if dimension not in OUTPUT_LAYER_NAMES:
        message = f"Unsupported geometry dimension {dimension}: {geometry.GetGeometryName()}"
        raise ConversionError(message)
    yield dimension, geometry.Clone()


def _string_value(value: Any) -> str:
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, (list, tuple)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _copy_attributes(
    source_feature: ogr.Feature,
    output_feature: ogr.Feature,
    output_field_types: dict[str, int],
) -> None:
    source_definition = source_feature.GetDefnRef()
    for field_index in range(source_definition.GetFieldCount()):
        if not source_feature.IsFieldSetAndNotNull(field_index):
            continue
        source_field = source_definition.GetFieldDefn(field_index)
        output_name = _source_field_name(source_field.GetName())
        value = source_feature.GetField(field_index)
        if output_field_types[output_name.casefold()] == ogr.OFTString and source_field.GetType() != ogr.OFTString:
            value = _string_value(value)
        output_feature.SetField(output_name, value)


def _write_geopackage(  # noqa: C901 - Preserve transaction and geometry-handling logic.
    source_dataset: gdal.Dataset,
    temporary_output: Path,
    *,
    assigned_crs: str | None,
) -> dict[str, int]:
    driver = gdal.GetDriverByName("GPKG")
    if driver is None:
        message = "The installed GDAL build does not provide the GPKG driver."
        raise ConversionError(message)
    output_dataset = driver.Create(str(temporary_output), 0, 0, 0, gdal.GDT_Unknown)
    if output_dataset is None:
        message = f"Could not create temporary GeoPackage: {temporary_output}"
        raise ConversionError(message)

    field_specs = _collect_field_specs(source_dataset)
    output_field_types = {spec.name.casefold(): spec.field_type for spec in field_specs}
    spatial_reference = None if assigned_crs is None else _spatial_reference(assigned_crs)
    layers = {
        dimension: _create_output_layer(output_dataset, name, spatial_reference, field_specs)
        for dimension, name in OUTPUT_LAYER_NAMES.items()
    }
    counts = dict.fromkeys(OUTPUT_LAYER_NAMES.values(), 0)

    transaction_active = False
    try:
        if output_dataset.StartTransaction() != ogr.OGRERR_NONE:
            message = "Could not start the GeoPackage output transaction."
            raise ConversionError(message)
        transaction_active = True
        for layer_index in range(source_dataset.GetLayerCount()):
            source_layer = source_dataset.GetLayerByIndex(layer_index)
            source_layer.ResetReading()
            for source_feature in source_layer:
                source_geometry = source_feature.GetGeometryRef()
                if source_geometry is None or source_geometry.IsEmpty():
                    continue
                for part_number, (dimension, geometry) in enumerate(_iter_geometry_parts(source_geometry), start=1):
                    output_layer = layers[dimension]
                    output_feature = ogr.Feature(output_layer.GetLayerDefn())
                    output_feature.SetField("cad_source_layer", source_layer.GetName())
                    source_fid = source_feature.GetFID()
                    if source_fid >= 0:
                        output_feature.SetField("cad_source_fid", source_fid)
                    output_feature.SetField("cad_part", part_number)
                    style = source_feature.GetStyleString()
                    if style:
                        output_feature.SetField("cad_style", style)
                    _copy_attributes(source_feature, output_feature, output_field_types)
                    output_feature.SetGeometryDirectly(geometry)
                    if output_layer.CreateFeature(output_feature) != ogr.OGRERR_NONE:
                        message = (
                            f"Could not write {OUTPUT_LAYER_NAMES[dimension]} geometry from "
                            f"{source_layer.GetName()} feature {source_fid}."
                        )
                        raise ConversionError(message)
                    counts[OUTPUT_LAYER_NAMES[dimension]] += 1
        if output_dataset.CommitTransaction() != ogr.OGRERR_NONE:
            message = "Could not commit the GeoPackage output transaction."
            raise ConversionError(message)
        transaction_active = False
    except Exception:
        if transaction_active:
            output_dataset.RollbackTransaction()
        raise
    finally:
        layers.clear()
        output_dataset.Close()

    return counts


def _validate_geopackage(path: Path, expected_counts: dict[str, int]) -> None:
    dataset = gdal.OpenEx(
        str(path),
        gdal.OF_VECTOR | gdal.OF_READONLY,
        allowed_drivers=["GPKG"],
        open_options=["IMMUTABLE=YES"],
    )
    if dataset is None:
        message = f"GDAL could not reopen the completed GeoPackage: {path}"
        raise ConversionError(message)
    try:
        for layer_name, expected_count in expected_counts.items():
            layer = dataset.GetLayerByName(layer_name)
            if layer is None:
                message = f"Completed GeoPackage is missing layer {layer_name!r}: {path}"
                raise ConversionError(message)
            actual_count = layer.GetFeatureCount(force=1)
            if actual_count != expected_count:
                message = (
                    f"Layer {layer_name!r} validation failed for {path}: "
                    f"expected {expected_count} features, found {actual_count}."
                )
                raise ConversionError(message)
    finally:
        dataset.Close()


def _file_digest(path: Path) -> bytes:
    digest = hashlib.sha256()
    with path.open("rb") as file_handle:
        while chunk := file_handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.digest()


def _copy_to_destination_staging(
    local_output: Path,
    destination_staging: Path,
) -> None:
    try:
        shutil.copyfile(local_output, destination_staging)
    except OSError as exc:
        message = f"Could not copy the validated GeoPackage to {destination_staging}: {exc}"
        raise ConversionError(message) from exc
    if local_output.stat().st_size != destination_staging.stat().st_size:
        message = f"Copied GeoPackage size verification failed: {destination_staging}"
        raise ConversionError(message)
    if _file_digest(local_output) != _file_digest(destination_staging):
        message = f"Copied GeoPackage hash verification failed: {destination_staging}"
        raise ConversionError(message)


def _publish_temporary_output(temporary_output: Path, output: Path, *, overwrite: bool) -> None:
    if overwrite:
        temporary_output.replace(output)
        return
    if output.exists():
        message = f"Output appeared during conversion and was not replaced: {output}"
        raise ConversionError(message)
    try:
        if os.name == "nt":
            # Windows rename refuses an existing destination; keep this for SMB shares.
            temporary_output.rename(output)
        else:
            # POSIX rename replaces an existing file. Hard-link creation is no-clobber.
            os.link(temporary_output, output)
            temporary_output.unlink()
    except FileExistsError as exc:
        message = f"Output appeared during conversion and was not replaced: {output}"
        raise ConversionError(message) from exc


def convert_drawing(source: Path, options: ConversionOptions) -> ConversionResult:
    """Convert one validated CAD input and atomically publish its GeoPackage."""
    output = (options.output_directory or source.parent) / source.with_suffix(".gpkg").name
    destination_staging = output.with_name(f".{output.stem}.{uuid.uuid4().hex}.tmp.gpkg")
    source_dataset: gdal.Dataset | None = None
    try:
        with tempfile.TemporaryDirectory(prefix="cad_to_gpkg_stage_") as temporary_directory:
            local_output = Path(temporary_directory) / output.name
            source_dataset = _open_vector(source, closed_lines_as_areas=options.closed_lines_as_areas)
            if source_dataset is None and source.suffix.lower() == ".dwg":
                if not options.autocad_export:
                    detail = gdal.GetLastErrorMsg().strip()
                    message = (
                        f"GDAL cannot read this DWG: {source}. Retry with --autocad-export to allow a temporary "
                        "AutoCAD DXF export."
                    )
                    if detail:
                        message = f"{message} GDAL reported: {detail}"
                    raise ConversionError(message)
                intermediate_dxf = Path(temporary_directory) / f"{source.stem}.dxf"
                _export_dwg_to_dxf(
                    source,
                    intermediate_dxf,
                    autocad_console=options.autocad_console,
                    timeout_seconds=options.autocad_timeout_seconds,
                )
                source_dataset = _open_vector(
                    intermediate_dxf,
                    closed_lines_as_areas=options.closed_lines_as_areas,
                )
                if source_dataset is None:
                    detail = gdal.GetLastErrorMsg().strip()
                    message = f"GDAL could not read AutoCAD's temporary DXF export for {source}."
                    if detail:
                        message = f"{message} GDAL reported: {detail}"
                    raise ConversionError(message)
            elif source_dataset is None:
                detail = gdal.GetLastErrorMsg().strip()
                message = f"GDAL could not read vector data from {source}."
                if detail:
                    message = f"{message} GDAL reported: {detail}"
                raise ConversionError(message)

            counts = _write_geopackage(source_dataset, local_output, assigned_crs=options.assigned_crs)
            source_dataset.Close()
            source_dataset = None
            _validate_geopackage(local_output, counts)
            _copy_to_destination_staging(local_output, destination_staging)
        _publish_temporary_output(destination_staging, output, overwrite=options.overwrite)
        return ConversionResult(source=source, output=output, counts=counts)
    finally:
        if source_dataset is not None:
            source_dataset.Close()
        if destination_staging.exists():
            destination_staging.unlink()


def main(
    *,
    input_paths: Sequence[Path] = INPUT_PATHS,
    output_directory: Path | None = OUTPUT_DIRECTORY,
    assigned_crs: str | None = ASSIGNED_CRS,
    closed_lines_as_areas: bool = CLOSED_LINES_AS_AREAS,
    overwrite: bool = OVERWRITE,
    autocad_export: bool = AUTOCAD_EXPORT,
    autocad_console: Path | None = AUTOCAD_CONSOLE,
    autocad_timeout_seconds: int = AUTOCAD_TIMEOUT_SECONDS,
) -> int:
    """Validate settings, convert all inputs, and return a process exit code."""
    try:
        options = _validate_options(
            input_paths=input_paths,
            output_directory=output_directory,
            assigned_crs=assigned_crs,
            closed_lines_as_areas=closed_lines_as_areas,
            overwrite=overwrite,
            autocad_export=autocad_export,
            autocad_console=autocad_console,
            autocad_timeout_seconds=autocad_timeout_seconds,
        )
    except ConversionError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print(f"cad_to_gpkg.py {SCRIPT_VERSION}")
    print(gdal.VersionInfo("--version"))
    failures = 0
    for source in options.input_paths:
        print(f"Converting: {source}")
        try:
            result = convert_drawing(source, options)
        except (ConversionError, OSError, RuntimeError) as exc:
            failures += 1
            print(f"ERROR: {exc}", file=sys.stderr)
            continue
        counts_text = ", ".join(f"{name}={count}" for name, count in result.counts.items())
        print(f"Created: {result.output} ({counts_text})")
    return 1 if failures else 0


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Convert DXF/DWG drawings to same-named GeoPackages with areas, lines, and points layers.",
        epilog=(
            'Example: python cad_to_gpkg.py "D:\\CAD\\services.dwg" --autocad-export. '
            "CAD coordinates are unchanged; --crs assigns metadata only."
        ),
    )
    parser.add_argument("inputs", type=Path, nargs="*", help="One or more .dxf or .dwg drawings.")
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=None,
        help="Existing destination directory. By default, each output is written beside its input.",
    )
    parser.add_argument(
        "--crs",
        default=None,
        help="Assign CRS metadata without transforming coordinates, for example EPSG:7850.",
    )
    parser.add_argument(
        "--closed-lines-as-areas",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Treat closed CAD polylines as polygons (default: editable setting, initially enabled).",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Replace an existing same-named GeoPackage only after a new output validates.",
    )
    parser.add_argument(
        "--autocad-export",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Allow an installed Autodesk console to export unsupported DWG versions to a temporary DXF.",
    )
    parser.add_argument(
        "--autocad-console",
        type=Path,
        default=None,
        help="Path to accoreconsole.exe. By default, installed AutoCAD/DWG TrueView versions are discovered.",
    )
    parser.add_argument(
        "--autocad-timeout-seconds",
        type=int,
        default=None,
        help="Maximum seconds allowed for one temporary AutoCAD DXF export.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {SCRIPT_VERSION}")
    return parser.parse_args()


def _effective_cli_value[T](cli_value: T | None, editable_value: T) -> T:
    return editable_value if cli_value is None else cli_value


if __name__ == "__main__":
    arguments = _parse_cli_arguments()
    selected_inputs = tuple(arguments.inputs) if arguments.inputs else INPUT_PATHS
    raise SystemExit(
        main(
            input_paths=selected_inputs,
            output_directory=_effective_cli_value(arguments.output_directory, OUTPUT_DIRECTORY),
            assigned_crs=_effective_cli_value(arguments.crs, ASSIGNED_CRS),
            closed_lines_as_areas=_effective_cli_value(
                arguments.closed_lines_as_areas,
                CLOSED_LINES_AS_AREAS,
            ),
            overwrite=_effective_cli_value(arguments.overwrite, OVERWRITE),
            autocad_export=_effective_cli_value(arguments.autocad_export, AUTOCAD_EXPORT),
            autocad_console=_effective_cli_value(arguments.autocad_console, AUTOCAD_CONSOLE),
            autocad_timeout_seconds=_effective_cli_value(
                arguments.autocad_timeout_seconds,
                AUTOCAD_TIMEOUT_SECONDS,
            ),
        )
    )
