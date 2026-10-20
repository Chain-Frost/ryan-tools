#!/usr/bin/env python3
"""Convert TUFLOW shapefiles using matching empty GeoPackage templates.

The TUFLOW type is the first two underscore-separated parts of the shapefile
name (for example, ``1d_nwk``). The matching ``<type>_empty.gpkg`` is copied,
the layer with the source geometry type is retained, and source features are
inserted into that template layer. A source is skipped when its attribute
schema is not compatible with the selected template layer.
"""

from __future__ import annotations

# The OSGeo GDAL Python bindings currently lack compatible static typing.
# Limit only GDAL-derived unknown-type diagnostics to this standalone converter.
# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# pyright: reportUnknownArgumentType=false, reportUnknownParameterType=false
# pyright: reportMissingTypeStubs=false, reportAttributeAccessIssue=false
import argparse
import csv
import fnmatch
import math
import os
import shutil
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, fields
from pathlib import Path

from osgeo import ogr, osr

ogr.UseExceptions()


@dataclass
class Result:
    status: str
    source: str
    output: str = ""
    template: str = ""
    source_layer: str = ""
    template_layer: str = ""
    geometry: str = ""
    feature_count: int | str = ""
    message: str = ""


def open_vector(path: Path | str, update: bool = False) -> ogr.DataSource | None:
    """Open a vector dataset, retrying transient network/SQLite lock failures."""
    last_error: RuntimeError | None = None
    for attempt in range(3):
        try:
            return ogr.Open(str(path), 1 if update else 0)
        except RuntimeError as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(1)
    if last_error is not None:
        raise last_error
    return None


def base_geometry(geometry_type: int) -> int:
    """Return the 2D single-part OGR geometry family."""
    flatten = getattr(ogr, "wkbFlatten", ogr.GT_Flatten)
    value = flatten(geometry_type)
    return {
        ogr.wkbMultiPoint: ogr.wkbPoint,
        ogr.wkbMultiLineString: ogr.wkbLineString,
        ogr.wkbMultiPolygon: ogr.wkbPolygon,
    }.get(value, value)


def field_signature(field: ogr.FieldDefn) -> tuple[str, int, int, int]:
    return (field.GetName(), field.GetType(), field.GetWidth(), field.GetPrecision())


def source_field_mapping(source_defn: ogr.FeatureDefn, template_layer: ogr.Layer) -> tuple[list[int], list[int]]:
    """Map output attributes to source fields, excluding a source/template FID clash."""
    template_count = template_layer.GetLayerDefn().GetFieldCount()
    template_fid = template_layer.GetFIDColumn().casefold()
    kept = list(range(min(template_count, source_defn.GetFieldCount())))
    dropped: list[int] = []
    for index in range(template_count, source_defn.GetFieldCount()):
        if template_fid and source_defn.GetFieldDefn(index).GetName().casefold() == template_fid:
            dropped.append(index)
        else:
            kept.append(index)
    return kept, dropped


def compatible_field(source: ogr.FieldDefn, target: ogr.FieldDefn) -> tuple[bool, str]:
    source_name = source.GetName().casefold()
    target_name = target.GetName().casefold()
    # Shapefile DBF names are limited to 10 characters. Long GPKG template
    # names therefore match their canonical DBF truncation as well.
    name_ok = source_name in {target_name, target_name[:10]}
    if not name_ok:
        return False, f"name {source.GetName()!r} != {target.GetName()!r}"

    source_type = source.GetType()
    target_type = target.GetType()
    compatible_types = {
        ogr.OFTString: {ogr.OFTString},
        # Real-to-integer is accepted here only provisionally. Every value is
        # checked for a lossless, in-range conversion below before writing.
        ogr.OFTInteger: {ogr.OFTInteger, ogr.OFTReal},
        ogr.OFTInteger64: {ogr.OFTInteger, ogr.OFTInteger64, ogr.OFTReal},
        ogr.OFTReal: {ogr.OFTInteger, ogr.OFTInteger64, ogr.OFTReal},
        ogr.OFTDate: {ogr.OFTDate},
        ogr.OFTDateTime: {ogr.OFTDate, ogr.OFTDateTime},
        ogr.OFTTime: {ogr.OFTTime},
    }
    if source_type not in compatible_types.get(target_type, {target_type}):
        return (
            False,
            f"type {source.GetTypeName()} != compatible {target.GetTypeName()}",
        )
    return True, ""


def compare_schema(source_layer: ogr.Layer, template_layer: ogr.Layer) -> list[str]:
    """Check required template fields; trailing source-only fields are allowed."""
    source_defn = source_layer.GetLayerDefn()
    template_defn = template_layer.GetLayerDefn()
    issues: list[str] = []
    if source_defn.GetFieldCount() < template_defn.GetFieldCount():
        issues.append(
            f"field count {source_defn.GetFieldCount()} != minimum template count {template_defn.GetFieldCount()}"
        )
    for index in range(min(source_defn.GetFieldCount(), template_defn.GetFieldCount())):
        source_field = source_defn.GetFieldDefn(index)
        template_field = template_defn.GetFieldDefn(index)
        ok, reason = compatible_field(source_field, template_field)
        if not ok:
            issues.append(f"field {index + 1}: {reason}")
    if source_defn.GetFieldCount() < template_defn.GetFieldCount():
        missing = [
            template_defn.GetFieldDefn(index).GetName()
            for index in range(source_defn.GetFieldCount(), template_defn.GetFieldCount())
        ]
        issues.append(f"missing source fields: {', '.join(missing)}")
    return issues


def validate_string_lengths(source_layer: ogr.Layer, template_layer: ogr.Layer) -> list[str]:
    template_defn = template_layer.GetLayerDefn()
    constrained = {
        index: template_defn.GetFieldDefn(index).GetWidth()
        for index in range(template_defn.GetFieldCount())
        if template_defn.GetFieldDefn(index).GetType() == ogr.OFTString
        and template_defn.GetFieldDefn(index).GetWidth() > 0
    }
    if not constrained:
        return []
    maxima = dict.fromkeys(constrained, 0)
    source_layer.ResetReading()
    for feature in source_layer:
        for index in constrained:
            if feature.IsFieldSetAndNotNull(index):
                maxima[index] = max(maxima[index], len(feature.GetFieldAsString(index)))
    source_layer.ResetReading()
    return [
        f"field {template_defn.GetFieldDefn(index).GetName()!r} contains "
        f"{actual} characters but template width is {constrained[index]}"
        for index, actual in maxima.items()
        if actual > constrained[index]
    ]


def validate_integer_coercions(source_layer: ogr.Layer, template_layer: ogr.Layer) -> list[str]:
    """Reject Real-to-Integer coercion unless every populated value is lossless."""
    source_defn = source_layer.GetLayerDefn()
    template_defn = template_layer.GetLayerDefn()
    checks: dict[int, tuple[int, int]] = {}
    for index in range(min(source_defn.GetFieldCount(), template_defn.GetFieldCount())):
        source_type = source_defn.GetFieldDefn(index).GetType()
        target_type = template_defn.GetFieldDefn(index).GetType()
        if source_type == ogr.OFTReal and target_type == ogr.OFTInteger:
            checks[index] = (-2147483648, 2147483647)
        elif source_type == ogr.OFTReal and target_type == ogr.OFTInteger64:
            checks[index] = (-9223372036854775808, 9223372036854775807)
    if not checks:
        return []

    issues: list[str] = []
    source_layer.ResetReading()
    for feature in source_layer:
        for index, (minimum, maximum) in checks.items():
            if not feature.IsFieldSetAndNotNull(index):
                continue
            value = feature.GetFieldAsDouble(index)
            if not math.isfinite(value) or value != int(value) or not minimum <= value <= maximum:
                name = template_defn.GetFieldDefn(index).GetName()
                issues.append(
                    f"field {name!r}, feature {feature.GetFID()}: {value!r} "
                    "cannot be converted to the template integer type without loss"
                )
                source_layer.ResetReading()
                return issues
    source_layer.ResetReading()
    return issues


def find_template_layer(dataset: ogr.DataSource, source_geometry: int) -> ogr.Layer | None:
    wanted = base_geometry(source_geometry)
    matches = [
        dataset.GetLayerByIndex(index)
        for index in range(dataset.GetLayerCount())
        if base_geometry(dataset.GetLayerByIndex(index).GetLayerDefn().GetGeomType()) == wanted
    ]
    return matches[0] if len(matches) == 1 else None


def spatial_transform(source_layer: ogr.Layer, target_layer: ogr.Layer):
    source_srs = source_layer.GetSpatialRef()
    target_srs = target_layer.GetSpatialRef()
    if (source_srs is None) != (target_srs is None):
        msg = "source and template/output must both have a CRS, or both have no CRS"
        raise ValueError(msg)
    if source_srs is None or target_srs is None or source_srs.IsSame(target_srs):
        return None
    source_srs = source_srs.Clone()
    target_srs = target_srs.Clone()
    source_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    target_srs.SetAxisMappingStrategy(osr.OAMS_TRADITIONAL_GIS_ORDER)
    return osr.CoordinateTransformation(source_srs, target_srs)


def verify_output(source_path: Path, template_dir: Path) -> Result:  # noqa: C901 - multi-stage vector validation
    output_path = source_path.with_suffix(".gpkg")
    result = Result(status="ERROR", source=str(source_path), output=str(output_path))
    parts = source_path.stem.split("_")
    if len(parts) < 2:
        result.message = "source name does not identify a template"
        return result
    template_path = template_dir / ("_".join(parts[:2]) + "_empty.gpkg")
    result.template = str(template_path)
    source_ds = open_vector(source_path)
    template_ds = open_vector(template_path)
    output_ds = open_vector(output_path)
    if source_ds is None or template_ds is None or output_ds is None:
        result.message = "source, template, or output could not be opened"
        return result
    source_layer = source_ds.GetLayerByIndex(0)
    template_layer = find_template_layer(template_ds, source_layer.GetLayerDefn().GetGeomType())
    result.source_layer = source_layer.GetName()
    result.feature_count = source_layer.GetFeatureCount()
    if template_layer is None:
        result.message = "matching template layer could not be identified"
        return result
    result.template_layer = template_layer.GetName()
    result.geometry = ogr.GeometryTypeToName(template_layer.GetLayerDefn().GetGeomType())
    issues: list[str] = []
    if output_ds.GetLayerCount() != 1:
        issues.append(f"output layer count {output_ds.GetLayerCount()} != 1")
    output_layer = output_ds.GetLayerByIndex(0)
    if output_layer.GetName() != source_path.stem:
        issues.append(f"output layer name {output_layer.GetName()!r} != source stem")
    output_defn = output_layer.GetLayerDefn()
    template_defn = template_layer.GetLayerDefn()
    source_defn = source_layer.GetLayerDefn()
    source_indexes, _ = source_field_mapping(source_defn, template_layer)
    if output_layer.GetFIDColumn() != template_layer.GetFIDColumn():
        issues.append(
            f"output FID column {output_layer.GetFIDColumn()!r} != template FID "
            f"column {template_layer.GetFIDColumn()!r}"
        )
    if output_defn.GetGeomType() != template_defn.GetGeomType():
        issues.append("output geometry definition differs from template")
    output_fields = [field_signature(output_defn.GetFieldDefn(index)) for index in range(output_defn.GetFieldCount())]
    template_fields = [
        field_signature(template_defn.GetFieldDefn(index)) for index in range(template_defn.GetFieldCount())
    ]
    template_count = template_defn.GetFieldCount()
    if output_fields[:template_count] != template_fields:
        issues.append("required output fields differ from the template definitions")
    if output_defn.GetFieldCount() != len(source_indexes):
        issues.append(
            f"output field count {output_defn.GetFieldCount()} != retained source field count {len(source_indexes)}"
        )
    else:
        for output_index in range(template_count, output_defn.GetFieldCount()):
            source_index = source_indexes[output_index]
            source_field = source_defn.GetFieldDefn(source_index)
            output_field = output_defn.GetFieldDefn(output_index)
            if (
                source_field.GetName() != output_field.GetName()
                or source_field.GetType() != output_field.GetType()
                or (source_field.GetType() == ogr.OFTString and source_field.GetWidth() != output_field.GetWidth())
            ):
                issues.append(
                    f"trailing field {output_index + 1} {output_field.GetName()!r} does not match the source definition"
                )
    output_srs = output_layer.GetSpatialRef()
    template_srs = template_layer.GetSpatialRef()
    if (output_srs is None) != (template_srs is None) or (
        output_srs is not None and not output_srs.IsSame(template_srs)
    ):
        issues.append("output CRS differs from template")
    try:
        transform = spatial_transform(source_layer, output_layer)
    except (RuntimeError, ValueError) as exc:
        transform = None
        issues.append(f"source/output CRS cannot be reconciled: {exc}")
    if output_layer.GetFeatureCount() != source_layer.GetFeatureCount():
        issues.append(f"feature count {output_layer.GetFeatureCount()} != {source_layer.GetFeatureCount()}")
    elif output_defn.GetFieldCount() == len(source_indexes) and not issues:
        source_layer.ResetReading()
        output_layer.ResetReading()
        for row_number, (source_feature, output_feature) in enumerate(
            zip(source_layer, output_layer, strict=True), start=1
        ):
            for output_index, source_index in enumerate(source_indexes):
                source_null = not source_feature.IsFieldSetAndNotNull(source_index)
                output_null = not output_feature.IsFieldSetAndNotNull(output_index)
                if source_null != output_null:
                    issues.append(f"row {row_number}, field {output_index + 1}: NULL state differs")
                    break
                if source_null:
                    continue
                source_value = source_feature.GetField(source_index)
                output_value = output_feature.GetField(output_index)
                if isinstance(source_value, float) and isinstance(output_value, float):
                    equal = math.isclose(source_value, output_value, rel_tol=1e-12, abs_tol=1e-12)
                else:
                    equal = source_value == output_value
                if not equal:
                    issues.append(f"row {row_number}, field {output_index + 1}: value differs")
                    break
            if issues:
                break
            source_geometry = source_feature.GetGeometryRef()
            output_geometry = output_feature.GetGeometryRef()
            if (source_geometry is None) != (output_geometry is None):
                issues.append(f"row {row_number}: geometry presence differs")
                break
            if source_geometry is not None and output_geometry is not None:
                expected_geometry = source_geometry.Clone()
                try:
                    if transform is not None:
                        expected_geometry.Transform(transform)
                    if not expected_geometry.Equals(output_geometry):
                        issues.append(f"row {row_number}: geometry differs")
                        break
                except RuntimeError as exc:
                    issues.append(f"row {row_number}: geometry comparison failed: {exc}")
                    break
    result.status = "VERIFIED" if not issues else "ERROR"
    result.message = "all output checks passed" if not issues else "; ".join(issues)
    return result


def convert_one(  # noqa: C901 - multi-stage vector conversion
    source_path: Path,
    template_dir: Path,
    overwrite: bool,
    dry_run: bool,
) -> Result:
    stem_parts = source_path.stem.split("_")
    result = Result(status="ERROR", source=str(source_path))
    if len(stem_parts) < 2:
        result.status = "SKIPPED_NO_TEMPLATE"
        result.message = "filename has fewer than two underscore-separated elements"
        return result

    tuflow_type = "_".join(stem_parts[:2])
    template_path = template_dir / f"{tuflow_type}_empty.gpkg"
    output_path = source_path.with_suffix(".gpkg")
    result.output = str(output_path)
    result.template = str(template_path)
    if not template_path.is_file():
        result.status = "SKIPPED_NO_TEMPLATE"
        result.message = f"template {template_path.name} was not found"
        return result
    if output_path.exists() and not overwrite:
        result.status = "SKIPPED_EXISTS"
        result.message = "output already exists (use --overwrite to replace it)"
        return result

    source_ds = open_vector(source_path)
    if source_ds is None or source_ds.GetLayerCount() != 1:
        result.message = "source could not be opened as a single-layer vector dataset"
        return result
    source_layer = source_ds.GetLayerByIndex(0)
    source_defn = source_layer.GetLayerDefn()
    result.source_layer = source_layer.GetName()
    result.geometry = ogr.GeometryTypeToName(source_defn.GetGeomType())
    result.feature_count = source_layer.GetFeatureCount()

    template_ds = open_vector(template_path)
    if template_ds is None:
        result.message = "template could not be opened"
        return result
    template_layer = find_template_layer(template_ds, source_defn.GetGeomType())
    if template_layer is None:
        result.status = "SKIPPED_GEOMETRY"
        result.message = f"template has no unique layer matching {result.geometry}"
        return result
    result.template_layer = template_layer.GetName()

    schema_issues = compare_schema(source_layer, template_layer)
    schema_issues.extend(validate_string_lengths(source_layer, template_layer))
    schema_issues.extend(validate_integer_coercions(source_layer, template_layer))
    if schema_issues:
        result.status = "SKIPPED_ATTRIBUTES"
        result.message = "; ".join(schema_issues)
        return result
    if dry_run:
        result.status = "READY"
        result.message = "schema and geometry match"
        return result

    temp_handle, temp_name = tempfile.mkstemp(
        prefix=f".{source_path.stem}.", suffix=".tmp.gpkg", dir=source_path.parent
    )
    os.close(temp_handle)
    temp_path = Path(temp_name)
    output_ds = None
    output_layer = None
    try:
        shutil.copy2(template_path, temp_path)
        output_ds = open_vector(temp_path, update=True)
        if output_ds is None:
            message = "copied template could not be opened for update"
            raise RuntimeError(message)

        selected_name = result.template_layer
        for index in range(output_ds.GetLayerCount() - 1, -1, -1):
            if output_ds.GetLayerByIndex(index).GetName() != selected_name:
                output_ds.DeleteLayer(index)
        output_layer = output_ds.GetLayerByName(selected_name)
        if output_layer is None:
            message = "selected template layer was lost while removing other layers"
            raise RuntimeError(message)
        if output_layer.Rename(source_path.stem) != ogr.OGRERR_NONE:
            message = "could not rename the template layer"
            raise RuntimeError(message)
        output_layer = output_ds.GetLayerByName(source_path.stem)
        template_field_count = output_layer.GetLayerDefn().GetFieldCount()
        source_indexes, dropped_indexes = source_field_mapping(source_defn, output_layer)
        for source_index in source_indexes[template_field_count:]:
            extra_field = source_defn.GetFieldDefn(source_index)
            if output_layer.CreateField(extra_field) != ogr.OGRERR_NONE:
                message = f"could not append source field {extra_field.GetName()!r}"
                raise RuntimeError(message)
        transform = spatial_transform(source_layer, output_layer)

        output_layer.StartTransaction()
        try:
            source_layer.ResetReading()
            for source_feature in source_layer:
                output_feature = ogr.Feature(output_layer.GetLayerDefn())
                for output_index, source_index in enumerate(source_indexes):
                    if source_feature.IsFieldSetAndNotNull(source_index):
                        value = source_feature.GetField(source_index)
                        source_type = source_defn.GetFieldDefn(source_index).GetType()
                        output_type = output_layer.GetLayerDefn().GetFieldDefn(output_index).GetType()
                        if source_type == ogr.OFTReal and output_type in {
                            ogr.OFTInteger,
                            ogr.OFTInteger64,
                        }:
                            value = int(value)
                        output_feature.SetField(output_index, value)
                    elif source_feature.IsFieldNull(source_index):
                        output_feature.SetFieldNull(output_index)
                source_geometry = source_feature.GetGeometryRef()
                if source_geometry is not None:
                    geometry = source_geometry.Clone()
                    if transform is not None:
                        geometry.Transform(transform)
                    output_feature.SetGeometry(geometry)
                if output_layer.CreateFeature(output_feature) != ogr.OGRERR_NONE:
                    message = f"failed while writing source feature {source_feature.GetFID()}"
                    raise RuntimeError(message)
            output_layer.CommitTransaction()
        except Exception:
            output_layer.RollbackTransaction()
            raise
        written_count = output_layer.GetFeatureCount()
        output_layer.SyncToDisk()
        output_ds.FlushCache()
        output_ds = None
        template_ds = None
        source_ds = None
        if written_count != int(result.feature_count):
            message = f"feature count verification failed: {written_count} written, {result.feature_count} expected"
            raise RuntimeError(message)
        temp_path.replace(output_path)
        result.status = "CONVERTED"
        extra_count = len(source_indexes) - template_field_count
        dropped_names = [source_defn.GetFieldDefn(index).GetName() for index in dropped_indexes]
        result.message = (
            "template schema retained, "
            f"{extra_count} trailing source field(s) appended, "
            f"{len(dropped_names)} conflicting source FID field(s) dropped, "
            "and feature count verified"
        )
        return result
    except Exception as exc:  # noqa: BLE001 - batch utility records per-file failures
        result.status = "ERROR"
        result.message = str(exc)
        return result
    finally:
        output_layer = None
        output_ds = None
        source_ds = None
        template_ds = None
        if temp_path.exists():
            try:
                temp_path.unlink()
            except PermissionError as exc:
                result.status = "ERROR"
                result.message = f"{result.message}; temporary-file cleanup failed: {exc}"


def write_report(path: Path, results: list[Result]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as stream:
        writer = csv.DictWriter(stream, fieldnames=[field.name for field in fields(Result)])
        writer.writeheader()
        writer.writerows(asdict(result) for result in results)


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).absolute().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=script_dir / "model" / "gis",
        help="directory searched recursively for shapefiles",
    )
    parser.add_argument(
        "--template-dir",
        type=Path,
        default=script_dir / "empty",
        help="directory containing <type>_empty.gpkg templates",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=script_dir / "gpkg_conversion_report.csv",
        help="CSV report path",
    )
    parser.add_argument("--overwrite", action="store_true", help="replace existing outputs")
    parser.add_argument("--dry-run", action="store_true", help="check only; write no GPKGs")
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="read back and validate existing GPKGs; ignore sources without an output",
    )
    parser.add_argument(
        "--include",
        action="append",
        help="optional shapefile-name glob to include; may be supplied more than once",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sources = sorted(args.source_dir.rglob("*.shp"), key=lambda path: str(path).casefold())
    if args.include:
        sources = [
            source for source in sources if any(fnmatch.fnmatch(source.name, pattern) for pattern in args.include)
        ]
    if not sources:
        print(f"ERROR: no shapefiles found beneath {args.source_dir}", file=sys.stderr)
        return 1
    results: list[Result] = []
    for source in sources:
        if args.verify_only:
            if not source.with_suffix(".gpkg").is_file():
                continue
            result = verify_output(source, args.template_dir)
        else:
            result = convert_one(source, args.template_dir, args.overwrite, args.dry_run)
        results.append(result)
        print(f"{result.status:24} {source} -- {result.message}", flush=True)
    write_report(args.report, results)

    counts: dict[str, int] = {}
    for result in results:
        counts[result.status] = counts.get(result.status, 0) + 1
    if not results and args.verify_only:
        print("SUMMARY no matching existing GPKG output files found for verification")
    else:
        print("SUMMARY " + ", ".join(f"{key}={value}" for key, value in sorted(counts.items())))
    print(f"REPORT {args.report}")
    return 1 if counts.get("ERROR", 0) else 0


if __name__ == "__main__":
    raise SystemExit(main())
