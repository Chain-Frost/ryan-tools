from __future__ import annotations
from pathlib import Path

WRAPPER_VERSION = "2.0.0"

import argparse
import sys
import subprocess
import concurrent.futures

from loguru import logger
from osgeo import gdal, ogr

from ryan_library.functions.path_stuff import to_single_path, to_path_list
from ryan_library.functions.gdal.vector_conversion import resolve_vector_format


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=f"Batch spatial filter/clip vector files by multiple extent polygons (v{WRAPPER_VERSION})."
    )
    parser.add_argument(
        "inputs",
        nargs="+",
        type=str,
        help="Input vector files to process.",
    )
    parser.add_argument(
        "--extents",
        "-e",
        nargs="+",
        type=str,
        required=True,
        help="One or more extent shapefiles/polygons to use as boundaries.",
    )
    parser.add_argument(
        "--mode",
        "-m",
        choices=["clip", "intersect", "within", "centroid-within"],
        default="clip",
        help=(
            "clip: Strict geometric cut at the boundary (splits features). "
            "intersect: Keeps intact features that touch/overlap the boundary. "
            "within: Keeps intact features entirely contained within the boundary. "
            "centroid-within: Keeps intact features whose centroid is inside the boundary."
        )
    )
    parser.add_argument(
        "--output-dir",
        "-o",
        type=str,
        default=None,
        help="Output directory (defaults to input directory).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print actions without modifying files.",
    )
    return parser.parse_args()


def _get_extent_geometry(extent_file: Path) -> ogr.Geometry:
    with gdal.ExceptionMgr():
        ds = ogr.Open(str(extent_file), 0)
        if ds is None:
            raise RuntimeError(f"Could not open extent dataset: {extent_file}")
        
        layer = ds.GetLayer()
        if layer is None:
            raise RuntimeError(f"No layers found in {extent_file}")
            
        # Union all features into a single geometry
        union_geom = ogr.Geometry(ogr.wkbGeometryCollection)
        for feat in layer:
            geom = feat.GetGeometryRef()
            if geom:
                union_geom.AddGeometry(geom.Clone())
        
        # Merge if multiple
        if union_geom.GetGeometryCount() > 1:
            union_geom = union_geom.UnionCascaded()
        elif union_geom.GetGeometryCount() == 1:
            union_geom = union_geom.GetGeometryRef(0).Clone()
        else:
            raise RuntimeError(f"No valid geometry found in {extent_file}")
            
        return union_geom


def process_via_ogr(input_file: Path, extent_file: Path, out_path: Path, mode: str) -> bool:
    try:
        format_name, spec = resolve_vector_format(out_path.suffix)
        driver = ogr.GetDriverByName(spec.driver)
        if driver is None:
            logger.error(f"GDAL driver {spec.driver} not available.")
            return False

        extent_geom = _get_extent_geometry(extent_file)

        in_ds = ogr.Open(str(input_file), 0)
        if in_ds is None:
            logger.error(f"Could not open input: {input_file}")
            return False

        out_ds = driver.CreateDataSource(str(out_path))
        if out_ds is None:
            logger.error(f"Could not create output: {out_path}")
            return False

        layer_count = in_ds.GetLayerCount()
        for i in range(layer_count):
            in_layer = in_ds.GetLayer(i)
            layer_name = in_layer.GetName()
            srs = in_layer.GetSpatialRef()
            geom_type = in_layer.GetGeomType()

            out_layer = out_ds.CreateLayer(layer_name, srs, geom_type)
            in_layer_defn = in_layer.GetLayerDefn()

            for fld_idx in range(in_layer_defn.GetFieldCount()):
                fld_defn = in_layer_defn.GetFieldDefn(fld_idx)
                out_layer.CreateField(fld_defn)

            # Apply spatial filter to massively speed up iteration
            in_layer.SetSpatialFilter(extent_geom)

            out_layer_defn = out_layer.GetLayerDefn()

            for in_feat in in_layer:
                geom = in_feat.GetGeometryRef()
                if geom is None:
                    continue

                keep = False
                if mode == "intersect":
                    if geom.Intersects(extent_geom):
                        keep = True
                elif mode == "within":
                    if geom.Within(extent_geom):
                        keep = True
                elif mode == "centroid-within":
                    centroid = geom.Centroid()
                    if centroid.Within(extent_geom):
                        keep = True

                if keep:
                    out_feat = ogr.Feature(out_layer_defn)
                    for fld_idx in range(out_layer_defn.GetFieldCount()):
                        out_feat.SetField(fld_idx, in_feat.GetField(fld_idx))
                    out_feat.SetGeometry(geom.Clone())
                    out_layer.CreateFeature(out_feat)

        out_ds = None
        in_ds = None
        return True
    except Exception as e:
        logger.error(f"OGR processing failed: {e}")
        return False


def process_single(input_file: Path, extent_file: Path, out_dir: Path, mode: str, dry_run: bool) -> bool:
    out_name = f"{input_file.stem}_{mode}_{extent_file.stem}{input_file.suffix}"
    out_path = out_dir / out_name
    
    if dry_run:
        logger.info(f"[DRY-RUN] Would create {out_name} using mode '{mode}'")
        return True

    if mode == "clip":
        cmd = [
            "ogr2ogr",
            str(out_path),
            "-clipsrc",
            str(extent_file),
            str(input_file)
        ]
        try:
            result = subprocess.run(cmd, check=False, text=True, capture_output=True)
            if result.returncode != 0:
                logger.error(f"ogr2ogr clip failed for {input_file.name}")
                logger.error(result.stderr)
                return False
            return True
        except Exception as exc:
            logger.error(f"Execution failed: {exc}")
            return False
    else:
        return process_via_ogr(input_file, extent_file, out_path, mode)


def main() -> None:
    args = _parse_cli_arguments()

    input_paths = to_path_list(args.inputs)
    extent_paths = to_path_list(args.extents)
    
    if not input_paths:
        logger.error("No input files provided.")
        sys.exit(1)
        
    for p in extent_paths:
        if not p.exists():
            logger.error(f"Extent file not found: {p}")
            sys.exit(1)

    out_dir = to_single_path(args.output_dir) if args.output_dir else input_paths[0].parent
    if not args.dry_run:
        out_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Found {len(input_paths)} inputs and {len(extent_paths)} extents. Will generate {len(input_paths) * len(extent_paths)} outputs using mode '{args.mode}'.")
    
    success_count = 0
    total = len(input_paths) * len(extent_paths)
    
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for input_file in input_paths:
            if not input_file.exists():
                logger.warning(f"Input file not found, skipping: {input_file}")
                continue
            for extent_file in extent_paths:
                futures.append(executor.submit(process_single, input_file, extent_file, out_dir, args.mode, args.dry_run))
        
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                success_count += 1
                
    if args.dry_run:
        logger.info("Dry run complete.")
    else:
        logger.success(f"Processing complete: {success_count}/{total} successful.")


if __name__ == "__main__":
    main()
