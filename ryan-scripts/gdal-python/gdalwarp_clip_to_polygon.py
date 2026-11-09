r"""Clip .tif rasters to a polygon with metadata-derived source CRS and safe publication.

Relative cutlines use the first input root; relative output directories use each
input root. Existing outputs fail unless --overwrite is explicitly selected.
Batch path collisions fail before output creation. Earlier published rasters
remain if a later raster fails. Symlinks/junctions and output sidecars are refused.

    python gdalwarp_clip_to_polygon.py -i D:\Rasters --cutline D:\clip.gpkg --dry-run --no-pause
"""

# moved from unsorted, not tested in production yet - 2026-08-20

from __future__ import annotations

from pathlib import Path

# ==============================================================================
# WRAPPER IDENTITY
WRAPPER_VERSION = "2026-11-08.2"

# EDITABLE DEFAULTS
DEFAULT_INPUT_DIR = Path()
DEFAULT_OUTPUT_DIR = Path(r".\clipped")
DEFAULT_SHAPEFILE = Path(r".\Result_Trim_Polygon.shp")
DEFAULT_CRS = "EPSG:28350"
# None respects each raster's CRS metadata. Override only to correct missing/wrong metadata.
SOURCE_CRS: str | None = None
# Replaces existing outputs only; source rasters and linked paths are always protected.
OVERWRITE = False
DRY_RUN = False
CONSOLE_LOG_LEVEL = "SUCCESS"
LOG_FILE: Path | None = Path("gdalwarp_clip.log")
FILE_LOG_LEVEL = "INFO"
PAUSE_ON_FINISH = True
# ==============================================================================

import argparse
import os
import tempfile

from loguru import logger
from osgeo import gdal  # type: ignore

# Enable GDAL exceptions so errors are raised as Python exceptions
gdal.UseExceptions()

from ryan_library.functions.gdal.raster_processing import geotiff_creation_options
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.path_stuff import PathOrList, to_path_list
from ryan_library.functions.wrapper_utils import change_working_directory, pause_console, print_wrapper_banner


def select_cutline_layer(cutline_path: Path) -> str | None:
    """Select an unambiguous layer from a vector cutline datasource."""
    try:
        dataset = gdal.OpenEx(str(cutline_path), gdal.OF_VECTOR | gdal.OF_READONLY)  # type: ignore
        if dataset is None:
            msg = "GDAL could not open the cutline datasource"
            raise RuntimeError(msg)

        layer_names: list[str] = []
        for layer_index in range(dataset.GetLayerCount()):  # type: ignore
            layer = dataset.GetLayer(layer_index)  # type: ignore
            if layer is None:
                msg = f"GDAL could not read cutline layer at index {layer_index}"
                raise RuntimeError(msg)
            layer_names.append(layer.GetName())  # type: ignore
        dataset = None
    except Exception:
        logger.exception("Could not inspect cutline datasource {}", cutline_path)
        return None

    if not layer_names:
        logger.error("Cutline datasource {} contains no layers.", cutline_path)
        return None
    if len(layer_names) == 1:
        logger.debug("Using the only layer {!r} from {}.", layer_names[0], cutline_path.name)
        return layer_names[0]

    matching_names = [name for name in layer_names if name.casefold() == cutline_path.stem.casefold()]
    if len(matching_names) == 1:
        logger.debug("Using filename-matched layer {!r} from {}.", matching_names[0], cutline_path.name)
        return matching_names[0]

    logger.error(
        "Could not select a unique cutline layer from {}. Available layers: {}",
        cutline_path,
        ", ".join(layer_names),
    )
    return None


def _reject_links(path: Path) -> None:
    for component in (path, *path.parents):
        if component.is_symlink() or component.is_junction():
            msg = f"Linked paths are not supported: {component}"
            raise ValueError(msg)


def _identity(path: Path) -> str:
    # Follow links only for physical identity checks, including mapped-drive/UNC aliases.
    return str(path.resolve()).casefold()


def _validate_destination(output: Path, protected: tuple[Path, ...], *, overwrite: bool) -> None:
    _reject_links(output)
    # Replacing only the TIFF must not leave old mask/metadata/overview dependencies.
    for suffix in (".aux.xml", ".msk", ".ovr"):
        sidecar = output.with_name(output.name + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            msg = f"Existing output sidecar requires manual handling: {sidecar}"
            raise ValueError(msg)
    for source in protected:
        if _identity(output) == _identity(source) or (output.exists() and source.exists() and output.samefile(source)):
            msg = f"Output aliases an input: {output}"
            raise ValueError(msg)
    if output.parent.exists():
        aliases = [p for p in output.parent.iterdir() if p.name.casefold() == output.name.casefold()]
        if aliases and (not overwrite or any(p.name != output.name for p in aliases)):
            msg = f"Output already exists or has a case alias: {output}"
            raise FileExistsError(msg)
    if output.exists() and not output.is_file():
        msg = f"Output is not a regular file: {output}"
        raise ValueError(msg)


def run_gdalwarp(
    input_file: Path,
    output_file: Path,
    cutline_path: Path,
    cutline_layer: str,
    crs: str,
    *,
    source_crs: str | None = None,
    overwrite: bool = False,
    protected_inputs: tuple[Path, ...] = (),
) -> bool:
    """Warp into owned staging, then publish one self-contained TIFF safely.

    Several outputs are independent publications: earlier successful outputs remain
    if a later input fails. No-overwrite publication never replaces a late creator.
    """
    protected = (input_file, cutline_path, *protected_inputs)
    try:
        _reject_links(input_file)
        _reject_links(cutline_path)
        _validate_destination(output_file, protected, overwrite=overwrite)
        with gdal.Open(str(input_file)) as source:  # type: ignore
            # GCP-georeferenced rasters can have no ordinary projection but a valid
            # ground-control-point CRS; let GDAL infer either metadata form.
            has_source_crs = source is not None and bool(source.GetProjectionRef() or source.GetGCPProjection())  # type: ignore
            if source is None or (source_crs is None and not has_source_crs):
                msg = "Source raster needs CRS metadata or an explicit --source-crs correction"
                raise ValueError(msg)
        output_file.parent.mkdir(parents=True, exist_ok=True)
        warp_options = gdal.WarpOptions(  # type: ignore
            format="GTiff",
            srcSRS=source_crs,
            dstSRS=crs,
            cutlineDSName=str(cutline_path),
            cutlineLayer=cutline_layer,
            cropToCutline=True,
            multithread=True,
            creationOptions=geotiff_creation_options("tuflow", "ALL_CPUS"),
        )
        with tempfile.TemporaryDirectory(prefix=f".{output_file.name}.staging-", dir=output_file.parent) as stage:
            temporary_output = Path(stage) / output_file.name
            ds = gdal.Warp(str(temporary_output), str(input_file), options=warp_options)  # type: ignore
            if ds is None:
                msg = "GDAL did not create an output dataset"
                raise RuntimeError(msg)
            try:
                ds.FlushCache()  # type: ignore
            finally:
                ds = None
            with gdal.Open(str(temporary_output)) as verified:  # type: ignore
                if verified is None or verified.RasterXSize < 1 or verified.RasterYSize < 1:  # type: ignore
                    msg = "GDAL output is empty or unreadable"
                    raise RuntimeError(msg)
            if set(Path(stage).iterdir()) != {temporary_output}:
                msg = "Output requires sidecars; single-file publication refused"
                raise RuntimeError(msg)
            _validate_destination(output_file, protected, overwrite=overwrite)
            if overwrite:
                temporary_output.replace(output_file)
            elif os.name == "nt":
                # Windows rename rejects an existing destination atomically.
                temporary_output.rename(output_file)
            else:
                # POSIX rename replaces; hard-link creation provides no-replace publication.
                os.link(temporary_output, output_file)
        logger.success("Clipped {} to {}", input_file.name, output_file)
        return True
    except Exception:
        logger.exception("Could not clip {} to {}", input_file, output_file)
        return False


def main(
    *,
    input_directories: PathOrList | None = None,
    output_directory: Path | None = None,
    cutline: Path | None = None,
    target_crs: str | None = None,
    source_crs: str | None = SOURCE_CRS,
    overwrite: bool | None = None,
    dry_run: bool | None = None,
) -> int:
    """Preflight the complete batch before creating directories or clipping files."""
    targets = [
        p.absolute() for p in to_path_list(DEFAULT_INPUT_DIR if input_directories is None else input_directories)
    ]
    if not targets or any(not target.is_dir() for target in targets):
        logger.error("Every input must be an existing directory.")
        return 1
    targets = list(dict.fromkeys(targets))
    # Capture the intended base before the wrapper changes the process directory.
    shapefile = DEFAULT_SHAPEFILE if cutline is None else cutline
    shapefile = (targets[0] / shapefile).absolute() if not shapefile.is_absolute() else shapefile.absolute()
    output_setting = DEFAULT_OUTPUT_DIR if output_directory is None else output_directory
    replace = OVERWRITE if overwrite is None else overwrite
    preview = DRY_RUN if dry_run is None else dry_run
    try:
        _reject_links(shapefile)
        if not shapefile.exists():
            msg = f"Cutline datasource does not exist: {shapefile}"
            raise FileNotFoundError(msg)
        plans: list[tuple[Path, Path]] = []
        seen_sources: list[Path] = []
        for directory in targets:
            _reject_links(directory)
            output_dir = (directory / output_setting).absolute()
            for source in sorted(directory.glob("*.tif")):
                _reject_links(source)
                if any(source.samefile(previous) for previous in seen_sources):
                    continue
                seen_sources.append(source)
                plans.append((source, output_dir / f"{source.stem}_clipped{source.suffix}"))
        if not plans:
            msg = "No .tif inputs found"
            raise ValueError(msg)
        protected = (shapefile, *seen_sources)
        destinations: set[str] = set()
        output_paths: list[Path] = []
        for _source, output in plans:
            _validate_destination(output, protected, overwrite=replace)
            key = _identity(output)
            if key in destinations or any(
                output.exists() and previous.exists() and output.samefile(previous) for previous in output_paths
            ):
                msg = f"Batch output collision: {output}"
                raise ValueError(msg)
            destinations.add(key)
            output_paths.append(output)
        layer = select_cutline_layer(shapefile)
        if layer is None:
            return 1
        if preview:
            for source, output in plans:
                logger.success("Would clip {} to {}", source, output)
            return 0
        if not change_working_directory(target_dir=targets[0]):
            return 1
        succeeded = sum(
            run_gdalwarp(
                source,
                output,
                shapefile,
                layer,
                DEFAULT_CRS if target_crs is None else target_crs,
                source_crs=source_crs,
                overwrite=replace,
                protected_inputs=protected,
            )
            for source, output in plans
        )
        logger.success("Clipped {}/{} rasters", succeeded, len(plans))
        return 0 if succeeded == len(plans) else 1
    except OSError, ValueError:
        logger.exception("Cutline batch preflight failed")
        return 1


def _parse_cli_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Clip TIFF rasters safely; source CRS defaults to raster metadata.")
    parser.add_argument("-i", "--input-directories", "--input_directories", type=Path, nargs="+", default=None)
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=None,
        help="Relative to each input root, or one absolute output directory.",
    )
    parser.add_argument("--cutline", type=Path, default=None, help="Relative to the first input root, or absolute.")
    parser.add_argument("--target-crs", default=None, help="Destination CRS; defaults to DEFAULT_CRS.")
    parser.add_argument("--source-crs", default=SOURCE_CRS, help="Explicit metadata correction; normally omit.")
    parser.add_argument("--overwrite", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--console-log-level", default=CONSOLE_LOG_LEVEL)
    logs = parser.add_mutually_exclusive_group()
    logs.add_argument("--log-file", type=Path, default=LOG_FILE)
    logs.add_argument("--no-log-file", action="store_true")
    parser.add_argument("--file-log-level", default=FILE_LOG_LEVEL)
    parser.add_argument("--pause", action=argparse.BooleanOptionalAction, default=PAUSE_ON_FINISH)
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_cli_arguments()
    print_wrapper_banner(
        wrapper_source="gdal-python/gdalwarp_clip_to_polygon.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
    )
    configured_log = None if args.no_log_file else args.log_file
    with setup_logger(
        console_log_level=args.console_log_level,
        log_file=str(configured_log.absolute()) if configured_log is not None else None,
        file_log_level=args.file_log_level,
    ):
        result = main(
            input_directories=args.input_directories,
            output_directory=args.output_directory,
            cutline=args.cutline,
            target_crs=args.target_crs,
            source_crs=args.source_crs,
            overwrite=args.overwrite,
            dry_run=args.dry_run,
        )
    print_wrapper_banner(
        wrapper_source="gdal-python/gdalwarp_clip_to_polygon.py",
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if args.pause:
        pause_console()
    raise SystemExit(result)
