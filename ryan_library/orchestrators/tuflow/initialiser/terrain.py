"""Build an ordered GDAL-backed DEM mosaic for watershed analysis.

First terrain source has highest priority. Lower-priority datasets fill only
missing/invalid cells; a provenance raster records which source was used.
The analysis DEM resolution is distinct from TUFLOW's hydraulic cell size.
"""

import math
from collections.abc import Sequence
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path

import fiona
import numpy as np
import rasterio
from rasterio.crs import CRS
from rasterio.enums import Resampling
from rasterio.features import rasterize
from rasterio.transform import from_origin
from rasterio.vrt import WarpedVRT
from rasterio.warp import transform_bounds, transform_geom
from rasterio.windows import Window

from .gis_types import RasterProfile


@dataclass(frozen=True, slots=True)
class TerrainMosaic:
    """Hydrological DEM, per-pixel source index, and analysis resolution."""

    dem: Path
    source_index: Path
    resolution_m: float


@dataclass(frozen=True, slots=True)
class TerrainLayer:
    """One priority-ordered terrain raster and optional polygon clip."""

    raster: Path
    clip: Path | None = None


def compose_terrain(
    terrain_sources: Sequence[Path],
    *,
    target_crs: CRS,
    directory: Path,
    resolution_m: float | None = None,
    max_cells: int = 100_000_000,
    assume_missing_crs: CRS | None = None,
    bounds: tuple[float, float, float, float] | None = None,
) -> TerrainMosaic:
    """Build a priority mosaic from multiple resolutions and coordinate systems.

    Inputs are high-to-low priority. The first non-NoData source wins.
    WarpedVRT delegates on-the-fly raster reprojection to GDAL. An explicit
    alpha band is critical for sources with no declared NoData value.
    """
    layers = tuple(TerrainLayer(Path(item).absolute()) for item in terrain_sources)
    paths = tuple(item.raster for item in layers)
    if not paths or len(paths) != len(set(paths)):
        msg = "Terrain sources must be nonempty and unique, in priority order"
        raise ValueError(msg)
    return compose_terrain_layers(
        layers,
        target_crs=target_crs,
        directory=directory,
        resolution_m=resolution_m,
        max_cells=max_cells,
        assume_missing_crs=assume_missing_crs,
        bounds=bounds,
    )


def compose_terrain_layers(  # noqa: C901 - staged raster composition is intentionally explicit
    terrain_layers: Sequence[TerrainLayer],
    *,
    target_crs: CRS,
    directory: Path,
    resolution_m: float | None = None,
    max_cells: int = 100_000_000,
    assume_missing_crs: CRS | None = None,
    bounds: tuple[float, float, float, float] | None = None,
) -> TerrainMosaic:
    """Build a high-to-low priority mosaic, including optional clip polygons."""
    layers = tuple(
        TerrainLayer(item.raster.absolute(), item.clip.absolute() if item.clip else None) for item in terrain_layers
    )
    paths = tuple(item.raster for item in layers)
    if not paths:
        msg = "At least one terrain layer is required"
        raise ValueError(msg)
    # TGC commands may reuse a raster with distinct clips; each occurrence
    # remains a separate priority layer and source-index value.
    if len(paths) > np.iinfo(np.uint16).max:
        msg = "At most 65535 terrain sources are supported"
        raise ValueError(msg)
    if not target_crs.is_projected or not math.isclose(target_crs.linear_units_factor[1], 1.0):
        msg = "Target CRS must be projected in metres"
        raise ValueError(msg)
    if max_cells <= 0:
        msg = "max_cells must be positive"
        raise ValueError(msg)
    for source in paths:
        if not source.is_file():
            raise FileNotFoundError(source)
    for layer in layers:
        if layer.clip is not None and not layer.clip.is_file():
            raise FileNotFoundError(layer.clip)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / "analysis_dem.tif"
    provenance = directory / "terrain_source_index.tif"
    if target.exists() or provenance.exists():
        msg = "Terrain output already exists"
        raise FileExistsError(msg)

    with ExitStack() as stack:
        sources = [stack.enter_context(rasterio.open(path)) for path in paths]
        source_bounds = [_validated_bounds(source, target_crs, assume_missing_crs) for source in sources]
        if resolution_m is None:
            first = sources[0]
            first_crs = first.crs or assume_missing_crs
            if first_crs is None or first_crs != target_crs or not first_crs.is_projected:
                msg = "Specify --analysis-resolution-m if top-priority DEM requires reprojection"
                raise ValueError(msg)
            resolution_m = max(abs(first.res[0]), abs(first.res[1]))
        if not math.isfinite(resolution_m) or resolution_m <= 0:
            msg = "Analysis resolution must be finite and positive"
            raise ValueError(msg)

        requested = bounds or (
            min(b[0] for b in source_bounds),
            min(b[1] for b in source_bounds),
            max(b[2] for b in source_bounds),
            max(b[3] for b in source_bounds),
        )
        if len(requested) != 4 or not all(math.isfinite(item) for item in requested):
            msg = "Terrain bounds must be four finite coordinates"
            raise ValueError(msg)
        if requested[0] >= requested[2] or requested[1] >= requested[3]:
            msg = "Terrain bounds must have positive width and height"
            raise ValueError(msg)
        west = math.floor(requested[0] / resolution_m) * resolution_m
        south = math.floor(requested[1] / resolution_m) * resolution_m
        east = math.ceil(requested[2] / resolution_m) * resolution_m
        north = math.ceil(requested[3] / resolution_m) * resolution_m
        width = round((east - west) / resolution_m)
        height = round((north - south) / resolution_m)
        if width < 3 or height < 3 or width * height > max_cells:
            msg = f"Unsafe analysis raster size {width}x{height}; change analysis resolution"
            raise ValueError(msg)
        transform = from_origin(west, north, resolution_m, resolution_m)
        vrts = [
            stack.enter_context(
                WarpedVRT(
                    src,
                    src_crs=src.crs or assume_missing_crs,
                    crs=target_crs,
                    transform=transform,
                    width=width,
                    height=height,
                    resampling=Resampling.bilinear,
                    add_alpha=True,
                )
            )
            for src in sources
        ]
        clips = [_clip_geometries(layer.clip, target_crs) if layer.clip is not None else None for layer in layers]
        profile: RasterProfile = {
            "driver": "GTiff",
            "dtype": "float32",
            "count": 1,
            "height": height,
            "width": width,
            "crs": target_crs,
            "transform": transform,
            "nodata": -3.4028234663852886e38,
            "compress": "deflate",
            "BIGTIFF": "IF_SAFER",
        }
        provenance_profile: RasterProfile = {**profile, "dtype": "uint16", "nodata": 0}
        with (
            rasterio.open(target, "w", **profile) as dem_output,
            rasterio.open(provenance, "w", **provenance_profile) as provenance_output,
        ):
            for row in range(0, height, 512):
                for col in range(0, width, 512):
                    window = Window(col, row, min(512, width - col), min(512, height - row))
                    tile_shape = (int(window.height), int(window.width))
                    values = np.full(tile_shape, profile["nodata"], dtype=np.float32)
                    selected = np.zeros(tile_shape, dtype=np.uint16)
                    for rank, (vrt, clip_geometries) in enumerate(zip(vrts, clips, strict=True), start=1):
                        pixels = vrt.read(1, window=window)
                        # WarpedVRT(add_alpha=True) exposes source coverage in its
                        # final alpha band; read_masks(1) can report all-valid.
                        alpha = vrt.read(vrt.count, window=window)
                        valid = (selected == 0) & (alpha > 0) & np.isfinite(pixels)
                        if clip_geometries is not None:
                            clip_mask = rasterize(
                                [(geometry, 1) for geometry in clip_geometries],
                                out_shape=tile_shape,
                                transform=rasterio.windows.transform(window, transform),
                                fill=0,
                                dtype="uint8",
                            ).astype(bool)
                            valid &= clip_mask
                        values[valid] = pixels[valid]
                        selected[valid] = rank
                    dem_output.write(values, 1, window=window)
                    provenance_output.write(selected, 1, window=window)
    return TerrainMosaic(target, provenance, float(resolution_m))


def _clip_geometries(path: Path, target_crs: CRS) -> list[dict[str, object]]:
    """Load polygon clip geometries and transform them to the analysis CRS."""
    geometries: list[dict[str, object]] = []
    with fiona.open(path) as source:
        if not source.crs_wkt:
            msg = f"Terrain clip has no CRS: {path}"
            raise ValueError(msg)
        source_crs = CRS.from_wkt(source.crs_wkt)
        for feature in source:
            if feature.geometry is None:
                continue
            geometry = feature.geometry.__geo_interface__
            if source_crs != target_crs:
                geometry = transform_geom(source_crs, target_crs, geometry)
            if geometry.get("type") not in {"Polygon", "MultiPolygon"}:
                msg = f"Terrain clip must be polygonal: {path}"
                raise ValueError(msg)
            geometries.append(geometry)
    if not geometries:
        msg = f"Terrain clip has no usable polygons: {path}"
        raise ValueError(msg)
    return geometries


def _validated_bounds(
    source: rasterio.io.DatasetReader,
    target_crs: CRS,
    assume_missing_crs: CRS | None = None,
) -> tuple[float, float, float, float]:
    """Validate source geometry and transform its extent into the target CRS."""
    source_crs = source.crs or assume_missing_crs
    if source.count != 1 or source_crs is None:
        msg = f"Terrain must be single-band with CRS (or an explicit CRS override): {source.name}"
        raise ValueError(msg)
    if source.transform.b or source.transform.d:
        msg = f"Rotated terrain is unsupported: {source.name}"
        raise ValueError(msg)
    return transform_bounds(source_crs, target_crs, *source.bounds, densify_pts=21)
