"""PyTUFLOW terrain-stack parsing and clipped mosaic tests."""

from pathlib import Path

import fiona
import numpy as np
import pytest
import rasterio
from rasterio.crs import CRS
from rasterio.transform import from_origin
from shapely.geometry import box, mapping

from ryan_library.functions.gdal.terrain_mosaic import TerrainLayer, compose_terrain, compose_terrain_layers
from ryan_library.orchestrators.tuflow.initialiser.tgc_terrain import read_tgc_terrain_stack


def _raster(path: Path, value: float) -> None:
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        count=1,
        width=10,
        height=10,
        crs="EPSG:28350",
        transform=from_origin(0, 10, 1, 1),
        dtype="float32",
        nodata=-9999,
    ) as output:
        output.write(np.full((10, 10), value, dtype=np.float32), 1)


def _clip(path: Path, bounds: tuple[float, float, float, float] = (4, 4, 6, 6)) -> None:
    with fiona.open(
        path,
        "w",
        driver="GPKG",
        layer="clip",
        crs_wkt=CRS.from_epsg(28350).to_wkt(),
        schema={"geometry": "Polygon", "properties": {}},
    ) as output:
        output.write({"geometry": mapping(box(*bounds)), "properties": {}})


def test_tgc_global_stack_reverses_tuflow_precedence(tmp_path: Path) -> None:
    low, high, dev = tmp_path / "low.tif", tmp_path / "high.tif", tmp_path / "dev.tif"
    clip = tmp_path / "clip.gpkg"
    for path in (low, high, dev):
        _raster(path, 1)
    _clip(clip)
    tgc = tmp_path / "model.tgc"
    tgc.write_text(
        "\n".join(
            (
                f"Read Grid Zpts == {low}",
                "If Scenario == DEV",
                f"  Read Grid Zpts == {dev}",
                "End If",
                f"Read Grid Zpts == {high} | {clip}",
                f"Read GIS Zpts == {clip}",
            )
        ),
        encoding="utf-8",
    )

    stack = read_tgc_terrain_stack(tgc, global_only=True)

    assert [item.raster for item in stack.layers] == [high, low]
    assert stack.layers[0].clip == clip
    assert len(stack.skipped_commands) == 1
    dev_stack = read_tgc_terrain_stack(tgc, scenarios=("DEV",))
    assert [item.raster for item in dev_stack.layers] == [high, dev, low]


def test_clipped_layer_and_explicit_bounds(tmp_path: Path) -> None:
    low, high = tmp_path / "low.tif", tmp_path / "high.tif"
    clip = tmp_path / "clip.gpkg"
    _raster(low, 1)
    _raster(high, 2)
    _clip(clip)

    mosaic = compose_terrain_layers(
        [TerrainLayer(high, clip), TerrainLayer(low)],
        target_crs=CRS.from_epsg(28350),
        directory=tmp_path / "mosaic",
        resolution_m=1,
        bounds=(2, 2, 8, 8),
    )

    with (
        rasterio.open(mosaic.dem) as source,
        rasterio.open(mosaic.source_index) as index,
    ):
        values = source.read(1)
        ranks = index.read(1)
        assert tuple(source.bounds) == (2, 2, 8, 8)
    assert np.count_nonzero(values == 2) == 4
    assert np.count_nonzero(ranks == 1) == 4
    assert np.all(values[ranks == 2] == 1)


def test_tgc_repeated_raster_with_different_polygon_clips(tmp_path: Path) -> None:
    low, patch = tmp_path / "low.tif", tmp_path / "patch.tif"
    west, east = tmp_path / "west.gpkg", tmp_path / "east.gpkg"
    _raster(low, 1)
    _raster(patch, 2)
    _clip(west, (1, 1, 7, 7))
    _clip(east, (5, 3, 9, 9))
    tgc = tmp_path / "repeat.tgc"
    tgc.write_text(
        "\n".join(
            (
                f"Read Grid Zpts == {low}",
                f"Read Grid Zpts == {patch} | {west}",
                f"Read Grid Zpts == {patch} | {east}",
            )
        ),
        encoding="utf-8",
    )

    stack = read_tgc_terrain_stack(tgc, global_only=True)
    assert stack.layers == (
        TerrainLayer(patch, east),
        TerrainLayer(patch, west),
        TerrainLayer(low),
    )

    mosaic = compose_terrain_layers(
        stack.layers,
        target_crs=CRS.from_epsg(28350),
        directory=tmp_path / "repeat_mosaic",
        resolution_m=1,
    )
    with rasterio.open(mosaic.dem) as source, rasterio.open(mosaic.source_index) as index:
        values = source.read(1)
        ranks = index.read(1)

    # The most recent TGC command wins where the west/east clips overlap.
    assert (values[4, 5], ranks[4, 5]) == (2, 1)
    assert (values[7, 2], ranks[7, 2]) == (2, 2)
    assert (values[9, 0], ranks[9, 0]) == (1, 3)


def test_explicit_raster_inputs_still_reject_duplicate_paths(tmp_path: Path) -> None:
    raster = tmp_path / "terrain.tif"
    _raster(raster, 1)
    with pytest.raises(ValueError, match="unique"):
        compose_terrain(
            [raster, raster],
            target_crs=CRS.from_epsg(28350),
            directory=tmp_path / "explicit",
        )
