"""Auditable D8 local subcatchment partitions, topology and reach profiles."""

import math
from pathlib import Path

import fiona
import numpy as np
import pytest
from numpy.typing import NDArray
from rasterio.crs import CRS
from rasterio.transform import from_origin
from shapely.geometry import box
from shapely.ops import unary_union

from ryan_library.functions.hydrology.d8 import contributing_cells
from ryan_library.orchestrators.tuflow.initialiser.subcatchment_outputs import (
    write_reach_subcatchment_summary,
    write_reach_subcatchments,
)
from ryan_library.orchestrators.tuflow.initialiser.subcatchments import (
    SubcatchmentResult,
    delineate_reach_subcatchments,
)


def _network() -> tuple[NDArray[np.int16], NDArray[np.float64], NDArray[np.bool_], NDArray[np.bool_]]:
    """Two headwaters converge into a trunk; one side slope bypasses streams."""
    d8 = np.zeros((11, 11), dtype=np.int16)
    d8[2, 3] = 6
    d8[3, 3] = 7
    d8[4, 4] = 7
    d8[2, 7] = 6
    d8[3, 7] = 5
    d8[4, 6] = 5
    d8[5:8, 5] = 6
    d8[6, 4] = 8
    d8[7, 6] = 4
    elevations = np.tile(np.arange(11.0, 0, -1, dtype=np.float64)[:, None], (1, 11))
    aoi = np.zeros(d8.shape, dtype=bool)
    aoi[8, 5] = True
    stream = np.zeros(d8.shape, dtype=bool)
    stream[3, 3] = True
    stream[4, 4] = True
    stream[3, 7] = True
    stream[4, 6] = True
    stream[5:9, 5] = True
    return d8, elevations, aoi, stream


def _run(
    *,
    stream_override: NDArray[np.bool_] | None = None,
    excluded: tuple[int, int] | None = None,
    metre_step: float = 1.0,
) -> tuple[SubcatchmentResult, NDArray[np.bool_]]:
    d8, elevations, aoi, stream = _network()
    contributing = contributing_cells(d8, aoi, require_complete=False)
    detailed = contributing.copy()
    if excluded is not None:
        detailed[excluded] = False
    active_stream = stream if stream_override is None else stream_override
    active_stream = active_stream & detailed
    step = metre_step
    polygon = box(5.15 * step, 2.15 * step, 5.85 * step, 2.85 * step)
    result = delineate_reach_subcatchments(
        d8,
        np.ones(d8.shape, dtype=float),
        contributing,
        detailed,
        active_stream,
        elevations,
        [polygon],
        transform=from_origin(0, 11 * step, step, step),
    )
    return result, contributing


def test_reach_local_subcatchments_conserve_area_and_have_downstream_links(tmp_path: Path) -> None:
    result, contributing = _run()
    assert result.channel_reach_count == 3
    assert result.analysed_cell_count == int(np.count_nonzero(contributing))
    assert result.excluded_cell_count == 0
    assert sum(item.local_area_m2 for item in result.subcatchments) == pytest.approx(
        float(np.count_nonzero(contributing))
    )
    polygons = [item.polygon for item in result.subcatchments]
    assert unary_union(polygons).area == pytest.approx(float(np.count_nonzero(contributing)))
    assert all(left.intersection(right).area == 0 for i, left in enumerate(polygons) for right in polygons[i + 1 :])
    reaches = [item for item in result.subcatchments if item.kind == "channel_reach"]
    assert len(reaches) == 3
    assert [r.reach_id for r in reaches] == [1, 2, 3]
    assert reaches[0].downstream_reach_id == 3
    assert reaches[1].downstream_reach_id == 3
    assert reaches[2].downstream_subcatchment_id is None
    assert reaches[2].cumulative_area_m2 == pytest.approx(float(np.count_nonzero(contributing)))
    assert reaches[2].cumulative_area_m2 is not None
    assert reaches[2].cumulative_area_m2 > reaches[2].local_area_m2
    assert all(item.mean_terrain_slope == pytest.approx(1) for item in reaches)
    assert all(item.stream_length_m is not None and item.stream_length_m > 0 for item in reaches)
    gpkg = tmp_path / "hydrology.gpkg"
    with fiona.open(
        gpkg,
        "w",
        driver="GPKG",
        layer="original_hydrology",
        schema={"geometry": "Polygon", "properties": {"id": "int"}},
        crs_wkt=CRS.from_epsg(28350).to_wkt(),
    ):
        pass
    write_reach_subcatchments(gpkg, result, crs_wkt=CRS.from_epsg(28350).to_wkt(), transform=from_origin(0, 11, 1, 1))
    assert set(fiona.listlayers(gpkg)) == {"original_hydrology", "subcatchments"}
    with fiona.open(gpkg, layer="subcatchments") as output:
        assert len(output) == len(result.subcatchments)
        assert output.crs.to_epsg() == 28350
        attrs = [feature.properties for feature in output]
        for item, attributes in zip(result.subcatchments, attrs, strict=True):
            if item.outlet is not None:
                row, col = item.outlet
                assert attributes["outlet_x"] == pytest.approx(col + 0.5)
                assert attributes["outlet_y"] == pytest.approx(11 - row - 0.5)
        areas = [item["local_area_m2"] for item in attrs]
        assert all(isinstance(area, (float, int)) for area in areas)
        assert sum(float(area) for area in areas if isinstance(area, (float, int))) == pytest.approx(
            float(np.count_nonzero(contributing))
        )
    report = tmp_path / "subcatchments.json"
    write_reach_subcatchment_summary(report, result)
    assert '"channel_reach_count": 3' in report.read_text(encoding="utf-8")


def test_stream_threshold_empty_still_covers_entire_aoi_contribution() -> None:
    _, _, _, stream = _network()
    result, contributing = _run(stream_override=np.zeros_like(stream, dtype=bool))
    assert result.channel_reach_count == 0
    assert len(result.subcatchments) == 1
    assert result.subcatchments[0].kind == "aoi_direct"
    assert result.subcatchments[0].reach_id is None
    assert result.subcatchments[0].aoi_id == 1
    assert result.subcatchments[0].stream_length_m is None
    assert result.subcatchments[0].local_area_m2 == pytest.approx(float(np.count_nonzero(contributing)))


def test_disabled_exclusion_omits_only_detailed_area_without_losing_baseline() -> None:
    result, contributing = _run(excluded=(3, 3))
    assert result.excluded_cell_count == 1
    assert result.analysed_cell_count + result.excluded_cell_count == int(np.count_nonzero(contributing))
    assert sum(x.local_area_m2 for x in result.subcatchments) == pytest.approx(float(result.analysed_cell_count))
    assert sum(x.full_local_area_m2 for x in result.subcatchments) == pytest.approx(
        float(np.count_nonzero(contributing))
    )
    assert any(x.profile_status == "partial_exclusion" for x in result.subcatchments)


def test_non_square_raster_cell_areas_and_mean_slopes() -> None:
    d8, elevations, aoi, stream = _network()
    contributing = contributing_cells(d8, aoi, require_complete=False)
    result = delineate_reach_subcatchments(
        d8,
        np.ones(d8.shape, dtype=float),
        contributing,
        contributing,
        stream,
        elevations,
        [box(10.2, 6.2, 11.8, 8.8)],
        transform=from_origin(0, 33, 2, 3),
    )
    assert sum(x.local_area_m2 for x in result.subcatchments) == pytest.approx(np.count_nonzero(contributing) * 6)
    assert all(
        math.isfinite(x.mean_terrain_slope) and x.mean_terrain_slope >= 0
        for x in result.subcatchments
        if x.mean_terrain_slope is not None
    )


def test_no_gaps_when_stream_contains_a_single_cell() -> None:
    d8, elevations, aoi, _ = _network()
    contributing = contributing_cells(d8, aoi, require_complete=False)
    isolated = np.zeros(d8.shape, dtype=bool)
    isolated[6, 5] = True
    result = delineate_reach_subcatchments(
        d8,
        np.ones(d8.shape, dtype=float),
        contributing,
        contributing,
        isolated,
        elevations,
        [box(5.15, 2.15, 5.85, 2.85)],
        transform=from_origin(0, 11, 1, 1),
    )
    assert result.channel_reach_count == 1
    assert result.subcatchments[0].stream_length_m == 0
    assert result.subcatchments[0].stream_slope_ea is None
    assert sum(x.local_area_m2 for x in result.subcatchments) == pytest.approx(float(np.count_nonzero(contributing)))


def test_failure_on_missing_channel_analysis_cells() -> None:
    d8, elevations, aoi, stream = _network()
    contributing = contributing_cells(d8, aoi, require_complete=False)
    detailed = contributing.copy()
    detailed[5, 5] = False
    with pytest.raises(ValueError, match="Stream and detailed masks"):
        delineate_reach_subcatchments(
            d8,
            np.ones(d8.shape),
            contributing,
            detailed,
            stream,
            elevations,
            [box(5.15, 2.15, 5.85, 2.85)],
            transform=from_origin(0, 11, 1, 1),
        )


def test_two_disconnected_aois_have_independent_direct_catchments() -> None:
    directions = np.zeros((11, 11), dtype=np.int16)
    directions[3:7, 2] = 6
    directions[4:8, 8] = 6
    seeds = np.zeros_like(directions, dtype=bool)
    seeds[7, 2] = True
    seeds[8, 8] = True
    contributing = contributing_cells(directions, seeds, require_complete=False)
    terrain = np.tile(np.arange(11.0, 0, -1)[:, None], (1, 11))
    result = delineate_reach_subcatchments(
        directions,
        np.ones(directions.shape),
        contributing,
        contributing,
        np.zeros(directions.shape, dtype=bool),
        terrain,
        [box(2.2, 3.2, 2.8, 3.8), box(8.2, 2.2, 8.8, 2.8)],
        transform=from_origin(0, 11, 1, 1),
    )
    assert result.channel_reach_count == 0
    assert [item.aoi_id for item in result.subcatchments] == [1, 2]
    assert [item.local_area_m2 for item in result.subcatchments] == [5.0, 5.0]
    assert result.full_contributing_cell_count == 10


def test_nested_aois_share_cumulative_cells_without_double_counting_local_area() -> None:
    directions = np.zeros((11, 11), dtype=np.int16)
    directions[2:9, 5] = 6
    seeds = np.zeros_like(directions, dtype=bool)
    seeds[5, 5] = True
    seeds[9, 5] = True
    contributing = contributing_cells(directions, seeds, require_complete=False)
    stream = np.zeros_like(directions, dtype=bool)
    stream[3:10, 5] = True
    terrain = np.tile(np.arange(11.0, 0, -1)[:, None], (1, 11))
    result = delineate_reach_subcatchments(
        directions,
        np.ones(directions.shape),
        contributing,
        contributing,
        stream,
        terrain,
        [box(5.2, 5.2, 5.8, 5.8), box(5.2, 1.2, 5.8, 1.8)],
        transform=from_origin(0, 11, 1, 1),
    )
    assert sum(item.local_area_m2 for item in result.subcatchments) == pytest.approx(np.count_nonzero(contributing))
    assert result.full_contributing_cell_count == 8
    assert result.subcatchments[0].cumulative_area_m2 == pytest.approx(8.0)


def test_signed_negative_reach_slope_is_retained() -> None:
    directions, terrain, aoi, stream = _network()
    terrain = -terrain
    contributing = contributing_cells(directions, aoi, require_complete=False)
    result = delineate_reach_subcatchments(
        directions,
        np.ones(directions.shape),
        contributing,
        contributing,
        stream,
        terrain,
        [box(5.15, 2.15, 5.85, 2.85)],
        transform=from_origin(0, 11, 1, 1),
    )
    channels = [item for item in result.subcatchments if item.kind == "channel_reach"]
    assert channels
    assert all(item.stream_slope_ea is not None and item.stream_slope_ea < 0 for item in channels)
