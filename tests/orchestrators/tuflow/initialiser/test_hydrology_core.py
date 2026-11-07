"""Small deterministic DEM graph and profile checks."""

import numpy as np
import pytest

from ryan_library.orchestrators.tuflow.initialiser.d8 import contributing_cells, headwater_extensions, stream_mask
from ryan_library.orchestrators.tuflow.initialiser.profiles import equal_area_slope, stream_reaches


def test_all_aoi_cells_not_only_outlet() -> None:
    # Two independent columns flow south and meet different AOI cells.
    dirs = np.full((5, 5), 6, dtype=np.int16)
    dirs[:, 0] = 0
    dirs[:, 4] = 0
    aoi = np.zeros_like(dirs, dtype=bool)
    aoi[3, 1] = True
    aoi[2, 3] = True
    mask = contributing_cells(dirs, aoi, require_complete=False)
    assert np.all(mask[:4, 1])
    assert np.all(mask[:3, 3])
    assert not mask[3, 3]
    assert not mask[0, 2]


def test_catchment_edge_must_fail() -> None:
    dirs = np.full((4, 4), 6, dtype=np.int16)
    seeds = np.zeros_like(dirs, dtype=bool)
    seeds[2, 2] = True
    with pytest.raises(ValueError, match="edge"):
        contributing_cells(dirs, seeds)


def test_min_area_changes_stream_not_catchment() -> None:
    acc = np.asarray([[1.0, 2.0, 3.0], [5.0, 7.0, 9.0]])
    assert stream_mask(acc, cell_area_m2=100, min_area_m2=500).sum() == 3
    assert stream_mask(acc, cell_area_m2=100, min_area_m2=800).sum() == 1


def test_headwaters_extend_from_threshold() -> None:
    dirs = np.zeros((5, 3), dtype=np.int16)
    dirs[:4, 1] = 6
    stream = np.zeros_like(dirs, dtype=bool)
    stream[2:5, 1] = True
    acc = np.zeros_like(dirs, dtype=float)
    acc[:, 1] = [1, 2, 3, 4, 5]
    paths = headwater_extensions(dirs, stream, acc)
    assert paths == [[(0, 1), (1, 1), (2, 1)]]


def test_stream_reaches() -> None:
    dirs = np.zeros((5, 3), dtype=np.int16)
    dirs[:4, 1] = 6
    stream = np.zeros_like(dirs, dtype=bool)
    stream[1:5, 1] = True
    assert stream_reaches(dirs, stream) == [[(1, 1), (2, 1), (3, 1), (4, 1)]]


def test_equal_area_slope_linear_profile() -> None:
    x = np.array([0, 100, 200], dtype=float)
    z = np.array([10, 11, 12], dtype=float)
    assert equal_area_slope(x, z) == pytest.approx(0.01)


def test_equal_area_slope_rejects_unordered() -> None:
    with pytest.raises(ValueError, match="increase"):
        equal_area_slope(np.array([0, 10, 5.0]), np.array([0, 1, 2.0]))


def test_unknown_d8_neighbour_fails_instead_of_becoming_a_sink() -> None:
    dirs = np.zeros((7, 7), dtype=np.int16)
    dirs[2:4, 3] = 6
    aoi = np.zeros_like(dirs, dtype=bool)
    aoi[4, 3] = True
    valid = np.ones(dirs.shape, dtype=bool)
    valid[2, 2] = False  # Unknown neighbour could discharge into the upstream route.
    with pytest.raises(ValueError, match="NoData/unknown D8 terrain"):
        contributing_cells(dirs, aoi, valid_cells=valid, require_complete=False)


def test_nodata_inside_aoi_fails_before_delineation() -> None:
    dirs = np.zeros((5, 5), dtype=np.int16)
    aoi = np.zeros_like(dirs, dtype=bool)
    aoi[2, 2] = True
    valid = np.ones_like(aoi)
    valid[2, 2] = False
    with pytest.raises(ValueError, match="AOI intersects NoData"):
        contributing_cells(dirs, aoi, valid_cells=valid, require_complete=False)


def test_valid_d8_sink_and_distant_nodata_remain_supported() -> None:
    dirs = np.zeros((7, 7), dtype=np.int16)
    dirs[2:4, 3] = 6
    aoi = np.zeros_like(dirs, dtype=bool)
    aoi[4, 3] = True
    valid = np.ones_like(aoi)
    valid[0, 0] = False
    result = contributing_cells(dirs, aoi, valid_cells=valid, require_complete=False)
    assert set(zip(*np.nonzero(result), strict=True)) == {(2, 3), (3, 3), (4, 3)}


def test_invalid_d8_validity_mask_shape_fails() -> None:
    dirs = np.zeros((5, 5), dtype=np.int16)
    aoi = np.zeros_like(dirs, dtype=bool)
    aoi[2, 2] = True
    with pytest.raises(ValueError, match="validity mask"):
        contributing_cells(dirs, aoi, valid_cells=np.ones((4, 4), dtype=bool), require_complete=False)
