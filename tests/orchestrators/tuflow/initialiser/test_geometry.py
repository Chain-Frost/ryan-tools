"""AOI union/circular-buffer behaviour and cardinal location tests."""

import numpy as np
import pytest
from rasterio.transform import from_origin
from shapely.geometry import box

from ryan_library.orchestrators.tuflow.initialiser.geometry import make_domain, mask_to_polygon


def test_mask_vectorisation() -> None:
    mask = np.zeros((4, 4), dtype=bool)
    mask[1, 1] = True
    polygon = mask_to_polygon(mask, from_origin(0, 4, 1, 1))
    assert polygon.area == pytest.approx(1.0)


@pytest.mark.parametrize("offset", [-4000, 0, 4000])
def test_location_has_margin_and_outward_400m_alignment(offset: int) -> None:
    domain = make_domain(
        np.zeros((20, 20), dtype=np.int16),
        aoi=box(offset + 400, offset + 400, offset + 800, offset + 800),
        transform=from_origin(offset - 2000, offset + 2000, 200, 200),
        raster_bounds=(offset - 2000, offset - 2000, offset + 2000, offset + 2000),
        buffer_m=400,
    )
    assert domain.location.bounds == (offset - 400, offset - 400, offset + 1600, offset + 1600)
    assert domain.location.covers(domain.active.buffer(400))


def test_location_rounds_fractional_bounds_outwards() -> None:
    domain = make_domain(
        np.zeros((20, 20), dtype=np.int16),
        aoi=box(411.5, 423.5, 811.5, 823.5),
        transform=from_origin(-2000, 2000, 200, 200),
        raster_bounds=(-2000, -2000, 2000, 2000),
        buffer_m=400,
    )
    assert domain.location.bounds == (-400, -400, 2000, 2000)
    assert domain.location.covers(domain.active.buffer(400))


def test_buffer_requires_terrain_coverage() -> None:
    directions = np.full((8, 8), 6, dtype=np.int16)
    directions[0, :] = 0
    directions[:, 0] = 0
    with pytest.raises(ValueError, match="buffer"):
        make_domain(
            directions,
            aoi=box(3, 3, 4, 4),
            transform=from_origin(0, 8, 1, 1),
            raster_bounds=(0, 0, 8, 8),
            buffer_m=500,
        )


def test_domain_rejects_unknown_d8_adjacent_to_upstream_catchment() -> None:
    directions = np.zeros((9, 9), dtype=np.int16)
    directions[2:5, 4] = 6
    valid = np.ones(directions.shape, dtype=bool)
    valid[3, 3] = False
    with pytest.raises(ValueError, match="NoData/unknown D8 terrain"):
        make_domain(
            directions,
            aoi=box(4.2, 3.2, 4.8, 3.8),
            transform=from_origin(0, 9, 1, 1),
            raster_bounds=(0, 0, 9, 9),
            buffer_m=0.25,
            valid_cells=valid,
        )
