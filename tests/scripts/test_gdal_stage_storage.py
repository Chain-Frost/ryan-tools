"""
Tests for the chunked histogram stage-storage logic.
"""

from pathlib import Path

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_origin

from ryan_library.functions.gdal.stage_storage import compute_stage_storage


@pytest.fixture
def synthetic_bowl_dem(tmp_path: Path) -> Path:
    """
    Creates a synthetic 10x10 bowl-shaped DEM.
    Cell size is 1x1.
    Elevations are defined such that the volume can be analytically verified.
    """
    dem_path = tmp_path / "bowl.tif"
    
    # Create a 10x10 array with known elevations
    # Let's make a flat bottom at elev=0 in the center 2x2 cells, 
    # surrounded by elev=1, then elev=2, etc.
    # Actually, let's just make it simpler:
    # 5 cells at elev = 1.0
    # 10 cells at elev = 2.0
    # 20 cells at elev = 3.0
    # others at NoData (-9999)
    
    data = np.full((10, 10), -9999.0, dtype=np.float32)
    data[0, 0:5] = 1.0
    data[1, 0:10] = 2.0
    data[2:4, 0:10] = 3.0  # 2 rows of 10 = 20 cells
    
    transform = from_origin(0, 10, 1, 1)  # 1x1 cells
    
    with rasterio.open(
        dem_path,
        "w",
        driver="GTiff",
        height=10,
        width=10,
        count=1,
        dtype=data.dtype,
        crs="EPSG:32750",
        transform=transform,
        nodata=-9999.0,
    ) as dst:
        dst.write(data, 1)
        
    return dem_path


def test_compute_stage_storage(synthetic_bowl_dem: Path):
    """
    Test the stage-storage volumes against known analytical volumes.
    """
    levels = [0.5, 1.5, 2.5, 3.5]
    
    volumes = compute_stage_storage(synthetic_bowl_dem, levels=levels)
    
    # Analytical verification:
    # Level = 0.5
    # No cells are below 0.5. Volume = 0
    assert volumes[0.5] == 0.0
    
    # Level = 1.5
    # 5 cells at Elev=1.0. 
    # Volume = 5 cells * 1m^2 * (1.5 - 1.0) = 2.5
    assert volumes[1.5] == 2.5
    
    # Level = 2.5
    # 5 cells at Elev=1.0. Volume contribution = 5 * (2.5 - 1.0) = 7.5
    # 10 cells at Elev=2.0. Volume contribution = 10 * (2.5 - 2.0) = 5.0
    # Total Volume = 12.5
    assert volumes[2.5] == 12.5
    
    # Level = 3.5
    # 5 cells at Elev=1.0. Volume = 5 * 2.5 = 12.5
    # 10 cells at Elev=2.0. Volume = 10 * 1.5 = 15.0
    # 20 cells at Elev=3.0. Volume = 20 * 0.5 = 10.0
    # Total Volume = 37.5
    assert volumes[3.5] == 37.5
