import os
from pathlib import Path
import pytest
import numpy as np
import rasterio
import subprocess

from ryan_library.functions.gdal.asc2asc_logic import compute_max, compute_diff, compute_stat

def create_dummy_raster(filename, data, nodata=-9999.0):
    transform = rasterio.transform.from_origin(150.0, -30.0, 1.0, 1.0)
    with rasterio.open(
        filename,
        'w',
        driver='GTiff',
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs='+proj=latlong',
        transform=transform,
        nodata=nodata
    ) as dst:
        dst.write(data, 1)

@pytest.fixture
def test_grids(tmp_path):
    f1 = tmp_path / "grid1.tif"
    f2 = tmp_path / "grid2.tif"
    
    # 10x10 grids with some nodata
    d1 = np.random.rand(10, 10).astype(np.float32)
    d1[0:2, 0:2] = -9999.0
    
    d2 = np.random.rand(10, 10).astype(np.float32)
    d2[1:3, 1:3] = -9999.0
    
    create_dummy_raster(f1, d1)
    create_dummy_raster(f2, d2)
    
    return f1, f2

def run_exe(args):
    exe = r"C:\TUFLOW\asc_to_asc.2024-06-AB\asc_to_asc_w64.exe"
    subprocess.run([exe, "-b"] + args, check=True, capture_output=True)

def compare_rasters(f1, f2, nodata=-9999.0):
    assert Path(f1).exists(), f"EXE output missing: {f1}"
    assert Path(f2).exists(), f"PY output missing: {f2}"
    
    with rasterio.open(f1) as r1, rasterio.open(f2) as r2:
        d1 = r1.read(1)
        d2 = r2.read(1)
        
        # Check nodata locations match
        mask1 = (d1 == nodata)
        mask2 = (d2 == nodata)
        np.testing.assert_array_equal(mask1, mask2, err_msg="Nodata masks do not match")
        
        # Check valid values match closely
        valid = ~mask1
        np.testing.assert_allclose(d1[valid], d2[valid], rtol=1e-4, atol=1e-4, err_msg="Valid values do not match")

def test_compute_max(test_grids, tmp_path):
    f1, f2 = test_grids
    out_exe = tmp_path / "out_exe_max.tif"
    out_py = tmp_path / "out_py_max.tif"
    
    run_exe(["-max", "-out", str(out_exe), str(f1), str(f2)])
    compute_max([str(f1), str(f2)], str(out_py))
    
    compare_rasters(out_exe, out_py)

def test_compute_diff(test_grids, tmp_path):
    f1, f2 = test_grids
    out_exe = tmp_path / "out_exe_diff.tif"
    out_py = tmp_path / "out_py_diff.tif"
    
    run_exe(["-diff", "-out", str(out_exe), str(f1), str(f2)])
    compute_diff(str(f1), str(f2), str(out_py))
    
    compare_rasters(out_exe, out_py)

def test_compute_diff_change(test_grids, tmp_path):
    f1, f2 = test_grids
    out_exe = tmp_path / "out_exe_diff_change.tif"
    out_py = tmp_path / "out_py_diff_change.tif"
    
    run_exe(["-diff", "-change", "-out", str(out_exe), str(f1), str(f2)])
    compute_diff(str(f1), str(f2), str(out_py), change=True)
    
    compare_rasters(out_exe, out_py)

def test_compute_diff_nowetdry(test_grids, tmp_path):
    f1, f2 = test_grids
    out_exe = tmp_path / "out_exe_diff_nowetdry.tif"
    out_py = tmp_path / "out_py_diff_nowetdry.tif"
    
    run_exe(["-diff", "-nowetdry", "-out", str(out_exe), str(f1), str(f2)])
    compute_diff(str(f1), str(f2), str(out_py), nowetdry=True)
    
    compare_rasters(out_exe, out_py)

def test_compute_stat_mean(test_grids, tmp_path):
    f1, f2 = test_grids
    out_exe_base = tmp_path / "out_exe_stat.tif"
    out_exe_real = tmp_path / "out_exe_stat_Mean_Val.tif"
    out_py = tmp_path / "out_py_stat.tif"
    
    run_exe(["-statMean", "-out", str(out_exe_base), str(f1), str(f2)])
    compute_stat("Mean", [str(f1), str(f2)], str(out_py))
    
    compare_rasters(out_exe_real, out_py)
