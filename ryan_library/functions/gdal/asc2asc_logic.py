from __future__ import annotations

import numpy as np
import rasterio
from rasterio.windows import Window
from loguru import logger
from typing import Any
from pathlib import Path

def _get_common_metadata(input_files: list[str]) -> dict[str, Any]:
    with rasterio.open(input_files[0]) as src:
        meta = src.meta.copy()
    return meta

def _parse_creation_options(extra_args: list[str] | None) -> dict[str, Any]:
    """Parse extra_args like ['-co', 'COMPRESS=DEFLATE'] into a rasterio profile dict."""
    options = {}
    if not extra_args:
        return options
    i = 0
    while i < len(extra_args):
        if extra_args[i].lower() == '-co' and i + 1 < len(extra_args):
            key_val = extra_args[i+1].split('=', 1)
            if len(key_val) == 2:
                key, val = key_val
                # rasterio expects lowercase keys for compress, tiled, etc.
                options[key.lower()] = val
            i += 2
        else:
            i += 1
    return options

def compute_max(input_files: list[str], output_file: str, extra_args: list[str] | None = None) -> None:
    """Computes the maximum value across multiple rasters using chunked processing."""
    meta = _get_common_metadata(input_files)
    options = _parse_creation_options(extra_args)
    meta.update(options)
    
    with rasterio.open(input_files[0]) as src0:
        block_shapes = list(src0.block_windows(1))
    
    with rasterio.open(output_file, 'w', **meta) as dst:
        for _, window in block_shapes:
            max_data = None
            any_valid = None
            for f in input_files:
                with rasterio.open(f) as src:
                    data = src.read(1, window=window)
                    nodata = src.nodata
                    valid_mask = (data != nodata) if nodata is not None else np.ones_like(data, dtype=bool)
                    data_filled = np.where(valid_mask, data, -np.inf)
                    
                    if max_data is None:
                        max_data = data_filled
                        any_valid = valid_mask
                    else:
                        max_data = np.maximum(max_data, data_filled)
                        any_valid = any_valid | valid_mask
            
            out_nodata = meta.get('nodata', -9999.0)
            out_data = np.where(any_valid, max_data, out_nodata)
            dst.write(out_data, 1, window=window)

def compute_diff(file1: str, file2: str, output_file: str, change: bool = False, nowetdry: bool = False, extra_args: list[str] | None = None) -> None:
    """
    Computes the difference between two rasters (file1 - file2) using chunked processing.
    - change: Assumes nodata is 0.0 and computes diff everywhere.
    - nowetdry: Only computes diff where both are valid (no wet/dry output grid).
    """
    meta = _get_common_metadata([file1])
    options = _parse_creation_options(extra_args)
    meta.update(options)
    
    out_nodata = meta.get('nodata', -9999.0)
    
    with rasterio.open(file1) as src1, rasterio.open(file2) as src2:
        nodata1 = src1.nodata
        nodata2 = src2.nodata
        
        with rasterio.open(output_file, 'w', **meta) as dst:
            for _, window in src1.block_windows(1):
                d1 = src1.read(1, window=window)
                d2 = src2.read(1, window=window)
                
                if change:
                    m1 = np.where(d1 == nodata1, 0.0, d1) if nodata1 is not None else d1
                    m2 = np.where(d2 == nodata2, 0.0, d2) if nodata2 is not None else d2
                    diff = m1 - m2
                    
                    # If both cells were nodata, output should remain nodata
                    mask1 = (d1 == nodata1) if nodata1 is not None else np.zeros_like(d1, dtype=bool)
                    mask2 = (d2 == nodata2) if nodata2 is not None else np.zeros_like(d2, dtype=bool)
                    diff[mask1 & mask2] = out_nodata
                else:
                    mask1 = (d1 != nodata1) if nodata1 is not None else np.ones_like(d1, dtype=bool)
                    mask2 = (d2 != nodata2) if nodata2 is not None else np.ones_like(d2, dtype=bool)
                    valid = mask1 & mask2
                    
                    diff = np.full(d1.shape, out_nodata, dtype=d1.dtype)
                    diff[valid] = d1[valid] - d2[valid]
                
                dst.write(diff, 1, window=window)
                
            # If not change and not nowetdry, typical asc_to_asc creates a _wd (wet/dry) grid
            if not change and not nowetdry:
                wd_file = str(Path(output_file).with_suffix("")) + "_wd.tif"
                with rasterio.open(wd_file, 'w', **meta) as wd_dst:
                    for _, window in src1.block_windows(1):
                        d1 = src1.read(1, window=window)
                        d2 = src2.read(1, window=window)
                        
                        mask1 = (d1 != nodata1) if nodata1 is not None else np.ones_like(d1, dtype=bool)
                        mask2 = (d2 != nodata2) if nodata2 is not None else np.ones_like(d2, dtype=bool)
                        
                        wd = np.full(d1.shape, out_nodata, dtype=d1.dtype)
                        # Was Wet, Now Dry (-99) -> Developed (f1) is dry, Existing (f2) is wet
                        wd[(~mask1) & mask2] = -99
                        # Was Dry, Now Wet (+99) -> Developed (f1) is wet, Existing (f2) is dry
                        wd[mask1 & (~mask2)] = 99
                        
                        wd_dst.write(wd, 1, window=window)

def compute_stat(stat_type: str, input_files: list[str], output_file: str, extra_args: list[str] | None = None) -> None:
    """Computes basic statistics across multiple rasters using chunked processing."""
    stat_type = stat_type.lower().replace('-stat', '')
    meta = _get_common_metadata(input_files)
    options = _parse_creation_options(extra_args)
    meta.update(options)
    
    with rasterio.open(input_files[0]) as src0:
        block_shapes = list(src0.block_windows(1))
    
    with rasterio.open(output_file, 'w', **meta) as dst:
        for _, window in block_shapes:
            stack = []
            valid_stack = []
            for f in input_files:
                with rasterio.open(f) as src:
                    data = src.read(1, window=window)
                    nodata = src.nodata
                    valid_stack.append((data != nodata) if nodata is not None else np.ones_like(data, dtype=bool))
                    stack.append(data)
            
            stack = np.stack(stack, axis=0)
            valid_stack = np.stack(valid_stack, axis=0)
            all_valid = np.all(valid_stack, axis=0)
            
            if stat_type == 'mean':
                res = np.mean(stack, axis=0)
            elif stat_type == 'median':
                res = np.median(stack, axis=0)
            elif stat_type == 'min':
                res = np.min(stack, axis=0)
            elif stat_type == 'max':
                res = np.max(stack, axis=0)
            else:
                logger.error("Unsupported stat_type: {}", stat_type)
                raise ValueError(f"Unsupported stat_type: {stat_type}")
            
            out_nodata = meta.get('nodata', -9999.0)
            out_data = np.where(all_valid, res, out_nodata)
            dst.write(out_data.astype(meta['dtype']), 1, window=window)
