"""
Robust Stage-Storage calculation algorithms.
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

import numpy as np
import rasterio
from loguru import logger


def compute_stage_storage(
    dem_path: Path,
    levels: Sequence[float],
    nodata_value: float | None = None
) -> dict[float, float]:
    """
    Computes exact stage-storage (volume) curves for a DEM across arbitrary elevation levels
    using a memory-safe, highly efficient chunked histogram algorithm.
    
    Returns:
        dict: Mapping of {level (m): volume (m^3)}
    """
    levels = np.asarray(levels, dtype=np.float64)
    if not np.all(np.diff(levels) > 0):
        levels = np.sort(levels)

    # We need bins that cover all possible elevations.
    # To capture cells exactly lower than levels, we create bin edges.
    # Bin edges: [-inf, levels[0], levels[1], ..., levels[n-1], inf]
    bin_edges = np.concatenate(([-np.inf], levels, [np.inf]))
    
    num_bins = len(bin_edges) - 1
    global_counts = np.zeros(num_bins, dtype=np.int64)
    global_elev_sums = np.zeros(num_bins, dtype=np.float64)
    
    with rasterio.open(dem_path) as src:
        # Determine cell area
        transform = src.transform
        cell_area = abs(transform[0] * transform[4])
        
        file_nodata = src.nodata
        if nodata_value is None:
            nodata_value = file_nodata
            
        logger.info(f"Computing stage-storage for {dem_path.name} ({src.width}x{src.height}) using block streaming...")
        
        for _, window in src.block_windows(1):
            block = src.read(1, window=window)
            
            # Mask nodata and NaNs
            valid_mask = ~np.isnan(block)
            if nodata_value is not None:
                valid_mask &= (block != nodata_value)
                
            valid_elevations = block[valid_mask]
            
            if valid_elevations.size == 0:
                continue
                
            # Compute counts and sum of elevations per bin for this block
            # np.histogram uses [a, b) for all bins except the last which is [a, b]
            counts, _ = np.histogram(valid_elevations, bins=bin_edges)
            elev_sums, _ = np.histogram(valid_elevations, bins=bin_edges, weights=valid_elevations)
            
            global_counts += counts
            global_elev_sums += elev_sums

    # Calculate cumulative sums across bins
    # Bin 0 corresponds to range [-inf, levels[0])
    # Bin k corresponds to range [levels[k-1], levels[k])
    # Therefore, sum of bins 0..k gives exactly all cells < levels[k]
    cum_counts = np.cumsum(global_counts)
    cum_elev_sums = np.cumsum(global_elev_sums)
    
    volumes = {}
    for k, level in enumerate(levels):
        total_cells_below = cum_counts[k]
        total_elev_sum_below = cum_elev_sums[k]
        
        # Volume = Area * (Level * Count - Sum(Elev))
        volume = cell_area * (level * total_cells_below - total_elev_sum_below)
        volumes[level] = float(volume)
        
    return volumes
