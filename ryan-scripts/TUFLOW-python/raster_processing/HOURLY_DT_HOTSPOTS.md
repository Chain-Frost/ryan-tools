# Hourly TUFLOW HPC timestep hotspots

Use create_hourly_dt_hotspots.py with a folder containing the four single-band
GeoTIFFs for each TUFLOW map-output timestamp:

- RUN_dt_001_00_00.tif — calculated local minimum timestep, in seconds;
- RUN_Nu_001_00_00.tif — Courant control number;
- RUN_Nc_001_00_00.tif — shallow-wave celerity control number;
- RUN_Nd_001_00_00.tif — diffusion control number.

The hour token must be three digits and both minute/second tokens must be
00. Each selected timestamp **must** have all four files; all files must share
a metre-based projected grid, and the input folder must contain only one selected
run prefix. Use --prefix if the folder contains several runs.

The overall *_dt_Min.tif is **not an input**. The dt map output shows the
calculated minimum timestep for the cell, not necessarily the actual solver
timestep at that instant. The nominal HPC control limits are Nu 1.0, Nc 1.0,
Nd 0.3. These may need adjusting for simulation-specific control settings.
High ratios are **not proof of the mechanism governing an adopted timestep**.

## Example

From Windows PowerShell with the matching ryan-tools package installed:

```powershell
py -3.14 .\create_hourly_dt_hotspots.py --grid-dir "Q:\job\results\HourlyDt\grids" --max-hour 36 --dry-run --no-pause
py -3.14 .\create_hourly_dt_hotspots.py --grid-dir "Q:\job\results\HourlyDt\grids" --max-hour 36 --no-pause
```

An optional --output is relative to the input grid folder. Existing GeoPackages
are protected unless --overwrite is specified. When replacing an existing
GeoPackage, the workflow builds and closes a temporary GeoPackage before the
final rename. The original is preserved if analysis or writing fails.

## Layers

| Layer | Meaning |
| --- | --- |
| hourly_dt_hotspots | One minimum-dt-cell point per connected low-dt component at each output timestamp. nu_peak, nc_peak and nd_peak are peaks **within that same component and same timestamp**, possibly at different cells. *_at_min values are read at the minimum-dt cell. highest_ratio_at_min is the largest ratio to its configured limit, not proven causation. |
| hourly_parameter_hotspots | One point per connected near-limit component for Nu, Nc or Nd at each timestamp. cells_low_dt counts cells overlapping low dt at the **same timestamp**. |
| overall_dt_hotspots | One minimum-dt point per connected union of all cells classified low dt across selected timestamps. worst_hour and *_at_worst share the same cell and timestamp. max_hours_at_cell and hours_at_worst_cell count distinct **output timestamps** per cell; sum_cell_hours is the sum of per-cell counts, **not** the number of affected hours for the whole union component. |
| analysis_metadata | Non-spatial layer recording run prefix, source directory, timestamp range and configured thresholds. |

A component uses 8-connected adjacency by default (including diagonal
neighbors); --connectivity 4 disables diagonal connectivity. The
--min-component-cells cutoff applies to each exported component; the overall
union includes **all** low-dt cells before the component-size cutoff.
NoData, infinities and nonpositive dt values are excluded from low-dt regions.
No resampling, CRS transformation, interpolation or raster modification occurs.

This first implementation reads one four-grid timestamp at a time and retains
three full-grid accumulators for the overall summary. Memory therefore scales
with the **size of the raster**, not with the number of hours, but is not yet a
window-streamed/out-of-core algorithm. Benchmark a representative large model
before enabling unattended runs over the full model extent. The script creates
point features rather than polygons; a point's area_ha reports its component
area, not a geometric polygon footprint.

## TUFLOW references

- [2025.2 map outputs: dt/Nu/Nc/Nd](https://docs.tuflow.com/classic-hpc/manual/2025.2/Map-2.html)
- [2025.2 HPC adaptive timestep criteria](https://docs.tuflow.com/classic-hpc/manual/2025.2/TimestepBackground-2.html)
