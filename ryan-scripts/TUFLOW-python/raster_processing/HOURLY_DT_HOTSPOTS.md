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

## Temporal and spatial interpretation

**Matching output timestamps do not guarantee matching hydraulic events.**
Project-data observations show that timestamped dt rasters can retain a minimum
computed earlier while Nu/Nc/Nd change. Whether and when this retention resets
depends on the output and engine; the exact engine contract remains unverified.

- `worst_hour` identifies an output **containing** the lowest dt, not necessarily
  the time when that low timestep first occurred.
- `max_hours_at_cell`, `hours_at_worst_cell` and `sum_cell_hours` count **output
  snapshots** with low dt, not continuous restriction duration or independent
  solver timesteps.
- Same-output Nu/Nc/Nd at the representative minimum-dt cell and same-hour
  peaks are spatial or snapshot associations; they are not proof of the
  condition which originally forced the retained dt minimum.
- Raster pixel counts and `area_ha` describe the raster-grid footprint.
  TUFLOW output pixels can be finer than the computational cells; mesh nodes
  and active mesh elements are different spatial supports.

Use the HPC timestep CSV/log, repeated solver steps and model inputs for a
defensible cause-of-low-timestep diagnosis. Do not interpret these hotspot
layers as solver-event reconstruction.

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

## XMDF alternative — mesh-vertex diagnostics

Use `--source xmdf` to analyse temporal **mesh-vertex** outputs through
PyTUFLOW 1.2 rather than reading hourly TIFs. PyTUFLOW is used **only for XMDF
decoding**, not for the GeoTIFF or HPC-history workflows; no TPC integration is
required. An associated `.2dm` geometry
file is required by PyTUFLOW; it normally finds the matching file automatically.
**XMDF does not embed a CRS**. You must supply the correct model CRS explicitly
using `--xmdf-crs`. Only metre-based projected coordinates are accepted.

```powershell
py -3.14 .\create_hourly_dt_hotspots.py --source xmdf `
  --xmdf "Q:\job\results\model.xmdf" `
  --xmdf-crs EPSG:7851 --start-time 0 --end-time 5.333333 `
  --dry-run --no-pause

py -3.14 .\create_hourly_dt_hotspots.py --source xmdf `
  --xmdf "Q:\job\results\model.xmdf" `
  --xmdf-crs EPSG:7851 --output "C:\Temp\xmdf_control_hotspots.gpkg" `
  --no-pause
```

Nu, Nc and Nd datasets are **required**, using names reported by the XMDF
reader; optional depth, velocity, water level and dt datasets are used when
present at the selected output times. Dataset aliases are auto-detected when
unambiguous. Use `--nu-dataset`, `--nc-dataset`, `--nd-dataset`,
or `--dt-dataset` to supply explicit names. The tool selects **all** actual
stored mesh times by default, including non-hourly times; `--start-time`,
`--end-time` and `--time-stride` provide optional sampling. The reported
`time_hr` is the mesh output timestamp, not a solver event reconstructed from
it. PyTUFLOW handles mesh coordinates and the associated activity flags.
The tool rejects non-aligned vertex arrays, missing control datasets and
invalid activity flags.

XMDF output is a **different analysis**, not a pixel-for-pixel substitute for
GeoTIFF connected-component statistics:

| XMDF layer | Meaning |
| --- | --- |
| `xmdf_control_hotspots` | Active near-limit mesh **vertex** values at stored timestamps, optional co-snapshot dt/depth/velocity/water level; one point per qualifying reported node |
| `xmdf_dt_hotspots` | Active nodes at or below low-dt threshold **only if a temporal dt dataset exists**; otherwise this layer is empty and metadata has `dt_available=0` |
| `xmdf_control_summary` | Maximum sampled control number and the output timestamp at which that peak was observed, with counts of near-limit output snapshots per mesh node |
| `xmdf_metadata` | One non-spatial record per source, with dataset names, output-time range, `dt_available`, node count, CRS, export cap and number of omitted records |

There is **no** `area_ha`, connected raster hotspot ID or raster recurrence
metric in XMDF mode. Mesh nodes have different support than raster pixels and
neither necessarily represents one independent solver cell. The default
`--max-points-per-type 1000` ranks the worst qualifying nodes by their control
number (lowest dt for dt records) and limits **exported** records per source,
dataset and timestamp. `near_nodes_total`, `low_nodes_total` and
`truncated_records` retain the number of qualifying nodes and omissions.
Increase the cap when a complete node inventory is required.

The original project XMDF snapshots examined to date contain Nu/Nc/Nd but
**no temporal dt dataset**. They can support near-control-limit node diagnostics
and optional h/d/V context, but **cannot establish dt hotspots without dt
data**. An absent dataset is never treated as 0 or back-calculated from controls.
A `dt` surface, where available, can itself contain historically retained
minima, so co-timestamp controls do not automatically explain why it was low.
Snapshots are selected using the **stored per-dataset XMDF Times arrays**,
not PyTUFLOW's public `XMDF.times()`, which can reconstruct regular intervals
that were never output (for example, fabricate hour 2 between stored hours
0, 1 and 2.5). The PyTUFLOW-only adapter accesses the supported Python mesh
driver's raw `times(data_type)` method and calls its `clear_cache()` after
each processed snapshot to prevent retaining every full mesh surface in
memory. That narrow driver bridge is version-sensitive and must be covered
by native acceptance. The XMDF `NumTimes` attribute alone is not an
authoritative length.

This new input mode has synthetic adapter tests but **still requires native
Windows/real XMDF acceptance**, including node-vs-element activity checks,
multi-zone output behaviour, performance and CRS verification before claiming
production support. Do not run against an XMDF that another process is
actively writing.

## TUFLOW references

- [2025.2 map outputs: dt/Nu/Nc/Nd](https://docs.tuflow.com/classic-hpc/manual/2025.2/Map-2.html)
- [2025.2 HPC adaptive timestep criteria](https://docs.tuflow.com/classic-hpc/manual/2025.2/TimestepBackground-2.html)
