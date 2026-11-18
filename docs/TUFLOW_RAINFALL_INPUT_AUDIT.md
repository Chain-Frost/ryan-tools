# TUFLOW rainfall and loss input audit (experimental)

The separate, read-only `audit_tuflow_rainfall.py` wrapper inspects the actual
scenario/event selections in BAT/CMD/TXT command lists. It **never runs TUFLOW**
or changes model data. It reuses the existing `ryan-tools` inert batch planner
and PyTUFLOW control, event, GIS, material, soil and BC database readers.

```powershell
py -3.14 ryan-scripts/TUFLOW-python/model_management/audit_tuflow_rainfall.py --runs run_tuflow_simulations.txt --json-report rainfall_audit.json --no-pause
```

Edit `RUN_FILES`, `WORKING_DIR`, `TCF`, and `JSON_REPORT` near the top of the
script for a copied wrapper. Multiple `--runs` inputs are supported; no TCF is
guessed. A new JSON file is created with exclusive creation (`x`), never
silently overwritten. The CLI exits nonzero for unresolved inputs or any
unsupported part of the assessment.

## What it currently establishes

- Actual planned run source, TCF path and event/scenario flags.
- Selected `Read GIS RF` polygon names and `f1`/`f2` attributes where the
  active PyTUFLOW GIS reader is available; unusual RF syntax is reported.
- Matching rows from the active, event-resolved `bc_dbase` file, including
  sources/columns, additive and multiplicative adjustment fields.
- Sum of the *BC-database-adjusted* rainfall value column as resolved by
  PyTUFLOW, in mm under the standard increment-series convention.
- Defined material rainfall IL/CL (mm, mm/h) and fraction impervious, and
  defined soil infiltration methods/parameters (including `ILCL`, `GA`, `HO`,
  `SCS` and `NONE` where supported by the installed PyTUFLOW).
- Grouping by equal *selected rainfall factor settings* separately from
  rainfall depths, so two AEP/duration events with the same factors stay
  in the same **factor-only** group.

## Restrictions

This is an **input inventory**, not yet a complete, verified model-specific
rainfall-excess calculation. A summed BC column is not automatically the final
applied rainfall depth: GIS `f1`/`f2`, global rainfall factors, spatial overlap,
interpolation, variable time steps and other rainfall inputs can further modify
it. The wrapper records these fields and leaves
`spatially_adjusted_sum_mm = null`; it does not claim a guessed product.
Malformed explicit BC adjustments, unsupported or ambiguous rainfall GIS inputs,
and incomplete boundary inventories prevent factor-equivalence grouping.
Missing material schemas and required soil parameters are reported unresolved,
not silently converted to zero losses.

A defined material/soil database entry is not proof of assignment to a wet,
active model cell. In particular, do **not** form a Cartesian product of all
listed material and soil IDs. Where verified active-cell check-grid pairs are
available, see the stacked #324 extension. Also distinguish material rainfall
losses (before 2D rainfall application) from soil infiltration (on the cell's
water balance); both can coexist. A zero-rainfall-loss material can still
influence infiltration through its fraction impervious.

The current audit reports `SPATIAL_COMBINATION_UNRESOLVED` until spatial
assignment is independently established, and does not use rainfall-screening
`DROP` findings to suppress simulations. Native TUFLOW material/soil numerical
parity remains a separate #319 acceptance activity. Cross-check production
models with `.tlf` and TUFLOW check outputs before engineering use.

Implementation issue: [#323](https://github.com/Chain-Frost/ryan-tools/issues/323).
Dependent extension: [#324](https://github.com/Chain-Frost/ryan-tools/issues/324).

## Stacked #324: observed material × soil loss groups

Supply the **aligned, same-run, single-band check-grid rasters** containing
material and soil IDs, and optionally an active-cell mask (nonzero means
active). The script does not infer/check which particular run generated
the files: the operator must verify that provenance. For multi-run plans,
identify exactly one run with `--grid-run-index`; it does **not** apply
its check grids to the other scenarios automatically.

```powershell
py -3.14 ryan-scripts/TUFLOW-python/model_management/audit_tuflow_rainfall.py `
  --runs run_tuflow_simulations.txt `
  --grid-run-index 1 `
  --material-check-grid check_material.tif `
  --soil-check-grid check_soil.tif `
  --active-cell-mask-grid check_active_cells.tif `
  --json-report audit_pairs.json --no-pause
```

Cell pairs are counted by matching raster indices, **not** by multiplying
defined database IDs. Both raster grids must have identical extent, CRS,
resolution and affine transform; masked/NoData cells are skipped. Without
an active-cell mask, the check grids' own valid-cell masks are used, so a
non-NoData raster pixel cannot alone certify an active TUFLOW 2D cell.
With an explicitly supplied active-cell mask, any active cell with a missing
material or soil ID **fails the pairing operation**; it is not silently omitted.
The script retains original material and soil IDs, observed pairs and cell
counts in JSON and aggregates **effective loss signatures**:

- Nonzero material rainfall loss + active soil infiltration: coupled group.
- Zero material rainfall loss and no imperviousness influence: soil only.
- Neutral soil (`NONE` or a supported zero-IL/zero-CL ILCL definition):
  material only, or no-loss when the material is also neutral.
- A zero-IL/zero-CL material with nonzero `Fraction Impervious` still changes
  the signature when soil infiltration can operate underneath.
- Method-specific GA/HO/CO/SCS parameters are retained, not interpreted as
  zero losses because their IL/CL fields are absent. Unsupported methods,
  missing numeric IL/CL, undefined IDs and missing impervious fractions for
  coupled infiltration **fail closed**, rather than collapsing groups.
GA, HO, CO and SCS soils also require their respective finite, method-specific
parameters; a method name alone is not a valid loss signature.

Groups collapse equivalent **loss-related properties**, not IDs, and do not
include Manning's roughness or unrelated material properties. A model with
2 materials and 3 defined soils does **not** automatically have 6 applied
combinations. Grouping is a *loss-potential configuration assessment* and
is not native proof of the rainfall volume removed or soil water storage.

Further validation needed: official TUFLOW check-grid naming/semantics,
model active-cell provenance and imperviousness behaviour, layered soils,
SGS/Quadtree domain interpretation, source rainfall footprints, installed
PyTUFLOW compatibility and representative model numerical acceptance.

### Direct native TUFLOW GIS check-file input

TUFLOW's own `_grd_check` GIS layer contains a cell record with `Code`,
`Material` and, when soil infiltration is activated, `Soil_ID`. This is the
preferred input because the co-location is already resolved by TUFLOW:

```powershell
py -3.14 ryan-scripts/TUFLOW-python/model_management/audit_tuflow_rainfall.py `
  --runs run_tuflow_simulations.txt `
  --grid-run-index 1 --grd-check model_grd_check.gpkg `
  --json-report audit_from_grd_check.json --no-pause
```

For a GeoPackage containing several candidate layers, pass
`--grd-check-layer <layer-name>`. The parser streams `Code=1` active cell
records, skips `Code=-1` null cells and rejects unknown code values.
If `Soil_ID` is absent, sentinel -1 is permitted only where the selected
model context has **no active soils database**. When a soils database is
selected, missing check-grid soil assignments are unresolved, not neutral.
The operator must verify that the supplied check file corresponds exactly
to the audited simulation/domain.

The material database parser uses the documented TUFLOW defaults for
**omitted** optional values: no material rainfall losses (IL=0, CL=0) and
fraction impervious=0. Explicitly malformed values stay unresolved rather
than silently becoming zero. See [TUFLOW 2026.3 materials](https://docs.tuflow.com/classic-hpc/manual/2026.3/06-2D-Domains.html)
and the [native `_grd_check` field descriptions](https://wiki.tuflow.com/Check_Files_2d_grd).
