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
