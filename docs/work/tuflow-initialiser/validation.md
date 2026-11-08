> **Historical record from [tuflow-initialiser](https://github.com/Chain-Frost/tuflow-initialiser).** Pending or proposed work is tracked in ryan-tools migration PR #246 and issues #242–#245; old issue numbers below are retained for provenance.

# Fail-closed integrity and preflight checks

For the shared real-DEM inputs, reproduction command and retained results, see
[base-workflow validation](base-workflow-validation.md).

- Each supplied terrain source must be readable, single-band with declared CRS, and carry valid NoData handling. Source list is ordered, unique and nonempty; output analysis grid uses a projected metre-based CRS and is capped at a configurable maximum cell count.
- A GDAL-backed provenance raster records source priority for every hydrology analysis DEM pixel. First valid source wins. Input terrain grids can differ in extent/resolution/CRS; reprojection is explicit. Vertical datum **cannot** be inferred from CRS alone; conflicting vertical datums and seams remain a required engineering review.
- AOI geometry must be valid, nonempty, and lie inside DEM coverage and use project CRS. All upstream contributing cells must be determinable, and unknown NoData/edge-truncated basin paths must stop the build.
- Exclusions require valid polygon geometry in project CRS. Delineate full AOI contribution first, then clip applied exclusions to contributing area. Reject exclusions with no overlap or any intersection with AOI circular buffer. Original catchment is retained in analysis GeoPackage; active TUFLOW polygons exclude it. **Detailed excluded-catchment analysis defaults to off** (opt in with `--analyse-exclusions`). Verify that default stream/headwater and future subcatchment products omit excluded cells. Connectivity/flow direction and accumulation still use the full hydrology DEM so downstream catchment boundaries are valid; this option is not a GRASS runtime shortcut.
- Preserve original DEM; any hydrology filling/breaching is separate and subject to configured max terrain change. Synthetic stream heads are not mapped channels; longitudinal slope uses original analysis DEM, not modified fill heights.
- Final `2d_code` and `2d_rf` **identical footprints**; `2d_loc` unrotated rectangular envelope. Reopen generated canonical TUFLOW schema GeoPackages to verify field list, geometry types, CRS and coverage.
- Stage the project and publish only after validations pass; do not overwrite an existing project.
- Warn in output if upstream catchments were excluded: resulting model may be hydraulically incomplete until external inflows / upstream interfaces are resolved by engineering design.

## Completed reference check and future project-specific verification

The installed Windows GRASS 8.5/TUFLOW 2026.3 reference build has reproduced
the original 805 stream paths and five rasters, and verified the 535 local
subcatchments and 111 per-entry longest paths by machine-readable QA. TUFLOW
was used to generate canonical empty GIS templates, **not** to simulate a
hydraulic model.

The following representative engineering checks are explicitly deferred to
separate tracked issues rather than treated as completed PR #1 acceptance:

- [#8](https://github.com/Chain-Frost/tuflow-initialiser/issues/8): inspect
  drainage systems, 535 reach-local catchments, 111 AOI entry crossings and
  DEM/D8 longest paths on QGIS maps with supporting source terrain.
- [#9](https://github.com/Chain-Frost/tuflow-initialiser/issues/9): real
  co-located LiDAR/SRTM overlap, source priority, NoData seams and vertical
  datum/units reconciliation.
- [#10](https://github.com/Chain-Frost/tuflow-initialiser/issues/10): realistic
  excluded contributing areas, preserved full catchment coverage and manual
  treatment of missing upstream hydraulic inflows.

Fail-closed D8 NoData boundaries, valid TUFLOW event/control syntax and
automated input/output tests **remain immediate code-quality and data-safety
requirements**; do not postpone known defects to the future validation issues.
All generated hydrology products still require project-specific engineering
review before hydraulic or planning decisions. Large regional performance
remains unverified.
