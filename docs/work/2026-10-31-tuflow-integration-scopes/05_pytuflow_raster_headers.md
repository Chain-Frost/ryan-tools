# 05 — Optional raster-header validation in PyTUFLOW

[Return to the scope index](00_scope_index.md)

**Prepared issue body and implementation handoff; no issue was created by this task.** The previous GitHub write returned `user rejected MCP tool call`, with no further reason. Do not treat this Markdown file as a published issue. Recheck for an existing issue before a newly authorised posting action.

Suggested issue title: **Add optional read-only raster header validation to batch input checks**.

## Problem and intended result

[Upstream PR17](https://github.com/TUFLOW-Support/PyTuflow/pull/17) checks required input-file availability, selected GIS layers and supported linked sources. It does not open raster headers. An existing file with an invalid raster header can therefore satisfy that narrower file-availability check.

Add an optional public capability that checks whether required raster sources can be opened read-only and expose usable dimensions and raster bands. Keep dependency collection free of this physical inspection. The current ryan-tools cleanup will remove its local raster-header implementation and restore the feature only after PyTUFLOW provides a supported public API.

Use accepted PR17 commit `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b` as the behavioural reference. Develop a separate contribution on an agreed branch containing that API; do not amend the accepted PR17 branch or silently switch ryan-tools to another experimental build. Recheck upstream status before choosing the integration base.

## Public contract

Introduce an opt-in field, provisionally `ValidationOptions(check_raster_metadata=True)`, defaulting to `False`. Preserve existing constructor compatibility and existing results/cost when disabled. Both `validate_dependencies()` and `validate_batch_inputs()` should support the option consistently.

Produce normal public validation findings, provisionally with kind `raster_metadata`, retaining run index, flags, owning command/control file and resolved path. Header failures must affect the normal `passed` result. Keep existing statuses: a missing source remains missing; an unreadable or invalid header is unresolved; an unsupported requested check is never silently accepted.

Expose useful inspection counters without confusing unique raster sources with root/subdataset open attempts. Keep names and documentation accurate. Do not add a second independent reporting model solely for this feature.

## Behavioural scope

- Select raster sources using parser semantics and explicit dependency roles.
- Resolve wildcards through the existing validator and inspect concrete sources only after successful ordinary file checks.
- Open read-only; require positive width, height and raster band count.
- Deduplicate across all requested runs and the complete batch-validation invocation.
- Close handles on success and failure. Preserve files, metadata and sidecar inventories.
- Return a finding for every affected run while avoiding duplicate work.
- Support direct grids and grid files selected through rainfall CSV databases.
- Define a bounded, documented NetCDF container/subdataset policy. If the parser identifies a particular required variable, validate that selection. Otherwise report only the limited container/header assurance actually established.

Do not read pixel arrays, calculate statistics, generate overviews or modify datasets. CRS consistency, terrain coverage/alignment, resolution suitability, NoData statistics, pixel-block corruption, rainfall chronology/units and hydraulic correctness are excluded. A readable header does not guarantee readable or valid pixel content.

## Integration points

### Preserve source roles correctly

[`FileReference` and `DeferredDatabaseSource`](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/pytuflow/_tmf/file_reference.py) already contain `requires_raster_metadata_scan`. [`deferred_database.py`](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/pytuflow/_tmf/deferred_database.py) marks rainfall-grid sources. The private `_Requirement` projection in [`validation.py`](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/pytuflow/validation.py) currently loses that information; propagate the role through ordinary and secondary requirements.

Review role assignment before consuming every current flag. `make_file_reference()` uses `command.is_read_grid()`, which does not distinguish command parts. For `Read Grid Zpts == terrain.tif | clip.shp`, the clip is a vector. [`Command.is_raster_file()`](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/pytuflow/_tmf/parsers/command.py) distinguishes grid part zero and the raster position in Modify Conveyance. Add focused classification coverage; do not infer everything from extensions.

### Handle both physical-validation passes

[`batch_validation.py`](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/pytuflow/batch_validation.py) validates primary sources, discovers GIS-linked dependencies, and can validate the extended projection again. Adding raster opens blindly to both passes repeats work. Ensure once-per-source inspection across the complete batch, using a final inspection stage or bounded invocation-local reuse. Do not introduce persistent caches that become stale after model edits.

`BatchValidationResult.dependencies` retains the original syntactic inventory. The final checked projection in [`validation_links.py`](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/pytuflow/validation_links.py) includes additional selected sources. Its `_linked_reference()` does not currently assign a raster role. Where supported GIS schemas yield raster dependencies, preserve that role deliberately; document unsupported cases rather than claiming universal coverage.

## Backend and dependency choice

Rasterio is already declared as a mandatory dependency in the [accepted package metadata](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/pyproject.toml). Use a lazy Rasterio read-only implementation unless review establishes a concrete reason otherwise. This feature needs no new dependency extra, QGIS installation or backend-selection framework.

Existing `TuflowPath.open_grid()`, `Grid` and `NCGrid` expose related reading behaviour, but do not provide this batch validation contract. Their first-band and format assumptions require review before reuse. Never call `as_array()`, `read()`, `ReadAsArray()` or statistics methods for this check. Follow the target PyTUFLOW repository's Python minimum and style; do not raise its supported Python version for the implementation.

## Behavioural references

The previous implementation and tests are preserved at immutable links:

- [ryan-tools raster helper](https://github.com/Chain-Frost/ryan-tools/blob/74dc7b2b89cf5061019285e90be942d2f065712c/ryan_library/functions/tuflow/raster_metadata_dependencies.py)
- [ryan-tools raster tests](https://github.com/Chain-Frost/ryan-tools/blob/74dc7b2b89cf5061019285e90be942d2f065712c/tests/functions/tuflow/test_raster_metadata_dependencies.py)

Use these as behavioural references and adapt the design to PyTUFLOW's own validation types and existing raster backend. Do not import ryan-tools or reproduce its `DependencyResult` wrapper. Immutable links keep the reference available after its removal from the ryan-tools branch.

Avoid inheriting known limitations: accepting any first usable NetCDF subdataset proves no particular variable; a single failed subdataset open can abort the old helper; its counter includes failed/subdataset attempts; and its caller opens rasters before filesystem inventory, creating duplicate missing/unresolved findings.

## Acceptance criteria

1. Disabled option performs no raster opens and preserves existing validation behaviour.
2. Valid GeoTIFF passes; invalid headers, zero dimensions or no usable bands fail truthfully.
3. Missing/unresolved paths and failed run contexts are not opened as rasters.
4. NetCDF root/container, empty container, selected variable, unsupported selection and subdataset-open errors follow the documented policy.
5. Shared files across runs and both physical passes are inspected once with correct attribution.
6. Wildcards, spaces, Windows UNC paths and rainfall CSV sources work through existing resolution rules.
7. Mixed raster/vector commands leave vector inputs out of raster inspection.
8. Instrument pixel-reading/statistics methods to fail if called; the metadata tests still pass.
9. Source/sidecar snapshots remain unchanged, including error cases; all handles close.
10. Direct and batch APIs agree; installed-package smoke checks verify the new public option and findings.

Deliver focused tests, a concise user-facing example, the exact supported formats/limitations and validation evidence. Keep unrelated parser changes, benchmarks and development-only tooling out of the proposed upstream contribution. After upstream support is accepted, a separate ryan-tools update may enable the public option; this issue does not authorize that consumer change now.
