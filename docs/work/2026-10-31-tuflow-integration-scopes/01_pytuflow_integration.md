# 01 — PyTUFLOW integration, compatibility and installation

[Return to the scope index](00_scope_index.md)

## Objective and fixed decisions

Replace the accumulated input-check implementations with one consumer of the public API in [PyTUFLOW PR17](https://github.com/TUFLOW-Support/PyTuflow/pull/17). Keep ryan-tools responsible for workflow policy and presentation; PyTUFLOW owns dependency interpretation and physical input validation.

This is an implementation handoff, not evidence that changes have been made. The reviewed source baseline is `74dc7b2b89cf5061019285e90be942d2f065712c` of open draft [PR159](https://github.com/Chain-Frost/ryan-tools/pull/159). The latest live head is `c1e35230d16ef014305ba0c196e68768dd4eb7cd` after a merge from `main`; the reviewed parser/runner code is unchanged. PR159 has not merged into `main`. Recheck its live head and concurrent work before editing.

The accepted parser baseline is `Chain-Frost/PyTuflow` commit `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b`, the reviewed PR17 head. Use this immutable source until a separately validated public release provides the required API. No old parser backend, compatibility adapter or fallback is required. Remove local raster-header checking from this release; restore it only through a supported PyTUFLOW public API.

## Ownership and coordination

Own `input_preflight.py`, the compatibility helper, installation integration, their focused tests and directly affected documentation. Delete `batch_preflight_adapter.py` after replacing its consumers. Coordinate edits to the two user wrappers with the command-planning and runner agents.

Work package 02 owns shared command planning, effective working directories, executable/TCF selection, multi-TCF grouping and stable outer run identities. Agree its request/result contract first. This work package consumes each effective TCF and its exact ordered structured flags, then maps PyTUFLOW's zero-based batch positions back to those outer identities. Never regenerate combinations, flatten values containing spaces, or infer a second run plan.

Read `AGENTS.md`, `docs/DEVELOPMENT_GUIDE.md`, the wrapper standard and the current work register. Use the normal Python 3.14 installation and current built-in annotation syntax. Do not introduce a virtual environment or repository-path import workaround.

## Implement the single validation path

Call `pytuflow.batch_validation.validate_batch_inputs()` with `pytuflow.validation.ValidationOptions`. Passing a parsed `pytuflow.TCF` is supported when the workflow already owns one; do not parse the same model again unnecessarily. Keep parsing lazy and limited to requested input checks.

Set `check_bc_csv_headers=True` explicitly when constructing options for the intended full input check. The options object's default differs from the convenience defaults used when `validate_batch_inputs()` receives no options; supplying resource limits must not accidentally disable BC column checks. Match other supported options deliberately and do not call private PyTUFLOW inspection helpers.

Translate public `BatchValidationResult`/`ValidationFinding` values into the existing user-facing report contract. Preserve run identity, source command, owning control file, path, layer, database context, status and useful detail. Preserve `missing`, `unresolved` and `unsupported` distinctions. A parser failure, empty/omitted result or incomplete run mapping must not become a successful check. Treat timing/reuse metrics as diagnostics rather than compatibility requirements.

Remove the old individual/batch switches, context reconstruction, synthetic-fixture fallbacks, eager/deferred alternatives and duplicate file/layer/CSV/GIS inspection paths. Search maintained callers before deleting helpers such as `dependency_index.py`, local CSV/GIS dependency inspectors and their tests; migrate affected callers in the same change. Retain unrelated functionality only where actual callers demonstrate a separate purpose.

Delete `raster_metadata_dependencies.py`, its integration switches, counters and exclusive tests. State clearly that this release checks required raster-file availability but does not inspect raster headers or pixels. Do not leave a disabled implementation awaiting restoration.

Remove obsolete comparison/benchmark/probe harnesses, retired-interface investigations and stale development documentation belonging solely to this work front. Update indexes and active work records. Do not broaden cleanup into unrelated repository history or submodules.

## Compatibility guard

Provide one reusable helper under `ryan_library/functions/tuflow`, provisionally `pytuflow_compatibility.py`. Check local imports and the public capabilities actually consumed. A version threshold is insufficient: the accepted PR17 build reports `1.2.7`, and official upstream commit `8012b8a530616a8e10dfe96a53189f8585ec2805` also reports `1.2.7` without the collection API.

Return actionable diagnostics: interpreter, installed distribution version, imported module location, missing API/import dependency and installation remedy. Record source provenance when available; absence of VCS metadata alone must not reject a correctly installed wheel. Check actual result shape/run mapping during validation. Do not perform model I/O, install packages, contact GitHub or execute a solver merely to probe compatibility.

| Entry point or mode | Incompatible or missing PyTUFLOW |
| --- | --- |
| Standalone input checker | Explicit failure and nonzero wrapper exit; no success report. |
| Runner preflight-only | Explicit failure; no solver launch. |
| Normal runner with advisory input checking | One clear unavailable/failed-check status; preserve advisory execution policy. |
| Checking disabled, help, command export or plan-only | No parser capability requirement or model parsing. |

Helpers return results or raise ordinary exceptions. Wrappers own exit codes, display and pausing. Distinguish a missing package from a broken transitive import. Never silently select a different parser or certify skipped checks.

## Installation integration

Extend the existing installer rather than creating a competing setup workflow. Its implementation is [repo-scripts/install_latest_wheel.py](https://github.com/Chain-Frost/ryan-tools/blob/74dc7b2b89cf5061019285e90be942d2f065712c/repo-scripts/install_latest_wheel.py).

Preserve `GIS_WHEEL_INDEX = "https://gisidx.github.io/gwi"` and the existing binary-wheel bootstrap for Fiona, Rasterio and GDAL, including the established route to cgohlke geospatial wheels. Use the selected interpreter's `sys.executable -m pip`; do not replace this with generic source-build instructions or require users to create a new environment.

Centralize the temporary parser requirement and use it in the real installation/development path:

```text
pytuflow @ https://github.com/Chain-Frost/PyTuflow/archive/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b.zip
```

Install the matching ryan-tools package and accepted parser together so dependency resolution cannot substitute an incompatible same-version build. Keep public package metadata publishable. Preserve explicit meanings for dependency-only and no-dependency recovery modes; neither may claim full compatibility verification without performing it. Finish normal installation with `pip check` and the shared capability diagnostic. Correct touched installer comments that still name retired distributions.

Update the relevant CI parser pins to the same immutable target and add a small real public-API validation smoke case. An old-pin green workflow or a version-only installation check is insufficient. If the installed same-version build is incompatible, the installer must replace it through its explicit installation path; runtime guards only diagnose and never install automatically.

## Acceptance and delivery

- Accepted PR17 passes; official same-version build without the API fails correctly.
- Missing package, broken imports and a shadowing checkout yield useful diagnostics.
- Help/export/checking-disabled paths work without importing the parser stack.
- Advisory checks never become a launch gate; explicit check modes never falsely pass.
- Structured values containing spaces, reordered/omitted results and multiple TCF groups preserve or reject mappings correctly.
- Existing missing-file, selected-layer, CSV and GIS-linked-source cases exercise the real public API rather than a recreated old context contract.
- Installer dry runs contain the immutable pin and existing GIS bootstrap; repeated installation retains the accepted build.
- Validate the installed package and copied wrappers on Windows with the documented normal interpreter. Historical Python 3.12 native evidence does not certify the current ryan-tools installation.

Run focused tests, repository Ruff/strict Pyright checks on modified files, wrapper smoke checks and the required package build. Update the single authoritative work record with exact revisions and limitations. Deliver a concise diff summary, deletion inventory, installation instructions and validation evidence. Coordinate commits/PR integration with the scope owner; do not merge or publish a release independently.

## Authoritative API references

- [Batch validation API](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/pytuflow/batch_validation.py)
- [Validation options and findings](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/pytuflow/validation.py)
- [Documented supported checks and limits](https://github.com/Chain-Frost/PyTuflow/blob/05e35476c02d6d8ed17c83caf1cb0d185ce51d1b/docs/examples/validating_batch_inputs.rst)
- [Official same-version build](https://github.com/TUFLOW-Support/PyTuflow/blob/8012b8a530616a8e10dfe96a53189f8585ec2805/pytuflow/__init__.py) and its [TCF interface](https://github.com/TUFLOW-Support/PyTuflow/blob/8012b8a530616a8e10dfe96a53189f8585ec2805/pytuflow/_tmf/cf/tcf.py)
