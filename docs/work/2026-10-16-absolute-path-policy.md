# Mapped-drive-safe filesystem paths and raster progress

| Field | Value |
| --- | --- |
| Status | Blocked |
| Owner | Unassigned (Windows package rebuild required) |
| Created | 2026-10-16 |
| Updated | 2026-10-16 |
| Next review | 2026-10-23 |
| Baseline | PR [#128](https://github.com/Chain-Frost/ryan-tools/pull/128); branch `phantom-branch-patch-1`, base `3122f644`, commit through `c9f201a8`; no parent or submodule modifications |

## Outcome and scope

Prefer `Path.absolute()` over `Path.resolve()` for everyday file, working-directory and output-path handling so mapped drives and displayed paths are preserved. Keep `resolve()` for physical file identity, symlink-aware security checks, containment and installation checks. Apply the convention to maintained library functions and wrappers.

For `verify_converted_tuflow_rasters.py`, keep Rich progress task names short and show full source/TIFF filenames and mismatches in readable multiline console entries. Preserve the existing conversion, CRS, pixel and destructive-operation safety logic.

## Current state

- Path policy added to `AGENTS.md`, `docs/DEVELOPMENT_GUIDE.md` and `ryan-scripts/WRAPPER_STANDARD.md`.
- Paths adjusted across first-party GDAL, TUFLOW, RORB, file-management, CAD, point cloud and other project scripts; physical-samefile and security guards explicitly retained.
- Raster verifier wrapper embedded version now `2026-10-16.3`. Added focused display and lexical path regression assertions.
- All code changes pushed to existing PR #128. No changes made to vendored dependencies/submodules.
- **Blocking package artifact:** CI Package step `Verify retained wheel` fails because committed `26.10.16.1` wheel contains old library source; wheel must be rebuilt after library edits. Do not remove or bypass package validation.

## Next action

In a Windows Python 3.14 checkout of the PR head, run `python repo-scripts/build_library.py`, verify the generated wheel with the repository's packaging checks and commit/push updated package version, wheel and any generated index metadata to **the same PR**. Then confirm all Policy, Package, Hosted tests and Python 3.13/MCP checks on the resulting commit and inspect a real long-path `Q:\\` TUFLOW raster batch.

## Completion criteria

- All required GitHub checks pass at the PR head; a rebuilt retained wheel is verified and contains the changed `wrapper_utils.py`.
- Windows smoke test confirms mapped-drive wrapper identity, readable long path output and no changes in CRS/mismatch/delete safeguards.
- Changes reviewed and merged on PR #128; this record and register updated to Complete.

## Validation and delivery

- 2026-10-16: Python 3.13 compatibility and MCP passed on earlier PR commits. Hosted tests passed on run #347.
- Policy failures (Ruff lint and formatting in changed files) were corrected through `c9f201a8`; a successful Policy run at that exact head is **not yet confirmed**.
- Package verification on earlier commits failed only at the retained-wheel/source consistency step, not at the raster comparison tests.
- Local Ruff, strict Pyright, focused pytest, wheel build and actual Windows `Q:\\` validation: **not run** in the remote connector-only environment.
- Delivery: commits pushed directly to PR #128. The PR remains open. Retained wheel still requires Windows rebuild.

## Progress

### 2026-10-16

Reviewed use of `resolve()`, added general path-handling guidance and migrated ordinary first-party paths to `absolute()`; retained deliberate physical-identity checks, including critical overwrite and containment guards. Updated raster verification console layout and tests. GitHub CI exposed formatting/syntax/typing issues in the widened changed-file scope; addressed those findings. Tracked-wheel mismatch remains an external build dependency.
