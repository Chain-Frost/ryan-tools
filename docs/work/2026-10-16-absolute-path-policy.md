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
- **Blocking package artifact:** CI Package step `Verify retained wheel` fails because committed `26.10.16.1` wheel contains old library source; wheel must be rebuilt after library edits. Strict Pyright in Policy also reports 226 errors across seven previously modified legacy/specialised scripts; those are independent of the archive wrapper. Do not remove or bypass package validation.

## Folder compression wrapper extension

- Modernised `ryan-scripts/file-management-python/archive_compress_folder.py` in the same PR with visible editable defaults for input paths, working directory, native exclusions, 7-Zip binary, workers, console progress, logging and log filename. Wrapper version is `2026-10-16.2`.
- Kept per-directory sibling `.7z` naming, native `-xr!` exclusion behaviour and skip-existing safety. Compression helper moved into `ryan_library.functions.archive_utils` as a picklable worker; the wrapper still coordinates the user-facing batch and startup banner.
- Rich progress displays folder count, a spinner and elapsed runtime; non-TTY consoles show per-folder completion records with full paths and result statuses. Byte-level percentages inside an archive are not available from the current captured 7-Zip process.
- Synthetic tests in `tests/scripts/test_archive_compress_folder_wrapper.py` cover exclusions, skip-existing, error reporting and wrapper defaults.
- CI run [#356](https://github.com/Chain-Frost/ryan-tools/actions/runs/38862219245): Hosted tests **passed**; Python 3.13/MCP **passed**; Ruff lint and formatting **passed**. Policy is still red due to 226 strict Pyright errors in seven previously touched, non-archive files. Package remains red due to retained-wheel mismatch. No archive-specific Pyright errors were reported.

## Next action

Resolve the repository-wide strict Pyright errors in the seven files changed earlier in PR #128 without weakening type-check policy. In a Windows Python 3.14 checkout of the PR head, run `python repo-scripts/build_library.py`, verify the generated wheel with the repository's packaging checks and commit/push updated package version, wheel and any generated index metadata to **the same PR**. Then confirm all Policy, Package, Hosted tests and Python 3.13/MCP checks on the resulting commit and inspect a real long-path `Q:\\` TUFLOW raster batch.

## Completion criteria

- All required GitHub checks pass at the PR head; a rebuilt retained wheel is verified and contains changed `wrapper_utils.py` and the archive compression helper.
- Windows smoke test confirms mapped-drive wrapper identity, readable long path output and no changes in CRS/mismatch/delete safeguards.
- Changes reviewed and merged on PR #128; this record and register updated to Complete.

## Validation and delivery

- 2026-10-16: Python 3.13 compatibility/MCP and Hosted tests passed on run #356 at `620adadb`. Ruff check/format passed; strict Pyright failed with 226 unrelated diagnostics.
- Policy failures (Ruff lint and formatting in changed files) were corrected through `c9f201a8`; a successful Policy run at that exact head is **not yet confirmed**.
- Package verification on earlier commits failed only at the retained-wheel/source consistency step, not at the raster comparison tests.
- Local Ruff, strict Pyright, focused pytest, wheel build and actual Windows `Q:\\` validation: **not run** in the remote connector-only environment. Hosted CI provides separate evidence.
- Delivery: commits pushed directly to PR #128. The PR remains open. Retained wheel still requires Windows rebuild.

## Progress

### 2026-10-16

Reviewed use of `resolve()`, added general path-handling guidance and migrated ordinary first-party paths to `absolute()`; retained deliberate physical-identity checks, including critical overwrite and containment guards. Updated raster verification console layout and tests. GitHub CI exposed formatting/syntax/typing issues in the widened changed-file scope; addressed those findings. Tracked-wheel mismatch remains an external build dependency.
