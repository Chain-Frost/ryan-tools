# Mapped-drive-safe filesystem paths and raster progress

| Field | Value |
| --- | --- |
| Status | In validation |
| Owner | ChatGPT |
| Created | 2026-10-16 |
| Updated | 2026-10-16 |
| Next review | 2026-10-23 |
| Baseline | PR [#128](https://github.com/Chain-Frost/ryan-tools/pull/128); branch `phantom-branch-patch-1`, base `3122f644`, head `822ec99f`; unrelated staged PowerShell change preserved |

## Outcome and scope

Prefer `Path.absolute()` over `Path.resolve()` for everyday file, working-directory and output-path handling so mapped drives and displayed paths are preserved. Keep `resolve()` for physical file identity, symlink-aware security checks, containment and installation checks. Apply the convention to maintained library functions and wrappers.

For `verify_converted_tuflow_rasters.py`, keep Rich progress task names short and show full source/TIFF filenames and mismatches in readable multiline console entries. Preserve the existing conversion, CRS, pixel and destructive-operation safety logic.

## Current state

- Path policy added to `AGENTS.md`, `docs/DEVELOPMENT_GUIDE.md` and `ryan-scripts/WRAPPER_STANDARD.md`.
- Paths adjusted across first-party GDAL, TUFLOW, RORB, file-management, CAD, point cloud and other project scripts; physical-samefile and security guards explicitly retained.
- Raster verifier wrapper embedded version now `2026-10-16.3`. Added focused display and lexical path regression assertions.
- All code changes pushed to existing PR #128. No changes made to vendored dependencies/submodules.
- The retained package was rebuilt and verified locally as `ryan_functions` `26.10.16.2` against the latest upstream `run_hy8` `v2026.10.16.1` and `ryan-culverts` `v26.10.16.1` submodule commits. Five isolated path substitutions were removed from legacy/specialised scripts whose unrelated pre-existing typing debt made them unsuitable for this PR; maintained library and wrapper changes remain.
- Strict Pyright passes across all 77 Python files remaining in the PR diff. Focused wrapper, path and raster tests pass locally. GitHub checks still need to confirm the pushed commit.

## Folder compression wrapper extension

- Modernised `ryan-scripts/file-management-python/archive_compress_folder.py` in the same PR with visible editable defaults for input paths, working directory, native exclusions, 7-Zip binary, workers, console progress, logging and log filename. Wrapper version is `2026-10-16.2`.
- Kept per-directory sibling `.7z` naming, native `-xr!` exclusion behaviour and skip-existing safety. Compression helper moved into `ryan_library.functions.archive_utils` as a picklable worker; the wrapper still coordinates the user-facing batch and startup banner.
- Rich progress displays folder count, a spinner and elapsed runtime; non-TTY consoles show per-folder completion records with full paths and result statuses. Byte-level percentages inside an archive are not available from the current captured 7-Zip process.
- Synthetic tests in `tests/scripts/test_archive_compress_folder_wrapper.py` cover exclusions, skip-existing, error reporting and wrapper defaults.
- CI run [#356](https://github.com/Chain-Frost/ryan-tools/actions/runs/38862219245): Hosted tests **passed**; Python 3.13/MCP **passed**; Ruff lint and formatting **passed**. Policy is still red due to 226 strict Pyright errors in seven previously touched, non-archive files. Package remains red due to retained-wheel mismatch. No archive-specific Pyright errors were reported.

## Next action

Commit the verified latest dependency pointers and push them to PR #128, then confirm Policy, Package, Hosted tests and Python 3.13/MCP checks before merge. The real long-path `Q:\\` TUFLOW raster batch remains an environment-specific follow-up if suitable project data is available.

## Completion criteria

- All required GitHub checks pass at the PR head; a rebuilt retained wheel is verified and contains changed `wrapper_utils.py` and the archive compression helper.
- Windows smoke test confirms mapped-drive wrapper identity, readable long path output and no changes in CRS/mismatch/delete safeguards.
- Changes reviewed and merged on PR #128; this record and register updated to Complete.

## Validation and delivery

- 2026-10-16: Python 3.13 compatibility/MCP and Hosted tests passed on run #356 at `620adadb`. Ruff check/format passed; strict Pyright failed with 226 unrelated diagnostics.
- Policy failures (Ruff lint and formatting in changed files) were corrected through `c9f201a8`; a successful Policy run at that exact head is **not yet confirmed**.
- Package verification on earlier commits failed only at the retained-wheel/source consistency step, not at the raster comparison tests.
- 2026-10-16 local Windows/Python 3.14: strict Pyright passed on all 77 changed Python files; focused pytest passed with 88 passed and 7 skipped; documentation and Loguru policy checks passed.
- 2026-10-16 local Windows/Python 3.14: `python repo-scripts/build_library.py` built and verified `ryan_functions-26.10.16.2-py3-none-any.whl` (711811 bytes, SHA-256 `d21ba8146769d558813e906ac2ad9b37034e9f55c0e1314f69ecfcd2aa3a4ea3`).
- GitHub run [#360](https://github.com/Chain-Frost/ryan-tools/actions/runs/38873057681) rejected that wheel at `822ec99f` because the parent commit still pinned older dependency revisions. Upstream `main` and `HEAD` were verified at `run_hy8` `8f2e5b50` (`v2026.10.16.1`) and `ryan-culverts` `403f2ed5` (`v26.10.16.1`); include those parent pointers so CI verifies the same sources used by the wheel.
- Local Ruff could not execute because workstation application control returned `WinError 5`; GitHub Policy remains the authoritative Ruff run. Actual shared-drive raster smoke validation has not been run.
- Delivery: local changes are not yet committed or pushed. The PR remains open.

## Progress

### 2026-10-16

Reviewed use of `resolve()`, added general path-handling guidance and migrated ordinary first-party paths to `absolute()`; retained deliberate physical-identity checks, including critical overwrite and containment guards. Updated raster verification console layout and tests. GitHub CI exposed formatting/syntax/typing issues in the widened changed-file scope; addressed those findings. Tracked-wheel mismatch remains an external build dependency.

Removed five one-line migrations from legacy/specialised scripts instead of expanding the PR into unrelated typing rewrites. Corrected the remaining maintained-code typing issues, rebuilt the retained wheel as `26.10.16.2`, and completed focused local validation. Verified and selected the latest dependency submodule revisions after CI exposed the parent-pointer mismatch.
