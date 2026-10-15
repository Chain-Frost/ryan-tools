# Safe GeoTIFF recompression

| Field | Value |
| --- | --- |
| Status | Ready for review |
| Owner | ChatGPT |
| Created | 2026-10-15 |
| Updated | 2026-10-15 |
| Next review | 2026-10-22 |
| Baseline | `feature/safe-geotiff-recompression` at `31560a5`; clean parent worktree |

## Outcome and scope

Add a maintained generic workflow that audits GeoTIFF compression, creates lossless replacements without overviews,
verifies decoded pixels and substantive metadata, and replaces originals only after successful validation. The workflow
uses a one-source-read streaming implementation only for constrained single-band Float32 TUFLOW-style GeoTIFFs and
falls back to `gdal.Translate` for all other supported rasters. Optional local staging avoids repeated network reads.

## Current state

Implementation, focused tests, documentation, MCP discovery and package validation are complete. Pull request delivery
remains. No blockers are known.

## Next action

Review and merge the pull request after CI passes.

## Completion criteria

- Audit-only is the default and reports compression without pixel reads.
- Destructive replacement requires explicit authorization and leaves failed sources untouched.
- Eligible single-band Float32 GeoTIFFs use a one-source-read streaming path with decoded-output verification.
- Other rasters use `gdal.Translate`, with optional verified local source staging.
- Focused tests, Ruff, strict Pyright, documentation checks and package build pass.
- The branch is committed, pushed and opened as a documented pull request.

## Validation and delivery

Validation completed on Python 3.14:

- 22 focused raster, MCP registry and GDAL wrapper tests passed.
- Strict Pyright passed with zero errors or warnings.
- Loguru formatting and repository documentation checks passed.
- Wrapper compilation and `--help` smoke checks passed using the worktree package.
- `ryan_functions-26.10.15.2-py3-none-any.whl` built and verified successfully.

Ruff could not be invoked because the locally installed `ruff.exe` is denied by Windows execution policy. Black formatted
all modified Python files, `git diff --check` passed, and CI remains the authoritative Ruff check.

## Progress

### 2026-10-15

Reviewed repository architecture, wrapper, documentation, work-tracking and validation requirements. Confirmed the
existing FLT/ASC verifier does not perform TIFF compression or safe in-place replacement and should remain focused on
format-conversion cleanup.

Implemented audit-only discovery, the constrained Float32 streaming path, generic GDAL Translate fallback, verified
local staging of the TIFF and GDAL-reported sidecars, exact metadata/pixel/mask checks, atomic replacement, wrapper,
MCP catalogue entry, documentation and synthetic tests.
