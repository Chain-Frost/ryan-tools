# Shared result export safety

| Field | Value |
| --- | --- |
| Status | Integrated into #183 — closure raw-output follow-up implemented |
| Owner | ChatGPT |
| Created | 2026-11-04 |
| Updated | 2026-11-07 |
| Next review | 2026-11-14 |
| Baseline | `fix/result-export-safety-194`, stacked directly on #183 at `94a63b89`; clean starting worktree |

## Outcome and scope

Resolve [#194](https://github.com/Chain-Frost/ryan-tools/issues/194) separately
from the wrapper/dashboard audit in #183 and Office safety PR #201. The issue
contains the requested next-action comment and disposable collision evidence.

The shared exporter now validates the whole batch, stages every output, publishes
without replacement and propagates all writer/publication failures. Existing
filenames and public signatures remain supported. Partial publication retains
completed outputs and identifies them in the exception and error log. See the
[output contract](../../ryan_library/functions/excel_export.md).

MCP assessment: the existing report wrappers remain the right discoverable
entry points; their catalogue purposes now state that existing reports are
refused. No new basic-helper tool or duplicate CLI is introduced.

## Validation

Windows, normal Python 3.14.6, 2026-11-04:

- Exporter, regression, real workflow/wrapper failure and MCP catalogue checks:
  **70 passed, 2 skipped**. Covers writer errors, same-minute and sanitized-name
  collisions, case variants, concurrent destinations, partial publication,
  interrupted CSV fallback, real Windows junction aliases and unsupported hard-link publication.
- Focused existing result workflow/POMM/wrapper tests: **104 passed**; live
  PyTUFLOW preflight is not included in this focused selection.
- Inventory checks: **22 passed**. Documentation/index and Loguru checks pass.
- Strict Pyright, Ruff format/lint pass on all five modified Python files.
- Built and installed `26.11.4.1` with `build_library.py --skip-pip`; verified
  wheel SHA-256 `932426d7a8aa4d1d74212eba452b25b998d8ce1354bb8747f1a2a4ee41b8dc24`.
  The version change is intentional; the wheel remains ignored under `dist`.
- Six copied result wrappers pass installed-wheel smoke: on/off workbook
  parity, CLI settings, existing-output preservation and forced writer failures
  with exit 1 / FAILED outcomes. No staged reports remain after writer errors.
- A broader TUFLOW directory selection completed **287 passed, 6 skipped,
  3 failed**. The failures are unrelated input-preflight tests: local PyTUFLOW
  1.2.6 lacks `pytuflow.batch_validation`; its optional h5py import also reports
  a native binary mismatch. This is recorded as environment failure, not a
  passing skip or an exporter regression. No dependency repair is included here.
- Final combined focused run: **196 passed, 2 skipped** (Windows MCP file and
  directory symlink creation privileges unavailable). The real Windows junction
  alias test passes. Final no-bump verification rebuild is installed; package
  metadata remains `26.11.4.1`.
- No native TUFLOW solver or live mapped/UNC publication was tested.

## Next action and delivery

[PR #202](https://github.com/Chain-Frost/ryan-tools/pull/202) was squash-merged
into the #183 feature branch as `a56e0c6b` on 2026-11-07. Its original Windows
validation above remains historical evidence. Current-main integration and the
remaining closure raw-output gap are delivered in #183, not a recreated child.

Commit `8878d42b` routes the legacy closure CSV/gzip-Parquet sidecars through the
same typed export batch as formatted reports. Summary computation completes
before any output is staged. All destinations are checked before writes; every
writer completes before publication. Failed writers leave no published output,
while a publication race preserves existing files and reports any prior successful
publications. Names, raw Parquet index semantics and numerical results are retained.
The mode contract is explicit: raw sidecars are always included; `export_mode`
selects formatted report types.

Validation on Linux Python 3.14.10 with Windows-style spawn selected in the task
environment: 82 focused closure/exporter/helper tests pass; the integrated
exporter/closure/result-wrapper selection has 99 passes (overlapping selections).
Ruff 0.16.6, strict changed-file Pyright, docs, help and Loguru checks pass.
The new 26.11.7.1 wheel verifies and passes all six existing copied result-wrapper
checks plus six installed closure cases covering all modes, collision preservation
and failed CSV writing. The [foundation record](2026-11-01-batch-dashboard-foundation-180.md)
owns the current package hash, parent-integration evidence and hosted delivery state.

Next: verify the updated #183 hosted checks and fresh Codex review. Actual mapped/UNC
hard-link support remains native acceptance; no live share or licensed solver was
used. #195 discovery outcomes and #196/#198 dashboard adoption remain separate
follow-ups. No main merge or release is performed.
