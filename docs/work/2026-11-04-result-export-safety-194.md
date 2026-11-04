# Shared result export safety

| Field | Value |
| --- | --- |
| Status | Needs review - PR #202 published |
| Owner | ChatGPT |
| Created | 2026-11-04 |
| Updated | 2026-11-04 |
| Next review | 2026-11-05 |
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

[PR #202](https://github.com/Chain-Frost/ryan-tools/pull/202) is published,
stacked on #183. Implementation `7d22927e` is committed and pushed. Review
its safety policy and verify all current-head GitHub checks and review threads.
The PR body and [checks tab](https://github.com/Chain-Frost/ryan-tools/pull/202/checks)
record terminal hosted evidence after this publication milestone. No main merge
or release is authorised.

Next shared audit front after this fix is #195 (invalid discovery inputs versus
valid empty selections); #196/#198 dashboard adoption depends on truthful
outcome contracts. Other overdue work-register fronts remain outside this scope.
