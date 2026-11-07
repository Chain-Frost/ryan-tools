# WP08 — POMM, PO QA and closure dashboard adoption (#198)

| Field | Value |
| --- | --- |
| Status | Draft implementation; current integrated-head CI and native acceptance pending |
| Owner | WP08 integration |
| Created / updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | `main@5ad632cc9a17b302ad3f4f2b55315a1e2e8d1d51`; stacked onto #251 |
| Delivery | [Draft PR #265](https://github.com/Chain-Frost/ryan-tools/pull/265), `feat/po-qa-dashboard-outcomes-198` |
| Issues | [#198](https://github.com/Chain-Frost/ryan-tools/issues/198), [#194](https://github.com/Chain-Frost/ryan-tools/issues/194), [#180](https://github.com/Chain-Frost/ryan-tools/issues/180), [#240](https://github.com/Chain-Frost/ryan-tools/issues/240) |

## Scope and ownership

Adopt the existing `LiveWorkflowDashboard` and terminal outcome conventions for
four maintained workflow families: PO peak checks, PO/1D Q stability checks,
POMM mean/median peak reports, and PO closure-duration summaries. Wrap their
whole discovery/aggregation/export lifecycle, preserving format selection,
file naming, established engineering calculations, location filtering,
raw closure sidecars, historical wrappers and no-overwrite export policy.

PO QA uses its existing ordered `ProcessPoolExecutor.map` results for
per-file visible states. Parser diagnostic statuses such as
`CSV_PARSE_FAIL` and `TIME_PARSE_FAIL` yield FAILED even when the
other files produce a report. Zero-byte and headerless CSVs (`EMPTY_FILE`,
`NO_COLUMNS`) are malformed, not valid empty inputs; `NO_DATA` yields SKIPPED.
Engineering warnings (e.g. `WARN_1H`, `UNSTABLE`) remain result data,
not execution failures.

POMM and closure orchestrators provide one truthful **workflow-level** task
covering aggregation and publication, rather than fabricated per-file counts.
When there are no reportable rows, they return SKIPPED (zero exit);
a propagated processing/export exception is FAILED with nonzero wrapper exit;
only a completed publication reports SUCCESS. The shared `pomm_utils.py`,
closure calculation helpers, processor/discovery APIs, and public report formats
are unchanged. Their internal **partial-input** outcome fidelity still depends
on WP07 #195 and WP04 #219/#223, plus WP03 #216/#217 and WP09 #215;
the current aggregate-only API cannot independently prove every input succeeded.
Do not claim #198 fully accepted until that contract is integrated and tested.

## Source implementation and tests

- PO QA: two wrappers and orchestrators, with per-file status mapping and
  synthetic malformed/empty/partial/export-failure regression tests.
- POMM: `pomm_max_items.py` and both peak wrappers; reusable renderer tracks
  no-data, aggregation/export exceptions and full publication without touching
  WP03's `pomm_utils.py` or statistics.
- Closure: `closure_durations.py` and maintained wrapper; dashboard persists
  through the existing shared protected export batch without modifying
  WP09's duration calculations or #194 raw-table publication.
- Test files: `test_qa_dashboard_outcomes_198.py`,
  `test_pomm_dashboard_outcomes_198.py` and
  `test_closure_dashboard_outcomes_198.py`.
- Installed/copied-wheel smoke in
  `repo-scripts/smoke_result_dashboard_installed_wheel.py` now covers PO QA,
  POMM mean/median and closure on/off report parity and invalid settings,
  alongside the six accepted dashboard wrappers.
- The existing #194 copied-collision smoke now pins timestamps in both the
  Excel exporter and culvert report orchestrator to avoid the known
  minute-rollover race; output-preservation assertions remain unchanged.

## Validation and native acceptance

The earlier **QA-only** head `391ade6e` passed all six GitHub CI jobs,
including Windows Python 3.14 hosted tests, strict Pyright and installed-wheel
checks. These are historical for the newly added POMM/closure scope and do
not validate the current head. Focused current-head Ruff, strict Pyright,
hosted tests, installed-wheel/copied-wrapper parity and failure outcomes
must pass before marking code acceptance.

No live TUFLOW solver, proprietary project, production workbook, mapped/UNC
SMB share or Office lock was exercised. Native Windows Excel/UNC no-overwrite
publication and representative project POMM/closure report parity remain separate.
WP03 #267 real POMM-only/RLL_Qmx/mixed and WP09 #215 dry-event/scenario
correctness are separate prerequisites, not covered by dashboard tests.

The GitHub connector provided isolated branches and hosted checks; the
agent's container uses Python 3.13 and cannot resolve github.com for Git clone.
A local Python 3.14 worktree and installed/copied-wrapper check on native
project data remain unperformed; no claims of such validation are made.

## Handoff

Refresh this child through a normal merge from the current #251 parent;
shared `docs/README.md`, `docs/work/README.md` and TUFLOW script
documentation must preserve WP01/WP02/WP06 and other pack owners' changes.
Review the current #265 head and integrate into #251 only after accepted
validation. **No `Fixes #198`** until partial-input and native acceptance
requirements are satisfied; use `Refs #198` for this child. No merge to
`main`, auto-merge, release or unassigned numerical work.
