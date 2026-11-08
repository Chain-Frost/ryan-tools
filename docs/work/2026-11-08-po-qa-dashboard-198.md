# WP08 — POMM, PO QA and closure dashboard adoption (#198)

| Field | Value |
| --- | --- |
| Status | Bounded native/installed/partial-input checks pass; WP04/WP09 and project/Excel/ACL gates open |
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
  alongside the six accepted dashboard wrappers. In the minimal installed-wheel
  environment with neither `pyarrow` nor `fastparquet`, the closure smoke
  instead requires a nonzero/FAILED outcome and no partial artifacts;
  successful copied closure export remains a separate optional-dependency gate.
- The existing #194 copied-collision smoke now pins timestamps in both the
  Excel exporter and culvert report orchestrator to avoid the known
  minute-rollover race; output-preservation assertions remain unchanged.

## Validation and native acceptance

**Code implementation verified at `ee5040b04fb58159de9e03814cc06461dd270528`:**
[Fast PR checks](https://github.com/Chain-Frost/ryan-tools/actions/runs/42401226388)
and [Python CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/42401226390)
passed all six jobs. Windows Python 3.14 hosted tests **2,585 passed,
14 skipped, eight deselected**. Ruff and format, strict Pyright (zero errors),
documentation, POSIX security, MCP, package build and copied installed-wheel
smoke passed. The smoke checks existing six result wrappers, both PO QA checks,
POMM mean/median, and closure failure preservation.

The isolated copied-wheel test has **neither Parquet engine**. Because closure
always generates raw gzip Parquet sidecars, the test verifies that the wrapper
returns FAILED/nonzero and does not leave partial outputs; it **does not
prove successful closure export**. Success on a normal installed wheel with
`pyarrow` or `fastparquet` requires native verification. The copied QA and POMM
report success paths passed on/off workbook parity.

No live TUFLOW solver, proprietary project, production workbook, mapped/UNC
SMB share or Office lock was exercised. Native Windows Excel/UNC no-overwrite
publication and representative project POMM/closure report parity remain separate.
WP03 #267 real POMM-only/RLL_Qmx/mixed and WP09 #215 dry-event/scenario
correctness are separate prerequisites, not covered by dashboard tests.
Full partial-input truthfulness depends on WP07 #195 and WP04 #219/#223.

The GitHub connector provided isolated branches and hosted checks; the
agent's container uses Python 3.13 and cannot resolve github.com for Git clone.
A local Python 3.14 worktree and installed/copied-wrapper check on native
project data remain unperformed; no claims of such validation are made.

The evidence above applies to the code head `ee5040b04`. A subsequent
documentation-only handoff update has a new commit SHA; confirm current-head
checks before closing review. Codex review was requested separately.

## Handoff

Child was refreshed through a normal merge from #251 at `63a36d48`; recheck that parent immediately before integration.
shared `docs/README.md`, `docs/work/README.md` and TUFLOW script
documentation must preserve WP01/WP02/WP06 and other pack owners' changes.
Review the current #265 head and integrate into #251 only after accepted
validation. **No `Fixes #198`** until partial-input and native acceptance
requirements are satisfied; use `Refs #198` for this child. No merge to
`main`, auto-merge, release or unassigned numerical work.

## 2026-11-08 native installed-wheel acceptance - bounded checks PASS, broader gates open

Refreshed the existing child through a normal merge from latest parent #251 **`d61925675dffd86811ae9277b9ca53615e172f1f`**, retaining branch/PR identity. Tested integrated code **`ff2b4ad4`**. Conflict resolution retained both the child's dashboard options and the parent's explicit-empty discovery semantics, configurable ARR duration schedule, expected TP count and mean-report coverage metadata. Parent `pomm_utils.py` and numerical closure helpers remain unchanged. No force-push or incorporation into #251.

Windows 10 build 19045; Python 3.14.6; pandas 3.0.5, openpyxl 3.1.5, pyarrow **24.0.0**; pinned PyTuflow **1.3.0 / 887534ea8c15b80d185a38a7ead1d42ab42a7167**. No-bump build verified wheel 26.11.8.2; installed into scratch target with origin verification. Maintained wrappers copied outside checkout; subprocesses use the target wheel plus normal user dependencies, rather than CI's minimal environment. No production input, solver, global environment change or unsorted inspection.

| Check | Outcome | Evidence |
| --- | --- | --- |
| Focused #198 source outcomes | PASS | 39 passed: QA, POMM and closure dashboard tests |
| Parent numerical/discovery and export safety regressions | PASS | 55 passed: POMM filtered outcomes, coverage, #218 inputs, finite population, closure exporter safety and closure calculations |
| Strict modified-file Pyright / Ruff / logging | PASS | Zero Pyright findings; three conflict-resolved files lint/format clean; Loguru policy passes |
| Copied installed wrappers, dashboard on/off | PASS | Both PO QA, POMM mean/median and closure: successful local and genuine UNC execution; selected workbook schema/numerical parity |
| Successful closure Parquet export/readback | PASS | Working pyarrow: required raw durex CSV/gzip-Parquet agree, local and UNC, both dashboard modes |
| Mapped closure export-mode both | PASS | Excel, both formatted Parquet products and raw CSV/Parquet reopen; source hashes/mtime unchanged |
| Input outcomes | PASS | 20 copied-wrapper cases: each of five wrappers with empty/filtered -> SKIPPED/0 and invalid/partial corrupt -> FAILED/1; source hashes/mtime unchanged |
| Forced export errors / identical-name no-clobber | PASS | Ten copied-wrapper checks across all five wrappers; forced PermissionError -> FAILED/nonzero, no partial output; fixed timestamps make repeat filenames identical, all previous output hashes retained |
| Mapped batch no-clobber | PASS | Closure export-mode both repeated at pinned timestamp: FAILED/nonzero, entire existing output set/hash/mtime unchanged, no owned stages |
| Synthetic engineering values and TP provenance | PASS (bounded) | POMM peaks 5/10 yield mean 7.5 with mean_TP TP01; accepted upper-median event 10/TP02; count_TP 2. Parent WP03 functions/coverage preserved |
| Actual locked Excel report, real ACL denial, representative private project data | NOT RUN | No production data or permission changes; injected permission error is not a real ACL test |
| Full WP04 #219/#223 and WP09 #215 acceptance | BLOCKED / separate prerequisite | These native dashboard checks do not accept all shared validity or dry-event/scenario closure semantics |

Closure parity is bounded to synthetic wet-event inputs and current parent baseline. No claim that independent WP09 hydraulic semantics are accepted. Missing/invalid/filtered outcomes are verified above, but this is not blanket cross-format #219/#223 validity acceptance. The workbook numerical snapshot ignores path/time metadata that naturally differs by output folder; raw file hashes are recorded per run and are not expected to be byte-equal across independent workbooks.

Commands: `py -3.14 installed_acceptance.py 198 <fresh-local-or-UNC> <result>`, `py -3.14 outcomes198.py`, `py -3.14 export198.py`, `py -3.14 verify_products.py`, `py -3.14 mapped_closure.py`, from private `E:/Github/wp08-native-20261108` with PYTHONPATH set to installed198. Full source pytest commands and per-wrapper commands/exits/output SHA256 are retained in adjacent logs/JSON; internal network paths remain private. Scratch harness adjustments corrected an initial expectation of two raw closure Parquet files (the maintained contract requires one raw durex product; formatted products are separately verified with export-mode both), without changing repository expectations or implementation.

**Disposition:** bounded installed/native success, partial-input outcome, failure and no-clobber checks pass. Broad #198 acceptance stays open for real-project/locked-workbook/ACL coverage and the explicitly separate WP04/WP09 prerequisites. No conditional Fixes #198, parent incorporation, main merge, auto-merge, release or issue closure. Existing draft PR receives the refresh/evidence and fresh review request; next review 2026-11-15.

### Fresh review follow-up at f3c1dddb

The requested independent review raised a new unresolved P2 at https://github.com/Chain-Frost/ryan-tools/pull/265#discussion_r4505507621: PO peak/stability use blocking ordered worker iteration while Rich automatic refresh is disabled, so the interactive dashboard cannot refresh elapsed time during slow CSV processing. The redirected on/off output-parity checks above do not test that interactive responsiveness and must not be used to dismiss this finding.

This is an additional review blocker, assigned to the dashboard implementation owner for a bounded polling/refresh correction with ordered aggregation preserved. No implementation change or passing responsiveness claim is made by this validation handoff. Native #198 acceptance remains open. At closeout five of six checks passed on f3c1dddb; Hosted tests was still running. These are separate delivery states.

### Interactive dashboard worker-wait review fix

Codex [P2 review](https://github.com/Chain-Frost/ryan-tools/pull/265#discussion_r4505507621) found that ordered `ProcessPoolExecutor.map` result iteration blocks the UI coordinator and leaves the Rich dashboard frozen while a slow CSV is being processed. This fix retains `map`'s ordered results and user-provided `chunksize`, but consumes its blocking `next()` calls in one owned helper thread. The coordinating thread waits on that future with a timeout set by `live_refresh_per_second` and forces a dashboard refresh between timeouts. Each file is marked running and immediately refreshed before waiting; only the coordinator updates the dashboard.

A deterministic test blocks the mock ordered iterator until the coordinator has forced two in-wait refreshes, covering both peak-check and stability workflows without a timing-sensitive sleep. This is a **hosted code/test fix**, not evidence of interactive terminal performance or native Windows/SMB acceptance. Current-head CI, code review and any actual live-dashboard responsiveness acceptance remain to be confirmed. Whole-workflow outcome and numerical aggregation semantics are unchanged. No parent/main integration or release is authorised.


## 2026-11-08 — partial input accounting follow-up (stacked on current #251)

WP08's dashboard adoption was integrated into #251, but POMM and closure were
still consuming only successful processors. The existing
`FileBatchProgress` classifier in `tuflow_common` already distinguishes
completed, filtered-empty and failed file outcomes (including missing
processor results); the two whole-report orchestrators were not consuming
that accounting. A mixed successful/failed batch could therefore publish
an incomplete workbook with a SUCCESS status.

The bounded #198 follow-up passes optional `batch_progress` through the
POMM discovery/aggregation helpers and uses it in both peak-report wrappers;
closure uses the same existing counter when reading PO inputs. When any file
failed or when the scheduled outcome count is incomplete, the workflow
raises an actionable exception, marks its dashboard FAILED and refuses export.
Successful, valid empty, filtered-empty, interrupted and exporter-failure
contracts are unchanged. The calculation functions, mean/median selection,
closure-duration populations, public report layouts and shared processor
worker implementation are untouched. Synthetic regressions exercise
dashboard on/off, both mean/median paths, failed or missing outcomes, no
partial export, counter forwarding and counter reset on valid empty scans.

**Stack base:** latest #251 at the time of delivery, including its
repository-wide strict typing repairs and 26.11.8.8 dependency/stub updates.
This is an independent follow-up PR, not a merge to `main`.
**Validation required:** final-head Windows Python 3.14 Ruff, strict Pyright,
focused POMM/closure tests, hosted package/copied-wheel checks and real
PO/POMM/closure sample parity after WP04 #284 and WP09 #281 settle.
Those separate hydraulic source-validity and dry-event/scenario-identity
contracts are not accepted by this dashboard-only change. Do not add
`Fixes #198` or close that issue until cross-pack and native acceptance pass.
