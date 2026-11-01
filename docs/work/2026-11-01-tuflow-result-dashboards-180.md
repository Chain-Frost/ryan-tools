# TUFLOW result dashboard adoption

| Field | Value |
| --- | --- |
| Status | Planned; scope first, then wait for the foundation |
| Owner | Unassigned; adoption agent when started |
| Created | 2026-11-01 |
| Updated | 2026-11-01 |
| Next review | 2026-11-08 |
| Baseline | Parent documentation seed `4505e1cd7d2e157e8d800181e582aa7cabb572d9`, based on main `99cd3ccaaabd83b682392f7e285d90b570d353cb` |
| Branch / base | `feat/tuflow-result-dashboards-180` -> `feat/batch-dashboard-foundation-180` |
| Issue | [#180](https://github.com/Chain-Frost/ryan-tools/issues/180), selected AB-025 / AB-032 work |
| Dependency | [Foundation PR #183](https://github.com/Chain-Frost/ryan-tools/pull/183) and its [consumer readiness record](2026-11-01-batch-dashboard-foundation-180.md#consumer-interface-readiness) |
| Adopted foundation SHA | None; foundation is NOT_READY |
| Delivery | Documentation-only handoff; no runtime integration or acceptance yet |

## Outcome and scope

Make the selected result workflows actually use the shared dashboard contract. For substantial file/item batches,
ordinary processing should stay compact while warnings/errors remain visible and report generation stays truthful.
Existing supported dashboard options should work; deleting accepted options is not the solution to missing integration.

**Optional progress measures belong to each workflow.** Select whichever measures help its users, with appropriate
labels/units and honest unknown values. File counts and data volume are possible choices, not compulsory columns.
A workflow can omit extra measures or use different ones without requiring a renderer change.

This is PR 2 in the stack. The user will start both agents together: begin Phase A immediately, then wait for PR 1's
implemented interface before changing runtime code. The agent is authorised to commit/push scoped work to this
existing child branch and update its draft PR. Do not merge to main, enable auto-merge, force-push or release.

Read [AGENTS.md](../../AGENTS.md), the [development guide](../DEVELOPMENT_GUIDE.md),
[work tracking](../WORK_TRACKING.md), [work register](README.md),
[wrapper standard](../../ryan-scripts/WRAPPER_STANDARD.md),
[TUFLOW wrapper guide](../../ryan-scripts/TUFLOW-python/README.md),
[processor notes](../../ryan_library/processors/tuflow/README.md) and the whole
[parent handoff](2026-11-01-batch-dashboard-foundation-180.md). Reconcile the current remote branches before resuming.

## Current state

The child is seeded from [foundation PR #183](https://github.com/Chain-Frost/ryan-tools/pull/183) and targets its
branch. It is ready for the adoption agent to begin Phase A. The foundation is **NOT_READY** and no foundation
implementation SHA has been adopted. No runtime edits or Phase A completion are claimed by this launch checkpoint.

The source observations below identify the intended scope; they do not replace Phase A's call-chain and test inventory.

### Launch checklist

- [ ] Confirm the actual child/parent branches and preserve unrelated worktree changes.
- [ ] Complete the six-workflow call-chain, effective-setting and existing-test inventory.
- [ ] Record workflow-selected optional metrics and their cheap sources/unknown semantics.
- [ ] Push a documentation-only scoping checkpoint with planned files, tests and required parent capabilities.
- [ ] If the foundation is not ready, record WAITING_FOR_FOUNDATION and its exact resume condition.
- [ ] Verify and merge the concrete parent implementation, then record the adopted SHA before runtime integration.

### Scoping worksheet

The adoption agent should replace pending entries with source evidence and the proposed smallest change.

| Area | Evidence to capture | Initial state |
| --- | --- | --- |
| Option propagation | Per-wrapper editable defaults, CLI overrides and currently ignored settings | Pending Phase A |
| Processing contracts | Ordering, skips/errors, pool fallback, raw-frame disposal and export lifecycle | Pending Phase A |
| Optional measures | Relevant subset per workflow, labels/units, available metadata, unknown/partial values | Pending Phase A |
| Paths and filters | Effective path bases before chdir, mean output directory, location and empty-filter semantics | Pending Phase A |
| Test ownership | Existing coverage, focused gaps, serial/spawn and copied-wrapper cases | Pending Phase A |
| Parent dependency | Required behaviour compared with the published callable/outcome contract | NOT_READY; no API assumed |

## Ownership

### Six wrappers

All paths in this table are under `ryan-scripts/TUFLOW-python/`.

| Wrapper | Primary workflow path |
| --- | --- |
| [culvert_results/combine_culvert_maximums.py](../../ryan-scripts/TUFLOW-python/culvert_results/combine_culvert_maximums.py) | [tuflow_culverts_merge.py](../../ryan_library/orchestrators/tuflow/tuflow_culverts_merge.py) |
| [culvert_results/combine_culvert_timeseries.py](../../ryan-scripts/TUFLOW-python/culvert_results/combine_culvert_timeseries.py) | [tuflow_culverts_timeseries.py](../../ryan_library/orchestrators/tuflow/tuflow_culverts_timeseries.py) |
| [culvert_results/create_mean_culvert_peak_report.py](../../ryan-scripts/TUFLOW-python/culvert_results/create_mean_culvert_peak_report.py) | [tuflow_culverts_mean.py](../../ryan_library/orchestrators/tuflow/tuflow_culverts_mean.py) |
| [culvert_results/create_median_culvert_peak_report.py](../../ryan-scripts/TUFLOW-python/culvert_results/create_median_culvert_peak_report.py) | Shared mean/median culvert aggregation; verify the exact dispatch in Phase A |
| [po_and_timeseries/combine_po_results.py](../../ryan-scripts/TUFLOW-python/po_and_timeseries/combine_po_results.py) | [po_combine.py](../../ryan_library/orchestrators/tuflow/po_combine.py) and the combination helper |
| [po_and_timeseries/combine_pomm_results.py](../../ryan-scripts/TUFLOW-python/po_and_timeseries/combine_pomm_results.py) | [pomm_combine.py](../../ryan_library/orchestrators/tuflow/pomm_combine.py) and the combination helper |

This table is a path map: wrapper paths are relative to the directory stated above; workflow modules live under
ryan_library/orchestrators/tuflow.

### Child-owned integration files

- `ryan_library/functions/tuflow/tuflow_common.py`
- `ryan_library/orchestrators/tuflow/_combination_workflow.py`
- `ryan_library/orchestrators/tuflow/tuflow_culverts_merge.py`
- `ryan_library/orchestrators/tuflow/tuflow_culverts_timeseries.py`
- `ryan_library/orchestrators/tuflow/tuflow_culverts_mean.py`
- `ryan_library/orchestrators/tuflow/po_combine.py`
- `ryan_library/orchestrators/tuflow/pomm_combine.py`
- `ryan-scripts/TUFLOW-python/README.md` and this work record.

### Child-owned existing tests

- `tests/functions/tuflow/test_tuflow_common.py` and
  `tests/functions/tuflow/test_tuflow_common_coverage.py`.
- `tests/orchestrators/tuflow/test_combination_export_compatibility.py` and
  `tests/orchestrators/tuflow/test_tuflow_culverts_mean.py`.
- `tests/scripts/tuflow/test_tuflow_culverts_merge.py`,
  `tests/scripts/tuflow/test_tuflow_culverts_timeseries.py`,
  `tests/scripts/tuflow/test_po_combine.py` and `tests/scripts/tuflow/test_pomm_combine.py`.

Use dedicated integration tests for new coverage instead of both agents modifying generic wrapper tests.
Shared conftest/fixtures and broad installed-wrapper test files are read-only unless ownership is recorded first.
The parent owns the renderer, adapter, logger, shared wrapper CLI helpers, log-summary reference, canonical dashboard
guide and global standards. Do not duplicate or modify those implementations independently.

The parent is the sole writer of the canonical audit/backlog during this stack; put this PR's AB-025/AB-032 evidence
here and hand it over for reconciliation. Re-read shared register/index files immediately before editing only your own
row/required links. Preserve all concurrent entries. Never edit the parent readiness record.

## Phase A: scope immediately

Before any runtime edits:

1. Inspect the current six wrappers and complete call chains through discovery, processing, aggregation and export.
   Record effective editable defaults, CLI propagation, logging ownership, paths/working-directory bases, location
   filters, pause toggles and dashboard options. Confirm what is implemented versus accepted but ignored.
2. Inventory focused existing tests and preserve the important contracts: selected inputs, deduplication, result
   ordering, typed results, empty/no-result handling, failure/skip classification, raw-frame disposal and serial fallback.
   In particular, examine the existing fallback for MaybeEncodingError/OSError; dashboard integration must not swallow
   the signal or make the fallback unreachable.
3. Propose the relevant display per workflow: phases, optional measures, labels/units, where cheap metadata is already
   available, unknown-value handling and which outcomes advance each measure. It is acceptable to omit data volume.
4. Identify the smallest integration points and regression cases. Keep aggregation mathematics, filtering decisions,
   report names, export formats and existing public call contracts stable.
5. Commit and push a documentation-only scoping update to this record. List planned file changes, test ownership,
   unresolved questions, required parent capabilities and one concrete next action. Do not mark source review as
   runtime acceptance.
6. Read the parent's consumer readiness record. If the gate below is not satisfied, set this record/register to
   Blocked with phase **WAITING_FOR_FOUNDATION**, preserve the scope findings, and report the dependency.

Do not implement against proposed signatures, create a temporary second adapter, copy parent implementation, or
cherry-pick selected parent commits during the wait. Do not slip otherwise useful runtime fixes into Phase A.
The waiting period is useful for completing source/test analysis, not speculative code.

## Foundation adoption gate

The gate is satisfied only when the remote parent record says **READY_FOR_ADOPTION** and provides all of:

- A pushed immutable implementation commit SHA, not only a plan, stub, PR number or future promise.
- Actual callable/import, configuration, optional-metric, diagnostic and worker contracts.
- Defined result ordering, outcomes, partial-failure/infrastructure-error, interruption and cleanup semantics.
- A working log-summary reference and named focused test/smoke evidence demonstrating the interface.
- Relevant compatibility limits and any remaining environment-specific acceptance.

Then, without asking for new user permission:

1. Fetch the actual remote parent branch and verify the recorded implementation SHA is an ancestor of that head.
2. Merge the parent branch normally into this child, preserving unrelated work. Do not force-push, reimplement the
   foundation or take only guessed API files.
3. Read and smoke-check the real merged interface and reference. Record the adopted implementation SHA, merged parent
   head and relevant evidence here. Confirm that optional metrics, diagnostics and failures have the promised semantics.
4. If the record claims readiness but the source/evidence does not support it, keep waiting and name the mismatch.
   If a necessary capability is absent, give the parent a concrete requirement rather than creating a parallel API.
5. Proceed to Phase B only after these checks. Record later parent updates and recheck any changed interface.

While actively working, checking for a newly published parent handoff is reasonable; do not busy-poll indefinitely.
If it remains unavailable at turn end, push the scoping checkpoint and state WAITING_FOR_FOUNDATION with the exact
resume condition. Do not imply a background watcher is running. Resume from this record when the agent next runs.

## Phase B: implement the scoped integrations

### Dashboard use

- Adopt the parent's actual shared contract across the six workflows, through their existing shared CSV/combination
  boundary where appropriate. Keep domain aggregation/export logic in the current workflow modules.
- Wire editable dashboard settings and CLI overrides through all layers. Preserve familiar setting names and existing
  flags. Add necessary enable/disable, refresh/row settings only consistently with the foundation and existing helpers.
  Every operational option remains editable in the wrapper; no blanket DEFAULT_* renaming or boilerplate comments.
- Use one coherent lifecycle for discovery, processing, aggregation and export. File processing completion cannot
  announce report completion. Preserve warnings and errors during and after the display.
- Supply only relevant optional measures. The common batch summarizer already obtains file-size metadata, but currently
  uses zero when stat fails. If those values feed a display, preserve unknown versus actual zero without turning an
  unavailable display measure into a new processing error.
- Label file-size aggregates as input size / completed input size / successful input size according to their meaning;
  do not call them bytes read. Use explicit MB or MiB. Do not invent rates/ETA from snapshots or double-count retries.
  Partial known totals must be marked partial or lack a percentage. Reuse existing cheap metadata; no extra content
  pass or storage ping just for the dashboard.
- Preserve enabled/disabled processing parity, queue cleanup, serial behaviour, input/result ordering, raw-frame
  disposal and existing recoverable multiprocessing fallback. An exception captured for display must not disappear
  from workflow error handling.

### Bounded wrapper corrections in the same selected call paths

- Capture the effective working directory and relative input/output bases before chdir; make ordinary user-visible
  paths absolute without resolving symlinks/junctions. Cover the mean-report output-directory path specifically.
- Expose the existing location filter as an editable setting in the four culvert wrappers and preserve CLI precedence.
- Make pause settings editable and overrideable in both directions across all six wrappers, using the shared helper
  contract. Keep pausing and exit handling at the process boundary.
- Preserve existing filter semantics, including the shared empty-filter normalizer, unless a directly necessary
  behaviour change is explicitly agreed and documented. Do not silently reinterpret an empty filter as a new feature.
- Preserve algorithms, time/event/duration/TP aggregation, output names, workbook/Parquet contents and error exit
  behaviour. No unrelated parser, export or orchestration refactor.

## Completion criteria and validation

Record exact commands, dates, environment, commit and pass/fail/not-run results. Use Python 3.14 and focused checks.

- All six wrappers propagate their effective editable/CLI settings; success and controlled failure paths work.
  Check explicit true/false and relevant zero values, headless execution, help/version and exit codes.
- Verify relative/absolute input and output paths with a deliberately different current directory and decoy files.
  Preserve mapped/UNC/linked-path spellings where affected; identify native-only checks honestly.
- Exercise representative real CSV processing and aggregation/export fixtures, comparing dashboard on/off data,
  selected inputs, ordering, report values, output names and export formats.
- Cover success/skip/failure accounting, optional/alternative metrics, unavailable/partial sizes, retries and truthful
  aggregation/export completion. Assert no additional content reads for progress.
- Cover existing serial and recoverable pool fallback, genuine spawned-worker warnings/errors and cleanup.
  Record native Windows Python 3.14 smoke separately; no licensed TUFLOW execution is needed for result fixtures.
- Run focused existing/new tests in the owned areas, Ruff format/lint, strict Pyright only on modified Python files,
  compilation, Loguru formatting checks when calls change, and documentation checks.
- Verify a no-version-bump package build after library changes. Run representative copied result wrappers outside the
  checkout against that installed build, with disposable fixtures and actual output assertions, not --help alone.
  Record source/package origins and any remaining workflow-specific installed-wrapper gaps.
- Update local wrapper documentation, this record and your own register/index entries. Give the parent precise
  AB-025/AB-032 evidence with limits; neither generic CI nor this six-wrapper subset closes the whole audit.
- Keep the child PR draft and based on the parent until the stack is reviewed. Merging or retargeting after a future
  parent merge is a separate delivery step, not authorised by this implementation handoff.

## Exclusions

Do not inspect or traverse the unsorted submodule, run licensed solvers, touch production data or retrieve expensive
storage for progress. Preserve accepted TUFLOW runner settings.OP.RUN defaults and selection trimming. Do not change
simulation scheduling/GPU/CSV continuation or the MCP catalogue work owned by
[#182](https://github.com/Chain-Frost/ryan-tools/pull/182). Do not expand into unrelated standalone workflows,
inventory-wide compliance or accepted/deferred audit decisions.

## Next action

Run Phase A on the child branch, push the scoped documentation checkpoint, and wait for the verified foundation gate.

## Validation and delivery

No runtime work or acceptance is claimed by this setup handoff. Documentation setup checks and PR references are
recorded in the draft PR body. The implementation agent must fill this section with its own commands and results.
The foundation SHA remains None until verified adoption.

## Progress

### 2026-11-01

Defined the six-wrapper boundary, child-owned pipeline/tests, optional workflow measures and two-phase concurrent
start. The setup explicitly prevents speculative integration while the parent interface is still being implemented.

### 2026-11-01 — child launch checkpoint

Created the child draft from the parent seed with an explicit scoping checklist and pending evidence worksheet.
Phase A can start immediately; runtime integration remains gated on the parent's pushed implementation and evidence.
