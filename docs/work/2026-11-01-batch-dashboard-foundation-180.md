# Batch dashboard foundation and wrapper contract

| Field | Value |
| --- | --- |
| Status | Needs review — integration complete; runner session extracted into library |
| Owner | ChatGPT (foundation recovery) |
| Created | 2026-11-01 |
| Updated | 2026-11-07 |
| Next review | 2026-11-14 |
| Baseline | main at `99cd3ccaaabd83b682392f7e285d90b570d353cb` (merged PR #159) |
| Branch / base | `feat/batch-dashboard-foundation-180` -> `main` |
| Issue | [#180](https://github.com/Chain-Frost/ryan-tools/issues/180), dashboard foundation and resumed wrapper/script audit |
| Dependent work | [TUFLOW result dashboard adoption](2026-11-01-tuflow-result-dashboards-180.md) |
| Delivery | #202, current main, closure safety and serial rendering corrections remain integrated. The requested runner extraction reduces the wrapper from 570 to 408 lines while preserving scheduling, progress, rendering and logging mechanisms. Package 26.11.7.2 and wrapper 2026-11-07.2 supply the new workflow boundary. Use current PR checks/review for published-head acceptance and keep Office #201 on this parent. No main merge/release. |

## Outcome and scope

Establish and implement the shared dashboard contract before migrating the selected result wrappers. The user prefers
dashboards for workflows processing many files or items: routine processing should produce a compact display while
actual warnings and errors remain visible. Keep useful ordinary logging when the dashboard is disabled or unavailable.

**Progress measures are optional and workflow-specific.** Each workflow selects relevant measures, labels, units and
metadata. File counts and data volume are examples, not required fields or a closed list. Future workflows may care
about groups, rows, tiles, model runs, phases or other measures. Do not require every wrapper to expose a metric picker
or a large configuration schema.

This is PR 1 of a two-PR stack. PR 2 can scope concurrently, but its runtime integration waits for the concrete
implementation handoff below. The user authorised these draft PRs and implementation-agent prompts; this setup commit
contains documentation only. The implementation agent may make, commit and push scoped changes to this existing
branch and update its draft PR. Merging, auto-merge, force-pushing, releases and unrelated remediation are outside scope.

Read [AGENTS.md](../../AGENTS.md), the [development guide](../DEVELOPMENT_GUIDE.md),
[work register](README.md), [work tracking](../WORK_TRACKING.md),
[wrapper standard](../../ryan-scripts/WRAPPER_STANDARD.md),
[script guide](../../ryan-scripts/README.md), [logging guide](../LOGGING.md) and relevant local READMEs before editing.
This work record defines the selected task; lasting policy belongs in canonical guides after implementation.

## Current state and evidence

The baseline already has a reusable Rich renderer, workflow adapter, Loguru transport and a log-summary consumer.
Extend those boundaries rather than creating a second dashboard, scheduler, plugin framework or general event bus.

| Baseline observation | Evidence / implementation consequence |
| --- | --- |
| Configurable columns and metadata already exist | Start with `ryan_library/functions/live_dashboard.py`, including its extra-metric support. Preserve existing consumers, including simulation displays. |
| Log-summary dashboard mode passes `worker_log_level="ERROR"` | Trace `ryan_library/orchestrators/tuflow/tuflow_logsummary.py`, `ryan_library/functions/dashboard_workflow.py` and `ryan_library/functions/loguru_helpers.py`: producer filtering can discard warnings and detailed records before sinks receive them. |
| Completed-file INFO messages are replayed after processing | Review the log-summary result logging so dashboard mode does not recreate routine per-file clutter after the display closes. Preserve meaningful skip/error diagnostics. |
| The adapter can catch failures and return only successes | Specify item outcomes and infrastructure failure propagation. A caller must be able to preserve its fallback/retry policy without guessing which results disappeared. |
| Live rendering and pausing are separate concerns | Redirected output needs a bounded plain-text path. `--no-pause` alone does not prove that cursor-control output has been disabled. |
| Parsing, aggregation and export have distinct completion points | Input completion must not announce overall success before export finishes. |

These are source observations at the baseline, not completed fixes or runtime acceptance. Recheck current source before
implementation; record any corrected interpretation. Relevant audit work includes AB-025 and the selected executable
wrapper subset of AB-032 in the [existing backlog](../audits/2026-10-22-script-remediation-backlog.md).
These two PRs do not complete the broader audit or all work in either backlog item.

## Ownership and boundaries

### Foundation agent owns

- `ryan_library/functions/live_dashboard.py`
- `ryan_library/functions/dashboard_workflow.py`
- Necessary logging integration in `ryan_library/functions/loguru_helpers.py`; do not redesign unrelated logging.
- Necessary backward-compatible shared CLI changes in `ryan_library/functions/wrapper_utils.py`.
- Reference wrapper `ryan-scripts/TUFLOW-python/log_processing/create_log_summary_report.py` and
  `ryan_library/orchestrators/tuflow/tuflow_logsummary.py`.
- Existing focused tests: `tests/functions/test_live_dashboard.py`,
  `tests/functions/test_dashboard_workflow.py`, `tests/functions/test_loguru_helpers.py`,
  `tests/functions/test_loguru_helpers_coverage.py`, `tests/support/loguru_multiprocessing_harness.py`,
  `tests/scripts/test_wrapper_utils.py`, `tests/scripts/tuflow/test_tuflow_logsummary.py` and
  `tests/orchestrators/tuflow/test_orchestrator_tuflow_logsummary.py`.
- A new canonical dashboard guide at docs/DASHBOARDS.md, minimal aligned updates to the wrapper/logging guides and,
  only where necessary, shared script/development guidance. Avoid duplicating policy in AGENTS.
- This work record and narrowly evidenced dashboard updates to the canonical audit/backlog.

### Adoption agent owns

The six result wrappers, their common CSV/result pipeline, dedicated tests and local wrapper README listed in the
[dependent record](2026-11-01-tuflow-result-dashboards-180.md). In particular,
`ryan_library/functions/tuflow/tuflow_common.py` and
`ryan_library/orchestrators/tuflow/_combination_workflow.py` are child-owned. Do not change them for the reference
integration without an explicit ownership handoff recorded by both agents.

Each agent owns its own work record. The child never edits this readiness section; it inherits it through normal
merges. For shared `docs/work/README.md` and `docs/README.md`, re-read the latest content and edit only your own
row/required links, preserving concurrent updates. Resolve shared-file conflicts mechanically without deleting the
other work front. The parent is the sole writer of the canonical audit/backlog during this stack.

Existing broad wrapper tests and shared conftest/fixture files are read-only by default. Prefer new dedicated tests
under the appropriate existing test directory; record ownership before a necessary shared fixture change. This
prevents both agents editing generic installed-wrapper tests for unrelated reasons.

## Required contract

### Display and diagnostics

1. Prefer the dashboard for substantial interactive batches. Keep an editable dashboard setting and CLI override
   where exposed. A trivial standalone task does not need a dashboard merely to satisfy a standard.
2. Keep routine item updates in the display. Warnings, errors and the final result must remain legible and retained;
   progress updates must not indefinitely delay diagnostic delivery.
3. Preserve records required by configured sinks. In particular, a DEBUG file sink must still receive detailed worker
   records when the console is concise. Suppress routine console presentation at the sink/display boundary, not with
   an ERROR producer threshold. Retain severity, context and actionable exception information.
4. Give one parent process ownership of rendering. Workers send small serializable updates using existing transport
   where suitable; do not send Rich consoles, live renderables, non-picklable callbacks or large results solely for UI.
5. Keep redirected/unsupported-terminal output concise and free from repeated ANSI/cursor-control updates. Headless
   execution must not wait for input. Do not conflate terminal detection with the wrapper's pause setting.
6. Bound refresh frequency and visible rows. Coalesce routine updates if needed while preserving diagnostics and final
   states. Measure representative overhead with many small tasks; a benchmark framework is unnecessary.

### Optional metrics and truthful progress

- The renderer presents caller-supplied values; the workflow owns meaning, computation and cheap collection.
  It must be possible to omit all extra metrics or supply different measures without changing renderer code.
- Support meaningful labels and optional units/totals/metadata, including totals discovered later. Do not impose file
  counts, bytes, throughput, percentage or ETA on workflows where they are irrelevant.
- Distinguish absent/unavailable values, unknown totals and actual zero. Omit unavailable measures or display them as
  unknown. A workflow with no meaningful denominator may show phase, elapsed time and available counts.
- Keep aggregate measures separate from current-item metadata. Define which states advance each measure; success,
  skipped and failed work must not accidentally be reported as successful output.
- File size is a workload measure. If displayed, distinguish scheduled input size, known size of completed inputs,
  successful input size and measured bytes actually read. Only instrumented reads justify a bytes-read label.
  Retried inputs must not double-count unique completion. Unknown sizes must not become fabricated zero-byte inputs.
- Store byte values numerically and format with explicit, consistent units: MB is decimal and MiB is binary.
  Do not infer byte throughput or ETA from file-size snapshots.
- Reuse metadata already available to the workflow. Do not reread contents, scan extra data or ping cold/network
  storage merely to populate optional metrics.
- A reference and focused tests must demonstrate optional/alternative measures. Do not implement every possible
  future metric or mandate a complex editable schema in every script.

### Lifecycle, outcomes and compatibility

- Provide a common lifecycle for relevant queued/running/completed/skipped/failed work and explicit phase transitions.
  Keep the whole workflow active through aggregation/export; export failure cannot produce an overall success summary.
- Dashboard on/off and absent optional metrics must preserve selected inputs, ordering, results, retries, failure
  propagation, interruption and cleanup. Display configuration must not change processing policy.
- Make per-item outcomes and infrastructure failures distinguishable. Document partial results and cancellation;
  do not silently turn processing/transport failure into success. Workflows retain their own fallback policy.
- Preserve useful serial execution and existing multiprocessing choices. Keep workers importable under spawn and
  avoid import-time setup. No wrapper-specific garbage-collection rituals.
- Validate user-facing display settings at the appropriate boundary, including finite numeric values and valid ranges.
  Give actionable errors for invalid effective configuration; do not silently ignore supported options or disguise
  invalid settings through clamping. Preserve sensible existing defaults and compatible public call sites.
- Keep operational settings editable in scripts, with CLI precedence including explicit false/zero values.
  Preserve familiar setting names; DEFAULT_* naming, boilerplate comments and compulsory orchestrators are unnecessary.

## Implementation sequence

1. Confirm the latest parent branch, baseline and working-tree ownership. Read both handoffs and inspect the existing
   interfaces/callers. Record the small interface change needed by the reference and child.
2. Implement the shared contract, logging correction and one real log-summary reference. Define the outcome/error
   contract before changing consumers. Preserve the simulation renderer's public compatibility.
3. Add focused executable coverage for the risks below. Write the canonical dashboard guide and align the wrapper and
   logging standards with the verified behaviour.
4. Push a usable implementation commit, then publish the readiness record below in a subsequent commit. The named
   implementation SHA must already exist remotely; do not use a self-referential placeholder.
5. Keep the advertised interface stable while the child adopts it. Finish remaining foundation acceptance and record
   specific evidence. Coordinate necessary corrections through the records/PRs; no independent child API fork.
6. Update this record, the relevant register/index entries and the draft PR body. Keep the PR draft and leave #180 open.

## Consumer interface readiness

**State: READY_FOR_ADOPTION**

**Pushed, immutable implementation SHA:** `023253ed612bb0bbd9a61826f27f553c7b2e9447` on `feat/batch-dashboard-foundation-180`. This commit has both the preceding foundation head `b42edfc8f38a8ec6861aa45d21165ced0241f253` and current `main` `82c1a1b6e30573971ac5e1ac5fb64b41b20e9495` as parents (a normal merge). The implementation SHA was pushed and validated **before this readiness publication**. The child must verify that this exact SHA remains an ancestor of the remote parent head before merging it normally into PR #184. Do not cherry-pick or infer a future API.

### Actual callable/configuration contract

- From `ryan_library.functions.live_dashboard`: `LiveWorkflowDashboard(title, subtitle="", enabled=True, refresh_per_second=2.0, max_rows=25, columns=None, transient=True, screen=False)`; `WorkflowColumn`; `WorkflowMetric(label, value=None, unit="", total=None, metadata="")`; `validate_dashboard_settings(refresh_per_second=..., max_rows=...)`. Display settings reject non-finite/nonpositive refresh rates and nonpositive/noninteger rows; workflow-selected metrics can be absent, alternative, unknown (`None`), zero, or assigned later with an optional total. Do not label file-size metadata as measured bytes read; do not reread file contents to populate metrics.
- Renderer methods: `set_tasks(labels, metadata=None)`, `mark_running(index=..., ...)`, `mark_finished(index=..., status=..., ...)`, `set_phase(phase)`, `set_metrics(sequence)`, `finish(outcome=..., detail="")`. Per-item statuses are `QUEUED`, `RUNNING`, `OK`, `SKIP`, `FAIL`; final outcomes are `SUCCESS`, `SKIPPED`, `FAILED`, `INTERRUPTED`. The renderer is stateful, nested-context safe and bounds live refresh and visible/history rows. It does **not** own discovery, aggregation, export or workflow success policy. Preserve the same outer lifecycle through export and issue SUCCESS only after a completed export.
- From `ryan_library.functions.dashboard_workflow`: `run_dashboard_workflow_report(items, process_item, dashboard, pool_size, status_for_result, detail_for_result, log_queue=None, worker_log_level=None, max_start_events=None, poll_interval=0.1, on_outcome=None)` (keyword arguments) returns `WorkflowRunReport[TResult]` with `outcomes: tuple[WorkflowItemOutcome[TResult], ...]`. The existing `run_dashboard_workflow(...)` accepts the same parameters and retains its legacy `list[TResult]` return.
- `WorkflowItemOutcome`: one-based `index`, terminal `status`, `result`, `has_result`, `error`. `has_result=True` includes a legitimate `None` result, and domain SKIP/FAIL returns remain distinguishable from errors. `WorkflowRunReport.results` includes all returned values in input order; `.failures` selects FAIL outcomes. Outcomes are ordered by input index, even if parallel completion and parent-only `on_outcome` callback order differ.
- Workers must receive importable/picklable callables and return domain results, not dashboard instances or giant UI objects. One parent process owns rendering. Serial mode is `pool_size=1`, pool mode is `pool_size>1`; spawned workers send bounded, nonblocking cosmetic start events. Missing start events do not change result accounting. `on_outcome` executes once per completed item in the parent, may update cheap metrics, and is not a retry hook.
- `run_dashboard_workflow_report` represents normal worker item exceptions as FAIL outcomes; the legacy list API re-raises unexpected item exceptions rather than hiding them. Crucially, pool `MaybeEncodingError` and transport `OSError` obtained from `ApplyResult.get()` propagate even from the **report API**, preserving consumer-owned serial fallback. Invalid status/detail adapters, callback faults, pool infrastructure faults and interruptions propagate; the helper never silently chooses a retry policy. The caller decides full-list fallback and keeps per-input unique progress accounting. Pool/queue/dashboard cleanup runs on normal and exceptional exits. A report with failures must not be described as an overall success or used as evidence that an export completed.
- Use `setup_logger(console_log_level=..., log_file=..., file_log_level=...)`. In dashboard mode choose a concise default console sink (normally SUCCESS), while configured DEBUG file sinks still receive worker DEBUG/INFO/WARNING/ERROR and exception tracebacks through the existing Loguru queue. The `worker_log_level=None` default captures the least restrictive required sink; a stricter override that would discard sink-required records is rejected. Console and file sinks filter independently; do not force workers to ERROR to quiet the display. With `enabled=False` use ordinary logging; redirected output has one plain final summary, not repeated ANSI frames.

### Working reference, evidence and compatibility limits

- The working reference is `ryan_library.orchestrators.tuflow.tuflow_logsummary` and `ryan-scripts/TUFLOW-python/log_processing/create_log_summary_report.py` **at the SHA above**. The same dashboard remains active through discovery, file processing, aggregation and export. Partial result failure and export failure are not overall SUCCESS; the wrapper propagates controlled failure exits, and ordinary per-file INFO completions are not replayed as dashboard clutter.
- **Hosted Windows Python 3.14.10, 2026-11-01:** [Python CI run #1658](https://github.com/Chain-Frost/ryan-tools/actions/runs/41295863941), exact implementation SHA above: **1,988 passed, 14 skipped, 8 deselected, 128 warnings, 0 failed** from the hosted test boundary `python -m pytest tests` with the workflow's explicit GDAL/promoted-utilities exclusions and pytest flags in `.github/workflows/ci.yml`. This includes `tests/functions/test_dashboard_contract_180.py`, `tests/orchestrators/tuflow/test_logsummary_dashboard_contract_180.py` and the **actual Windows-spawned worker harness**, including 10,000 small tasks, worker DEBUG/WARNING/ERROR, interrupted callback, real exported result parity and transport fault signalling.
- The same run's **Policy** job passed Ruff check/format and strict Pyright **0 errors, 0 warnings, 0 informations** on changed Python files, `python repo-scripts/check_documentation.py` (seven documents and central index), explicit MCP link checks, and `python repo-scripts/check_loguru_formatting.py`. **Package** passed a fresh wheel build at existing version `26.11.1.1`, wheel verification and `python repo-scripts/smoke_dashboard_installed_wheel.py --expected-root <installed-wheel-root>`. Its log verifies a **copied wrapper outside checkout**, installed source origin, dashboard on/off, workbook output, DEBUG, warnings and controlled failure. MCP smoke and POSIX security also passed. [Fast PR check run #1292](https://github.com/Chain-Frost/ryan-tools/actions/runs/41295864184) passed.
- Previous hosted failure was **three test-only mistakes**: two mocks used a parameter named `path` instead of the production keyword `logfile`, and the 10,000-task test expected a SUCCESS record on a WARNING-level console. Fixed at `b42edfc8f38a8ec6861aa45d21165ced0241f253`; all three pass on the merged implementation SHA. The earlier adapter exception findings were corrected, and the P1 large-batch queue hang was addressed with a bounded, nonblocking progress queue plus detachable worker feeder. The Codex P1 thread was answered and resolved after green CI.
- Limits: these are real **hosted/native Windows** Python 3.14 tests with synthetic data, not licensed TUFLOW numerical/engineering acceptance, not mapped-drive/UNC/locked-network-file acceptance and not a production workload. No release, PyPI publication, licensed solver execution, destructive data operation or broader audit closure is claimed. The independent six-wrapper adaptation and its domain regression/installed-copy checks remain wholly child-owned.

**Interface stability commitment:** Keep these callable, outcome, diagnostic and optional-metric contracts stable while PR #184 adopts the foundation. A breaking correction must withdraw/supersede this readiness with a new pushed SHA and explicit coordination. Child verification of ancestor and smoke of the actual merged API is still mandatory.

## Completion criteria and validation

Use Python 3.14 and repository validation guidance. Tests below express requested behaviour; avoid unrelated suites.

- Focused renderer/adapter tests cover optional and alternative metrics, no extra metrics, unknown/zero/later totals,
  bounded updates, terminal fallback, item states and honest multi-phase completion.
- Reference tests cover success, unfinished/skipped logs, item failure, infrastructure failure, export failure and
  enabled/disabled output parity. Preserve existing report values, ordering and output naming.
- Verify warning/error visibility during live rendering and persistence afterward; requested DEBUG file logging must
  retain worker records. Verify ordinary logging remains useful with the dashboard off.
- Exercise genuine spawned workers, serial execution, queue/lifecycle cleanup and controlled interruption. A mock pool
  alone does not prove multiprocessing behaviour. Record native Windows Python 3.14 smoke separately from other hosts.
- Verify existing shared-renderer callers remain compatible with focused checks; do not launch licensed simulations.
- Run Ruff format/lint, strict Pyright only on modified Python files, compilation, relevant help/default/override checks,
  Loguru formatting checks when its calls change, and documentation checks.
- Rebuild the package with `python repo-scripts/build_library.py --no-bump --skip-pip` for verification when dependencies
  are already installed. Validate a copied log-summary wrapper outside the checkout against that build with disposable
  log fixtures, including a useful output and a controlled failure. Record actual module origins.
- Record exact commands, date, environment and pass/fail/not-run results. Keep native or installed-package gates
  explicit if unavailable; do not equate generic CI or synthetic report parity with engineering acceptance.

## Exclusions and coordination

Do not inspect or traverse the unsorted submodule, run licensed solvers, retrieve expensive storage or operate on
production data. Preserve accepted TUFLOW runner settings.OP.RUN defaults and generated-selection trimming from
merged #159; this work does not redesign simulation scheduling, GPU selection, CSV continuation or command planning.

Keep MCP catalogue changes out of this stack while [#182](https://github.com/Chain-Frost/ryan-tools/pull/182) owns
discovery work; record any necessary compatibility follow-up. Do not reopen accepted HY-8 evidence or unrelated
deferred audit items. The existing inventory counts are source/resource counts, not wrapper compliance evidence.

## Next action



Review the consolidated [PR #183](https://github.com/Chain-Frost/ryan-tools/pull/183)
against main before any independently authorised main merge. PR #184 and PR #186 have
already been merged into this feature branch; there is no remaining child-branch
integration step. Issue #180 remains open for the broader audit.

## Validation and delivery

The complete evidence and remaining environment-specific limits are in the READY_FOR_ADOPTION record above. At implementation head `023253ed612bb0bbd9a61826f27f553c7b2e9447`, the repository's six required CI checks are green, including Windows Python 3.14.10 spawned-worker regression and copied installed-wheel smoke. All source changes, the normal main merge and the test corrections were pushed to the existing parent branch, with no force push. This readiness/documentation commit follows the already validated implementation SHA; any new checks on documentation-only commits should be reported separately rather than silently assumed green. Parent PR #183 remains draft and unmerged, and no release/auto-merge is enabled.

## Progress

### 2026-11-01

Agreed a dashboard-first standard for substantial batches, preserved diagnostic capture, workflow-selected optional
metrics and a concrete implementation gate for concurrent child adoption. Seeded the two handoffs in the parent so
their relative links exist from the start; after branching, each agent owns only its own status record.

### 2026-11-01 — foundation recovery, merge and readiness

Recovered the existing foundation after the prior agent stopped. Verified adapter transport/callback fixes and the P1 bounded-queue correction; fixed three Windows test assertions in `b42edfc`. Merged main normally with two-parent commit `023253ed` preserving both the dashboard and independent MCP changes. Six CI checks passed at the implementation SHA, including 1,988 hosted tests, strict changed-file Pyright, real spawned-worker coverage and a copied installed-wheel log-summary smoke. Replied to and resolved the outstanding Codex thread. Published READY_FOR_ADOPTION with the exact code SHA and stable interface contract for child #184; neither PR was merged to main.


### 2026-11-01 — stack consolidated into foundation feature branch

The user authorised merging child PRs #186 and #184 into #183, without touching main.
PR #186 passed all six Windows CI checks and current Codex review with no findings
at `0a8f8ab`; its branch was reconciled with the current #184 branch using a normal
two-parent merge, then non-forced fast-forwarded into #184. GitHub closed #186 as merged.

The #184 reviewer found one PO location-filter classification problem: valid input
with zero selected locations was reported FAIL. A dedicated filtered-empty outcome
now reports SKIP without admitting empty results to the collection or masking
malformed data, and retains raw-DataFrame disposal. The fix was pushed at
`8ac1f0f`, with Ruff spacing correction at `beb5a2c`; the follow-up Codex review
found no new issues and the original review thread was resolved.
The [Windows CI run](https://github.com/Chain-Frost/ryan-tools/actions/runs/41332053247)
passed all six required checks at `beb5a2c`, including **2,013 passed,
14 skipped, 8 deselected**, strict typing, and copied installed-wheel smoke.
PR #184 was then non-forced fast-forwarded into #183; GitHub closed #184 as merged.

No main merge, auto-merge, PyPI publication, licensed solver execution or production
model/UNC acceptance occurred. This documentation-only record update follows
the tested source SHA and requires its own fresh CI receipt.


### 2026-11-01 — consolidated review follow-up for RLL_Qmx filter misses

The consolidated Codex review identified the same valid-but-filtered-empty
classification issue in `RLLQmxProcessor` that was previously fixed for PO.
This source follow-up adds an explicit filtered-empty SKIP only after a
successful read and a non-empty valid required-column reshape. Malformed
RLL_Qmx inputs remain FAIL; exported collection contents remain unchanged.
Focused tests cover mixed SKIP/OK accounting and malformed input. Updated
Windows CI and review confirmation are required before this follow-up is
called accepted. No main merge or publication is authorised by this update.

### 2026-11-01 — phase-transition visibility review follow-up

The consolidated review also identified that a short phase change could be
lost to routine dashboard refresh throttling just before a blocking export.
`set_phase` now forces a render, while ordinary item and metric updates retain
the configured throttle. A focused regression asserts this separation.
Fresh Windows CI and review evidence remain required before final acceptance.


## 2026-11-04 — broader issue #180 audit continuation

The user resumed the all-wrapper/script audit on #183 and requested separate
issues for broader orchestrator/processor defects and appropriate dashboard use.
The checkout began clean on `feat/batch-dashboard-foundation-180`, matching
published PR head `6d1178ce9e0c282b107ee925027c7c848e75cd0d`. Earlier pending
RLL_Qmx/phase fixes are already in that baseline, not newly pending work.

### Current result

- Eleven TUFLOW entry points have bounded local fixes: six result dashboard
  wrappers now expand home paths before joining the working directory and
  convert configuration failures to exit 1; the log summary also expands home
  paths; three PO analysis wrappers no longer apply relative working directories
  twice and expose editable pausing; PO label export uses a selected input,
  read-only/closed SQLite connection and exclusive output creation.
- Preserve processing algorithms, dashboard defaults and existing shared APIs.
  No library/orchestrator changes are mixed into these wrapper fixes.
- Seven independently reviewable issues were raised: [#194](https://github.com/Chain-Frost/ryan-tools/issues/194)
  export failure/collisions, [#195](https://github.com/Chain-Frost/ryan-tools/issues/195)
  discovery outcomes, [#196](https://github.com/Chain-Frost/ryan-tools/issues/196)
  master append, [#197](https://github.com/Chain-Frost/ryan-tools/issues/197)
  archive publication, [#198](https://github.com/Chain-Frost/ryan-tools/issues/198)
  remaining result dashboards, [#199](https://github.com/Chain-Frost/ryan-tools/issues/199)
  Office containment/publication and [#200](https://github.com/Chain-Frost/ryan-tools/issues/200)
  cutline safety/CRS. They are for separate stacked PRs, not fixed here.
- Existing inventory and AB backlog remain authoritative. All 151 tracked entries
  reconcile (85 wheel files, 66 excluded, 34 MCP targets). No lifecycle or MCP
  membership changes; AB-008 stays deferred and AB-023 stays closed.
- Disposable probes reproduce swallowed Parquet PermissionError (normal return,
  zero outputs), missing-root empty discovery, archive failure deleting an existing
  sentinel, and Office worksheet traversal modifying an external sentinel while
  reporting success. No production files, licensed solver or real Office document
  was used. Cutline CRS behavior is source-reviewed, not native validated.

### Validation and delivery

Windows, normal Python 3.14.6, 2026-11-04:

- Focused wrapper checks: **105 passed in 38.16 seconds** across result wrappers,
  PO label safety, generic wrappers, copied behavior/help, GDAL wrapper configuration,
  editable defaults, archive folder wrapper, file inventory and RORB hydrographs.
- `python -m ruff format`, `python -m ruff check` and strict `python -m pyright`
  on the 13 modified/new Python files: pass; Pyright 0 errors/warnings.
- A final PO-label follow-up preserves UNC spelling in SQLite read-only URIs
  without requiring URI-authority support: 5 focused label checks pass (15
  unrelated tests deselected). The UNC test intercepts connect before network
  access; it does not claim a live share test. Final label help/version also pass.
- All 74 wheel-selected Python entry points compile (excluding __init__ resources);
  all 66 guarded argparse entry points pass --help subprocess checks against the
  final rebuilt/installed wheel. Eight non-argparse entry points are not executed.
  Per-entry evidence is retained in the existing inventory.
- Default documentation/index checks and explicit links-only checks pass.
- `python repo-scripts/audit_installed_scripts.py --check` passes. Local detailed
  artifacts are under ignored `.pytest_tmp/issue180/`.
- Initial copied checks had 8 failures because the normal user installation was
  stale `26.10.21.2`; matching-package rerun passed. Rebuilt with
  `python repo-scripts/build_library.py --no-bump --skip-pip`, verified and installed
  `26.11.1.1` using `pip install --no-deps --force-reinstall`.
  Final wheel SHA-256: `a3d2999c021f3c255b9c9f99147690095c8ad52368c355bbe4f6b0c202d00948`.
  Package metadata/version unchanged; wheel is ignored under `dist`.
- All source/documentation changes remain **unstaged and uncommitted**. No push,
  merge, release or current-head hosted CI was performed for these local edits.
  Historical hosted counts are not evidence for this new patch.
- Native licensed TUFLOW, production/UNC, native GDAL spatial outputs and Office
  fidelity remain unverified where relevant. The broad audit is not complete.

### Next action

Review the local wrapper patch and current per-entry evidence, then continue
AB-032 effective-setting/CLI checks for the remaining maintained scripts.
Prioritise separate #199/#197 safety fixes, then #194/#195 report outcomes;
stack #196/#198 dashboard adoption on explicit shared outcome contracts.
Preserve manual delivery/merge boundaries. Next review: 2026-11-05.

Unrelated register fronts still have overdue review dates (vendor integration,
installed scripts, release automation, floodway exclusions, GeoTIFF recompression
and shared verification). Reading their dates did not authorize their remediation.

### GitHub continuation

The user requested GitHub delivery and further work. Publish the validated wrapper
audit patch to #183, then address #199 in a separate branch stacked on that head.
The local-only delivery statements above describe the preceding audit milestone.
No merge or release is authorized. Hosted checks must be verified on the new heads.


### Current-main reconciliation

Audit commit 50cf9c6e is published on #183. GitHub reported merge conflicts,
which prevent pull-request checks from starting. Reconciled main ec873425 in a
normal merge, preserving WorkflowMetric/phase/outcome support and WorkflowAlert,
live stdout routing, current release smoke dependencies plus the required MCP
SDK, current main inventory/lifecycle decisions and both sets of work records.
No changes were merged into main.

Validation: 85 dashboard/logging/wrapper checks plus 165 integration/release
checks pass (2 skips); strict Pyright for the merged dashboard/logging files,
Ruff, docs and Loguru policy pass. No-bump build 26.11.3.9 verifies; SHA-256
1ab2a0fa72aff3513cc1bf49c00bfb32739584faf242137ce33931b7357b6781.
Current static inventory is 156 tracked, 83 selected, 33 MCP targets. The earlier
151/85/34 and per-entry CLI evidence remain explicitly historical. Hosted
acceptance must now run on the merge head. Office issue #199 is implemented in
a separate child branch awaiting this parent integration.

### Export safety continuation

Parent #183 now passes all six GitHub checks at `94a63b89`; its review threads
are resolved. Office PR #201 independently passes all six checks at `2c06d8d4`
(2,120 hosted passes, 14 skips, 8 deselected). Both remain unmerged.
The next selected work is [shared export safety #194](2026-11-04-result-export-safety-194.md),
stacked directly on #183. Discovery #195 and later dashboard adoption remain
separate follow-ups. Earlier pending/local-only statements above are historical.


### 2026-11-07 — Current runner and export integration

The maintainer requested continuation of WP08 in issue #240. A live recheck found
that #202 had already been squash-merged into #183 as `a56e0c6b`; its exporter
implementation is retained. Current main `e317e4ef` is integrated by a normal
merge, preserving the main PyTuflow pin and recent runner progress, grouping,
GPU ownership and terminal rendering fixes.

The dashboard lifecycle combines nested processing/export contexts and plain
redirected output with main's console transaction lock and single refresh owner.
Integration checks also preserve runner console colours through the default
stdout sink, omit phase text for callers that do not supply it, and render serial
starts/final batch counts before a blocking stage can leave stale rows visible.

Local validation on Linux Python 3.14.10, with spawn explicitly selected to match
the supported Windows process model: 84 dashboard/logging/workflow checks pass;
380 runner/GPU/progress/result-pipeline checks pass, with 12 platform-specific
skips. Ruff format/lint and strict Pyright on the resolved integration files pass.
The earlier local attempt used an incomplete subprocess dependency path and the
Linux forkserver default, which cannot create Unix sockets in this executor;
those environment failures were resolved in the task environment without
changing product behavior. Native Windows acceptance remains the hosted gate.

Next: complete the #194 closure raw-output batch, verify the installed wheel and
updated hosted head, request a fresh Codex review, and refresh #201 from the
settled parent. #196/#198 remain separate subsequent changes; broader audit #180
stays open. The merge is local at this checkpoint; no main merge or release.


### 2026-11-07 — Closure gap and installed-package acceptance

Commit `8878d42b` adds typed raw-table sidecars to the existing shared exporter.
The closure workflow completes its summary before preparing any outputs; raw
CSV/gzip-Parquet and selected formatted reports then share one destination
preflight, staging and publication batch. Existing names, values, Parquet index
behavior and legacy raw outputs in all three modes are preserved. The wrapper,
CLI help, exporter guide and MCP catalogue state that `export_mode` selects the
formatted reports. Numerical closure-population issue #215 is unchanged.

Validation in the same Python 3.14.10 task environment:

- 99 exporter, closure, failure-propagation and result-wrapper tests pass on the
  integrated tree. The closure implementation's separate focused selection also
  passed 82 tests; these selections overlap and must not be added together.
- 125 MCP discovery/catalogue/overview and inventory tests pass (one existing
  pandas deprecation warning). Static inventory remains 156 tracked scripts,
  with no audit issues.
- Strict Pyright for all 47 changed Python files, targeting Windows: zero errors
  or warnings. Ruff 0.16.6 format/lint, docs and Loguru checks pass.
- Normal build selects `26.11.7.1`; wheel verification passes. SHA-256:
  `0b98658cc90ec08b2a56126d0c12425bda85f726f575f901a0733eb7553a930e`.
- The isolated installed wheel passes the copied log-summary check and all six
  copied culvert/PO/POMM dashboard on/off, workbook-parity, collision and writer
  failure checks. Source checkout imports are excluded.
- Six additional installed closure-wrapper cases pass: Excel, Parquet and both
  successes with raw value readback; raw collision, report collision and CSV
  writer failure with exit 1, prior-file preservation and no staging residue.
- Independent source review found no consequential closure-publication issue.
  This is separate from the fresh hosted Codex review requested after pushing.

The existing #183 branch carries the integrated source and validation records.
Fresh hosted checks and review are pending on its new head; the older 94a63b89
and 6d1178ce results do not establish current-head acceptance. Office #201 has
passed local source, installed-package and inventory validation for its normal
parent refresh; its hosted checks and review are also pending. Native Office fidelity and real UNC hard-link support
remain separate acceptance. No main merge, auto-merge or release is performed.


### 2026-11-07 — Hosted review follow-up

The first published integration `ca788bf4` passed all six hosted checks:
2,368 tests passed, 14 skipped and eight deselected; separate POSIX checks had
25 passes and two skips. Policy, MCP and installed-package smoke checks passed.
Office `811b936b` also passed all six checks (2,390 hosted passes, 14 skips, eight
deselected) and its Codex review found no major issues. These results identify
the earlier heads and do not replace acceptance of this subsequent correction.

Fresh Codex review identified the separate TUFLOW serial adapter in
`tuflow_common.py`: unlike the generic execution helper, it still used a
throttled refresh before a blocking read. Commit `26d6e223` forces the RUNNING
frame before each read and flushes final counters before returning to the outer
dashboard. Both small batches and recoverable pool fallback use this adapter.
Other direct task-start callers (the simulation runner and raster batch) already
force their refreshes.

Both new real-render regressions failed before the fix and pass afterward.
The focused pipeline/common/coverage selection has 24 passes. Ruff 0.16.6,
formatting, strict changed-file Pyright and independent review pass. The two
new rendering cases also pass against the isolated installed wheel, with source
checkout imports excluded. All six copied result-wrapper on/off, workbook parity,
collision and writer-failure checks pass again.

A no-bump verification rebuild retains version `26.11.7.1`; current verification
wheel SHA-256 is
`f114a3ba7e27349815407e9113cd9615e782b21f72c79c8fea6688e10dc2de72`.
The earlier hash above belongs to the first integration build.

Publication preserves normal ancestry and refreshes Office #201 from the corrected
parent. The [current #183 checks and review](https://github.com/Chain-Frost/ryan-tools/pull/183)
and [Office #201 checks and review](https://github.com/Chain-Frost/ryan-tools/pull/201)
record hosted acceptance of the final published heads. No main merge, auto-merge
or release is performed; native Office and real UNC acceptance remain separate.


### 2026-11-07 — Extract the complete runner session

The user requested a smaller copied runner after confirming that older copied
wrappers may be replaced with the current maintained wrapper. No old-callback
colour compatibility layer is added.

`simulation_workflow.py` now owns plan preparation, optional-file diagnostics,
export/preflight dispatch, CSV continuation, advisory lifetime and completion
callback wiring. Its typed `WorkflowControls` rejects invalid operation,
continuation and boolean values before work; `WorkflowOutcome` reports the
completed operation, output path, strict check result, batch and skipped count.
The existing `RunnerSettings` remains the single normalized run configuration.

The wrapper is 408 lines, down from 570 on the previous #183 head (571 on
current main). It retains all editable settings, CLI normalization, logging
configuration and dependency forwarding, launch-only SIGINT handling, final
reporting, follow-up execution, banners and pause. TXT/CSV locations derive from
the actual supplying wrapper. All/Resume validation and history ordering,
full-plan dashboard columns, failed-batch chaining and interrupted-batch policy
remain unchanged. Advisory cancellation and history release finish before the
workflow returns; slow optional input I/O is not joined during cleanup.

Independent review found no consequential regression. The original settings,
signal context, follow-up launcher, CLI parser and CLI cleanup are AST-identical;
all 28 original top-level constants are retained except the wrapper revision.
The renderer, scheduler, progress modules, logging helpers and history
implementation have no diff. Existing behavior assertions remain, with mocks
moved to the new workflow owner rather than adding compatibility imports.

Local Python 3.14.9 validation on 2026-11-07 uses the same task dependencies and
explicit spawn selection as earlier checks:

- 211 runner, launch-log, history, renderer and logging tests passed; ten
  existing platform-specific skips.
- 100 copied-wrapper, CSV continuation and advisory-runner tests passed; one
  existing Windows-native command-export skip. Two new real `_cli_main` cases
  prove advisory cancellation, actual CSV lock release, SIGINT/dependency
  logger restoration and Loguru sink closure before pause after success and
  after a launcher exception following START.
- Four new direct-library workflow tests passed: forced export-only,
  strict preflight success/failure and resumable execution outside the main
  thread, with no process signal changes and no source-wrapper dependency.
- Ruff 0.16.6, strict changed-file Pyright targeting Windows, compilation,
  `--help`, documentation and Loguru checks passed.

Normal `repo-scripts/build_library.py --skip-pip` selected `26.11.7.2`; the
verification rebuild uses `--no-bump`. Installed-wheel and published-head
acceptance are recorded below and in the current PR checks/review. Native
licensed Windows acceptance is retained from the earlier runner work; it is
not repeated for this extraction.

MCP discovery assessment: the existing `run_tuflow_batch` catalogue entry
continues to expose the unchanged maintained CLI and its explicit external
execution policy. The new Python coordinator is documented in the TUFLOW README;
it does not need a duplicate MCP tool or package CLI.

Installed acceptance uses the verified `26.11.7.2` wheel, SHA-256
`2f7d25fd012bd9a56c440adf0153459885806058e2890e046718b0ce56f887da`.
The isolated package and copied installed wrapper run outside the checkout,
with user site disabled and installed module paths verified. Nine scenarios
across ten subprocess invocations passed: help, solver-free export with no CSV,
completed Resume without a solver and unchanged history, missing-solver All
without RESET, two failed-child Resume attempts with durable failure records,
and invalid operation/continuation/export/check settings without output writes.
The four direct workflow cases also passed against that installed package.

The refactor is delivered on #183 using normal history; current-head hosted
CI and Codex review are tracked in the PR. Office #201 receives a normal
parent refresh with its implementation and tests unchanged. No main merge,
auto-merge or release is part of this continuation.
