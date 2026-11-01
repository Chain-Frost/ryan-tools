# Batch dashboard foundation and wrapper contract

| Field | Value |
| --- | --- |
| Status | Needs final review — #184 and #186 consolidated into PR #183; main unchanged |
| Owner | ChatGPT (foundation recovery) |
| Created | 2026-11-01 |
| Updated | 2026-11-01 |
| Next review | 2026-11-08 |
| Baseline | main at `99cd3ccaaabd83b682392f7e285d90b570d353cb` (merged PR #159) |
| Branch / base | `feat/batch-dashboard-foundation-180` -> `main` |
| Issue | [#180](https://github.com/Chain-Frost/ryan-tools/issues/180), bounded dashboard work |
| Dependent work | [TUFLOW result dashboard adoption](2026-11-01-tuflow-result-dashboards-180.md) |
| Delivery | Foundation implemented at `023253ed`; consolidated dashboard/PO/MCP code `beb5a2c5526aed3229daa4a4bcee3e7995a7a7c0` passed six Windows checks (2,013 hosted tests). PR #183 is open and unmerged into main. |

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

