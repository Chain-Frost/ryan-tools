# Batch dashboard foundation and wrapper contract

| Field | Value |
| --- | --- |
| Status | Planned; implementation has not started |
| Owner | Unassigned; foundation agent when started |
| Created | 2026-11-01 |
| Updated | 2026-11-01 |
| Next review | 2026-11-08 |
| Baseline | main at `99cd3ccaaabd83b682392f7e285d90b570d353cb` (merged PR #159) |
| Branch / base | `feat/batch-dashboard-foundation-180` -> `main` |
| Issue | [#180](https://github.com/Chain-Frost/ryan-tools/issues/180), bounded dashboard work |
| Dependent work | [TUFLOW result dashboard adoption](2026-11-01-tuflow-result-dashboards-180.md) |
| Delivery | Documentation-only seed for a draft PR; runtime implementation and acceptance remain outstanding |

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

**State: NOT_READY**

The documentation seed does not satisfy this gate. PR creation, a proposed signature, a stub implementation or elapsed
time does not make the interface ready. Change this section to **READY_FOR_ADOPTION** only with all of:

- Immutable, pushed implementation commit SHA and branch.
- Actual public imports/signatures and configuration, worker/update, optional-metric and diagnostic contracts.
- Return/outcome, ordering, partial-failure, infrastructure-error, interruption and cleanup semantics.
- Working log-summary reference code at that SHA.
- Exact focused commands, environment and results demonstrating the implemented interface, warning/error preservation,
  optional/unknown metrics, enabled/disabled parity and a real spawned-worker smoke. Clearly list acceptance still
  pending; unavailable native Windows evidence is not a pass.
- Relevant compatibility limits and an explicit promise to keep the interface stable during child adoption.

The child verifies that the implementation commit is an ancestor of the remote parent head, merges that parent
normally, checks the actual merged API and records the adopted SHA in its own record. A required breaking correction
must explicitly withdraw or supersede readiness and name a new pushed implementation SHA. Compatible fixes still
need an updated record when they affect the child's assumptions.

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

Start the foundation implementation on the named parent branch, inspect the existing renderer/adapter/logging
contracts, and record the minimal concrete interface decision. The child may perform documentation-only scoping now.

## Validation and delivery

The setup contains no runtime changes. Documentation validation and the pushed seed/PR references are recorded in the
draft PR body. Implementation, runtime tests, Windows smoke and installed-wheel acceptance have not been performed
for this work front. Keep these distinct from historical evidence in other records.

## Progress

### 2026-11-01

Agreed a dashboard-first standard for substantial batches, preserved diagnostic capture, workflow-selected optional
metrics and a concrete implementation gate for concurrent child adoption. Seeded the two handoffs in the parent so
their relative links exist from the start; after branching, each agent owns only its own status record.
