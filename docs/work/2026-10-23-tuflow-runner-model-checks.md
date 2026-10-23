# TUFLOW runner: background model checks and GPU warnings

| Field | Value |
| --- | --- |
| Status | Planned; scoping draft, runner implementation not started |
| Owner | Unassigned |
| Created / Updated | 2026-10-23 |
| Next review | 2026-10-30 |
| Pull request | [ryan-tools #159](https://github.com/Chain-Frost/ryan-tools/pull/159) |
| Branch | `feat/tuflow-runner-model-checks` |
| PR base | `test/native-graph-benchmark-controls`, the branch for [ryan-tools #158](https://github.com/Chain-Frost/ryan-tools/pull/158) |
| Initial parent | `f4a214a4cd6260098c939ae6beb59a9ec6ef0945` |
| Parent integration | `9cd839247c430dbd9a2ec4a7e2187f1e80cea91d` from #158, refreshed after its diagnostic implementation landed on that draft |
| Merged consumer baseline | `bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2` (PR #143, including #156) |
| Merged parser baseline | `e162c04330d24c37bee3ade94479cc4602651060` (PyTUFLOW #12, including #13) |
| Next action | Define worker/reporting/shutdown details, then implement the agreed background advisory policy and focused tests in this draft |

## Outcome and delivery boundary

Improve [run_tuflow_simulations.py](../../ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py)
so model checking runs alongside the requested simulations and reports useful
information without delaying their launch or stopping the batch. Establish this
branch as the foundation for later, separately scoped runner improvements.

The user requested this additional PR and further scoping, then explicitly
selected background, warning-only model checking and non-blocking GPU warnings.
**This draft currently changes documentation only.** It contains no new Python
behaviour, dependency pin or package version. The user decisions below are
requirements for implementation, not a description of delivered behaviour.
Keep the PR draft while implementation and acceptance remain incomplete.

## Agreed user requirements

The user's 2026-10-23 follow-up supersedes the initial draft's proposed strict
whole-batch launch gate for normal runner operation.

| Situation | Required normal-runner behaviour |
| --- | --- |
| Checking takes time | Run one check of the full final run set in a separate thread; begin the TUFLOW batch without waiting for the result. |
| Complete check passes | Emit an INFO message that all required TUFLOW input files for the checked run set are present, with useful run/file counts. |
| Files are missing | Print all warnings with affected-run/source information; keep running and queued simulations proceeding normally. |
| References are unresolved, the requested checking API is unsupported, or the checker fails | Warn that checking is incomplete or failed. Continue the simulation batch and never emit a false all-files-present message. |
| Selected GPU is used by another live TUFLOW process | Warn immediately before that launch; do not stop, postpone or reassign the run because of external occupancy. |
| Pause/restart functionality | Separate future PRs; no new pause, retry, restart or resume behaviour in this PR. |

Do not truncate the findings to the current first 20 issues. Retain the detailed
report and every finding's provenance. A failed check remains a failed check in
the library's report; the runner's advisory policy is what allows launching to
continue. There is no permission to mark incomplete input validation as passed.

These requirements concern advisory model/GPU findings. They do not remove
existing launch-configuration validation or the runner's own configured
concurrency/GPU-slot limits, and do not require launching an empty plan.

## Current behaviour and concrete changes needed

At the recorded baseline the runner builds simulations from parameter products,
text files or both, deduplicates the final list, preserves its established order
and assigns final run indices. It optionally exports commands, then converts
that same list into public `RunRequest` values and makes one synchronous
`check_simulation_inputs()` call. A failed report currently raises before launch.
Only the first 20 findings are logged. These are the behaviours this PR must
change for normal advisory checking.

Checking is currently opt-in: `preflight_check` defaults to `False`, and
`--preflight` / `--preflight-only` enable it. The runner never passes
`context_mode`, so the checker uses its default `"individual"` collection.
The public consumer also supports explicit `"batch"` collection, and already
shares physical inventory work across the whole run set. It materialises
wildcard, database, GIS attribute and table-link references, checks raster
metadata, and preserves per-run findings.

The consumer API has no `"auto"` mode, graph switch or model-factory parameter.
Explicit batch mode refuses unavailable or incomplete parser APIs. In the
proposed normal runner, that refusal must become an accurate advisory warning,
not a launch failure or an automatic claim of successful fallback.

`--preflight-only` currently returns before the scheduler and post-run script,
but still validates the TUFLOW executable, GPU and priority settings through
parameter construction. Retaining a useful, explicit check-only workflow and
separating its prerequisites from launch prerequisites remain scoping details.
The runner remains Windows-only.

### GPU warning behaviour already exists

The runner's `warn_gpu_sharing=True` setting is already the default.
`launch_simulations()` checks the actual selected `-puN` values after GPU
assignment and immediately before starting each process. It uses the maintained
[gpu_occupancy helpers](../../ryan_library/functions/tuflow/gpu_occupancy.py) to
inspect live local TUFLOW processes, alongside runs owned by this launcher.

Confirmed overlaps warn with GPU/PID information. Unknown GPU assignments or
incomplete process inspection produce uncertainty warnings. External occupancy
does not reserve a slot or block a launch. Preserve and verify this behaviour;
do not add a competing process scanner, infer occupancy from stale TLF records,
or report an uninspectable GPU as free. Retain the runner's existing internal
slot/concurrency scheduling.

See the [current preflight guide](../TUFLOW_INPUT_PREFLIGHT.md) for delivered
checking limits and GPU-warning behaviour; it must be updated with the eventual
advisory-runner implementation.

## Stack and dependency plan

| Work | Relationship to this draft |
| --- | --- |
| ryan-tools #143 / #156 | Completed runtime foundation, already in `main`; do not reopen their scope. |
| [ryan-tools #158](https://github.com/Chain-Frost/ryan-tools/pull/158) | Git parent and acceptance/evidence dependency for collection-mode performance. The runner must not import repository test tooling. |
| [This runner draft, #159](https://github.com/Chain-Frost/ryan-tools/pull/159) | Background advisory checking, complete result reporting and verification of existing non-blocking GPU warnings. |
| Further runner PRs | Branch from this runner branch when they depend on its contract; pause/restart and other lifecycle improvements get separate scopes. |
| [PyTUFLOW #14](https://github.com/phantom-branch/PyTuflow/pull/14) / [#15](https://github.com/phantom-branch/PyTuflow/pull/15) | Independent parser optimisations; neither is a Git parent or an unconditional prerequisite for basic runner integration. |

The public individual/batch API already exists on `main`. Stacking on #158
records validation and merge order, not a runtime dependency on diagnostics.
Do not stack this work on unrelated packaging, QGIS, RORB or raster PRs.

After #158 merges, retarget this PR to `main` and carry forward only its own
changes, accounting for a possible squash merge. Refresh child branches against
the resulting runner head and inspect each diff against its immediate parent.
Do not fold unfinished child work into a parent merge. Keep dependency order,
review status and exact tested revisions in each PR body.

## First implementation slice

1. **Capture the exact final plan before launching.** Build an independent
   snapshot of the ordered, deduplicated `RunRequest` values and copied flag
   mappings before the scheduler mutates `Simulation` GPU/process state.
   Resolve ordinary paths against their intended base using `Path.absolute()`.
   The checker worker must not read changing `Simulation` objects.
2. **Own checking in one worker thread.** Invoke the public checker once for
   the full captured plan, with its model and resources owned by that worker.
   Make the public individual/batch collection choice explicit and typed.
   Start the scheduler without waiting on a result, join or completion callback.
   Do not parse a second model just to probe capability or repeat checks in each
   simulation worker.
3. **Deliver accurate feedback while simulations run.** On a complete passing
   report, emit one INFO completion message. Print every missing/unresolved
   finding as a warning and retain detailed report output. Unsupported APIs,
   worker exceptions and report-write failures need distinct warnings; do not
   treat them as proof that every file is present.
4. **Keep findings advisory.** A checker result must not remove queued runs,
   terminate running processes, gate later launches, change GPU allocation or
   invoke pause/restart logic. Do not let retrieving a worker exception
   accidentally reintroduce the current fatal `RuntimeError` path.
5. **Coordinate output and lifetime.** Keep scheduler state, signal handling and
   the Rich live dashboard in the main thread. Define a thread-safe result/event
   route so completion and all warnings remain visible with the dashboard on
   or off. Drain feedback while runs are active and at completion; do not lose
   messages behind dashboard redraws or silently abandon pending checking.
6. **Verify the existing GPU warning at the launch boundary.** Reuse the current
   process/flag helpers and warn on confirmed overlap without blocking.
   Preserve uncertainty reporting and existing internal slot limits.

Moving checking to a thread removes the explicit wait-before-launch contract;
it is not proof of parser speedup or zero CPU/SMB contention. Keep controlled
#158 comparisons separate from observations made while TUFLOW is also running.

### Remaining design details

| Detail | Constraint / starting recommendation |
| --- | --- |
| CLI/settings and migration | Define background-check enablement and collection-mode controls, including what existing `--preflight` means. Normal checking must follow the agreed advisory policy; document the change from the old gate. |
| Automatic individual/batch selection | Optional. If useful, define a supported public consumer capability decision within the single-model invocation. Never probe private caches, invent run-count thresholds or catch arbitrary errors as a fallback signal. |
| Explicit unsupported batch mode | Report checking as incomplete and continue the simulation batch. Do not silently label an individual scan as successful batch validation. |
| Batch finishes before checking | Define whether normal shutdown waits for remaining feedback or reports incomplete checking under an explicit policy. Any final wait must not gate simulation launches or silently delay a chained batch. |
| Interrupt, launch failure or slow SMB I/O | Define worker/report cleanup and visible incomplete status. Avoid an unbounded accidental shutdown wait; a join timeout alone does not cancel filesystem work. Do not add new user-facing pause/restart behaviour. |
| Check-only / `run_simulations=False` | Preserve a deliberate diagnostic path that never launches. Define its completion/exit and executable/GPU prerequisites independently of normal advisory launching. |

The background/advisory/GPU requirements are already agreed; these implementation
details must not be used to reinstate a strict gate. If automatic selection or
lifecycle restructuring materially expands the PR, split that work into a child.

## Repository boundaries and later PRs

**PyTUFLOW** owns parsing, dependency graphs, proof eligibility, constructor
reuse and parser cache correctness. Its cached-batch contract assumes stable
parsed source within an invocation; broader invalidation is separate work.

**ryan-tools library** owns reusable checking, materialisation, physical
inventory and truthful findings through public parser APIs. Preserve
`PreflightReport.passed` and batch-adapter validity semantics; the runner owns
the advisory interpretation. A public API extension must be deliberate.

**Runner** owns project settings, CLI, the captured launch plan, worker lifecycle,
presentation and process boundaries. Keep changes proportionate to its current
structure. Library code must not pause, call `SystemExit` or launch TUFLOW.
`#158` remains repository diagnostic tooling, not a runtime import or installed
product API. Its graph comparison does not replace public preflight validation.

Use `Path.absolute()` for ordinary paths. Reserve `resolve()` for documented
physical identity, containment or module verification. Preserve existing
invocation-local identity/layer memoisation and original paths for I/O; do not
add persistent success caches. Checking observes inputs at that time, does not
lock the model, and cannot guarantee that TUFLOW will complete successfully.

**Pause/restart/resume work is explicitly deferred to future PRs.** Other
separate candidates include scheduler/concurrency policy, dashboard redesign,
persistent results with a freshness contract, broader CLI restructuring and
supported MCP exposure. Parser input dependencies do not by themselves establish
safe simulation execution order. Create a child only for a concrete dependency;
independent improvements can target `main`.

## Validation and completion criteria

### Documentation scaffold

Validation on 2026-10-23 uses Linux/Python 3.12.14:

- `python repo-scripts/check_documentation.py`: seven default documents and central-index coverage.
- `python repo-scripts/check_documentation.py --links-only docs/work/2026-10-23-tuflow-runner-model-checks.md docs/work/README.md docs/README.md ryan-scripts/TUFLOW-python/README.md`: all four changed documents.
- `git diff --check`; source/scope review against the current runner and public API.

Record final outcomes in the PR body. No Python source or package metadata has
changed; runtime tests, package builds, solver execution and native benchmarks
are not delivered by this scaffold.

### Implementation acceptance

Use normal Windows Python 3.14 and focused
[runner tests](../../tests/scripts/test_run_tuflow_simulations.py), plus existing
[GPU helper tests](../../tests/functions/tuflow/test_gpu_occupancy.py). The current
runner tests cover helpers/defaults/flags but lack main/preflight integration
coverage. Use fake processes and deterministic thread events, not timing sleeps:

- Hold checking on an event and prove a simulation launches before releasing
  it. Verify later launches also continue while checking is pending.
- Verify exactly one checker call receives the full frozen ordered/deduplicated
  request set, correct mode and unchanged flags despite scheduler GPU mutation.
- A complete passing report emits the INFO completion message exactly once.
- More than 20 missing findings all produce visible warnings with provenance.
- Missing/unresolved findings do not remove, terminate, postpone or gate runs.
- Worker exceptions, unsupported batch capability and report-write failures
  produce accurate warnings without stopping execution or false all-clear output.
- Feedback remains visible with the Rich dashboard enabled and disabled.
- Confirmed external GPU occupancy warns and still reaches process launch;
  unknown/inaccessible occupancy never becomes a claim that the GPU is free.
- Exercise the selected completion/interrupt/check-only contracts, disabled
  checking, paths with spaces, mapped drives and UNC paths. Never execute TUFLOW.

Run Ruff format/check, strict Pyright on modified Python, compilation, CLI help
and relevant focused tests. Run consumer/ordered-adapter tests if their contracts
change and the required package build if library/package files change. Reuse
#158's complete Windows 161-TCF/174-run acceptance at the selected source; rerun
affected gates if consumer semantics change. Do not substitute Linux archive
results for Windows acceptance or run unrelated full suites.

Use the owner's new model on another SMB server for later native evidence.
Record exact parser/consumer revisions, interpreter, mode, actual run counts
and storage/load conditions. Keep private paths/flags/reports private and share
anonymised findings. Distinguish controlled comparison from background checks
during solver activity; older models' timings are not comparable evidence.

Completion requires the agreed behaviour, relevant correctness and concurrency
checks, review findings addressed, upstream readiness reconciled and the tested
head recorded. Opening this draft does not authorise merging or releasing it.

## Self-contained continuation prompt

Continue [ryan-tools #159](https://github.com/Chain-Frost/ryan-tools/pull/159) on
`feat/tuflow-runner-model-checks`, stacked on #158. Read this record,
[AGENTS.md](../../AGENTS.md), the [development guide](../DEVELOPMENT_GUIDE.md),
[work register](README.md), [wrapper standard](../../ryan-scripts/WRAPPER_STANDARD.md),
[preflight guide](../TUFLOW_INPUT_PREFLIGHT.md) and
[#158 handoff](2026-10-23-tuflow-native-graph-benchmark.md). Inspect live branch/PR
state and preserve unrelated work. Never initialise, inspect, traverse, search,
lint or test `unsorted`.

Implement the user's agreed normal-runner policy: one background thread checks
the exact final simulation plan while the batch starts immediately; complete
success gives INFO; all missing/unresolved findings and checker failures warn
without stopping execution. Preserve truthful public check results and verify
the already-existing warning-only GPU occupancy check immediately before launch.
The old strict normal-launch gate is superseded. Keep check-only diagnostic
semantics explicit and pause/restart work in future PRs.

Resolve worker ownership, safe output delivery, shutdown and CLI details against
the current code. Use copied request data and one public checker call, with
focused deterministic concurrency tests. Keep parser changes in PyTUFLOW and
benchmark controls in #158. Record validation, refresh the stack and update this
record/register at milestones. Keep the PR draft until implementation/review/
Windows acceptance are complete. Do not execute TUFLOW or publish private data.

## Progress and delivery

### 2026-10-23

Opened draft #159 with a source-checked scope and continuation prompt above #158.
The runtime API is already merged; the parent supplies validation evidence and
development order. Added work-register, documentation-index and local TUFLOW
README links.

The user's subsequent instructions replace the initial strict-gate proposal:
background model checking is advisory, successful completion reports at INFO,
all missing-file warnings are printed, and the batch continues. Existing GPU
occupancy warnings must be retained and verified. Pause/restart work is reserved
for future PRs. Updated this authoritative scope and PR description accordingly.
Implementation and native acceptance remain outstanding.

Refreshed the stack against #158 at `9cd839247c430dbd9a2ec4a7e2187f1e80cea91d`
after its implementation advanced during this session. Resolved the work-register
overlap by retaining its current benchmark status and this separate runner row.
The runner-specific diff remains four documentation files; inherited benchmark
code is owned and validated by #158.
