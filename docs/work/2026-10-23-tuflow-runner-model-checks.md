# TUFLOW runner: intelligent use of model checks

| Field | Value |
| --- | --- |
| Status | Planned; scoping draft, runner implementation not started |
| Owner | Unassigned |
| Created / Updated | 2026-10-23 |
| Next review | 2026-10-30 |
| Branch | `feat/tuflow-runner-model-checks` |
| PR base | `test/native-graph-benchmark-controls`, the branch for [ryan-tools #158](https://github.com/Chain-Frost/ryan-tools/pull/158) |
| Initial parent | `f4a214a4cd6260098c939ae6beb59a9ec6ef0945` |
| Merged consumer baseline | `bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2` (PR #143, including #156) |
| Merged parser baseline | `e162c04330d24c37bee3ade94479cc4602651060` (PyTUFLOW #12, including #13) |
| Next action | Agree the first runner integration slice and the open policy decisions below; implement it in this draft after checking live upstream state |

## Outcome and delivery boundary

Improve [run_tuflow_simulations.py](../../ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py)
so it makes efficient, understandable use of the maintained model checker when
planning and launching an exact set of simulations. Establish this branch as
the foundation for later, separately scoped runner improvements.

The user requested an additional PR and explicitly expects further scoping.
This initial change contains a source-checked work record and discovery links.
**It changes no Python, launch policy, dependency pin or package version.**
The proposed choices below are not implemented behaviour or agreed defaults.
Keep the PR draft while scope, implementation and acceptance remain incomplete.

## Current behaviour and concrete gaps

At the recorded baseline the runner builds simulations from parameter products,
text files or both, deduplicates the final list, preserves its established order
and assigns final run indices. It optionally exports commands, then converts
that same list into public `RunRequest` values and makes one
`check_simulation_inputs()` call before any launch. A failed report blocks the
entire batch. `--preflight-only` returns before the scheduler and post-run script.

Checking is currently opt-in: the editable `preflight_check` setting defaults to
`False`, and `--preflight` / `--preflight-only` enable it. The runner never passes
`context_mode`, so the public checker always uses its default `"individual"`
collection. It cannot select the existing `"batch"` path. This is the immediate
integration gap; checking files separately for every simulation is not needed.

The public checker already collects the whole requested run set and shares
physical inventory work. It materialises wildcard, database, GIS attribute and
table-link references, checks raster metadata, and preserves per-run findings.
Explicit batch mode refuses an unavailable or incomplete parser API. The
consumer API has no `"auto"` mode, graph switch or model-factory parameter.

One further gap needs a deliberate decision: `--preflight-only` still calls
`get_parameters()` / `build_parameters()` / `check_and_set_defaults()`, including
validation of the TUFLOW executable, GPU and priority settings. It therefore
does not currently provide an engine-independent checking path. The runner also
remains Windows-only. See the [current preflight guide](../TUFLOW_INPUT_PREFLIGHT.md)
for the delivered compatibility and checking limits.

## Stack and dependency plan

| Work | Relationship to this draft |
| --- | --- |
| ryan-tools #143 / #156 | Completed runtime foundation, already in `main`; do not reopen their scope. |
| [ryan-tools #158](https://github.com/Chain-Frost/ryan-tools/pull/158) | Git parent and acceptance/evidence dependency. Complete its diagnostic controls before relying on graph performance to choose runner policy. The runner must not import repository test tooling. |
| This runner draft | Integrate the public checker with the exact launch plan and establish a reviewed policy boundary. |
| Further runner PRs | Branch from this runner branch only when they depend on its contract; give each a bounded scope and separate acceptance evidence. |
| [PyTUFLOW #14](https://github.com/phantom-branch/PyTuflow/pull/14) / [#15](https://github.com/phantom-branch/PyTuflow/pull/15) | Independent parser optimisations. Track the tested public parser revision, but neither is a Git parent or an unconditional prerequisite for basic runner integration. |

The public individual/batch API already exists on `main`. Stacking on #158
records the desired validation and merge order; it does not create a runtime
dependency on diagnostic code. Reassess the relationship if scope changes.
Do not stack this work on unrelated packaging, QGIS, RORB or raster PRs.

After #158 merges, retarget this PR to `main` and carry forward only its own
changes, accounting for a possible squash merge. Refresh any child branches
against the resulting runner head and inspect each diff against its immediate
parent. Do not merge a child into its parent merely to clear the stack, include
unfinished child work in a parent merge, or assume existing review evidence
covers a new head. Keep dependency order and exact revisions in each PR body.

## Proposed first implementation slice

1. **Make the collection choice explicit.** Add a typed editable setting and
   suitable CLI override for the public `"individual"` / `"batch"` modes.
   Define precedence alongside the existing enable/check-only controls. Keep
   when to check separate from how dependencies are collected. Preserve the
   current opt-in/individual defaults until a change is explicitly agreed.
2. **Check the final plan once.** Use the final ordered, deduplicated simulation
   list, the shared flag extractor and one public checker call. Do not rebuild
   a different event/scenario product for validation, parse a second model just
   to select a mode, or repeat checks in each worker. Preserve run indices,
   flags, command export and GPU assignment semantics.
3. **Keep launch decisions strict and visible.** Missing or unresolved inputs,
   malformed projections, unsupported explicitly requested batch mode,
   exceptions and an empty check request must not launch simulations or run
   post-processing. Keep the current whole-batch gate. Do not turn a failed
   check into success by retrying a different mode or dropping affected runs.
4. **Explain what happened.** Report whether checks ran, requested/effective
   collection mode, any capability decision, run/path/issue counts, affected
   run identities and report destination. Reuse `PreflightReport` rather than
   inventing another parser or issue model. Preserve detailed provenance;
   decide a bounded console summary separately from detailed report output.
5. **Define check-only prerequisites.** Decide whether engine/GPU validation
   can move behind the launch boundary so model checking works without a local
   TUFLOW installation. Keep relevant model/run-plan validation. Test the
   accepted behaviour explicitly and retain the existing Windows support
   boundary unless a separate portability change is selected.

### Decisions to resolve before expanding the slice

| Decision | Starting recommendation / required evidence |
| --- | --- |
| Should checks become the launch default? | Preserve opt-in initially; decide after compatibility, workflow and native overhead are understood. |
| Is automatic mode selection useful? | Consider it after explicit modes are working. If selected, define a supported public consumer capability contract that chooses within the existing single-model invocation. |
| How should `auto` behave without batch support? | A documented capability decision may choose individual mode before checking; an actual collection/validation failure must still block launch. Explicit batch remains strict. |
| Should run count or storage location choose the mode? | Do not invent thresholds or infer speed from an SMB/UNC path. Use controlled #158 evidence on the same workload and source pair. |
| Should check-only need the executable or GPU settings? | Prefer separating check prerequisites from launch prerequisites if the run-plan contract allows it; confirm affected defaults and CLI behaviour first. |
| Should valid runs launch when other runs fail? | Keep all-or-nothing in this PR. A selective-launch policy needs its own child PR, explicit user choice and exit/report semantics. |

An automatic mode, if agreed here, is a public consumer policy change and needs
its own focused contract tests. Never probe parser private internals or catch
arbitrary failures as a signal to fall back. If that design materially expands
this PR, put it in a child PR rather than silently growing the first slice.

## Repository and API boundaries

- **PyTUFLOW** owns parsing, dependency graphs, proof eligibility, constructor
  reuse and parser cache correctness. The runner does not reproduce those
  algorithms or manipulate private caches. The existing cached-batch contract
  assumes stable parsed source during an invocation; broader invalidation is
  separate work.
- **ryan-tools library** owns reusable model-check coordination, materialisation,
  physical inventory and findings through public parser APIs. If a reusable
  selection/reporting contract is needed, extend the maintained consumer API
  deliberately. The current API cannot expose graph-off/on preflight behaviour.
- **Runner** owns editable project settings, CLI precedence, exact launch-plan
  handoff, presentation, exit status and launch gating. Keep the existing mixed
  runner structure proportionate; this is not a wholesale scheduler extraction.
- **#158** owns repository diagnostic comparison and native benchmark evidence.
  It does not become a runtime import, copied-wrapper requirement or installed
  product API. Its graph comparison does not replace the separate public
  preflight gate, including raster checks.

Use `Path.absolute()` for ordinary project, script, working-directory and report
paths, preserving their intended base before changing directories. Follow links
with `resolve()` only for a documented physical-identity, containment or import
verification requirement. Preserve existing invocation-local identity/layer
memoisation; do not add repeated canonicalisation or persistent success caches.
A check observes files at that time; it does not lock the model against later
changes or replace TUFLOW's own validation.

## Later runner PRs

These are possible follow-ups to scope separately, not implementation assigned
by this record: selective launch, retry/resume, scheduler/concurrency/GPU policy,
dashboard/progress/cancellation, persistent check results with an invalidation
contract, broader CLI/configuration restructuring, and supported MCP exposure.
Parser input dependencies do not by themselves define safe simulation execution
order. Do not turn the parser graph into a scheduler without a separate design.

Create only a child that has a concrete purpose and genuinely depends on the
runner integration; otherwise base an independent improvement on `main`.

## Validation and completion criteria

For this documentation scaffold, check the changed links, indexed work record,
filenames, source claims and whitespace. No runner execution, solver run or
native performance result is delivered by this change.

Documentation validation on 2026-10-23 (Linux, Python 3.12.14):

- `python repo-scripts/check_documentation.py`: passed all seven default documents and central-index coverage.
- `python repo-scripts/check_documentation.py --links-only docs/work/2026-10-23-tuflow-runner-model-checks.md docs/work/README.md docs/README.md ryan-scripts/TUFLOW-python/README.md`: passed all four changed documents, including the new record before staging.
- `git diff --check` and an explicit whitespace check of the new record: passed.
- Independent source/scope review: no substantive corrections identified.

These are documentation checks, not Windows runtime acceptance. No Python
source, package metadata or launch defaults were changed, so runtime tests and
package builds were not run for this scaffold.

For the eventual implementation, use normal Windows Python 3.14 and focused
coverage in [the runner tests](../../tests/scripts/test_run_tuflow_simulations.py).
They currently cover helpers/defaults/flag combinations, with no main/preflight
orchestration coverage. Add meaningful checks for:

- One checker invocation with the exact final ordered/deduplicated run requests;
  configuration/CLI precedence and public mode forwarding.
- Unsupported batch, missing/unresolved findings, checker/report-write exceptions
  and empty requests preventing scheduler and post-run calls.
- Check-only success/failure invoking neither launcher nor post-run script nor
  interactive pause; accepted executable/GPU-prerequisite changes.
- Disabled checking retaining established behaviour, and unchanged flags,
  ordering, indices and GPU placement.
- Spaces, mapped drives, UNC paths and working-directory interpretation using
  the existing path policy. Stub launch boundaries; never execute TUFLOW.

Run Ruff format/check, strict Pyright on modified Python, compilation, CLI help
and the relevant focused tests. Run consumer/ordered-adapter tests if their
contracts change, and the package build required by the development guide if
library/package files change. Reuse #158's complete Windows 161-TCF/174-run
acceptance evidence at the selected source; rerun affected gates if consumer
semantics change. Do not substitute Linux archive results for Windows acceptance
or broaden to unrelated full-suite testing.

Before making efficiency/default claims, obtain controlled evidence using the
owner's new model on another SMB server, after #158 is ready. Record exact parser
and consumer Git revisions, interpreter, mode, actual run count and cache/server
conditions. Keep model paths, flags and detailed reports private; share anonymised
counts/timings/findings only. Historical model timings are not comparable evidence
for this workload. Coordinate with the owner when a new benchmark is needed.

The implementation is complete only after the selected policy is documented,
relevant correctness and launch-boundary checks pass, review findings are dealt
with, upstream readiness is reconciled and the work record/PR reflect the tested
head. Opening this draft does not authorise merging or publishing a release.

## Self-contained continuation prompt

Continue the draft on `feat/tuflow-runner-model-checks` in `Chain-Frost/ryan-tools`.
Read this record, [AGENTS.md](../../AGENTS.md), the
[development guide](../DEVELOPMENT_GUIDE.md), [work register](README.md),
[wrapper standard](../../ryan-scripts/WRAPPER_STANDARD.md),
[preflight guide](../TUFLOW_INPUT_PREFLIGHT.md) and
[#158 handoff](2026-10-23-tuflow-native-graph-benchmark.md). Inspect live PR/branch
state and preserve unrelated work. Never initialise, inspect, traverse, search,
lint or test the `unsorted` submodule.

First review the actual runner and public checker against this scope. Present a
concrete first slice and identify any unresolved policy choice that materially
changes behaviour; do not treat proposed `auto` or default enablement as approved.
Then implement the agreed slice in this PR, using the public consumer API and
focused launch-boundary tests. Keep benchmark tooling in #158 and parser changes
in PyTUFLOW. Use separate child PRs for the follow-ups above. Maintain the stack,
record exact validation and update this status/register at milestones. Keep the
PR draft while scope or acceptance is incomplete; request code review once the
implementation is ready. Do not run TUFLOW or publish private model data.

## Progress and delivery

### 2026-10-23

Source audit completed and scoping record prepared for a documentation-only
draft, stacked above #158 at the recorded initial parent. The runtime API is
already merged; the parent supplies diagnostic evidence and development order.
Added discovery links in the work register, documentation index and local TUFLOW
README. Runner implementation, final policy decisions and native acceptance are
outstanding. Record the published draft and validation results in the PR body;
keep this record as the authoritative implementation handoff.
