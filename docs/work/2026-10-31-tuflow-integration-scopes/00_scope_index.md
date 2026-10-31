# TUFLOW integration: agreed scope and agent handoffs

Prepared: 2026-10-31, Australia/Perth. Status: scopes 01-04 integrated in #159; owner-confirmed cleanup and acceptance tracked in the current work record. This pack records the owner's latest decisions and supersedes incompatible proposals in earlier PR discussions and investigation notes.

## Delivery and baselines

- Main integration: [ryan-tools PR #159](https://github.com/Chain-Frost/ryan-tools/pull/159), branch `feat/tuflow-runner-model-checks`, targeting `main`. The PR was open and non-draft at the latest review; this scope pack does not authorize a merge or release.
- Current PR head when preparing this pack: `c1e35230d16ef014305ba0c196e68768dd4eb7cd`. The reviewed TUFLOW implementation is unchanged from `74dc7b2b89cf5061019285e90be942d2f065712c`; the intervening merge brought unrelated duplicate-check work from main. Recheck the live head and preserve concurrent changes before implementing.
- PyTUFLOW target: [upstream PR #17](https://github.com/TUFLOW-Support/PyTuflow/pull/17), immutable reference `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b`, branch `contrib/upstream-batch-validation-final` in `Chain-Frost/PyTuflow`. Do not substitute historical fork implementations.
- CSV continuation is integrated into #159 with the runner. Its lifecycle and persistence acceptance remain independently reviewable within the combined product.
- Raster-header validation is an independent PyTUFLOW contribution. It does not block this ryan-tools release. The active independent work is [PyTUFLOW PR #59](https://github.com/Chain-Frost/PyTuflow/pull/59); its current scope supersedes the original scope-05 handoff where they differ.

Before implementing, read repository `AGENTS.md`, `docs/DEVELOPMENT_GUIDE.md`, `docs/work/README.md`, the relevant current work record and the wrapper standard. Use the user's normal Python 3.14 environment for ryan-tools. Preserve PyTUFLOW's own declared Python minimum. Do not inspect or initialise `unsorted`.

## Work packages

| ID | Handoff | Destination and ownership | Dependency |
| --- | --- | --- | --- |
| 01 | [PyTUFLOW integration, installation and retirement](01_pytuflow_integration.md) | #159: public validation consumption, capability guards, installation pin, local raster removal and retired implementations | Agree the plan-to-preflight contract with 02 |
| 02 | [Shared BAT/text command plan](02_shared_command_plan.md) | #159: syntax, per-run TCF/cwd/arguments, strict TCF conflict checks and grouped provenance | Establish the shared plan contract before 03 consumes it |
| 03 | [Runner, wrappers and exports](03_runner_wrappers_export.md) | #159: workflow extraction, child ownership, advisory checks, startup GPU warning, user wrappers and exports | Uses 01 and 02 |
| 04 | [Single-CSV continuation](04_csv_resume.md) | #159: persistent log, Resume and Run all | Stable plan and lifecycle from 03 |
| 05 | [PyTUFLOW raster-header validation](05_pytuflow_raster_headers.md) | Independent `Chain-Frost/PyTuflow` issue/PR for another agent | Separate from #159; use PR #17's public contract |
| 06 | [Integration and release acceptance](06_integration_acceptance.md) | Integrating/review agent: cross-package verification, installed-wrapper acceptance and final cleanup | Combined #159: scopes 01-04 |

The initial component implementations are integrated. Use the existing [work record](../2026-10-23-tuflow-runner-model-checks.md) for current owners, outstanding corrections and delivery evidence.

## Shared decisions

### Validation ownership

Use public `pytuflow.batch_validation.validate_batch_inputs()` for supported physical checks. Retain application workflow, run grouping and reporting in ryan-tools. Remove all previous parser backends, artificial context adapters, duplicate validation paths and unused interim investigation scaffolding. Update affected callers and tests in the same change; there is no shim or old-parser compatibility requirement.

Remove local raster-header checking from this release, including its exposed controls. Ordinary upstream file checks remain. Restore header checking only through a supported PyTUFLOW option after scope 05 is delivered.

The standalone checker and explicit check-only operations report inability to validate as failure. Normal runner preflight remains advisory and can run once in the background against the frozen final run set. Missing capabilities or incomplete findings must never produce an all-inputs-present result. Plan/configuration errors are fatal and are not advisory findings.

### One plan for every consumer

Generated parameters and imported BAT/text commands must produce the same immutable planned-run contract. Preserve each occurrence's effective TCF, working directory, executable, supported model arguments, source file/line and original order. Do not collapse two different TCFs because their event/scenario flags match. Preserve intentional duplicates.

BAT syntax and plain command-list syntax remain distinct input formats. Support the agreed static subset, including comments, sequential `SET`, named variables and multiple TCFs. Parse the entire input set before validation, export or launch; unsupported constructs must not lead to a partially executed list.

**TCF conflict rule:** a configured Python TCF is both a consistency constraint and a fallback for supported abbreviated rows. Every effective imported TCF must match it. A conflict is a configuration error with both effective paths and source location. `None` permits multi-TCF input. Missing both a supported fallback and an effective command TCF is an error. Interpret the Python path against its explicitly established configuration base, and an imported path against that row's effective directory. Comments and unused assignments are not model selections.

The Python runner owns common engine switches through BATCH_COMMANDS, concurrency, priority and runtime GPU allocation. Imported engine and GPU switches are ignored when preparing the effective runner plan; imported model selections and per-run directories remain intact. Common `START` wrappers supply ordered run definitions; this is not general CMD execution. Unsupported execution modifiers must be diagnosed. Use individual `-puN` tokens only, and document that source batch/GPU switches do not control execution or model identity.

### GPU and activity boundary

Use one local GPU/activity snapshot at the start of an actual runner invocation. Warn about confirmed lack of configured GPU capacity, preserve uncertainty where inspection cannot establish availability, and keep the warning independent of launch policy. Do not rescan between models or on dashboard refresh. Continue polling owned child processes for the scheduler.

Do not integrate full `build_activity_report()` polling, CodeMeter-based scheduling, global simulation-log scans, TLF scans or TSF monitoring into this runner scope. Keep the standalone activity tool and existing reusable CodeMeter/GPU helpers available for their own purposes.

### Export and wrapper behaviour

Exports use editable shared executable/common-command variables and explicit TCF variables where there are multiple models. Preserve order and per-run differences. Export-only does not inspect GPU occupancy, allocate GPUs or require the target executable to exist locally.

Follow the maintained wrapper standard: commented editable defaults, optional CLI overrides, version banners, meaningful exit codes, shared pause handling, explicit path bases and no repository `sys.path` injection. Use the maintained serial Loguru configuration and live-dashboard renderer. Keep process-boundary behaviour in wrappers and workflow coordination in orchestrators; do not add a configuration framework.

The Windows command export is `<wrapper-stem>.txt` beside the Python wrapper. Refresh this fixed file with the complete regenerated plan, including all-complete Resume; require `.txt` for explicit destinations too. The extension prevents accidental double-click execution. No timestamped outputs or session folders. Validate/reparse before replacement and protect imported sources, model controls, wrapper and history, including aliases. The checker issues CSV remains opt-in and exclusively created.

### Continuation contract

Scope 04 uses one persistent CSV beside the supplying Python wrapper. Recompute the current run list; do not save a separate plan, JSON manifest, per-invocation folder or session report set. Read the CSV once and skip only runs whose latest attempt in the active history cycle has an observed successful finish. Retry failed, unclosed and never-started runs in current plan order. Only the current launch queue is filtered; retain the full plan and CSV history. Preflight and presentation accept sparse original run indices.

Append attempt-start and observed-finish records promptly. Killing TUFLOW or the runner is normal use; absent completion means pending on Resume. Resume assumes the prior batch has been stopped and performs no TLF, activity or process-recovery scans. This does not remove the separate startup-only GPU availability message.

Provide Resume and Run all. Run all appends a reset boundary in the same CSV, retaining history while preventing old successes from satisfying an interrupted fresh batch. Configuration changes alter run identity where appropriate; changes to model contents at unchanged paths require explicit Run all. There is no model-content fingerprinting requirement.

Selected reruns use the model's configured output destinations. Preserving CSV history does not preserve old solver outputs. Automatic result archiving, deletion, output renaming and TUFLOW restart-file management are outside scope.

## Delegation and integration order

Use this assignment text with the selected handoff:

> Implement scope **[ID]** from this pack. Read the scope index and that package before editing, recheck the live repository head, and coordinate the named shared interfaces. Complete its focused checks and return a reviewable change with exact validation evidence. Deliver scoped commits through the agreed integration branch; keep scope 05 independent in PyTUFLOW when assigned that implementation. Do not merge, release, rewrite another agent's work or run production simulations.

The preparation of this pack itself performs no runtime implementation. Once an implementation task is assigned, follow its stated boundaries without reopening already settled design decisions.

1. Give each agent this index and its own scope. Ask it to report exact baseline, owned files and intended interfaces before substantial changes.
2. The 02 owner publishes the minimal planned-run contract; 01 agrees the grouped validation boundary. They may then work concurrently in their owned files.
3. The 03 owner consumes those contracts and owns the runner wrapper and new runner orchestrator. Do not have multiple agents independently rewrite that wrapper.
4. Integrate 01-04 into #159 and run scope 06 acceptance. Keep changes focused and remove superseded artifacts rather than preserving unused implementations.
5. Review scope 04's history and continuation cases independently within the integrated product.
6. Delegate 05 separately in PyTUFLOW. Restore the consumer feature only after an accepted supported API is available and tested.

Implementation agents should return: changed files and rationale, exact tested revision, focused test/static/build evidence, remaining limitations and PR/branch delivery state. No agent should merge, release, force-push another agent's branch or run private production simulations without the relevant instruction.

## Documentation and status ownership

Keep one authoritative scope pack and use PR comments as a short index and decision record. Update the existing #159 work record and work register rather than creating a competing progress ledger. The scope files describe agreed work, not delivered behaviour. User-facing documentation must describe the behaviour actually implemented.

Once a package is delivered, record completion and its acceptance evidence in the existing work record. Retire superseded investigation material through the scoped cleanup; do not reopen unrelated projects merely because they appear in the register.
