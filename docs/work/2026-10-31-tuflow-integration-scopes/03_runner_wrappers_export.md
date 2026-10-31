# Agent 03 — runner, wrappers and command export

**Superseding integration decision (31 October 2026):** Imported BAT/TXT is
model-selection data only and is never executed. Python controls actual
executable and launch cwd. Keep the first distinct TCF/flags/model-option
selection and discard later duplicates; historical instructions below
requiring duplicate preservation or source execution context are superseded.

**Status:** Agreed implementation scope, prepared for delegation. This document records planned work; its preparation has not changed runtime code. Implement when assigned, coordinating delivery with the integrator. No merge or release is included.

[Scope index and shared decisions](00_scope_index.md) · [Stacked CSV resume scope](04_csv_resume.md)

## Objective and starting point

Clean up the runner in PR #159 around the agreed shared `PlannedRun` contract. Keep the Python file convenient to edit and copy into a project; place workflow coordination in a maintained orchestrator. Generated runs, imported commands, input checking, command export and process launch must describe the same effective simulations.

The reviewed source baseline is `Chain-Frost/ryan-tools` PR #159 at `74dc7b2b89cf5061019285e90be942d2f065712c`. The latest checked head is `c1e35230d16ef014305ba0c196e68768dd4eb7cd`, following a main-branch merge; runner/planner source is unchanged. The PR remains an open integration draft. Refresh the head before implementation. Read `AGENTS.md`, `docs/DEVELOPMENT_GUIDE.md`, `ryan-scripts/WRAPPER_STANDARD.md`, the work register and #159 status record. Never inspect or initialize `unsorted`.

Coordinate the shared-plan interface with its owner before moving callers. This task adopts that interface; it must not create another command parser or a competing run-description class.

## Owned work and boundaries

Own `ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py`, its backing runner orchestrator, runner presentation/export helpers where justified, and focused runner/wrapper tests. Update the standalone checker's wrapper behavior only in agreement with the preflight owner. Shared parsing and PyTUFLOW compatibility belong to their respective agents.

The orchestrator owns planning coordination, scheduling, subprocess ownership, advisory-worker lifecycle and structured outcomes. The wrapper owns editable settings, CLI overrides, working-directory selection, logging configuration, banners, pausing and process exit. Neither library helpers nor the orchestrator may pause or call `SystemExit`.

Do not expand this into a general scheduler framework. CSV continuation is integrated under scope 04: provide the agreed run identity and lifecycle integration points, but leave CSV state selection and persistence policy to that task.

## Wrapper-standard changes

Apply the useful existing standard, preserving the current user workflow:

- Keep a purpose/input/output docstring, wrapper version and clearly commented editable defaults near the top.
- Retain no-argument configuration through Python settings. CLI arguments are optional overrides, normally defaulting to `None`; distinguish absent values from explicit false or zero.
- Use `main(...) -> int`: zero for success, one for processing/execution failure and two for invalid arguments. Invalid CLI input remains an argument-parser error.
- Use shared opening/closing version banners and `pause_console()` with `--no-pause`. Do not introduce a second pause mechanism.
- Resolve each relative path against its intended base before changing directory. Use `.absolute()` for ordinary paths; do not dereference mapped-drive aliases merely for display.
- Require the matching installed package for copied wrappers. Do not add repository-root `sys.path` changes.
- Load optional preflight/GIS dependencies only when their operation is selected. Help, planning and ordinary execution must not fail because an unused parser capability is unavailable.

Use the maintained serial Loguru setup and shared dashboard renderer. Do not expose every cosmetic setting as a CLI switch or add a configuration-file system. Target the repository's Python 3.14 annotation conventions and retain declared compatibility requirements.

## One plan and the TCF consistency rule

Adapt parameter-product generation and supported BAT/text import to one unique model-selection plan. Preserve effective TCF, event/scenario values, relevant model arguments and first source location. Python supplies the launch executable and directory; drop repeated selections. Runtime GPU assignment and process fields must not mutate the logical plan.

The Python TCF setting has an explicit consistency contract:

1. When configured, it must agree with **every** imported run's effective TCF.
2. Any disagreement is a hard planning error identifying the Python setting and offending source rows.
3. Raise that error before model validation, report/export writes or any launch. Do not silently replace imported TCFs or select only matching rows.
4. When the Python TCF setting is `None`, accept the imported effective TCFs, including multiple TCFs.
5. Bare flag rows still need an unambiguous TCF from the agreed parser context; missing context is an error.

Compare lexically normalized effective paths using the shared Windows path rules. The Python TCF is relative to the wrapper's explicitly selected working directory, typically the wrapper directory; do not hardwire its parent as the base. Imported TCFs use their effective command directories. Do not use filesystem resolution to make contradictory source settings appear equivalent.

Each child launches using Python's configured `WORKING_DIR`. BAT/TXT source directory changes are only parsed to resolve source TCF references, never executed or replayed. Group advisory/preflight requests by effective TCF and working directory while preserving original run identities and all findings.

## Execution policy for imported commands

Runner settings own common engine switches through BATCH_COMMANDS, GPU slots, concurrency and process priority. Drop source batch engine/GPU switches when preparing imported runs; apply the Python settings before identity, export and launch. Preserve event/scenario values, other supported model switches such as -etN and effective source TCFs; ignore source executable and launch directory. Treat this policy as an explicit import contract, not an incidental result of discarded tokens.

The bounded BAT reader is not `cmd.exe`. Do not promise arbitrary `START` semantics. Inventory the supported modifiers and diagnose execution modifiers that cannot be represented. Explain any source scheduling/GPU/priority settings superseded by the runner policy; do not silently claim faithful replay of those settings. Preserve accepted model switches and reject unsupported material before producing a partial executable plan.

Use subprocess argument lists directly. Do not execute imported text through a shell. Preserve source provenance so a planning error can be traced to its file and physical line range.

## Process lifecycle and advisory behavior

Register a child immediately after `Popen` succeeds, before operations such as priority adjustment that can fail. Ensure failed launches release their scheduler slot and produce a truthful outcome. Handle a child that exits immediately and a priority operation that raises without losing ownership of an already-started process.

Keep normal input checks advisory: unavailable capabilities, missing inputs and checker failures produce visible warnings without delaying or stopping the batch. Explicit preflight-only operation waits for the check, returns a meaningful result and launches nothing. Freeze its requests before runtime GPU assignment.

Run at most one advisory operation for the selected plan. Drain every returned finding through the normal presentation route. If execution ends before checking can complete, report checking as incomplete; do not make shutdown wait indefinitely for network I/O or falsely report a passing check.

Return structured batch/per-run outcomes. A failed process must affect the wrapper's final exit status. Preserve the established post-script policy explicitly; do not accidentally suppress chaining because exit reporting is corrected. Use the running Python interpreter for Python follow-up scripts.

Retain explicitly configured follow-up/chaining after normal batch termination under the existing policy, including when a child reports failure; keep the aggregate failure exit status truthful. Planning errors and interrupted batches must not accidentally trigger follow-up execution. Scope 04 treats an all-complete Resume as successful normal completion for that configured policy.

Keep interrupt handling at the process boundary, with cleanup delegated to the runner. Record only outcomes actually observed; do not equate a termination request with a confirmed exit. Restore temporary signal handling. Reject an unintended empty execution plan.

## Startup GPU information and activity boundary

Perform the agreed external-GPU inspection once at startup and show its warnings. Unknown assignments remain unknown. Do not rescan external processes at each launch or on a timer. Continue tracking the runner's own children and configured slots through its existing scheduler state.

Keep the normal message short. If no running models are detected, say so. If other models are detected but the configured pool has capacity, report capacity without a detailed activity listing. Warn with relevant GPU/PID information when no configured GPU group is available. Incomplete inspection remains uncertain and must not produce a false all-clear. This is warning-only: do not automatically filter the configured pool, reassign slots or block the batch.

Do not call `build_activity_report()` from the launch loop or add CodeMeter, TLF, TSF or global-activity polling in this workstream. The standalone activity tool remains independent. No licence-based scheduling, adaptive GPU reassignment, pause/resume or automatic retries are included.

## Command export

The runner and exports target Windows. Refresh a fixed `<wrapper-stem>.txt` beside the Python wrapper, retaining export by default even for an all-complete Resume. TXT is required for explicit destinations too, to avoid accidental double-click execution. No timestamped files, backups or session folders. Render/reparse the full plan before atomically replacing an existing export. Protect imported source, model, wrapper and history files, including aliases; a conflict requires another export path or disabled export.

Export the validated shared plan using editable common variables `TUFLOW_EXE` and `BCMD`. Use one `TCF` variable when shared; use clearly mapped `TCF_1`, `TCF_2`, and so forth when several effective TCFs occur. Preserve each run's directory and argument values with correct Windows quoting. Emit the agreed `START /LOW /MIN /WAIT "" "%TUFLOW_EXE%" %BCMD% ... "%TCF%"` form when those settings apply; retain the equivalent per-run TCF reference for multiple models.

Factor values only when they are common. Preserve supported per-run differences explicitly, or diagnose an unsupported difference before writing; never silently replace it with a global value. Keep generated exports readable and supported by the shared reader, including their fixed variable headers. Verify a round trip against the logical plan, not a string-equality approximation.

Export-only mode must not require an existing solver executable, inspect GPUs, launch processes or load unused PyTUFLOW capabilities. Validate plan structure and output destination. Follow the agreed output overwrite policy and never overwrite an imported source file.

## Acceptance and delivery

Use focused fake-process and deterministic-event tests. Required cases include multi-TCF/per-child-directory launch; Python/imported TCF conflicts before side effects; duplicate/order/provenance preservation; quoted paths; export round trips; no executable/GPU prerequisite for export-only; and copied-wrapper execution against the installed wheel.

Retain tests proving launches continue while advisory checking is held and every warning is visible with or without Rich. Add immediate-exit, priority-failure, launch-failure, interrupt, empty-plan and final-exit-status cases. Verify exactly one startup external-GPU snapshot and no activity polling. Completion recording must occur when observed, independently of ordered console printing.

Run the repository's proportionate formatting, modified-file strict typing, wrapper/help, focused-test and package checks. Native Windows/process/network acceptance remains separate and requires an approved environment. Do not run TUFLOW during ordinary tests.

Deliver a concise change/evidence summary and update the existing #159 work record through the integrator. Coordinate scoped commits and branch delivery so another agent's work is not overwritten. Keep implementation in the #159 integration workstream; no independent merge or release is included. Do not claim a built package proves native solver acceptance.

## Source anchors

- [Reviewed runner](https://github.com/Chain-Frost/ryan-tools/blob/74dc7b2b89cf5061019285e90be942d2f065712c/ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py)
- [Existing shared command plan](https://github.com/Chain-Frost/ryan-tools/blob/74dc7b2b89cf5061019285e90be942d2f065712c/ryan_library/functions/tuflow/command_plan.py)
- [Wrapper standard](https://github.com/Chain-Frost/ryan-tools/blob/74dc7b2b89cf5061019285e90be942d2f065712c/ryan-scripts/WRAPPER_STANDARD.md)
