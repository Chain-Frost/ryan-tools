# Agent 04 — persistent CSV log and simple batch continuation

**Status:** Agreed stacked implementation scope, prepared for delegation. Implement when assigned after the parent interfaces settle. Preparing this document has not changed runtime code. Keep the child PR separate; no merge or release is included.

[Scope index and shared decisions](00_scope_index.md) · [Parent runner scope](03_runner_wrappers_export.md)

## Objective

Add a fast continuation workflow in a separate PR stacked on #159. Regenerate the current run plan from the supplying Python runner and its BAT/text inputs, read one persistent CSV, skip confirmed completed runs, and run failed, unfinished and remaining simulations in the current plan's order.

For a 400-model plan, restarting after interruption should skip observed successes and queue the three failures, any unfinished attempts and the remaining models. This starts new processes for those models; it does not use TUFLOW `Read Restart File` or resume a solver checkpoint.

The owner explicitly selected a simple design. There is **one CSV per supplying Python runner file**. Do not introduce saved plans, JSON manifests, per-session directories, databases, output archives or recovery scans.

## Starting point and ownership

Reviewed source: `74dc7b2b89cf5061019285e90be942d2f065712c`; latest checked #159 head: `c1e35230d16ef014305ba0c196e68768dd4eb7cd`, with unchanged runner/planner source. Implement against the approved parent after its interfaces settle. Read the repository instructions, wrapper standard and #159 work record. Never inspect or initialize `unsorted`.

Own the focused CSV history helper, continuation selection, runner integration and their tests. Coordinate changes to the runner orchestrator with its sole owner. Reuse the common `PlannedRun`; do not build another parser or reconstruct runs by executing strings from the log.

The parent supplies a frozen logical plan, reliable subprocess ownership, structured outcomes and a place to observe start/completion events. This child owns persistence and continuation decisions. The work is sequentially integrated, not two agents concurrently rewriting the scheduler.

## One persistent file

The default is a predictable adjacent file, for example `run_tuflow_simulations_runs.csv` beside `run_tuflow_simulations.py`. The supplying wrapper computes the path from its own location and passes it into the library. Never derive it from the orchestrator's `__file__` or a changing process working directory.

Append the history of every execution to that file. Never truncate valid history at ordinary startup. Missing log means a first execution: every current planned run is pending. A renamed/copied wrapper naturally has its own adjacent log unless the owner explicitly configures another path.

Use an explicit schema version and clear CSV columns. Refuse an unknown schema or the old completion-only report format with an actionable error. Do not add legacy migration or infer a recoverable plan from the old human-readable command column.

## Append lifecycle records

Use append-only transition records, not whole-file rewrites of mutable attempt rows:

| Record | Meaning |
| --- | --- |
| `START` | An attempt has been initiated; record before calling `Popen`. |
| `FINISH` | An observed process outcome or definitive launch error, tied to that attempt. |
| `RESET` | An explicit All operation starts a fresh completion baseline while preserving history. |

Include a stable run key, unique attempt identifier, timestamp, outcome and return code where applicable. Keep enough readable TCF/flags/command context for a person to understand the row. Runtime GPU/PID information may be diagnostic fields; it is not replay configuration.

Flush records promptly. Append completion as soon as polling observes it, independently of console ordering. If model 2 finishes while model 1 continues, model 2's success must already be recorded on disk. Do not buffer durable results until ordered display catches up.

A start without a matching finish is unfinished. Abruptly killing the Python runner or TUFLOW is an expected workflow, not a reason to demand another evidence source. Do not record a success without an observed zero process return code.

## Stable run identity and repeated entries

Build a deterministic key from the effective TCF, working directory, executable and relevant model arguments. Use the frozen logical plan, before runtime GPU assignment. Exclude GPU placement, display index, source line number and Windows scheduling priority.

Use consistent lexical Windows path normalization without resolving network aliases or opening model files. Preserve meaningful argument values and ordering; normalize known event/scenario flag mappings so merely changing their presentation order does not create different simulations. Frame hashed values unambiguously. Internal serialization for hashing does not require another saved artifact.

Preserve deliberate duplicate runs by adding an occurrence discriminator within the regenerated plan. Two identical requested occurrences are separate planned work: completion of occurrence one cannot skip occurrence two. Do not restore the old accidental deduplication behavior.

The executable path participates in identity, so changing that path creates new work. Replacing a binary at the same path is not detected. Changing model-file contents without changing the configured run is also not detected. This scope intentionally performs no content hashing or freshness scans; use All when previous completion should be disregarded.

## Resume and All

Expose the approved editable run-mode setting with optional CLI override. Keep two operations sufficient for this scope:

- **Resume:** regenerate the plan, consult the current completion baseline, skip each run whose latest attempt finished successfully, and select all other runs in original order.
- **All:** append and flush a reset marker, then execute the entire regenerated plan without inheriting earlier successes.

Use Resume as the normal default: a missing log already means run everything. All is an explicit fresh execution choice. Finish parsing and validating the complete plan, including TCF consistency, before writing a reset or attempt record. Neither an export-only invocation nor a configuration error changes continuation history.

After an interrupted All execution, the next Resume considers only attempts after its reset marker. Otherwise old successes for models not yet rerun would incorrectly hide remaining work. Preserve all earlier rows for inspection; the reset changes selection, not stored history.

Within a baseline, the latest attempt wins. Associate finishes by attempt identifier. An older attempt finishing later must not override a newer failed or unfinished attempt. An old failure followed by success is skipped; an old success followed by failure or an unclosed start is pending.

No log entry means pending. A missing return code is not success. A CSV row's absence is not an error when the current plan includes a newly added run. Removed runs remain in history but are absent from current selection.

Pass the selected subset through the parent's existing launch/preflight path without regenerating a parameter product. Retain original identity and provenance even if the UI numbers the selected subset separately. A no-work Resume succeeds with a clear message, launches no models and performs no GPU scan. Treat it as successful batch completion for the existing explicitly configured follow-up/chaining policy; do not introduce an automatic post-script.

## Interruption, tail repair and writer ownership

Use correctly quoted CSV records and validate record shape, version, event and required fields. Keep log records to a form whose incomplete final write can be identified reliably; do not permit arbitrary embedded record newlines to defeat tail recovery.

On reopening, ignore an incomplete trailing fragment for state selection and remove only that torn fragment before appending again. Preserve every valid preceding record. A torn finish leaves its attempt pending. A torn reset cannot be treated as an established new baseline. Explain a repair without creating another report file.

Do not silently skip corruption in earlier complete records or truncate the whole log. Report the problem clearly. Allow one active writer for a supplying runner log; use proportionate standard-library protection and a clear failure if that ownership cannot be obtained, without another service or directory structure.

There is an unavoidable interval between process launch and recording its outcome. Resume deliberately treats an unclosed attempt as pending. It does not inspect PIDs or require proof that an old process has ended. The user controls stopping previous executions before continuing.

## Explicit exclusions and output behavior

Do not inspect TLFs, TSFs, global activity logs, CodeMeter or external processes to decide continuation. Retain the parent's single startup external-GPU check; add no scans at launch or on a timer. Existing tracking of owned children continues normally.

Do not add automatic retries, adaptive scheduling, solver hot starts, output cleanup, backup or archiving. Reexecuting a model uses its current configured output locations and may replace existing TUFLOW outputs. Preserving this CSV does not preserve those output files. Do not silently rewrite model names or output paths.

The log is history, not executable input. Rebuild from current Python/BAT/text settings each time. Do not launch a stored command string or require a separate manifest. Historical zero return codes establish observed process completion, not independently validated hydraulic results.

## Acceptance and delivery

Use focused tests with fake processes and disposable CSVs. Cover first execution, all-complete Resume, the 400-model continuation example, immediate persistence of out-of-order completion, failed/unfinished/unstarted runs, and latest-attempt precedence. Include deliberate duplicates, changed display/GPU settings, changed logical configuration, spaces/UNC paths and per-child working directories.

Exercise interrupted All followed by Resume, a torn start/finish/reset tail, unknown schema, earlier corruption and single-writer behavior. Assert no TLF/activity/process-recovery reader is called. Check launch errors, abrupt missing finishes and empty selected subsets without requiring a solver.

Run proportionate repository checks and validate the copied wrapper with its matching installed package. Record exact parent/child revisions and limitations. Deliver scoped commits and a draft child PR against the agreed parent when assigned this implementation. Do not fold automatic retries or recovery infrastructure into #159. No merge, release or real solver execution is included.

## Source anchor

[Reviewed runner and existing completion-only CSV](https://github.com/Chain-Frost/ryan-tools/blob/74dc7b2b89cf5061019285e90be942d2f065712c/ryan-scripts/TUFLOW-python/model_management/run_tuflow_simulations.py)
