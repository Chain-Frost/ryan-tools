# 02 — Shared BAT/text command planning and multi-TCF support

**Superseding integration decision (31 October 2026):** Imported BAT/TXT is
model-selection data only and is never executed. Python controls actual
executable and launch cwd. Keep the first distinct TCF/flags/model-option
selection and discard later duplicates; historical instructions below
requiring duplicate preservation or source execution context are superseded.

## Status, baseline and goal

This is an implementation handoff based on source review; implementation is pending. Read [00_scope_index.md](00_scope_index.md) for shared decisions and coordination. The code-review baseline is PR159 commit `74dc7b2b89cf5061019285e90be942d2f065712c`; the latest rechecked PR head is `c1e35230d16ef014305ba0c196e68768dd4eb7cd`, with relevant planner/runner code unchanged. Re-read the live head and repository instructions before implementation. The sole PyTUFLOW target is PR17 commit `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b`.

Make the checker and runner consume one complete, ordered command plan. Both must support active commands for multiple TCFs in a BAT or text file, including sequential `SET TCF` reassignment and explicitly referenced named variables such as `%TCF_05%` and `%TCF_06%`.

At the reviewed baseline, [command_plan.py](https://github.com/Chain-Frost/ryan-tools/blob/74dc7b2b89cf5061019285e90be942d2f065712c/ryan_library/functions/tuflow/command_plan.py) already supports much of this static syntax, and the checker uses it. The runner instead extracts flag combinations, deduplicates them and reconstructs commands with global TCF/executable settings. Replace that lossy ingestion through the runner integration owned by scope 03; extend the existing shared reader only where necessary.

## Required Python TCF contract

The optional Python TCF setting is **both a consistency constraint and a fallback**:

1. Determine each active run's effective imported TCF. An explicit TCF argument wins. For a supported abbreviated/bare flag row, use the current `SET TCF` value when present, otherwise the Python TCF fallback.
2. Full executable commands must supply their TCF argument. Do not broaden their syntax by silently filling a missing argument.
3. If Python TCF is not `None`, every effective run TCF must match it. A different model is a **configuration error**, before validation, command export or any launch. Never replace the imported model with the Python model.
4. If Python TCF is `None`, multiple effective imported TCFs are valid. An abbreviated row with neither an effective imported TCF nor a fallback is an error.
5. Check active runs, not every assignment or string. Commented commands, unused variables and an assignment overwritten before use do not create mismatches.

Resolve the Python setting against an explicitly supplied configuration base/working directory, usually the supplying wrapper's directory. The wrapper establishes and documents this base before changing directories. Resolve imported relative TCFs against each row's effective launch directory; `%~dp0` refers to the source command file's directory. Compare absolute, lexically normalized paths using the appropriate Windows path rules. Preserve mapped-drive/UNC spelling; do not use `.resolve()` or probe filesystem aliases to make different configured paths match.

For example, a source that runs `Model_05.tcf` and then `Model_06.tcf` is valid with Python TCF `None`, but fails with Python TCF set to `Model_05.tcf`. A disabled `REM` command for model 06 does not fail the constraint.

## Supported input formats

Retain the format distinction: `auto` selects BAT syntax for `.bat`/`.cmd`, and command-list syntax otherwise; an explicit format remains available. This is unrelated to the obsolete PyTUFLOW parser/backend selection modes, which are removed under scope 01.

Support the existing bounded, static subset:

| Construct | Required behavior |
| --- | --- |
| Direct TUFLOW command or supported bare flag row | Preserve effective model, executable selection and arguments. |
| `SET "name=value"`, plain `SET name=value` | Sequential, case-insensitive variable names; values retain their case. Named TCF variables require explicit reference. |
| `REM`, `@REM`, `::` | Disabled commands do not become runs. Command-list files additionally accept full-line `#`, `;`, `//`. |
| Double quotes and supported caret continuations | Preserve spaces and physical source line ranges; reject malformed quoting/continuations. |
| `%NAME%`, `%~dp0`, `%CD%` | Expand from explicit reader state; reject undefined or unsupported expansion. |
| Supported `CD`/`CHDIR`, `CD /D`, `PUSHD`/`POPD`, `START /D` | Track directories without changing process cwd. `START /D` affects only its child run. Retain existing restrictions on ambiguous drive state and UNC directory changes. |
| Common `START` command forms | Correctly distinguish the quoted title from the executable. The Python runner owns scheduling; imported BAT files are ordered launch lists. |

Document the treatment of customary `START /WAIT` and priority syntax with scope 03. Do not silently accept CPU-affinity/NUMA or other execution modifiers the runner cannot honor; reject unsupported modifiers clearly. General `IF`, `FOR`, `CALL`, arbitrary jumps, delayed expansion, shell operators, pipes and redirection remain unsupported. Diagnostics identify the source file and physical line range. Never execute source BAT files or invoke `cmd.exe` to interpret them.

## Plan and consumer boundaries

Each immutable planned occurrence retains its source file, source lines, global position, effective absolute TCF, effective launch directory, explicit executable or fallback requirement, event/scenario flags, and complete supported arguments. Preserve argument values and explicit engine switches. Scope 03 materializes the final executable and applies the agreed GPU/launch policy; this reader does not inspect GPUs or schedule processes.

Keep the first equivalent TCF/event/scenario/retained-model-option selection and discard later duplicates, even across independent source files. Read multiple input files in their configured order, resetting assignment/directory state for each independent file. Retain the correct source for directory requirements as well as run rows. Parse and check configuration consistency across the complete input set before publishing exports, validating models or launching any run. Read/decode failures must stop preparation, not silently drop one file.

Keep model checking grouped by effective TCF and launch directory. Scope 01 supplies the sole public PR17 validation integration. Restore group-local upstream result indices to global unique selections, including interleaved models. A failed/incomplete group must yield explicit affected-run findings. No validation step changes process cwd. The standalone checker remains strict; ordinary runner input findings remain advisory. Planning/configuration errors are preparation errors in both tools.

Scope 03 converts parameter-product inputs into the same plan contract; Python sets the single configured child working directory. Generated/exported BAT commands must roundtrip through this reader with equivalent effective TCF, executable, cwd, arguments, order and occurrence count. Avoid global `SET TCF` string replacement that changes imported models.

## Ownership and coordination

Primary owned files:

- `ryan_library/functions/tuflow/command_plan.py` and `tests/functions/tuflow/test_command_plan.py`.
- `ryan_library/orchestrators/tuflow/command_preflight.py` and `tests/orchestrators/tuflow/test_command_preflight.py`: plan grouping, provenance and consumer result mapping. Agree the validation callable with scope 01 before editing its call site.
- `ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py` and `tests/scripts/tuflow/test_command_preflight_wrapper.py`: input settings, consistency checking, plan-only behavior and format help.
- Relevant existing command-input documentation and examples.

Scope 01 owns PyTUFLOW guards, installation and model-validation internals. Scope 03 owns runner extraction/adoption, executable resolution, scheduling, subprocess calls and exporter implementation. Provide its agent the plan contract and roundtrip cases; do not concurrently refactor the runner wrapper. Scope 04 owns persistent CSV/resume behavior and derives run identity from the agreed plan fields. Do not add session directories, manifests, persistence services or a separate history model.

## Essential verification and acceptance

Retain meaningful existing parser tests and add focused cases:

- Reassigned `TCF` and named `TCF_05`/`TCF_06` produce correct distinct runs with identical flags; exact duplicate rows remain present.
- Python TCF `None` permits multi-model input; a matching constraint passes; any active mismatch fails before validator/exporter/launcher calls, including a mismatch late in a later file.
- Relative-path comparisons use the correct bases; unused assignments and disabled commands do not fail consistency; missing model information fails clearly.
- Quotes, comments, cwd changes and continuations retain provenance; independent files do not leak environment/directory state.
- Invalid syntax or unreadable/undecodable input produces no partial queue. Full commands without event/scenario flags retain their default model context.
- Interleaved groups restore global result indices correctly; mocked validation failure cannot appear as a passing run.
- Coordinate checker/runner plan parity and generated/exported BAT roundtrip tests with scope 03, using fake process launchers only.

Deliver a reviewable branch/PR with the supported subset, changed files, test evidence and remaining limitations. Complete the agreed integration with adjacent scopes. Do not merge or run real simulations.
