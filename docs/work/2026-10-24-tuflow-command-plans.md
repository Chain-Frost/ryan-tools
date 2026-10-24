# Stateful TUFLOW command plans and multi-TCF preflight

| Field | Value |
| --- | --- |
| Status | Active — implementation present; hosted validation pending |
| Owner | ChatGPT |
| Created / Updated | 2026-10-24 |
| Next review | 2026-10-25, or exact-head CI completion |
| Baseline | main `bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2` |
| Branch | `feat/stateful-command-preflight` |
| Scope | Static run planning and existing public preflight; no solver, benchmark or parser optimisation |

## Outcome

The standalone checker now reads the effective TCF for each active command,
not a single global TCF applied to every row. Sequential reassignment takes
effect only from that statement onwards; earlier runs remain unchanged.
Order, duplicate rows, event/scenario values and physical source-line ranges
are retained. Full START commands, direct TUFLOW invocations and bare flag rows
are supported. Bare rows are an intentional input-list extension, not a claim
that a leading `-e1` command is executable Windows batch syntax.

The reusable [planner](../../ryan_library/functions/tuflow/command_plan.py)
contains no shell execution, process launch or model inspection. The
[orchestrator](../../ryan_library/orchestrators/tuflow/command_preflight.py)
groups by effective TCF and launch directory and calls the existing
`check_simulation_inputs()` API once per group. It preserves original global
indices in every finding; interleaved TCF groups do not lose their source order.
The existing model parser, materialisation, physical inventory and cache policy
are unchanged. Unique-file work is still shared within each group, not across
independent TCF groups.

The [maintained wrapper](../../ryan-scripts/TUFLOW-python/model_management/check_tuflow_inputs.py)
adds plan-only inspection, an optional private run manifest, initial working
directory, input encoding, syntax selection and explicit individual/batch mode.
Individual remains the default. Explicit batch mode needs the existing compatible
PyTUFLOW API and does not silently fall back.

## Usage

Use the matching installed package and copied wrapper; no `sys.path` editing.
First inspect the proposed run set, without reading any model inputs:

```powershell
python check_tuflow_inputs.py --runs "run_commands.bat" --working-directory "T:\project" --plan-only --plan-report "new_plan.json" --no-pause
```

Then check the effective models and write a new issue register:

```powershell
python check_tuflow_inputs.py --runs "run_commands.bat" --working-directory "T:\project" --report "new_issues.csv" --no-pause
```

A bare flag list can use `--tcf "runs\model_~e1~.tcf"`. That option and the
editable `DEFAULT_TCF` are fallbacks for bare rows only: an explicit TCF argument
wins, then a current `SET TCF` for bare rows, then the fallback. Full executable
commands must contain their own effective TCF argument. Filename placeholders
such as `~e1~` stay literal for PyTUFLOW.

**Initial directory is the directory from which the batch would be launched,
not automatically its parent.** With no `--working-directory`, the current
process directory is used. `%~dp0` refers to the command file's folder. Relative
TCF values are resolved at the simulation statement, after preceding supported
directory changes. START `/D` changes only that run's launch directory. The
checker never changes its actual process working directory.

`--plan-only` confirms only supported syntax and the extracted plan. It does not
check directory existence, model dependencies, executable availability or engine
validity. Full preflight checks that required directories exist before model
collection; unavailable directories fail rather than assuming CD/PUSHD succeeded.
Readability/permissions and filesystem changes after planning remain operational
limits, not a promise to reproduce every possible shell failure.

## Supported static syntax

| Area | Implemented contract |
| --- | --- |
| Assignments | Plain `SET name=value` and `SET "name=value"`, reassignment/unset, case-insensitive names and single statement-time expansion. |
| Variables | Explicitly supplied environment, `%NAME%`, `%CD%`, `%~dp0`; wrapper supplies a snapshot of its own environment. Undefined expressions are errors. |
| Scope | SETLOCAL with enabled extensions/disabled delayed expansion and ENDLOCAL restore environment, not current directory. |
| Directories | Quoted CD/CHDIR with optional `/D`, local/mapped PUSHD and POPD; lexical paths, no canonicalisation. |
| Commands | Standard TUFLOW executable names, START title/priority/window/wait options, child `/D`, numeric `/NODE`, hexadecimal `/AFFINITY`; bare numbered event/scenario rows. |
| Engine switches | `-e1..-e9`, `-s1..-s9`, `-b`, `-x`, `-t`, `-nlc`, numeric `-pu` assignments and `-et` values. Other arguments fail explicitly. |
| Source text | UTF-8/BOM by default, explicit `--encoding` for legacy text, quoted paths with spaces, unquoted terminal-caret continuation and source-line ranges. |
| Comments | REM/@REM and full-line `::`; ordinary ECHO/PAUSE/TITLE/CLS do not create runs. In command-list mode only, `#`, `;`, `//` are also comments. |
| End | Normal EXIT, EXIT /B with optional zero code, and GOTO :EOF stop the remainder; simple labels alone are ignored. |

`.bat`/`.cmd` select batch syntax in auto mode; other suffixes select command-list
syntax. Both accept bare rows as the documented extension. A malformed active
statement prevents **any** partial plan being returned or checked. Comments are
recognised before extracting event flags, including a disabled malformed quoted
simulation command.

This is a deliberately bounded reader, **not a general CMD emulator**. IF/FOR,
CALL/subroutines, ordinary GOTO, parenthesised blocks, chained commands, pipes,
redirection, inline caret escaping, delayed `!variable!` expansion, variable
substring/replacement expressions, shell-dependent commands and unknown engine
switches are refused with source-line diagnostics. Export an explicit ordered run
list instead; unsupported constructs are never silently skipped.

Drive-relative paths and cross-drive CD without `/D` are refused. A UNC TCF
argument is preserved, but UNC initial batch directories and UNC PUSHD/CD need
CMD-specific mapping state and are not inferred. Use an existing mapped drive.
Spaced event/scenario values are refused because the existing public RunRequest
argument serialisation cannot preserve them unambiguously. This increment does
not change that public API.

Reference semantics: [CMD](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/cmd),
[SET](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/set_1),
[START](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/start),
and [REM](https://learn.microsoft.com/en-us/windows-server/administration/windows-commands/rem).
These references do not imply that every documented shell feature is implemented.

## Reporting and safety

CSV findings contain source file, start/end line, original run index, effective
TCF, launch directory, flags, status, origin, reference and detail. All console
findings are printed. A group exception marks all its runs unresolved and other
independent groups are still checked. An incomplete group report cannot pass.
Interrupts propagate; the checker does not claim interrupted work is complete.

CSV and JSON reports require **new destinations** (`open("x")`); existing files,
including inputs, are never overwritten. Reports can contain private paths and
commands and are not automatically uploaded. A run manifest records the plan,
not benchmark acceptance or a claim that preflight ran.

The legacy flags-only `read_run_commands()` and the frozen #158 diagnostic stay
unchanged. The new standalone wrapper is the integration boundary. Runner #159
and its asynchronous/advisory lifecycle remain separate work. Do not switch the
launcher to this reader or change historical benchmark inputs implicitly.

MCP assessment: expose this read-only/planning workflow through its maintained
wrapper only after validation of the new CLI. No second MCP parser, tool or
catalogue entry is introduced in this increment. Existing imports/default
published PyTUFLOW dependency and wheel-selection policy remain unchanged.

## Validation and delivery

Local Linux CPython **3.13.5**: **86** planner, group-routing and wrapper tests
pass using the actual new modules in a minimal source staging directory:

```text
python -m pytest -q tests/functions/tuflow/test_command_plan.py tests/orchestrators/tuflow/test_command_preflight.py tests/scripts/tuflow/test_command_preflight_wrapper.py
```

The group tests replace only the group backend with explicit controlled test
objects. They do not constitute real PyTUFLOW or Windows acceptance. Two additional
real-backend tests are committed separately and **not run locally**; the existing
hosted Windows suite must execute them and the existing preflight regressions.

Two user-provided command texts were read privately without execution. Every
ordered `(line, effective TCF, flags)` matched an independent extraction: one
had **480** active runs and one TCF, the other **240**, split **2/238** between
successive TCFs, with **four REM simulation rows excluded**. Original bytes were
unchanged. No project names, paths, flags, hashes or source files are committed.
This validates extraction for those files, not their model dependencies.

Local GitHub/PyPI DNS access is unavailable. Connected GitHub reads/writes are
used for delivery; no full checkout, installed GIS stack, Ruff/Pyright or package
build is available locally. The existing hosted Policy/Hosted tests/Package jobs
must supply those results. Compilation, source hashes and focused unit results
are reported separately. No native model check, NAS benchmark or solver ran.

## Next action and completion criteria

Inspect exact-head hosted checks, correct policy or real-backend regressions,
then request a complete review. Require Windows tests, strict changed-file
Pyright, pinned Ruff, documentation checks and a no-bump verified wheel before
marking ready. Keep draft while any of those are pending. No merge, release,
production repin or benchmark is authorised.

The work register also contains older due reviews unrelated to this task; they
were not advanced or treated as permission to start those workstreams. Agents
A-D own their existing PyTUFLOW PRs; their source and shared CI are unchanged.
