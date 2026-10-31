# Stateful TUFLOW command plans and multi-TCF preflight

**2026-10-31 delivery update:** Consolidated into [PR #159](https://github.com/Chain-Frost/ryan-tools/pull/159) for one eventual merge and release. The [integration record](2026-10-23-tuflow-runner-model-checks.md) owns current status, validation and next actions. This record retains detailed scope and historical evidence; earlier separate merge/stack instructions are superseded. No merge or release is authorised.


**29 October 2026 — curated PR C integration:** Explicit `context_mode="batch"` now uses `TCF.collect_file_dependencies()` from [PyTuflow PR C #50](https://github.com/Chain-Frost/PyTuflow/pull/50). The parser's removed `collect_batch_dependencies()`/graph switches are not used for ordinary batch preflight. Each group retains its exact TCF, launch directory, flags and original command source-line provenance. This is not a cross-TCF cache or a released dependency; native acceptance and exact-head CI remain required.


| Field | Value |
| --- | --- |
| Status | Needs review; exact-head hosted validation and local review complete |
| Owner | ChatGPT |
| Created / Updated | 2026-10-24 / 2026-10-25 |
| Next review | 2026-11-01 for delivery |
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
Individual remains the default. Explicit batch mode requires the PR C
`TCF.collect_file_dependencies()` API and does not silently fall back.

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

Local Linux CPython **3.13.5**: **93** planner, group-routing and wrapper tests
pass using the actual new modules in a minimal source staging directory:

```text
python -m pytest -q tests/functions/tuflow/test_command_plan.py tests/orchestrators/tuflow/test_command_preflight.py tests/scripts/tuflow/test_command_preflight_wrapper.py
```

The group tests replace only the group backend with explicit controlled test
objects. They do not constitute real PyTUFLOW or Windows acceptance. Two additional
real-backend tests were executed by the initial hosted Windows suite and passed;
they remain **not run locally**. Both are explicitly present in its retained JUnit report.

Two user-provided command texts were read privately without execution. Every
ordered `(line, effective TCF, flags)` matched an independent extraction: one
had **480** active runs and one TCF, the other **240**, split **2/238** between
successive TCFs, with **four REM simulation rows excluded**. Original bytes were
unchanged. No project names, paths, flags, hashes or source files are committed.
This validates extraction for those files, not their model dependencies.

Local GitHub/PyPI DNS access is unavailable. Connected GitHub reads/writes are
used for delivery; no full checkout, installed GIS stack, Ruff/Pyright or package
build is available locally. Hosted Policy/Hosted tests/Package jobs supply those
results. Compilation, source hashes and focused unit results are reported
separately. No native model check, NAS benchmark or solver ran.

### Hosted checkpoint and correction

Implementation was delivered as [PR #160](https://github.com/Chain-Frost/ryan-tools/pull/160).
Initial head `3bf4220d782dc0d1d33111f75d0ef2313155961f` passed its Windows
CPython 3.14.9 focused suite (86 tests). Its
[hosted regression and packaging jobs](https://github.com/Chain-Frost/ryan-tools/actions/runs/40073651826)
passed: JUnit contains 1,485 passing tests, six skips and no errors/failures,
including both new real-PyTUFLOW multi-TCF tests; the no-bump wheel build,
verification and installed-wheel smoke also passed. Policy failed on style;
that initial revision is not claimed fully accepted.

At runtime/test revision `d1758479df5722589836913ca97cd049704e698d`, reviewed
Ruff fixes/formatting and split assertions preserve the tested contracts. An
additional regression fixes a leading empty caret continuation losing its first
physical source line. Local focused tests now pass all 87 with warnings treated
as errors, and private uploaded-file extraction was repeated with exact ordered
matches and unchanged bytes. Delivered Python blobs match locally tested bytes.
The [refreshed focused Windows contracts](https://github.com/Chain-Frost/ryan-tools/actions/runs/40074359630)
and [Fast PR checks](https://github.com/Chain-Frost/ryan-tools/actions/runs/40074359646)
have passed. Refreshed Python CI and the requested complete review are tracked
on PR #160; later terminal outcomes there supersede this checkpoint. No source
or fixture is changed merely to mark an asynchronous job as completed.

### Review and strict typing follow-up

Review identified a command-list-only omission: a full-line `#`, `;` or `//`
comment ending in a caret could absorb the next active statement. Six regressions
reproduced it before correction. These comments are now recognised as complete
physical lines before continuation joining; active rows retain their original
positions, and an unsupported following statement is still rejected. All 93
focused tests pass locally. REM/`::` continuation refusal remains unchanged.
Strict Pyright also identified five unparameterised dataclass list factories;
explicit element types fix these without ignores or changed runtime behaviour.

## Next action and completion criteria

Review and authorize delivery of the local documentation reconciliation, then require
exact-new-head checks before a separate merge decision. Require Windows tests, strict changed-file
Pyright, pinned Ruff, documentation checks and a no-bump verified wheel before
marking ready. Keep draft while any of those are pending. No merge, release,
production repin or benchmark is authorised.

The work register also contains older due reviews unrelated to this task; they
were not advanced or treated as permission to start those workstreams. Agents
A-D own their existing PyTUFLOW PRs; their source and shared CI are unchanged.


## 2026-10-25 - Independent final review and terminal CI

Live PR #160 is draft/open at `23c71935f32dd9435b6bc1487b44990f61e02d2a`,
branch `feat/stateful-command-preflight`, based on main
`bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2`. Review covered the complete current
diff, bounded static syntax, state/directory ownership, fail-closed planning,
group failure/provenance and exclusive report creation. No unresolved consequential
code finding. The comment-continuation review thread is resolved; the refreshed
[exact-head automated review](https://github.com/Chain-Frost/ryan-tools/pull/160#issuecomment-6381472313)
reports no findings. This local review does not post an approval.

All **six jobs in three workflows** are now terminal/success at this exact head:

| Workflow | Run | Result |
| --- | --- | --- |
| Python CI | [40075215949](https://github.com/Chain-Frost/ryan-tools/actions/runs/40075215949) | Policy, Hosted tests, Package, Python 3.13 compatibility and MCP all success |
| Fast PR checks | [40075215961](https://github.com/Chain-Frost/ryan-tools/actions/runs/40075215961) | Ruff and whitespace success |
| Command-plan contracts | [40075215930](https://github.com/Chain-Frost/ryan-tools/actions/runs/40075215930) | Windows focused suite and pinned style success; 93 tests |

Hosted tests log: **1,492 passed, 6 skipped, 7 deselected**, zero failures.
This supersedes the PR body's pending-hosted-regressions checkpoint. Package
completed the fresh no-bump wheel build, verification and isolated installed-wheel
smoke. All five checks required by live main rules pass. The current PR description
needs the same pending-to-success correction when GitHub writes are authorized;
no description or comment was posted here.

Local normal Windows Python **3.14.6**, installed PyTUFLOW **1.2.6**:

- `python -m pytest -q -W error tests/functions/tuflow/test_command_plan.py tests/orchestrators/tuflow/test_command_preflight.py tests/scripts/tuflow/test_command_preflight_wrapper.py`:
  **93 passed**, no skips.
- Adding `tests/orchestrators/tuflow/test_command_preflight_real.py`:
  **95 passed**, exit 0. That invocation also printed a Windows HDF5/h5py DLL
  diagnostic (`0xc0000139`) during PyTUFLOW import; this is recorded rather than
  described as a clean native-stack smoke. Hosted Windows evidence above is independent.
- Ruff check/format, strict Pyright (zero errors/warnings), compilation on the
  planner/orchestrator/wrapper: passed. No Python was modified in this review.
- Source-wrapper help succeeds through `python -c` / `runpy.run_path` from the
  source root. Direct copied-wrapper invocation against the user's older installed
  library cannot import the new planner, confirming the matching-package requirement.
  No global installation was changed; hosted Package proves the installed-wheel route.
- Documentation and whitespace results are recorded below after reconciliation.

Keep #158 separate. Its frozen diagnostic `f1f7958` and accepted native baseline
are unchanged; #160 introduces no runtime dependency on it. When shared register
edits meet, retain both this feature row and #158's completed #143/#156 closure.
The original parent #143 is already merged at `bcc8e5c`; previous unmerged/pending
entries are historical, not current blockers. The maintained standalone wrapper
is this PR's boundary: #159 does not acquire multi-TCF runner semantics merely
because this reader exists. Only adopt it later if the runner contract warrants it.
#161/PyTUFLOW streaming work and #145/RORB acceptance remain independent.

Delivery: documentation-only corrections remain unstaged/uncommitted in detached
`E:/Github/ryan-tools-review-160` at the exact head above. Runtime source, package
version, wheel, frozen harness and original checkout are unchanged. No local build
was required for this documentation-only follow-up. No staging, commit, push, merge,
publication, GitHub write, private model check, native benchmark or solver launch.
Next review: 2026-11-01. After authorized delivery, require CI on the changed head;
the green old head does not validate uncommitted documentation.


### Verified squash-merge and release boundary - 2026-10-25

Read current-main `.github/workflows/auto-publish.yml`,
`.github/workflows/pypi-publish.yml` and `repo-scripts/release_skip_policy.py`;
these files are unchanged by this PR. A successful post-merge main push Python CI
can initiate the automatic tag/release workflow, then trusted PyPI publication.
Green PR checks alone neither publish nor authorize a release.

Live repository allows squash merges only; ordinary merge and rebase merge are
disabled. Both #158 and #160 have CLEAN remote merge state against current main
`bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2`; local `git merge-tree --write-tree
origin/main HEAD` also exits 0 without touching the working tree or index.
Both remain drafts, so an authorized delivery must also mark each ready separately.

Exact publication opt-out: retain/add the case-insensitive `skip-release` PR label
before the merge and retain it through post-merge release processing; include the
literal `[skip release]` in the final squash commit title/body as the immutable
backstop. Both automatic release selection and the final publisher honor either
condition. The repository uses COMMIT_MESSAGES for the default squash body, so
inspect/edit the actual final squash message rather than assuming PR prose is
included. Do not change global automation. Recheck label, exact head, main tip,
required checks, draft/merge state and unresolved threads immediately before any
separately authorized squash merge.

Live #158 already has `skip-release` (and `docs`); #160 has no labels. Therefore
#158 currently opts out, while a #160 main merge with no opt-out could publish.
Recommend explicit suppression for these independent deliveries unless the owner
separately chooses publication. No labels, PR titles, commit messages or release
settings were changed. Local policy checks confirm label/marker each return skip,
and absence of both returns publish.


### Documentation validation and delivery state

Normal Windows Python 3.14.6: `python repo-scripts/check_documentation.py`
passes all seven default documents and central-index coverage. Explicit
`--links-only` checks pass for every locally changed Markdown file in this
review worktree; `git diff --check` passes. All local changes are documentation
only, unstaged and uncommitted. No new remote head or hosted run exists for
these corrections. Revalidate the exact delivered head after authorized commit/push.


### Authorized local commit delivery

The owner subsequently authorized local commits. This work is retained separately
on local branch `review/pr160-final-review` in the review worktree above.
Local validation is complete; the completion report identifies the resulting commit.
No push or new hosted run exists. Earlier uncommitted/scaffold-only delivery
statements are historical checkpoints, superseded by this authorized local delivery.
Next action: obtain authorization to push this existing PR's changes, then require
CI/review on its exact delivered head. No merge, publication or GitHub discussion
mutation is authorized. Original `E:/Github/ryan-tools` remains unchanged.


### Current validation priority

The owner explicitly deferred hosted CI until local development has progressed.
Hosted checks are not on this work's current critical path. Continue local review,
implementation and proportionate tests; record exact local source and validation.
An authorized future push may run CI for later checking. Earlier statements making
new-head hosted checks an immediate development blocker are superseded by this
instruction. Push, merge and publication remain separate authorization boundaries.
Generated `.validation/` evidence is excluded locally and is not part of the commit.


### Authorized PR-branch push

The owner authorized pushing completed work. Reviewed local change commit
`ed99fbcc26b03bcf735ea364e123fa82af64a230` and this delivery note are delivered independently to existing PR
branch `feat/stateful-command-preflight`. The remote was fetched immediately before delivery and
matched the expected parent; use an ordinary fast-forward push, never force.
Earlier unpushed/push-authorization-pending statements are historical checkpoints.
Hosted CI may run after the push and will be checked later, per owner direction;
no earlier-head hosted checks are claimed to validate the new head. Original
checkout source/index remain unchanged. No merge, release, GitHub comment or
native benchmark/solver execution is authorized by this delivery.
