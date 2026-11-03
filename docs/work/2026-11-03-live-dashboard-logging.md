# Live dashboard logging regression

| Field | Value |
| --- | --- |
| Status | PR #191 CI formatting repaired; final-head CI pending |
| Owner | ChatGPT |
| Created / Updated | 2026-11-03 |
| Next review | 2026-11-04 |
| Branch | `fix/live-dashboard-console-routing`, based on `b2b07da1` |
| Pull request | [#191](https://github.com/Chain-Frost/ryan-tools/pull/191) |
| Git delivery | Logging fix `3257de20`, handoff `96ce004c`, selection/startup `1c586fb2`, and GPU/batch display follow-up delivered in PR #191. |
| Next action | Verify terminal CI on the pushed formatting repair, then review; no merge or release authorised. |

The reported copied runner uses wrapper 2026-11-01.5 and library 26.11.2.3.
The shared renderer did not change between that version and the initially inspected
26.11.3.1 checkout. The 1 November runner integration (`99cd3cca`) replaced the older
runner's Rich console messages with Loguru output. A stdout stream captured before
Rich Live starts bypasses its cursor clearing and redraw, matching the partial
headers accumulating at launches and completions.

Main already includes PR #190 (`b2b07da1`, declared library 26.11.3.6), which supplies
a dynamic stdout writer in the new wrapper. Older copied wrappers still use the
library default. This fix makes that default follow current stdout on every write,
without changing explicit writers, file sinks, notebook routing or solver execution.
No new MCP surface is needed: this repairs existing presentation without changing
workflow inputs, outputs or safety semantics.

The focused regression fails on the captured-stream baseline and passes with the
fix. It checks clear-before-message/redraw-after-message sequences, multiline text,
three terminal sizes, file-log isolation, post-Live output, redirected output and
explicit custom sinks. Actual VS Code disconnect/reconnect is not reproduced;
terminal reflow is a separate remaining limitation, not a claimed fix.

The user's running process, project wrapper and simulation outputs remain untouched.
No release or merge is authorised by the request to create a PR.

## Validation and delivery

- Focused logging, dashboard/workflow and simulation-runner tests: **96 passed,
  2 skipped** on Windows Python 3.14.8. The two skips require Windows symlink
  creation privileges; no TUFLOW solver was launched.
- Strict Pyright with explicit `-p pyproject.toml` on both changed Python files:
  **0 errors, 0 warnings**. Explicit configuration avoids an unrelated drive-root
  Python 3.12 configuration discovered by the local Pyright launcher.
- Documentation/index checks, Loguru formatting policy and Git whitespace pass.
- `python repo-scripts/build_library.py --skip-pip` built and verified
  `dist/ryan_tools-26.11.3.7-py3-none-any.whl`; the local ignored wheel is not committed.
  Installed user packages are unchanged.
- Local Ruff execution is denied by Windows. The owner requested logic work here
  and Ruff/CI on another system; those checks remain explicitly pending.
- The local pytest cache directory is inaccessible; focused runs use a temporary
  cache outside the checkout and ignore failed `pytest-cache-files-*` entries.

## Run selection follow-up

The owner also reported that identical TCF names hide which event/scenario is
running. The dashboard now shows a `Selection` column using the immutable run's
flag/value pairs, for example `e1 01.0p e2 00030m s1 BigModel`. The model filename
remains in a separate, ellipsized column. Flags are display text only: launch
arguments, CSV identities and original Resume run numbers do not change.
Runs without flags show `(no flags)`.

The existing fake-process regression now verifies event/scenario selections in
both active and completed rows, alongside the original sparse Resume index and
TCF name. Focused runner/dashboard/logging tests pass **76 tests, 2 symlink-privilege
skips**; strict Pyright on the changed runner and test passes with **0 errors,
0 warnings**. Documentation and Loguru checks pass. The current 26.11.3.7 wheel
was rebuilt and verified with `--no-bump --skip-pip` for this same PR. Ruff and CI remain
deferred to another system at the owner's request.

## Startup output follow-up

Comparison with the pre-#159 runner confirmed that its `CoreParameters.dump()`
and `dump_simulations_preview()` printed settings and every planned run. The shared
planner now restores this output at INFO, including all effective runner settings,
each unique run's original index, model arguments, TCF and source location. The
heading explicitly says the preview is before Resume filtering and GPU assignment.
Older copied wrappers receive this library behaviour without changing their settings.

Wrapper 2026-11-03.2 additionally reports effective operation, continuation, export,
input-check, console-level, pause and follow-up controls after CLI overrides. The
clean external-GPU snapshot message is now SUCCESS, per owner request; incomplete
or unknown inspections remain warnings. Execution and continuation are unchanged.

Combined selection/startup checks: **145 passed, 2 Windows symlink-privilege skips**.
The preview regression checks all 480 rows; copied-wrapper export-only checks cover
source locations and effective controls, and the GPU test captures at SUCCESS level.
Strict Pyright on all five additionally changed Python files passes. Compilation,
source-wrapper help, documentation, Loguru policy and whitespace checks pass.
Direct script help against the user's older installed package cannot use the new
wrapper's `params.LOG`; validation uses matching checkout code and the rebuilt wheel,
without upgrading the user's installation. Ruff and hosted CI remain external work.

Selection/startup wheel verification and copied-wrapper smoke pass: installed the wheel without
dependencies into a disposable target, confirmed that import location, ran `--help`
and `--export-only --no-pause` on synthetic text input, and checked the full plan,
effective controls and TXT export with no CSV history or solver launch. The user's
normal Python installation is unchanged. Wheel SHA-256:
`d027d669dab30719bd7faa93bf246f40d432de42fc10667930eeb4046f414fc0`.

## GPU and batch display follow-up

The older dashboard had a GPU column. Restore this from the actual group passed
to the child, rather than predicting an assignment from row order. Active and
completed rows show `-pu0`, `-pu0 -pu1`, etc.; automatic placement says `Auto`,
queued rows say `Pending`, and unsuccessful launches say `Not launched`.
The persistent footer shows effective batch switches, such as `Batch: -b -t`,
excluding GPU tokens in BATCH_COMMANDS because the runner's GPU groups replace them.
Configured batch commands remain visible in the startup settings.

The rendered-output regression covers automatic, single-GPU and multi-GPU groups,
active/completed rows, and exclusion of an overridden `-pu9` batch token. Focused
runner/advisory checks: **76 passed, 2 symlink-privilege skips**. Strict Pyright on
both changed Python files, documentation, Loguru policy and whitespace checks pass.
The same PR wheel was rebuilt and verified without another version bump (SHA-256
`df5365f59aefab0c741cdaf953014eeeb54a8986fdb319d89aca07c8a8d61ef6`). Ruff/CI remain deferred
to another system, as requested; no installed package or running batch is modified.

## CI formatting repair - 3 November 2026

Verified PR head e15ab794 before repair. Fast PR checks and Policy both failed
only Ruff formatting of the SUCCESS-level assertion in
`tests/scripts/test_tuflow_advisory_runner.py`. Hosted tests, Package, MCP smoke
and POSIX security already passed on that head. Applied the pinned Ruff
formatter; six focused advisory tests and strict Pyright pass. Ruff lint and
format pass across all seven PR-changed Python files. Test-only formatting
requires no wheel rebuild. Repair committed/pushed to PR #191; exact new-head
hosted checks pending. Next review: 2026-11-04. No solver, merge or release.
