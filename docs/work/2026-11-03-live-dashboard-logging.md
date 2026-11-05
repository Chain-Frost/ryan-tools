# Live dashboard logging regression

| Field | Value |
| --- | --- |
| Status | PR #208 single-refresh follow-up; native reproduction and local checks pass |
| Owner | ChatGPT |
| Created / Updated | 2026-11-03 / 2026-11-05 |
| Next review | 2026-11-06 |
| Branch | `fix/dashboard-single-refresh`, stacked on PR #206 at `fffc13e3` |
| Pull request | [#208](https://github.com/Chain-Frost/ryan-tools/pull/208); historical PR #192 is merged. |
| Git delivery | Single-refresh source/test/version commit `af358f48` pushed; documentation follow-up is on the same branch. |
| Next action | Verify final-head PR #208 CI and the user's installed test; keep both #206 and #208 unmerged unless authorized. |

## 2026-11-05: native partial-frame reproduction and single refresh owner

The user's fixed-size VS Code terminal still retained partial dashboard headers
with wrapper 2026-11-04.2 and library 26.11.5.5, despite correct dynamic stdout routing.
The startup delay was separately resolved by PR #206. The table fragments appeared
before the input-check success message, so that message alone was not the trigger.

Read-only native console inspection confirmed ANSI cursor support and three visible
dashboard headings in the terminal buffer. Simpler synthetic dashboards did not
reproduce it. A controlled native Windows terminal at 151 columns by 23 rows, using
real background input validation and fake solver processes, retained four headings
and four summary rows. No solver was launched, and no CSV/export callbacks were
supplied. Private model/control paths and raw captures remain outside the repository.

The runner already refreshes the renderer from its coordinator loop, but Rich Live
also started a background refresher. Disabling that redundant thread eliminated the
partial frames in the controlled reproduction: all three single-refresh trials
retained one heading and one summary, including two trials of the isolated installed
wheel. Input checks passed in every trial. This is native console evidence, not a
claim that every terminal/resize scenario is covered. A focused regression checks
that no auto-refresh worker exists and SUCCESS/WARNING messages clear and redraw
the grouped table; it fails under the previous refresh policy.

Validation: 172 focused tests passed, two Windows symlink tests skipped. Ruff lint
and format, strict Pyright on both changed Python files, Loguru policy and whitespace
passed. Wheel 26.11.5.6 was built and verified (SHA256
`1521a1417e30aad89aaa62f844cdcfb1c3a8749d57fe02be575cc03f72b57e92`), installed in
an isolated target for the native trials, then installed into the user's normal
Python 3.14 for the requested test. The project wrapper's `--help` passed. Existing
run settings and model outputs were preserved; normal Resume can skip completed
cases, so the user may choose `--run-mode all` when deliberately retesting them.

PR #208 is stacked on #206 to retain the one-second optional inspection budget and
CodeMeter troubleshooting. The primary checkout's unrelated version edit remains
untouched. This is an internal display correction; existing MCP workflow discovery
is sufficient. Final-head CI is tracked on PR #208. No merge or public release is
authorized. The user's actual post-install interactive rerun remains to be confirmed.

## Historical 2026-11-03 investigation

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

## Presentation follow-up - 3 November 2026

PR #191 is merged as `31f898de`. This new PR restores grey default settings,
cyan changed/required settings and green SUCCESS messages; RUN_VARIABLES prints
one Python-style entry per line. The old wrapper's defaults map was removed in
#159. Comparisons now use current RunnerSettings defaults (-b, LOW, minimized)
and explicit current run-control defaults, independent of edited wrapper values.
Required paths and nonempty run selections have no universal baseline.

Separate event/scenario dashboard columns are computed once from the full
proposed plan before Resume filtering, sorted e1..e9 then s1..s9. Missing cells
are blank; launch arguments, CSV identities and original run numbers are unchanged.
The wrapper passes the complete plan solely for presentation; only pending runs
are launched. Older callers can omit this optional argument.

Validation: 140 focused logging/runner/wrapper tests passed, two Windows symlink
privilege skips; strict Pyright passed on all six changed Python files. Loguru
policy passed. Wheel 26.11.3.9 built and verified, and a disposable installation
passed copied-wrapper --help and --export-only --no-pause smoke with colour
assertions, no CSV and no solver. The normal installed package is unchanged.
Local Ruff execution is blocked by Windows (WinError 5); hosted formatting/CI
remains pending. No new MCP surface: existing runner presentation only.

Implementation is in a separate temporary worktree to preserve the user's
pre-existing main-checkout pyproject.toml version edit. Changes are committed and pushed in [PR #192](https://github.com/Chain-Frost/ryan-tools/pull/192);
Hosted Ruff/whitespace passes on `848afa18`; final-head Python CI remains pending. Documentation and whitespace checks pass. Next review: 2026-11-04.

Follow-up review against the last single-file implementation found three remaining
presentation losses, outside this PR: completion lines omit GPU/duration/selections
and use print time rather than observed finish time; the dashboard no longer explains
finished results waiting for ordered printing; launch commands lost token-specific
colours. Startup-only external GPU checks and the one-CSV Resume redesign were
explicit integration decisions. This was a source comparison, not full runtime parity
validation; no additional execution defect was confirmed.

Hosted Ruff identified one combined test assertion; it was split at `848afa18`.
The existing release tag v26.11.3.8 points to merged PR #191, so this PR's wheel
was advanced and rebuilt as 26.11.3.9 to avoid reusing that published version.

Final version/record follow-up is committed with this entry. The 26.11.3.9 installed-wheel
copied-wrapper help/export smoke also passes. Main retains only the original version edit.
