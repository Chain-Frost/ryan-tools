# WP07 — Physical discovery and truthful input outcomes

| Field | Value |
| --- | --- |
| Status | Needs review — #203 and #195 implementations hosted validated; native acceptance and parent integration outstanding |
| Owner | WP07 agent / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | `main@5ad632cc9a17`; WP01 PR #251 on `fix/tlf-hyphenated-variables-239`; WP07 #257 on `fix/wp07-physical-file-discovery-203`, child #268 on `fix/wp07-discovery-outcomes-195` |

## Outcome and scope

Allocation [#240](https://github.com/Chain-Frost/ryan-tools/issues/240), [#203](https://github.com/Chain-Frost/ryan-tools/issues/203) and [#195](https://github.com/Chain-Frost/ryan-tools/issues/195). WP07 owns physical file discovery and truthful invalid/partial/empty/filtered inputs. The parent [#251](https://github.com/Chain-Frost/ryan-tools/pull/251) remains the integration branch: draft [#257](https://github.com/Chain-Frost/ryan-tools/pull/257) targets it, while draft [#268](https://github.com/Chain-Frost/ryan-tools/pull/268) targets #257; neither child targets main. Integration into WP01 and any final squash into main require maintainer review and explicit authorization. No solver runs or production storage scans.

## Current state

- **#203 implemented, not yet accepted.** The finder tracks stat-derived physical directory keys across symlinks, junctions and overlapping roots, retaining user path spelling; inaccessible or missing roots and incomplete directory traversals now raise rather than return partial success. Returned paths are sorted and deduplicated by spelling; file hardlinks and symlink aliases intentionally remain separate.
- Focused tests cover self/ancestor directory symlinks, root aliases, overlapping roots, nonrecursive selection, lexical path preservation, matched-file symlink policy, unreadable subdirectory and explicit empty versus missing roots; Windows `mklink /J` is included.
- **#195 implementation in [draft #268](https://github.com/Chain-Frost/ryan-tools/pull/268), stacked on #257, not yet accepted.** The shared collector and orchestrators reject invalid/empty type selections, missing or non-directory roots, matched empty/unavailable files and partial scans. Explicit wrapper overrides remain explicit. Log-summary input discovery rejects zero-byte TLFs; the existing parser's swallowed read failures are detected as FAIL for nonempty unreadable logs, and completed logs with malformed headers fail while genuinely incomplete runs skip.
- Synthetic regressions now cover invalid/mixed roots, valid empty directories, empty/zero-byte matched files, wrapper preflight behavior, TLF unreadable/malformed/incomplete cases and dashboard on/off status parity. Existing historical tests that assumed invalid inputs were silently ignored were updated.
- WP08 #265 independently owns PO QA dashboards (CI passes as of this review), WP03 remains responsible for numerical result population and WP04 for processor lifecycle. WP07 does not change their implementations. #195 final native/CI acceptance still needs evidence.

## Next action

Review the validated #268 diff and integration dependencies with WP01/WP08; verify new/copied wrapper headless success/failure behavior beyond the package CI smoke as needed. Reconcile native Windows junction/mapped-drive/UNC/SMB acceptance separately; do not claim it from synthetic hosted tests. Keep both children stacked and unmerged until reviewed for integration into #251, and add closure references to the parent only after incorporation.

## Completion criteria

- Physical directories visited once even through junction/symlink cycles; stable, justified returned-path policy and ordering.
- Missing, unreadable, zero-byte/malformed and partial discovery are visibly failed, while genuinely empty and intentionally filtered batches remain nonfailures.
- PO/POMM, culvert maximums/timeseries, mean/median and log-summary workflow final statuses and exit codes agree with dashboard enabled/disabled.
- Required synthetic tests, changed-file Ruff and strict Pyright, focused workflows, package/copied-wrapper checks and native linked-path evidence recorded accurately.

## Validation and delivery

2026-11-08: first-delivery changes and regression tests committed/pushed to #257; subsequently stacked on the #251 head through non-force merge commits. GitHub verified the PR file comparison is limited to WP07 code/test files. **Both #257 and #268 passed all six hosted checks:** #257 at `6666a6fa` (2,534 passed, 14 skipped, 8 deselected), #268 code+register head `065d807a` ([Python CI 42400583651](https://github.com/Chain-Frost/ryan-tools/actions/runs/42400583651), [Fast checks 42400583617](https://github.com/Chain-Frost/ryan-tools/actions/runs/42400583617); 2,558 passed, 14 skipped, 8 deselected; strict Pyright, Ruff, package/installed-wheel smoke, MCP and POSIX security passed).** Local Python 3.14, strict Pyright, focused pytest, package build and copied-wrapper checks have **not** been run in the connector environment; Git HTTPS DNS resolution failed, so a separate local filesystem worktree could not be provisioned. This is a known validation limitation, not acceptance. Native Windows junction and real mapped-drive/UNC path behavior remain open until observed test results are recorded.

## Progress

### 2026-11-08

Read #240, #203, #195, current AGENTS.md, DEVELOPMENT_GUIDE, register and existing dashboard status records; confirmed no implementation PR named by WP07. Implemented the bounded finder fix and focused regressions first, established draft #257, then followed the maintainer's request to stack #257 onto WP01 #251 with no force push, preserving reviewable issue boundaries and parent PR history.

### 2026-11-08 — Second delivery (#195)

Created isolated branch `fix/wp07-discovery-outcomes-195` from #257 and draft [#268](https://github.com/Chain-Frost/ryan-tools/pull/268), stacked on #257 → #251. Implemented strict shared discovery semantics, early orchestrator preflight, preserved explicit empty list overrides in eight maintained wrappers, and made zero-byte/unreadable/malformed completed TLF inputs fail rather than skip. Added focused synthetic cases and updated stale empty-input expectations. No main merge or auto-merge authorised. Full current-head CI, native Windows/SMB active-writer checks and installed copied-wrapper acceptance are not yet recorded as passing.

### 2026-11-08 — Hosted verification reconciliation

At the first comprehensive #268 hosted test run, 2,553 passed with five failures caused by historical tests asserting that malformed inputs were returned as empty/invalid processor objects or that an unreadable nonempty TLF was an intentional skip. The assertions now check FAIL outcomes, retaining intentional location-filter skips. Strict typing of modified legacy mock tests and Ruff formatting were also corrected. Current-head checks on the later commit remain pending; earlier failed/cancelled CI runs must not be treated as final acceptance. The validated-empty culvert collection now uses an informational SKIPPED log rather than an ERROR log. No local worktree/native share acceptance is claimed.

### 2026-11-08 — Hosted acceptance milestone

Latest WP07 #195 code and register were validated together at `065d807a`: all six GitHub Actions jobs green on Windows/Python 3.14, with 2,558 hosted tests passed, 14 skipped and eight deselected. Earlier five assertion failures and legacy strict-typing diagnostics were repaired before this passing head. The updated status record is a documentation-only handoff after the validated code SHA, not evidence of a new local worktree or native share acceptance. A valid empty collection now logs an informational SKIP. No additional implementation scope was authorized. The remaining native gate is real mapped-drive/UNC/SMB path, permission, lock and active-writer behavior; WP01 must retain precise parser/read-failure diagnostics, and WP08 must preserve outcome parity when integrating its report wrappers. Issue closure on #251 is not configured until reviewed child changes are actually incorporated there.
