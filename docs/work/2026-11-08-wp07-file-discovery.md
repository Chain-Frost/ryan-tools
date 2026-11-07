# WP07 — Physical discovery and truthful input outcomes

| Field | Value |
| --- | --- |
| Status | Active — #203 hosted validated; #195 implemented in a stacked draft pending exact-head CI |
| Owner | WP07 agent / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | `main@5ad632cc9a17`; WP01 PR #251 on `fix/tlf-hyphenated-variables-239`; WP07 PR #257 on `fix/wp07-physical-file-discovery-203` |

## Outcome and scope

Allocation [#240](https://github.com/Chain-Frost/ryan-tools/issues/240), [#203](https://github.com/Chain-Frost/ryan-tools/issues/203) and [#195](https://github.com/Chain-Frost/ryan-tools/issues/195). WP07 owns physical file discovery and truthful invalid/partial/empty/filtered inputs. The parent [#251](https://github.com/Chain-Frost/ryan-tools/pull/251) remains the integration branch: draft [#257](https://github.com/Chain-Frost/ryan-tools/pull/257) targets it, not main. Integration into WP01 and any final squash into main require maintainer review and explicit authorization. No solver runs or production storage scans.

## Current state

- **#203 implemented, not yet accepted.** The finder tracks stat-derived physical directory keys across symlinks, junctions and overlapping roots, retaining user path spelling; inaccessible or missing roots and incomplete directory traversals now raise rather than return partial success. Returned paths are sorted and deduplicated by spelling; file hardlinks and symlink aliases intentionally remain separate.
- Focused tests cover self/ancestor directory symlinks, root aliases, overlapping roots, nonrecursive selection, lexical path preservation, matched-file symlink policy, unreadable subdirectory and explicit empty versus missing roots; Windows `mklink /J` is included.
- **#195 implementation in [draft #268](https://github.com/Chain-Frost/ryan-tools/pull/268), stacked on #257, not yet accepted.** The shared collector and orchestrators reject invalid/empty type selections, missing or non-directory roots, matched empty/unavailable files and partial scans. Explicit wrapper overrides remain explicit. Log-summary input discovery rejects zero-byte TLFs; the existing parser's swallowed read failures are detected as FAIL for nonempty unreadable logs, and completed logs with malformed headers fail while genuinely incomplete runs skip.
- Synthetic regressions now cover invalid/mixed roots, valid empty directories, empty/zero-byte matched files, wrapper preflight behavior, TLF unreadable/malformed/incomplete cases and dashboard on/off status parity. Existing historical tests that assumed invalid inputs were silently ignored were updated.
- WP08 #265 independently owns PO QA dashboards (CI passes as of this review), WP03 remains responsible for numerical result population and WP04 for processor lifecycle. WP07 does not change their implementations. #195 final native/CI acceptance still needs evidence.

## Next action

Review and repair exact-head CI on #268 (parent #257 already has all six hosted jobs green), then verify installed/copied-wrapper no-pause success/failure parity and TLF read-failure propagation. Reconcile native Windows junction/mapped-drive/UNC/SMB acceptance separately; do not claim it from synthetic hosted tests. Keep both children stacked and unmerged until reviewed for integration into #251, and add closure references to the parent only after incorporation.

## Completion criteria

- Physical directories visited once even through junction/symlink cycles; stable, justified returned-path policy and ordering.
- Missing, unreadable, zero-byte/malformed and partial discovery are visibly failed, while genuinely empty and intentionally filtered batches remain nonfailures.
- PO/POMM, culvert maximums/timeseries, mean/median and log-summary workflow final statuses and exit codes agree with dashboard enabled/disabled.
- Required synthetic tests, changed-file Ruff and strict Pyright, focused workflows, package/copied-wrapper checks and native linked-path evidence recorded accurately.

## Validation and delivery

2026-11-08: first-delivery changes and regression tests committed/pushed to #257; subsequently stacked on the #251 head through non-force merge commits. GitHub verified the PR file comparison is limited to WP07 code/test files. **All six #257 hosted jobs passed at `6666a6fa` (2,534 passed, 14 skipped, 8 deselected); new #268 CI is pending final-head verification.** Local Python 3.14, strict Pyright, focused pytest, package build and copied-wrapper checks have **not** been run in the connector environment; Git HTTPS DNS resolution failed, so a separate local filesystem worktree could not be provisioned. This is a known validation limitation, not acceptance. Native Windows junction and real mapped-drive/UNC path behavior remain open until observed test results are recorded.

## Progress

### 2026-11-08

Read #240, #203, #195, current AGENTS.md, DEVELOPMENT_GUIDE, register and existing dashboard status records; confirmed no implementation PR named by WP07. Implemented the bounded finder fix and focused regressions first, established draft #257, then followed the maintainer's request to stack #257 onto WP01 #251 with no force push, preserving reviewable issue boundaries and parent PR history.

### 2026-11-08 — Second delivery (#195)

Created isolated branch `fix/wp07-discovery-outcomes-195` from #257 and draft [#268](https://github.com/Chain-Frost/ryan-tools/pull/268), stacked on #257 → #251. Implemented strict shared discovery semantics, early orchestrator preflight, preserved explicit empty list overrides in six maintained wrappers, and made zero-byte/unreadable/malformed completed TLF inputs fail rather than skip. Added focused synthetic cases and updated stale empty-input expectations. No main merge or auto-merge authorised. Full current-head CI, native Windows/SMB active-writer checks and installed copied-wrapper acceptance are not yet recorded as passing.
