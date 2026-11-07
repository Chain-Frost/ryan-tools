# WP07 — Physical discovery and truthful input outcomes

| Field | Value |
| --- | --- |
| Status | Active — #203 first delivery pushed in draft; #195 pending shared outcome integration |
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
- **#195 not implemented.** Validated roots/types before any output or log mutation, zero-byte/malformed selection, mixed-validity statuses, and direct/dashboard exit parity need shared consumers. Coordinate the outcome model with WP08, TLF read errors with WP01, and processor success/invalid semantics with WP04. Do not modify their owned modules without coordination.

## Next action

Review exact-head CI for #257, repair any changed-file failures, and get real Windows junction/mapped-drive/UNC acceptance for the chosen physical-identity behavior. Then implement #195 in a separately reviewable child of #257 after agreeing the existing shared outcome API with WP08. Preserve a valid empty scan as a skip while rejecting invalid and incomplete inputs before publication.

## Completion criteria

- Physical directories visited once even through junction/symlink cycles; stable, justified returned-path policy and ordering.
- Missing, unreadable, zero-byte/malformed and partial discovery are visibly failed, while genuinely empty and intentionally filtered batches remain nonfailures.
- PO/POMM, culvert maximums/timeseries, mean/median and log-summary workflow final statuses and exit codes agree with dashboard enabled/disabled.
- Required synthetic tests, changed-file Ruff and strict Pyright, focused workflows, package/copied-wrapper checks and native linked-path evidence recorded accurately.

## Validation and delivery

2026-11-08: first-delivery changes and regression tests committed/pushed to #257; subsequently stacked on the #251 head through non-force merge commits. GitHub verified the PR file comparison is limited to WP07 code/test files. **Fast PR checks (Ruff + whitespace) passed on 64a1d344; broader Python CI was queued at that check.** Local Python 3.14, strict Pyright, focused pytest, package build and copied-wrapper checks have **not** been run in the connector environment; Git HTTPS DNS resolution failed, so a separate local filesystem worktree could not be provisioned. This is a known validation limitation, not acceptance. Native Windows junction and real mapped-drive/UNC path behavior remain open until observed test results are recorded.

## Progress

### 2026-11-08

Read #240, #203, #195, current AGENTS.md, DEVELOPMENT_GUIDE, register and existing dashboard status records; confirmed no implementation PR named by WP07. Implemented the bounded finder fix and focused regressions first, established draft #257, then followed the maintainer's request to stack #257 onto WP01 #251 with no force push, preserving reviewable issue boundaries and parent PR history.
