# Master log append outcomes and safe publication — #196

| Field | Value |
| --- | --- |
| Status | Draft implementation; current-head hosted/strict checks under review |
| Owner | WP08 integration |
| Created / updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Base | `main@5ad632cc9a17b302ad3f4f2b55315a1e2e8d1d51` (after #183 merged) |
| Delivery | [Draft PR #255](https://github.com/Chain-Frost/ryan-tools/pull/255), branch `fix/master-log-append-outcomes-196` |
| Primary issue | [#196](https://github.com/Chain-Frost/ryan-tools/issues/196); allocation [#240](https://github.com/Chain-Frost/ryan-tools/issues/240) and audit [#180](https://github.com/Chain-Frost/ryan-tools/issues/180) |

## Live baseline and ownership

The original dashboard PR [#183](https://github.com/Chain-Frost/ryan-tools/pull/183) is **merged into main**.
The Office safety PR #201 and shared exporter PR #202 were merged into that parent before its merge.
Do not reopen/rebuild these PRs. #194's closure raw CSV/Parquet handling is present in the merged parent;
live UNC hard-link support remains unverified. #199's native Office fidelity/UNC checks remain separate.

This bounded PR owns the **master append** orchestration, its maintained wrapper, the existing Excel
append helper and new dedicated regressions. It does not alter the POMM numerical files (WP03), the
processor outcome contract (WP04), discovery traversal (WP07) or closure statistics (WP09).

## Implemented contract pending validation

- Explicit `.xlsx` master selection is required via editable `MASTER_WORKBOOK` or `--master-workbook`.
  `WORKING_DIR=None` uses the invoking directory; `TABLE_NAME=None` chooses the first existing table
  or used range. No project `Q:` path ships as a default.
- A single dashboard remains active while duplicate keys are read, new TLFs are processed, complete
  frames are built and the workbook is saved. The existing per-log dashboard shares that instance.
- No new logs or all newly discovered incomplete logs yield SKIPPED with exit zero and no append.
  Failed log parsing or a partial batch (a mix of completed and failed/incomplete logs) yields FAILED
  with nonzero exit, even when completed rows are appended. Missing worker outcomes fail explicitly;
  only `OK` rows are eligible for aggregation.
- A staged sibling workbook is fully written before replacing the original master. The source's
  inode/size/mtime are rechecked before replace and its basic mode is retained. Writer/replace errors
  leave original bytes untouched and remove the owned temporary file; diagnostic errors propagate.
- The append intentionally updates an existing workbook, unlike the shared report export no-overwrite
  policy. No backup is automatically created. `.xlsm` and other extensions fail closed because this
  path does not guarantee VBA preservation. Users should keep a separate backup before intentional append.
  Cross-process races **after** the final source check are not eliminated; Windows Excel locks,
  SMB behaviour, file ACL preservation, mapped shares and concurrent-writer arbitration require native review.

## Validation and open gates

The initial draft implementation was pushed as `a817ec3ff6e2`; Ruff found five deterministic
issues and a follow-up lint repair was pushed as `aa05cded4f50`.
Focused disposable tests cover normal/empty/partial/failed/missing per-log outcomes,
saving and replacement failures, unexpected source modification, and macro refusal.

Hosted Python 3.14 checks on the **current head** must pass before reporting completion.
A normal Python 3.14 source checkout, strict changed-file Pyright, installed wheel and
copied-wrapper smoke are still to be confirmed; this session's container has Python 3.13
and cannot resolve github.com for a Git clone. The GitHub connector can publish commits,
but that does not substitute for a locally checked-out full repository worktree.
No production workbook, licensed solver, native Excel lock or UNC share was exercised.

**Next action:** resolve CI findings on PR #255; run/record exact-head validation including
copied installed wrapper success, failure and no-new-data parity; maintain draft status.
Then work on remaining dashboard adoption #198 in a separate reviewable PR, coordinating
with the POMM/closure/processor pack owners. Do not merge or release.
