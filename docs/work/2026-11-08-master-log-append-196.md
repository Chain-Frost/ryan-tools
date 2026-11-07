# Master log append outcomes and safe publication — #196

| Field | Value |
| --- | --- |
| Status | Code and copied-wrapper CI green; native Excel/UNC acceptance outstanding |
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
  path does not guarantee VBA preservation. The selected workbook must be a regular file,
  not a filename symlink or hard-linked inode; those are refused before mutation and
  checked again before replace to avoid silently severing another path to the master.
  Users should keep a separate backup before intentional append.
  Cross-process races **after** the final source check are not eliminated; Windows Excel locks,
  SMB behaviour, file ACL preservation, mapped shares and concurrent-writer arbitration require native review.

## Validation and open gates

The reviewed implementation is on draft [#255](https://github.com/Chain-Frost/ryan-tools/pull/255),
branch `fix/master-log-append-outcomes-196`, stacked on #251, never merged to main.

**Validated code head:** `19393f7d528f28518be9c6e6759ae6f20af112a1`.
[Fast PR checks](https://github.com/Chain-Frost/ryan-tools/actions/runs/42402187889)
and [Python CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/42402187709)
passed all six jobs: Ruff/whitespace, Windows Python 3.14 hosted tests
(**2,560 passed, 14 skipped, eight deselected**), strict changed-file Pyright
(zero errors), documentation/logging policy, MCP smoke, POSIX wrapper
security, package build and installed-wheel tests.

The newly added copied **master append** wrapper smoke passes on the isolated
installed wheel in Windows CI. It verifies dashboard on/off workbook parity,
a completed TLF append while retaining existing rows, duplicate and
incomplete-log skip preservation, invalid display settings and a forced
staged writer exception with source bytes and temporary files preserved.
Existing copied log-summary and six result-wrapper checks also pass.

Codex initially found a P2 linked-workbook publication defect on `06951e59`.
The implementation now rejects symbolic links and hard-linked master workbook
files before mutation, and checks again before staged replace. Focused
regressions cover both aliases and a hardlink added during staging. A fresh
Codex review on `19393f7d` found no major issues, and the resolved thread
records the fix. This does not prove live share/Excel behaviour.

**Native acceptance outstanding:** actual Excel-file lock/replace behaviour,
Office workbook feature/metadata fidelity, mapped/UNC/SMB concurrent writers
and filesystem link permissions/races, plus a user-site copied wrapper test
against real permitted project data. No production workbook, licensed solver
or real network share was accessed. The GitHub connector provided isolated
remote branches and hosted CI because the agent container cannot clone
GitHub and has Python 3.13. No local Python 3.14 Git worktree was created.

This status-record-only commit follows validated code head `19393f7d`;
verify its new exact-head documentation/CI checks before reviewer sign-off.

**Next action:** review the draft and obtain only the outstanding native Excel
and network acceptance on disposable permitted workbooks. Do not merge into
main. Related [#265](https://github.com/Chain-Frost/ryan-tools/pull/265)
covers QA, POMM and closure dashboard adoption separately, with partial-input
and native acceptance tracked under #198. The child PR carries proposed
`Fixes #196`; the consolidation parent #251 must not carry an issue-closing
keyword until this child is reviewed and actually incorporated.
