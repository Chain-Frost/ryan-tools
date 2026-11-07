# Master log append outcomes and safe publication — #196

| Field | Value |
| --- | --- |
| Status | Code head validated in six GitHub CI jobs; draft review and native acceptance outstanding |
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

The implementation has been committed and pushed to draft PR #255 with its
focused disposable workbook regressions (completed/empty/partial/failed/missing
per-log outcomes, staged save and replace failures, concurrent source edits,
and `.xlsm` refusal). Ruff and strict Pyright issues from early iterations
were corrected before full CI success.

**Validated code head:** `d72775a7ea1f74b4ffba14019f446614024c5984`.
[Fast PR checks](https://github.com/Chain-Frost/ryan-tools/actions/runs/42391707881)
and [Python CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/42391707880)
completed with all six jobs passing: Ruff and whitespace, POSIX wrapper security,
MCP smoke, Python 3.14 hosted tests (**2,534 passed, 14 skipped, eight deselected**),
Policy (strict changed-file Pyright **zero errors**, documentation check) and
Package (built wheel, installed-wheel smoke, copied log-summary dashboard
on/off/writer-failure and six existing result-wrapper smoke checks).

**Native acceptance outstanding:** current real Excel lock/replace behaviour,
mapped or UNC/SMB concurrent writers, original Excel feature fidelity,
symlink/hardlink policy, and copying the revised master-append wrapper into
a user's installation. Mocked write failures are not equivalent to an actual
locked Office workbook. No production workbook, licensed solver, or share
was used, and no such acceptance is claimed. This environment had no usable
GitHub DNS and no Python 3.14 interpreter, so the full repo could not be
checked out into a local Git worktree; GitHub connector commits provided
the isolated remote branch and hosted CI exercised the code. This missing
local worktree is a handoff constraint, not a validated step.

A documentation-only status refresh after `d72775a` will have a different
head SHA. Validate that updated head's checks before review completion.

**Next action:** review draft #255; seek native Excel/SMB/UNC acceptance and
copied master-wrapper verification on an installed Python 3.14 wheel, without
modifying production workbooks. Bounded QA adoption is in separate draft
[#265](https://github.com/Chain-Frost/ryan-tools/pull/265) (#198).
POMM/closure adoption remains coordinated with WP03/WP09. Do not merge or release.
