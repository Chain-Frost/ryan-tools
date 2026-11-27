# Master log append outcomes and safe publication — #196

| Field | Value |
| --- | --- |
| Status | Local append and safe SMB refusal validated; current-head hosted CI/review pending |
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

## 2026-11-08 native Windows/Excel/SMB acceptance - FAILED / needs correction

Refreshed the existing child branch by normal merge of latest parent #251 `d61925675dffd86811ae9277b9ca53615e172f1f`; no force-push or child integration into parent. Tested code **`217638ce`**. Only the shared work-register conflict required resolution, retaining both owners' rows. This record update does not alter that tested implementation.

Windows 10 build 19045, Python 3.14.6, pandas 3.0.5, openpyxl 3.1.5, native Excel **16.0**, PyTuflow 1.3.0 at pinned `887534ea8c15b80d185a38a7ead1d42ab42a7167`. A no-bump build verified package 26.11.8.2, installed using `pip --no-deps --target` into disposable storage. Copied wrappers executed outside checkout, with wheel origin checked; ordinary user-site dependencies were retained. No global package installation, production input, solver or unsorted access.

| Check | Outcome | Evidence |
| --- | --- | --- |
| Focused source tests | PASS / one skip | `py -3.14 -m pytest -q -rs tests/orchestrators/tuflow/test_logsummary_append_outcomes_196.py`: 13 passed, one symlink privilege skip; `tests/functions/tuflow/test_logsummary_excel_append.py`: 4 passed |
| Independent installed wheel/copied wrapper, local + UNC | PASS | Dashboard on/off; completed append, duplicate/incomplete skip, injected writer error, invalid settings; original skip/failure bytes and owned-stage cleanup checked |
| Partial completed/incomplete batch | PASS | Copied wrapper appends completed row but returns exit 1 / FAILED |
| Existing table and workbook fidelity | PASS (bounded) | Local, UNC and mapped: Logs table expands A1:B2 to A1:B3; original row, formula, existing font/style, core title, freeze pane and second sheet retained |
| Actual Excel workbook lock | PASS | Owned invisible Excel 16.0 instance opens disposable workbook; append raises PermissionError; SHA256 unchanged; owned workbook/application closed |
| Windows sharing lock | PASS | Real CreateFileW deny-delete/write sharing handle, local/UNC/mapped: source preserved and owned stage cleaned |
| Save/replace failures and competing modification | PASS | Injected save/replace errors preserve bytes/listing; competitor modification during staged save is detected and retained |
| Local hardlink rejection | PASS | No mutation or staging residue |
| SMB/UNC and mapped hardlink rejection | **FAIL** | Both Python stat and Windows GetFileInformationByHandle report link count 1 despite samefile=True and equal inode for two existing hardlinks. The guard permits append, replaces the selected path and leaves the alias with old content |
| Concurrent writers | **FAIL** | Controlled overlap after both final identity checks, with ordered native replacements: both calls return OK but only writer2 survives. Reproduced local, UNC and mapped |
| Filename symlink | SKIPPED | Windows privilege unavailable; do not treat as passing native evidence |
| ACL preservation/denial, macros/charts/pivots/external links and private project workbooks | NOT RUN | No ACL changes or production files; bounded xlsx features above only |

The SMB hardlink failure is at `logsummary_excel_append.py`'s `_require_unlinked_master_workbook` link-count guard: a successful query cannot establish multiplicity on this server. Independent `samefile` and file IDs confirmed this was a real hardlink, not an ordinary copy. Do not claim supported rejection on this share. A safe correction needs a validated identity/link strategy or an explicit fail-closed supported-filesystem boundary; do not silently weaken protection.

The concurrent-writer reproduction coordinates only the gap between the existing signature check and `Path.replace`, without mocking the replacement itself. Both staged workbooks were built from the old master; both pass the check; writer1 replaces, then writer2 replaces. The final rows are old + writer2, with writer1 lost. This is the documented arbitration gap, now a measured native acceptance failure, not permission to accept it. The next owner should add whole-transaction writer arbitration or agree an explicitly narrower contract and obtain review before integration.

Commands/harnesses: `py -3.14 installed_acceptance.py 196 <fresh-local-or-UNC> <result>`, `py -3.14 native_workbook.py <fresh-local-UNC-or-mapped> <result>`, `py -3.14 partial196.py`, `py -3.14 excel_lock.py`, from private `E:/Github/wp08-native-20261108` with PYTHONPATH set to its installed196 wheel target. JSON evidence records output hashes, destination listings and row outcomes; raw logs and internal share paths stay private. Main and unrelated worktrees were preserved.

**Disposition:** native acceptance is incomplete and **must not be signed off**. No implementation fix was attempted as part of validation; the server identity limitation and writer-arbitration policy need scoped review. Keep #255 draft; no parent incorporation or conditional Fixes #196, main merge, release or issue closure. Next action: owner review of the two reproductions and a narrowly scoped safety correction; next review 2026-11-15.

## Coordinated append publication correction — 2026-11-08

The native test on [#255](https://github.com/Chain-Frost/ryan-tools/pull/255#issuecomment-6719032302) found two simultaneous writers could both return OK after reading the same master and overwrite one appended row. A new **exclusive on-filesystem `.master.xlsx.writing` claim** now protects the *entire* master workflow — including duplicate detection, source discovery, processing, staging and replacement — not only the final signature check. A separate public append helper acquires the same claim when used independently; the orchestrator passes its existing claim through to avoid self-deadlock. A competing invocation **fails clearly**, rather than claiming success or losing a row. The claim's own inode/device identity is checked before cleanup, so an invocation does not knowingly delete a replacement claim. Crashed processes can leave a stale claim requiring explicit operator inspection before deletion. Basic local regressions check claim contention, exclusion before workbook discovery and cleanup.

**Remaining native blocker:** The SMB server reports hard-link count 1 even for genuine aliases, so `st_nlink` does not establish the safety of replacement. This code change **does not** solve that case; the [reported SMB alias severing](https://github.com/Chain-Frost/ryan-tools/pull/255#issuecomment-6719032302) remains an acceptance failure. Before supporting master replacements on this share, independently probe Windows `FindFirstFileNameW` hard-link enumeration (documented for SMB 3.0, but dependent on server capabilities); when multiplicity cannot be established, **fail closed** or obtain explicit maintainer acceptance of a narrower local-filesystem-only contract. Do not assume the link-count flag is accurate across SMB servers.

The new exclusive claim requires **native Windows/Excel/SMB concurrent-writer validation** after this code change; hosted regression results are not enough. No code or data were merged into #251 or `main`. #196 remains open and the PR remains draft.

## 2026-11-08 outstanding completion: supported filesystem boundary

Refreshed the existing #255 branch from immediate parent #251 at
`dc6d74455e092d3611b975f0fc3975a977578ecc` by normal merge `cdb83ad9`.
Shared docs/register conflicts retain both owners' rows and all new parent CI,
PyTuflow wheel and accepted work. The exclusive claim and calibrated hardlink
guard are unchanged. Unsupported enumeration now produces a clear ValueError
explaining that master append is unsupported on that filesystem, with the native
OSError retained as its cause. The maintained script README documents the boundary,
claim cleanup and local NTFS alternative. No unsafe fallback is introduced.

Native alternative-API investigation (owned genuine hardlinks, Windows 10,
Python 3.14.6, local/mapped/UNC): GetFileInformationByHandleEx FileStandardInfo
reports **1 then 2 links on NTFS**, but **1 then 1 on both SMB spellings** despite
samefile=True. NtQueryInformationFile FileStandardLinkInformation returns **1/2
correctly locally**, but **STATUS_NOT_SUPPORTED (0xc00000bb) on SMB**. FileIdInfo
works locally but returns **ERROR_INVALID_PARAMETER (87) on SMB**. Earlier
FindFirstFileNameW ERROR_NOT_SUPPORTED (50) remains decisive. A unique file ID
alone cannot establish absence of other names; probing additional APIs does not
justify weakening the calibrated guard. Hardlinks are supported by the server;
reliable enumeration/multiplicity is not. Ordinary master append is therefore
**unsupported on this tested SMB server**. No claim that SMB in general cannot
support it is made.

Windows source focused tests: **23 PASS / 1 symlink privilege SKIP**. Strict
modified-file Pyright, Ruff and formatting pass. Native direct follow-up:
**12 PASS** (ordinary append or explicit share refusal, genuine hardlink refusal,
held claim and abandoned claim across NTFS/mapped/UNC); **the same 12 PASS on an
isolated installed wheel**. Refusals preserve workbook/alias bytes; no owned
staging or calibration files remain. Local successful append preserves table,
formula, font and other sheet. The historical two-real-process native test at
`f6db94f7` remains valid for the unchanged claim implementation: OK/BUSY on NTFS,
REFUSED/BUSY on this SMB server. Do not relabel it as a new-head process run.
Unaffected accepted Excel-lock tests were not repeated. SMB successful publication
and broader Office features/ACL preservation remain NOT RUN/unsupported.

Built/verified 26.11.8.3 wheel SHA256
`c5466e9a1dd63f04bf90b7022306b23ed4c638444d7843df2777e14e40eca272`;
installed with --no-deps --target outside checkout. Global packages untouched.
Private evidence/harnesses: `E:/Github/wp0508-completion-20261108`.
Earlier native failure section is retained as historical and superseded by the
claim correction already reviewed at f6db94f7 plus this explicit boundary.
MCP assessment: existing maintained master append workflow remains sufficient;
no new endpoint is needed for this diagnostic correction.

Next: exact-head current CI and fresh Codex review. #196's dashboard/failure/safe
local append acceptance is implemented, subject to final checks; unrestricted
SMB master publication is explicitly not offered. Keep #255 draft pending review,
no parent incorporation, issue closure, auto-merge or release. Next review 2026-11-15.
