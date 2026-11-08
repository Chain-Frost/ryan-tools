# WP07 — Physical discovery and truthful input outcomes

| Field | Value |
| --- | --- |
| Status | Needs review — PASS WITH NATIVE LIMITATIONS; Windows 10 local/SMB acceptance recorded, parent integration outstanding |
| Owner | WP07 agent / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | `main@5ad632cc9a17`; WP01 PR #251 on `fix/tlf-hyphenated-variables-239`; WP07 #257 on `fix/wp07-physical-file-discovery-203`, child #268 on `fix/wp07-discovery-outcomes-195` |

## Outcome and scope

Allocation [#240](https://github.com/Chain-Frost/ryan-tools/issues/240), [#203](https://github.com/Chain-Frost/ryan-tools/issues/203) and [#195](https://github.com/Chain-Frost/ryan-tools/issues/195). WP07 owns physical file discovery and truthful invalid/partial/empty/filtered inputs. The parent [#251](https://github.com/Chain-Frost/ryan-tools/pull/251) remains the integration branch: draft [#257](https://github.com/Chain-Frost/ryan-tools/pull/257) targets it, while draft [#268](https://github.com/Chain-Frost/ryan-tools/pull/268) targets #257; neither child targets main. Integration into WP01 and any final squash into main require maintainer review and explicit authorization. No solver runs or production storage scans.

## Current state

- **Latest acceptance:** [2026-11-08 native report](https://github.com/Chain-Frost/ryan-tools/pull/268#issuecomment-6717640488) records PASS WITH NATIVE LIMITATIONS at source `292b5047` / code `065d807a`, installed verified wheel 26.11.7.13. Local junctions, copied-wrapper outcomes/exports and 25 authorised mapped/UNC/SMB cases passed. Windows 11, privileged symlinks and real ACL-denied-directory traversal remain unverified. Earlier connector-only limitations below are historical, superseded only by the explicitly recorded cases.
- **#203 implemented, not yet accepted.** The finder tracks stat-derived physical directory keys across symlinks, junctions and overlapping roots, retaining user path spelling; inaccessible or missing roots and incomplete directory traversals now raise rather than return partial success. Returned paths are sorted and deduplicated by spelling; file hardlinks and symlink aliases intentionally remain separate.
- Focused tests cover self/ancestor directory symlinks, root aliases, overlapping roots, nonrecursive selection, lexical path preservation, matched-file symlink policy, unreadable subdirectory and explicit empty versus missing roots; Windows `mklink /J` is included.
- **#195 implementation in [draft #268](https://github.com/Chain-Frost/ryan-tools/pull/268), stacked on #257, not yet accepted.** The shared collector and orchestrators reject invalid/empty type selections, missing or non-directory roots, matched empty/unavailable files and partial scans. Explicit wrapper overrides remain explicit. Log-summary input discovery rejects zero-byte TLFs; the existing parser's swallowed read failures are detected as FAIL for nonempty unreadable logs, and completed logs with malformed headers fail while genuinely incomplete runs skip.
- Synthetic regressions now cover invalid/mixed roots, valid empty directories, empty/zero-byte matched files, wrapper preflight behavior, TLF unreadable/malformed/incomplete cases and dashboard on/off status parity. Existing historical tests that assumed invalid inputs were silently ignored were updated.
- WP08 #265 independently owns PO QA dashboards (CI passes as of this review), WP03 remains responsible for numerical result population and WP04 for processor lifecycle. WP07 does not change their implementations. #195 final native/CI acceptance still needs evidence.

## Next action

Review the [native acceptance report](https://github.com/Chain-Frost/ryan-tools/pull/268#issuecomment-6717640488) and accept or schedule the remaining Windows 11/symlink/ACL-directory limits. Incorporate through #268 → #257 → #251 only after review, preserving WP01 parser read/path diagnostics and WP08 outcome parity; rerun affected validation on the integrated head. Closing references `Fixes #203` and `Fixes #195` belong on #251 only after actual incorporation. No merge or auto-merge is authorised.

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

### 2026-11-08 — Native Windows and authorised SMB acceptance

[Full acceptance report](https://github.com/Chain-Frost/ryan-tools/pull/268#issuecomment-6717640488). Actual host is Windows 10 Pro 10.0.19045 with Python 3.14.6, not the requested Windows 11. Downloaded exact #268 source `292b5047cbb8adb9ec1cc4838bae0ece2fc5fa04` and verified artifact 13584589094 from code `065d807ac2d511e1cb911fc1deece09e0b4e7e8d`; installed wheel 26.11.7.13 into a disposable `--target` without altering existing packages. All 193 library Python files match the source byte for byte. No Git CLI or solver used, no main or existing checkout changes, no submodule inspection.

The exact requested pytest command was blocked at session startup by absent ZIP test-data resources. The same six synthetic files with `--noconftest -q -rs` produced **47 passed, 0 failed, 4 symlink-privilege skips** (WinError 1314). Native self/ancestor junctions, reversed alias/overlap roots, deterministic ordering and hardlink spelling passed; junction cleanup was nonrecursive. A coordinated real disappearing-directory scan raised incomplete with path/OS context. Nine copied wrappers exercised empty/invalid inputs; positive synthetic exports passed across eight result wrappers, with reopened supplemental workbooks and preserved inputs/output sentinels. Corrected real exclusive-handle CSV/TLF cases exit 1 with path context on both dashboard paths; shareable reads exit 0. Incomplete active-writer TLF reads skip rather than fail.

The user authorised a unique disposable child of `T:\\temp`; **25 SMB cases passed** using mapped and equivalent UNC roots: spelling, physical alias deduplication in both orders, missing/empty distinction, 16 exclusive/shareable held-file wrapper cases, active mapped writer/UNC reader (20 SKIPs), and a real directory removed through the alternate spelling before enumeration. Server/share identifiers and raw evidence remain private. No production data, permissions or mappings changed.

Recommendation **PASS WITH NATIVE LIMITATIONS**; no WP07 defect reproduced or correction proposed. Windows 11, privileged symlinks, real ACL-denied directories, other SMB configurations and interactive dashboard visuals remain outside established acceptance. POMM peak root tests use copied `main` because no roots CLI is exposed; those two workflows have no dashboard execution parameter. Initial harness setup errors are disclosed in the report and superseded by corrected cases.

Private evidence: `C:\\Users\\Ryan\\.codex\\reports\\wp07-acceptance-2026-11-08-0c82a229` (report, scripts, JSON, JUnit and logs). Source head was rechecked unchanged at testing completion. Delivery consists of the PR comment and documentation-only record/register updates on #268; no implementation change, parent closing keywords, merge or publication. Earlier hosted CI evidence remains tied to `065d807a`; documentation-update CI is separate. Next review remains 2026-11-15.
