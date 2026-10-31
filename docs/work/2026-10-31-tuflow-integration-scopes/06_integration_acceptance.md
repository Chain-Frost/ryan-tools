# Scope 06 — Integration, acceptance and handoff

**Superseding integration decision (31 October 2026):** Imported BAT/TXT is
model-selection data only and is never executed. Python controls actual
executable and launch cwd. Keep the first distinct TCF/flags/model-option
selection and discard later duplicates; historical instructions below
requiring duplicate preservation or source execution context are superseded.

Status: prepared for delegation on 2026-10-31. This is a verification scope, not evidence that the changes have been implemented or accepted. Read [the scope index](00_scope_index.md) and the candidate repository's current instructions.

## Objective and boundary

Verify the combined behaviour of scopes 01-04 on PR #159, including the independently reviewable CSV lifecycle cases. Keep scope 05 independent; raster-header validation is excluded from the main release.

The reviewer/integrator owns cross-package checks, current documentation and delivery records. Individual implementation owners retain their focused tests and files. Do not add unrelated tests, benchmarks, packaging redesign or broad formatting changes. Do not treat old native benchmark results or green checks on an earlier revision as acceptance of a new candidate.

## Required main-PR acceptance

| Case | Required observation |
| --- | --- |
| Correct PR #17 installation | The expected public validation API imports and a small synthetic batch validates through the installed package. Record the immutable parser revision and actual import origin. |
| Published package with the same version but missing capability | Checker/check-only fails with an actionable installation diagnostic; normal advisory runner warns accurately; no fallback to retired code. |
| Optional parser is unused | Help, export and ordinary execution with preflight disabled do not require unused PyTUFLOW capabilities. Actual execution still validates its executable and performs the one startup GPU snapshot. |
| Help and export-only | No runtime executable-existence check, GPU allocation or occupancy scan is performed. Plan/configuration checks still apply to export. |
| Two TCFs with identical flags | Checker and launcher both retain two correctly attributed runs. Interleave TCF A/B/A to exercise regrouping and global index restoration. |
| Python TCF conflict | Any effective imported mismatch stops the entire plan before validation, export, CSV reset/attempt writes or launches. Report Python path, imported path and file/line. |
| Optional Python TCF | `None` permits multiple explicit TCFs; supported abbreviated rows use a configured fallback; missing both is a clear error. Equivalent path spellings are compared at their intended bases; on Windows, verified mapped-drive/UNC aliases also match without opening the model. |
| Stateful source file | REM/commented rows are ignored; sequential SET/named TCF variables and supported directory changes affect only the appropriate later runs. |
| Multiple input files | File ordering and per-file provenance are retained, with no implicit variable/directory-state leakage between independent files. |
| Unsupported or unreadable later input | No earlier valid commands have launched. No partial export is presented as a complete plan. |
| Explicit defaults and duplicates | A full TUFLOW command without scenario/event flags remains a run; later equivalent selections are discarded before validation/export/launch. |
| Launch fidelity | Fake processes capture Python-configured executable, child cwd and exact model argument lists. No imported executable, CD or START setting controls execution; source TCF paths retain correct resolution. |
| Editable TXT export | Export and reparse produce equivalent unique effective runs and report duplicate removals. Python engine settings control BCMD; imported batch/GPU switches do not. Refresh the fixed wrapper-stem TXT, including all-complete Resume; no timestamps, backups or BAT/CMD outputs. Shared EXE/BCMD/TCF variables remain editable; no predicted GPU allocation is embedded. |
| Export replacement | Rendering failure preserves prior output; imported/model/wrapper/history paths and aliases are protected. A temporary replacement file is cleaned after success or failure. |
| Sparse Resume preflight | Filter only the launch queue, retain full plan/history and original positive IDs, and validate all pending runs even when their IDs have gaps. |
| Inherited BAT environment | Checker and runner capture equivalent environment state; SET/directory changes in one file do not leak into another. |
| Advisory input validation | One frozen final selected run set is checked. The scheduler does not wait for the result. Missing/unresolved/unsupported/error/incomplete states cannot become a false all-present message. Every finding retains provenance. |
| Startup GPU check | Exactly one external occupancy snapshot per actual invocation; none on export; none on dashboard refresh or between runs. It does not alter the configured scheduling policy. |
| Process lifecycle | Register a child immediately after successful spawn, retain ownership if priority-setting fails, reap observed completions, restore signal handlers and return accurate aggregate status. |
| Completion observation and dispatch | Emit the structured completion outcome and invoke its recording hook immediately when observed, independently of ordered console output. Scope 04 owns durable CSV history and tests its persistence. |
| Wrapper boundary | Installed/copied wrappers work without repository path hacks; help/no-pause/default overrides/version banners and exit behaviour follow the maintained standard. |
| Retired functionality | No active caller imports deleted adapters, parser-mode branches or local raster-header inspection. Necessary regression coverage targets the sole supported API. |

Use synthetic models and fake processes for deterministic launch and failure tests. Real TUFLOW execution is unnecessary to prove argument propagation or most lifecycle cases. A native process smoke check should use harmless child processes first; any private/production model run needs separate task authority.

## Additional acceptance for the CSV/resume child

Recreate the user's representative batch with synthetic/fake execution: 400 current runs, 100 observed successes, three failures and the remainder unstarted or unfinished. Resume queues 300 runs, keeps their current plan order and skips successes wherever they occur.

Also verify:

- One CSV belongs to the supplying wrapper, independent of per-model cwd; repeated invocations append rather than truncate it.
- START is persisted before launch and FINISH immediately when observed; launch failures and killed/unclosed attempts remain distinguishable.
- Latest attempt is selected by its attempt identity/order, so an old success cannot hide a newer failed/unclosed attempt and a late finish cannot complete the wrong attempt.
- Runtime GPU, display index, working directory, executable path and TCF directory do not change run identity. TCF filename and effective simulation flags (including engine/model options) do. Reject ambiguous same-name/flag selections across different TCFs. Repeated equivalent selections are removed before identity construction.
- Resume with no log runs the complete plan. Old rows not present in the regenerated plan are retained but not executed.
- Run all starts a new history cycle in the same file. Interrupting it halfway then using Resume cannot inherit successes from the previous cycle.
- An interrupted final CSV record cannot manufacture a successful finish or destroy earlier valid history; earlier corruption or unsupported schema is reported clearly.
- A log-open/write failure is explicit. Single-writer behaviour is documented and tested at the chosen implementation boundary.
- Continuation reads only the regenerated plan and CSV. No TLF/TSF/global-activity/process-recovery scan, saved manifest or per-session folder appears.
- Reruns deliberately use current model inputs/output destinations; there is no automatic archive/delete/rename or solver checkpoint workflow.

## Environment and verification sequence

1. Record candidate/base SHAs, current PyTUFLOW PR #17 SHA, interpreter, OS and installed package origins. Refresh the candidate after concurrent integration changes.
2. Run focused planner, preflight, runner and wrapper tests for the changed behaviour. Use the scope-specific suites; broaden only for a concrete remaining risk.
3. Run repository Ruff formatting/lint and strict Pyright on modified Python files. Run the Loguru check if logging calls changed.
4. Build according to the repository instructions and verify the installed wheel from a neutral directory with a copied wrapper. Do not substitute worktree imports for installed-package acceptance.
5. On Windows normal Python 3.14, exercise the existing GIS-wheel bootstrap and the pinned PyTUFLOW public API. Record DLL/import failures honestly; a skipped parser-dependent test is not acceptance.
6. Verify new/updated documentation links and user-facing settings against the delivered implementation. Scope-only documentation edits need documentation validation, not solver or full regression execution.
7. Record hosted and native evidence against the exact code revision and distinguish historical or environment-limited results.

No private SMB benchmark, claim of unchanged performance or full-model parity is required merely to approve this refactor. If making such a claim, provide a separately scoped matched test rather than recycling older measurements.

## Delivery

Update the existing #159 work record and relevant register row with completed packages, open items, exact evidence and next action. Ensure current PR text links the scope index and does not present removed parser modes, raster-header checking or recurring activity polling as the intended release.

Keep #159 and the CSV child independently reviewable. Review their diffs against the intended bases; preserve unrelated main-branch changes. No merge, release, automatic merge setting or production execution is authorised by this checklist.
