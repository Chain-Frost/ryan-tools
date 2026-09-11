# Repository improvement roadmap

## Status and baseline

| Item | Value |
| --- | --- |
| Reviewed | 2026-09-06 |
| Status | Planned: remaining work needs a bounded task selection |
| Owner | Unassigned |
| Next review | 2026-09-20 |
| Branch / commit inspected | `main` / `f7118e2` |
| Package version inspected | `26.08.31.6` |

This is the current high-level backlog, refreshed from the 8 August roadmap against repository source and documents.
The dated [lifecycle audit](audits/2026-08-08-ryan-library-lifecycle-plan.md) remains historical evidence; its file counts
and classifications are not a current inventory. Canonical architecture remains in the
[development guide](DEVELOPMENT_GUIDE.md).

Find concurrent work and review dates in the [work register](work/README.md). Follow
[work tracking](WORK_TRACKING.md) when starting or handing off a substantial task. This roadmap owns the repository-wide
backlog; a selected implementation project should have its own linked status record without duplicating its progress here.

## Completed or substantially implemented

| Area | Evidence and remaining qualification |
| --- | --- |
| Repository hygiene and packaging | Python 3.14, Ruff formatting and strict Pyright configuration; authoritative package metadata in `pyproject.toml`; maintained build/install utilities; generated coverage ignored. |
| Resource extraction | QGIS and Excel resources are separate pinned submodules; `setup.py` stages required QML resources into the package. No history rewrite is planned. |
| Maintained wrappers | [Wrapper standard](../ryan-scripts/WRAPPER_STANDARD.md) and shared wrapper utilities establish CLI, editable defaults, logging and process-boundary behavior. This does not certify every legacy script. |
| Documentation foundation | Architecture, environments, logging and the central documentation index are established. |
| Package import repairs | [Package initializer](../ryan_library/__init__.py) has explicit lazy legacy aliases; [compatibility initializer](../ryan_functions/__init__.py) forwards requested modules lazily instead of importing every discovered module. Separate source/wheel runtime verification was not repeated in this refresh. |
| Logging pipeline | [Implementation outcome](audits/2026-08-08-logging-pipeline-implementation-plan.md#implementation-outcome) records independent thresholds, queue context, deterministic shutdown, notebook setup, AST policy checks and Windows validation. Current implementation retains these mechanisms; historical test results are not a fresh run. |
| Compatibility inventory | [Compatibility policy](COMPATIBILITY_POLICY.md) records replacements and deadlines. Remaining inventory reconciliation is listed below. |
| Lifecycle decisions | `data_processing.py` is deprecated through 2026-12-31; `tkinter_utils.py` is absent; [missing-run analysis](../ryan_library/orchestrators/tuflow/tlf_missing_runs.py) has an orchestrator; the [HY-8 wrapper](../ryan-scripts/tuflow/tuflow_to_hy8.py) calls the bridge. These are no longer simply undecided removal/experimental candidates. |
| PO/POMM combination | Both maintained orchestrators use [shared workflow coordination](../ryan_library/orchestrators/tuflow/_combination_workflow.py) and expose `export_results`. |
| Timeseries checks | Notebook helpers and the peak/stability orchestrators reuse `po_timeseries_checks`; wider notebook collection/orchestration consolidation still requires assessment. |
| Root document cleanup | The former testing architecture/task documents and logging checklist are absent. [README implementation plan](../implementation_plan.md) records implementation; its archival disposition remains open. |

## Remaining work

### 1. Reconcile lifecycle and import evidence

Priority: medium. Select a bounded review before further deletion or consolidation.

- Reconcile the compatibility inventory with actual namespaces, warnings, supported replacements and known callers,
  including `ryan_functions`. Do not treat the old 88-file classification as current.
- Verify ordinary source-checkout and installed-wheel imports separately, including optional-dependency isolation.
- Assess remaining notebook discovery/processing duplication against orchestrators. Preserve notebook return values,
  serial defaults and process-local logging; shared timeseries checks are already implemented.
- Record current maintained status and validation for the HY-8 and missing-run entry points in a new dated lifecycle
  review rather than silently rewriting the original audit.

No published API should be removed solely because a static search finds no callers. Check documentation, history and
known external use, and respect recorded support deadlines.

### 2. Continue script triage by workflow family

Priority: medium. Detailed migration status belongs in the [unsorted upgrade roadmap](UNSORTED_UPGRADE_ROADMAP.md).

- Reconcile its remaining unchecked and in-progress items with current source, submodule disposition and environment
  validation before selecting the next family. Its previous test results do not prove later changes were validated.
- Include `python-not-polished`, `other`, `unsorted-python`, root standalone utilities and legacy batch folders in
  candidate inventories. `misc-python` exists again and contains tracked HEIC conversion and Excel utilities.
- Classify each candidate before moving it. Keep narrow utilities standalone unless reuse is demonstrated; move
  reusable processing into functions and complete reusable workflow coordination into orchestrators.
- Preserve copied-wrapper behavior, editable project settings and independent submodule state.

### 3. Resolve upstream HY-8 demo ownership

Priority: medium-low; separate upstream follow-up, not a completed milestone.

The inspected vendored checkout still contains TUFLOW/1D-network demo scripts. The parent repository has the HY-8
mapping wrapper and bridge. Verify live upstream status and ownership, then propose removal or deprecation of duplicate
domain-specific demos upstream if appropriate. Do not modify vendored content as routine parent-repository cleanup.
Live upstream status was not checked during this refresh.

### 4. Finish documentation disposition and maintain work visibility

Priority: medium-low.

- Deliberately archive or retain the completed root `implementation_plan.md`, updating index links if moved.
- Use the [work register](work/README.md) for concurrent work, due reviews and resumable handoffs.
- Keep local guides beside their code and link every repository-owned Markdown file from [the index](README.md).
- Preserve dated audits as evidence; use a new dated audit for a materially changed baseline.

### 5. Keep directory renames deferred until caller inventory

Priority: low. No broad rename is scheduled.

Potential normalization of `TUFLOW-python`, `RORB-python`, `AutoCAD-python` and `12D-python` requires an inventory of
copied wrappers, shortcuts, batch files, workspace tasks, packaging and external project references first. Retain
`ryan-scripts` and `repo-scripts`. Renaming solely for style does not justify migration risk.

### 6. Maintain proportionate validation

Ongoing maintenance, not a permanently completable project.

Use the [validation matrix](DEVELOPMENT_GUIDE.md#validation-by-change-type): focused checks for bounded changes,
environment-specific smoke checks where required, and the full Windows runner only when scope justifies it. Keep
synthetic fixtures, repository-local Windows temporary/cache paths and explicit source-checkout HY-8 setup. Remove
tests for deliberately removed APIs as part of their lifecycle change. New runtime defects should get their own
bounded work record rather than reopening the entire completed logging project.

## Scheduled compatibility removal

After 2026-12-31, review `ryan_library.scripts`, the deprecated GDAL environment/runners modules, `data_processing`
and associated aliases against the [compatibility inventory](COMPATIBILITY_POLICY.md). Confirm callers have migrated,
then remove expired code with documentation, focused checks and wheel-content verification. The inventory owns exact
deadlines and replacements. Other namespaces require an explicit support decision; do not infer a deadline for them.

This work is deferred until the support date, with a review scheduled for 2027-01-04 in the work register.

## Out of scope or deferred proposals

- Broad processor refactors, submodule cleanup as parent code, history rewriting and blanket folder renaming.
- GitHub Actions enablement without a concrete automation requirement and maintenance owner.
- Python 3.15 lazy-import migration: future proposal only. Reassess supported Python, actual language behavior and
  measurable benefit when the repository changes its Python baseline; do not treat declarations as proof of lazy loading.

## Latest handoff — 2026-09-06

- Reviewed repository source, linked plans, file presence and branch/package metadata at `f7118e2`.
- Replaced stale critical/high-priority implementation tasks with completed evidence and bounded remaining work.
- Next action: select one remaining lifecycle review or script family, assign an owner and create/link its work record.
- Blockers: no technical blocker identified; remaining implementation scope is not selected.
- Validation on 2026-09-06 in normal user Python: `python repo-scripts/check_documentation.py` passed all seven default
  documents and central-index coverage; explicit `--links-only` checks passed all eight touched Markdown files,
  including the new work guide/register. `git diff --check` passed. No runtime tests, wheel imports or build were
  rerun for these documentation-only changes; earlier runtime validation is explicitly historical.
- Git state at handoff: this documentation refresh is uncommitted. Pre-existing staged timeseries stability,
  water-level profile orchestrator and profile-table script changes are unrelated and were preserved.
