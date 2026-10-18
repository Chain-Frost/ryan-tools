# Repository improvement roadmap

## Status and baseline

| Item | Value |
| --- | --- |
| Reviewed | 2026-10-06 |
| Status | Deferred: opportunity inventory only; no repository-wide implementation front is selected |
| Owner | Unassigned |
| Next review | 2027-04-05 |
| Branch / commit inspected | `main` / `12441a31` |
| Package version inspected | `26.10.6.3` |

This is a high-level opportunity inventory, reviewed on 6 October 2026 against current `main`. It is deliberately
deferred: none of the remaining opportunities is an authorised implementation front merely because it appears here.
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
| Package import repairs | Historical lazy script aliases and the `ryan_functions` alias were removed from the PR #120 branch (2026-10-18), leaving maintained direct imports in `ryan_library`. The prior 26.10.6.3 smoke test validated the old interface, not this breaking removal. |
| Logging pipeline | [Implementation outcome](audits/2026-08-08-logging-pipeline-implementation-plan.md#implementation-outcome) records independent thresholds, queue context, deterministic shutdown, notebook setup, AST policy checks and Windows validation. Current implementation retains these mechanisms; historical test results are not a fresh run. |
| Compatibility inventory | [Compatibility policy](COMPATIBILITY_POLICY.md) records replacements and deadlines. Remaining inventory reconciliation is listed below. |
| Lifecycle decisions | `data_processing.py` is deprecated through 2026-12-31; `tkinter_utils.py` is absent; [missing-run analysis](../ryan_library/orchestrators/tuflow/tlf_missing_runs.py) has an orchestrator; the [HY-8 wrapper](../ryan-scripts/tuflow/tuflow_to_hy8.py) calls the bridge. These are no longer simply undecided removal/experimental candidates. |
| PO/POMM combination | Both maintained orchestrators use [shared workflow coordination](../ryan_library/orchestrators/tuflow/_combination_workflow.py) and expose `export_results`. |
| Timeseries checks | Notebook helpers and the peak/stability orchestrators reuse `po_timeseries_checks`; wider notebook collection/orchestration consolidation still requires assessment. |
| Root document cleanup | The former testing architecture/task documents and logging checklist are absent. The completed [README implementation plan](../implementation_plan.md) is deliberately retained at the repository root as indexed historical implementation evidence; no archival move is pending. |

## Remaining opportunities

These are deferred opportunities, not due tasks. Do not start them opportunistically or report them as overdue before
the next review date. If a maintainer selects one, create a bounded work record and move that work front to the
[work register](work/README.md).

### 1. Reconcile lifecycle and import evidence

Tracked in [ryan-tools issue #98](https://github.com/Chain-Frost/ryan-tools/issues/98).

Priority: medium when explicitly selected.

- Reconcile the compatibility inventory with actual namespaces, warnings, supported replacements and known callers,
  including `ryan_functions`. Do not treat the old 88-file classification as current.
- Verify ordinary source-checkout and installed-wheel imports separately, including optional-dependency isolation, as
  part of that bounded review. Recent wheel verification proves the current package can be built and imported for its
  tested paths; it is not a complete lifecycle inventory.
- Assess remaining notebook discovery/processing duplication against orchestrators only if there is a concrete
  maintenance need. Preserve notebook return values, serial defaults and process-local logging.
- Record current maintained status and validation for the HY-8 and missing-run entry points in a new dated lifecycle
  review if this opportunity is selected.

No published API should be removed solely because a static search finds no callers. Check documentation, history and
known external use, and respect recorded support deadlines.

### 2. Resolve upstream HY-8 demo ownership

Tracked upstream in [run-hy8 issue #4](https://github.com/Chain-Frost/run-hy8/issues/4).

Priority: medium-low; separate upstream follow-up.

`run-hy8` is now an independent PyPI dependency, not a `vendor/run_hy8` submodule. The parent
repository retains a maintained HY-8 bridge and wrapper. If duplicate TUFLOW/domain-specific demos
still exist upstream, review them in the `run-hy8` repository; do not reintroduce vendoring as part
of parent-repository cleanup.

### 3. Keep directory renames deferred until caller inventory

Priority: low. No broad rename is scheduled.

Potential normalization of `TUFLOW-python`, `RORB-python`, `AutoCAD-python` and `12D-python` requires an inventory of
copied wrappers, shortcuts, batch files, workspace tasks, packaging and external project references first. Retain
`ryan-scripts` and `repo-scripts`. Renaming solely for style does not justify migration risk.

### 4. Maintain proportionate validation

This is ongoing maintenance policy, not a backlog project that can become overdue.

Use the [validation matrix](DEVELOPMENT_GUIDE.md#validation-by-change-type): focused checks for bounded changes,
environment-specific smoke checks where required, and the full Windows runner only when scope justifies it. Keep
synthetic fixtures, repository-local Windows temporary/cache paths and explicit source-checkout HY-8 setup. New runtime
defects should get their own bounded work record rather than reopening completed projects.

## Separately tracked or removed from this backlog

- **Unsorted migration:** incomplete but deferred. It is tracked separately in the
  [unsorted roadmap](UNSORTED_UPGRADE_ROADMAP.md) and work register. It is not being actively progressed and should not
  be treated as part of the general repository-improvement backlog unless explicitly resumed.
- **README implementation-plan disposition:** resolved by retaining `implementation_plan.md` at the repository root as
  indexed historical evidence. Moving it solely for tidiness would add churn without improving discovery.
- **Scheduled compatibility removals:** the scripts namespace was brought forward and retired in PR #120
  on 2026-10-18. Other deprecations remain tracked in the
  [compatibility policy](COMPATIBILITY_POLICY.md) with their existing deadlines.

## Scheduled compatibility removal

`ryan_library.scripts` was brought forward for removal in PR #120 on 2026-10-18. After 2026-12-31,
review the separate deprecated GDAL environment/runners modules, `data_processing`, and
POMM wrapper-compatibility APIs against the [compatibility inventory](COMPATIBILITY_POLICY.md).
Verify remaining callers before removing expired code. The unrelated deprecation deadlines
are unchanged and the scheduled review remains 2027-01-04.

## Out of scope or deferred proposals

- Broad processor refactors, submodule cleanup as parent code, history rewriting and blanket folder renaming.
- GitHub Actions enablement without a concrete automation requirement and maintenance owner.
- Python 3.15 lazy-import migration: future proposal only. Reassess supported Python, actual language behavior and
  measurable benefit when the repository changes its Python baseline; do not treat declarations as proof of lazy loading.

## Latest review — 2026-10-06

- Reviewed current `main` at `12441a31` / package `26.10.6.3` after PR #94.
- Reconciled the roadmap with the work register: the unsorted migration remains incomplete but is deferred and tracked
  separately, so it is not an active repository-improvement task and should not be surfaced as overdue before its
  explicit review date.
- Resolved the completed README plan's disposition by deliberately retaining and indexing it as historical evidence.
- Kept lifecycle/import reconciliation, upstream HY-8 demo ownership and possible directory renames as deferred
  opportunities only. None is authorised merely because it is listed here.
- Left scheduled compatibility removals under their separate policy and 4 January 2027 review.
- Next repository-wide backlog review: 5 April 2027, unless a maintainer explicitly selects an opportunity earlier.
  Agents should not repeatedly surface this deferred backlog before that date.
- This review changes documentation/status only; it does not claim runtime validation beyond the current evidence
  already recorded by the relevant completed work.
