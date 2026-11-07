# PO QA dashboard and truthful workflow outcomes — #198

| Field | Value |
| --- | --- |
| Status | Draft implementation — focused hosted validation and review outstanding |
| Owner | WP08 integration |
| Created / updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | `main@5ad632cc9a17b302ad3f4f2b55315a1e2e8d1d51` |
| Delivery | Draft PR (created on handoff), `feat/po-qa-dashboard-outcomes-198` |
| Issues | [#198](https://github.com/Chain-Frost/ryan-tools/issues/198), [#180](https://github.com/Chain-Frost/ryan-tools/issues/180), [#240](https://github.com/Chain-Frost/ryan-tools/issues/240) |

## Scope and ownership

This is a bounded #198 adoption delivery for existing maintained PO QA workflows:
`check_po_timeseries_peaks.py`, `check_timeseries_stability.py`, and their two
existing orchestrators. It uses the shared `LiveWorkflowDashboard` lifecycle
without changing peak/stability calculations, input filters, export file names,
output schemas, the existing `ProcessPoolExecutor`/map execution, or interactive
pausing. Data transformation remains in existing `po_timeseries_checks` helpers.

The master-log append is a separate first-delivery draft PR #255 (#196). The
#183/#201/#202 integration is already merged in main. The #198 POMM mean/median
report and closure-duration dashboard **remain outstanding**, because WP03 owns
POMM aggregation/data semantics and WP09 owns closure durations. Coordinate
a follow-on PR for display-only POMM/closure changes after those contracts settle.

## Proposed behaviour

- One shared renderer per QA batch, from discovery through processing,
  aggregation and export. Default enabled in both wrappers, configurable
  `USE_LIVE_DASHBOARD`, `LIVE_REFRESH_PER_SECOND`, `LIVE_MAX_ROWS`,
  overridden by the existing common CLI switches including `--no-live-dashboard`.
- Per-input `OK`/no-data `SKIP` statuses, and explicit overall `SUCCESS`
  only after the exporter returns. Empty discovery or no matching series is
  `SKIPPED` (zero exit, no output). A worker or exporter exception is
  `FAILED`, propagates through the orchestrator and gives nonzero wrapper exit.
- Dashboard on/off does not alter selected rows, result ordering, export format,
  or the existing processing engine. The pool's ordered `map` results supply
  completion updates; a displayed `RUNNING` row denotes the coordinator's
  current awaited result, not an authoritative all-worker utilisation view.
  File stat metadata and ETA are not required.
- Validate dashboard numeric settings before input scans. Preserve editable
  list-based user filters; explicit empty CLI overrides are not silently ignored.
- Existing `main_processing` callers remain valid because display arguments
  are optional; the return value becomes `bool` for an explicit successful or
  skipped workflow and exceptions continue to indicate failures.

## Validation and native acceptance

New disposable/mock regressions check dashboard on/off, no-input skips,
worker failures, exporter failures, preservation of output-on-failure
behaviour, and validation before discovery. Existing PO QA and wrapper
smoke tests remain applicable. Actual Windows Python 3.14 hosted checks,
changed-file Ruff and strict Pyright, installed wheel, copied wrapper
and report parity are required before sign-off; no such result is claimed
until those checks pass.

No proprietary TUFLOW project, licensed solver, SMB/UNC share or locked Excel
workbook has been exercised. Native mapped-drive and output-lock acceptance
remains separately unverified. This agent's container could not clone GitHub
(DNS unavailable), so this branch was isolated remotely through the GitHub
Git APIs; a real local worktree and native copied-package run were not available.

Next action: clear current-head hosted/strict checks and obtain review on this
bounded QA draft. Coordinate POMM/closure adoption only after WP03/WP09
ownership handoff. Do not merge to main, enable auto-merge or release.
