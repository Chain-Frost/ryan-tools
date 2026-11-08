# WP10 — PO scenario plotting fidelity (#225)

| Field | Value |
| --- | --- |
| Status | Active — implementation prepared; CI and native acceptance pending |
| Owner | WP10 implementation agent / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Allocation | [#240](https://github.com/Chain-Frost/ryan-tools/issues/240), [#225](https://github.com/Chain-Frost/ryan-tools/issues/225) |
| Consolidation | Draft [#251](https://github.com/Chain-Frost/ryan-tools/pull/251), feature-branch only |
| Delivery | Branch: work/wp10-po-scenario-plots-225; PR and exact head recorded in the PR description |

## Scope and lifecycle decision

The plotter remains a maintained, **project-specific standalone script** under
ryan-scripts/TUFLOW-python/po_and_timeseries. It consumes the existing PO processors;
there is no new shared orchestrator, PyTuflow format reader or HDF cache.
This is the explicit AB-017 lifecycle decision: project-specific editable roots,
plot selections and interactive Matplotlib are intentional. Editable SHOW_PLOTS
and EXPORT_PNG settings now permit headless output without running a solver.
The wheel-selected entry point stays in place; copied-script behaviour must
still be validated against an installed wheel on Windows.

## Implementation

- A literal case-insensitive INTERNAL_NAME_FILTER rejects absent/no-match run identities
  instead of restoring the unfiltered frame. Selected LOCATION_FILTER is enforced
  after either CSV processing or Parquet loading; absent columns fail.
- Each location/measurement is plotted on a separate figure with recognized
  units and a numeric simulation-hours axis. Series retain full internalName,
  scenario label and measurement Type, rather than mixing Q/H or two runs.
- Time/value coercion requires finite numeric values; numeric strings remain
  simulation hours. No datetime conversion or row-index fallback.
- The **separate PO plot Parquet cache** carries an embedded provenance manifest:
  schema/script version, ryan-tools version, POProcessor source hash, CSV path,
  size and mtime_ns, root/glob, and ingest filters. Cache mode verifies source
  inventory and manifest, rejecting old, stale or incompatible files.
- CSV mode refuses to replace an existing cache without
  REPLACE_PARQUET_CACHE=True. Completed temp files publish via an atomic rename
  only on explicit overwrite; otherwise a same-directory exclusive hardlink
  provides race-safe no-clobber creation and **fails closed** if unavailable.
  A write failure leaves the previous target unchanged.
- Optional per-measurement PNG export uses deterministic bounded names and the
  same protected publication policy, controlled by OVERWRITE_PNG=False.

The manifest uses filesystem size/mtime rather than hashing entire source CSVs;
externally altered files that preserve both metadata fields cannot be detected.
Cache mode deliberately requires accessible source CSV metadata. Legacy Parquet
files without the embedded manifest require explicit reprocessing.

## Tests and acceptance

Focused regressions: tests/scripts/test_po_scenario_plot_225.py. Assertions
cover actual Matplotlib line values, separate measurements and run labels,
numeric-string hours, literal/no-match/missing-column selections, CSV/cache
location parity, stale/incompatible manifests, no-clobber and interrupted
Parquet publication, and headless PNG bytes.

Validation status at first delivery: GitHub authoring connector cannot access
a local repository worktree, Windows Python 3.14, SMB shares or installed wheel.
**No local pytest, Ruff, Pyright, Windows/SMB or copied-wrapper execution has
been claimed.** Exact-head hosted CI must be checked after the PR push; do not
replace this statement with historical parent passes.

## Dependencies and remaining gates

- [#219](https://github.com/Chain-Frost/ryan-tools/issues/219) (WP04):
  shared dry/no-data sentinel normalization before interpreting hydraulic values.
  This plotter does not redefine that contract.
- [#228](https://github.com/Chain-Frost/ryan-tools/issues/228) and
  [#229](https://github.com/Chain-Frost/ryan-tools/issues/229) (WP11):
  native representative fixture/performance and selected PO read memory.
- [#180](https://github.com/Chain-Frost/ryan-tools/issues/180):
  maintained-script inventory/AB-017 lifecycle and installed-wheel behaviour.
- Review headless PNG/Parquet parity on Windows 3.14 against genuine mixed
  Q/H and multiple unique run-code exports; test local NTFS creation,
  replacement and intentional fail-closed SMB no-clobber. Use disposable files.
- Codex review and any actionable corrections are required before permission
  to integrate into #251. Do not merge into #251 or main automatically.

## Next action

Inspect the exact-head CI results and Codex review; correct actionable failures
without expanding into WP04/WP05/WP08. Complete the Windows local-agent
acceptance with real representative PO inputs and an installed/copied wrapper.
Retain this status open until reviewed and accepted.
