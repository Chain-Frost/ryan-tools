# WP10 — PO scenario plotting fidelity (#225)

| Field | Value |
| --- | --- |
| Status | Native Windows source/installed-wheel acceptance passed; current parent-reconciled head pending exact-head CI/review |
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
  A standalone Agg-backed figure requires no interactive GUI in headless mode,
  and tight saved bounds retain the full external run-identification legend.
- CSV source files are rediscovered before publication to reject a newly added
  matching run during processing, as well as changed/deleted source files.

The manifest uses filesystem size/mtime rather than hashing entire source CSVs;
externally altered files that preserve both metadata fields cannot be detected.
Cache mode deliberately requires accessible source CSV metadata. Legacy Parquet
files without the embedded manifest require explicit reprocessing.

## Tests and acceptance

Focused regressions: tests/scripts/test_po_scenario_plot_225.py. Assertions
cover actual Matplotlib line values, separate measurements and run labels,
numeric-string hours, literal/no-match/missing-column selections, CSV/cache
location parity, stale/incompatible manifests, a newly discovered matching PO
CSV, no-clobber and interrupted Parquet publication, Agg-only headless figure
construction, tight-legend PNG bounds and PNG bytes.

Historical validation status at **first delivery only**, before the independent Windows acceptance below: GitHub authoring connector could not access
a local repository worktree, Windows Python 3.14, SMB shares or installed wheel.
**No local pytest, Ruff, Pyright, Windows/SMB or copied-wrapper execution had then
been claimed.** Subsequent native tests are documented below. The parent-reconciled
head must separately pass exact-head hosted CI; earlier native evidence is retained.

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

### 2026-11-08 — Review corrections

At source head `435a84ef`, Ruff/whitespace, POSIX wrapper security
and package checks passed; Policy/strict Pyright failed 38 source/test
typing diagnostics. Hosted tests failed during the Fiona wheel download
(remote GitHub timeout), so functional tests were **not run** in that job.
MCP smoke was blocked by the failed Windows acceptance prerequisite,
not an independent MCP test failure. Codex raised three actionable P2
issues: headless display backend, clipped full-code legends and a
new source appearing during ingestion. The next implementation revision
addresses these along with the type checks. The final SHA/recheck
status belong in PR #280; earlier CI remains historical.


## Independent Windows acceptance — 2026-11-08

Validated original head `a59d528e4c732aa6740068ac7fff96ebe1ada50c` in a separate detached worktree.
Windows 10 19045 / normal Python 3.14.6; pandas 3.0.5, NumPy 2.5.3, PyArrow 24.0.0.
Focused source tests: 17 passed, no skips. Ruff, formatting, Loguru and documentation checks pass.
Strict Pyright initially flagged the test's reference to incompletely typed Matplotlib savefig;
added a narrow member-type allowance at that boundary, matching the existing adjacent allowance.
Strict Pyright then passed with zero errors/warnings. No plotter implementation change.

Copied plotter outside checkout, imported installed wheel 26.11.8.8 from the normal site-packages
(wheel SHA256 2b64ffc4bf82722eb38e40efddcb919bc292866f7bb10b42743d7f281a696e64).
All 17 copied-script regressions passed. Independently read genuine pinned test-data CSVs
`M09_5m_01p_1hr_001_PO.csv` and `M09_5m_02p_1hr_001_PO.csv`, submodule
`d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58`, tutorial Module_09/results.
Only disposable NTFS copies were processed. Both runs share Flow location Loc1_L and water-level
location Loc1_P; all four plotted series match all 181 source x/y samples over 0–3 hours.
Flow maxima: 88.4452 / 70.0862; water-level maxima: 43.1789 / 43.0983.
Distinct measurement figures retain both complete run codes and correct units. These genuine
filenames contain AEP/duration but no TP: configured SCENARIO_DIMENSIONS to those actual dimensions;
default mandatory-TP configuration correctly rejects them. No TP label or renamed source was invented.

CSV/cache selected-row/value parity: 1,086 rows, exact values. Parquet restores duration_numeric
as int64 rather than ordered categorical; comparison explicitly permits that dtype change while
requiring exact values. Input SHA256 remains unchanged for both originals and copies.
NTFS cache/PNG no-clobber, explicit replacement and interrupted-write preservation passed.
Existing tests cover literal/no-match filters, invalid time, missing columns, stale provenance and
source additions; long-legend test checks tight PNG bounds. An unavailable hardlink was simulated
and failed closed; live SMB was NOT RUN because no disposable share was selected.
Artifacts, comparison scripts and genuine-result.json: E:/Github/wp10-native-evidence/.

No solver, production/share mutation, merge or issue closure. Keep #225 open; WP04 #219 owns
sentinel validity. Publish the test-only repair plus this record together; verify final-head CI
before integration. Earlier overdue register reviews remain unrelated and were not resumed.

## Parent reconciliation and identity regression — 2026-11-08

The accepted local implementation at `56150c0afed3e2dc7a5a8071e3ac0fa83c8b44c4` is reconciled with draft #251 parent `2a2530dd18f5d8e3146a911ab2ac2d9be01b401f`.
Parent changes in the same plotter were strict-typing refinements to the superseded plotting implementation;
the new independently typed plotting functions remain the authoritative WP10 code. Parent documentation
index rows are preserved, rather than replaced with stale child copies. The maintained
`SCRIPT_VERSION` script now calls `print_wrapper_banner` with its literal original `wrapper_source`
and copied-script path, preserving the global versioned-installed-entry-point contract checked by
`tests/scripts/test_wrapper_utils.py::test_versioned_installed_scripts_keep_identity_after_renaming`.
This integration-only correction changes only the startup banner, not plotting/PO cache results;
the prior 17/17 native source and copied tests therefore do not claim to have executed this new head.

New-head Windows CI and fresh review are required before integration into #251. Live SMB is not
accepted and sentinel semantics remain WP04 #219. No change to `main` or release.
