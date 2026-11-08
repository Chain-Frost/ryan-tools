# WP09 — PO closure population and scenario identity (#215)

| Field | Value |
| --- | --- |
| Status | In validation; draft child implementation, not integrated |
| Owner | WP09 agent |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | Draft #251 feature branch at dc6d74455e092d3611b975f0fc3975a977578ecc; main at 8de77a67fb614ad32cd3c151a338f23eeb871412 |
| Scope | [#215](https://github.com/Chain-Frost/ryan-tools/issues/215), allocated by [#240](https://github.com/Chain-Frost/ryan-tools/issues/240) |
| Draft parent | [#251](https://github.com/Chain-Frost/ryan-tools/pull/251) |

## Outcome and scope

Repair closure statistics for the full *observed valid* PO event population, not only events exceeding a threshold.
Preserve scenario identity through statistics and protected raw CSV/Parquet/Excel exports. Keep the established
sample-count timestep rule, upper-middle finite median, selected critical duration, and compatibility fields.
Issue #92 (interval/interpolation/AATOC and broader closure semantics) is assigned elsewhere and is excluded.

## Implementation

- Emit one raw result per observed event, location and threshold, including legitimate zero-duration events.
- Invalid times, missing/nonfinite hydraulic samples, the TUFLOW -99999 no-data marker, and unusable timesteps
  produce NaN durations with an explicit UNUSABLE event status instead of an invented zero closure.
- Retain the existing parsed trim_runcode key while grouping and reporting distinct scenarios.
- Reuse the shared finite-member median helper. Report contributing and unusable event counts, valid and unusable
  duration-bin counts, and OK/PARTIAL/UNUSABLE population status using the WP03 population vocabulary.
- Preserve existing report fields and raw artifact names. New status/count fields are additive. The existing
  protected exporter (#194/#202) and WP08 dashboard are reused unchanged.
- Synthetic regression cases cover 0/0/1-hour example, wholly dry events, thresholds, two correctly named
  scenarios, nonfinite/sentinel/missing timesteps, incomplete observed ensembles, multiple durations,
  all-invalid groups and stable TP ties. A dependency-gated real export readback test checks raw CSV,
  gzip Parquet and the Summary worksheet.

## Limitations and dependencies

- This work counts only **observed** event groups. An absent/unreadable PO file does not produce a fabricated TP.
  Without a supplied expected-run manifest, an apparently smaller valid ensemble is not certified complete.
- The present POProcessor may drop missing sample rows before passing processed data to the helper. Consequently
  some partially corrupt source series can no longer be identified here. Coordinate source validity and failed-file
  outcomes with WP04 #219/#223; do not silently describe this pack as full source-to-report missingness acceptance.
- No solver, network filesystem, production project data, source parser rewrite or #92 engineering semantics.
- This environment cannot clone the repository for a true worktree or perform Windows/Python 3.14,
  installed-wheel and copied-wrapper testing. The isolated harness recreates the relevant finite-statistics
  helper from the parent; exact checkout validation remains mandatory.

## Validation and delivery

On 2026-11-08, an isolated Python 3.13.5 / pandas 2.2.3 pytest harness ran the new cases:
**13 passed; 1 skipped** (real export requires pyarrow); zero failures. Python compile checks passed.
This is component evidence, not full source-tree, Windows or installed-wheel acceptance.
Ruff 0.16.6 and strict Pyright are unavailable in this environment; hosted checks and review remain pending.
Do not report skipped or unexecuted tests as passes. No merges, releases or issue closures performed.

## Next action

Check the draft WP09 PR's exact-head Windows hosted CI (focused and package tests, lint, formatting, strict
Pyright and actual exporter readback); correct actionable Codex findings; then obtain independent Windows 10
Python 3.14 installed-wheel/copied-wrapper verification for zero durations and scenario identity.
Confirm native record parity and PO missingness limitations with WP04 before requesting integration into #251.
Do not integrate without authorisation and do not merge #251 to main.
