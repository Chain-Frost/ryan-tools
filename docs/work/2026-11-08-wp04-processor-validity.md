# WP04 — TUFLOW processor input health, lifecycle and culvert selection

**Status:** Windows native acceptance exposes three remaining parent defects; fixes pass locally on the scoped follow-up branch. Fresh child CI/review and parent integration remain required.
**Updated:** 2026-11-09. **Next review:** 2026-11-15.
**Allocation:** [#240](https://github.com/Chain-Frost/ryan-tools/issues/240).
**Original issues:** [#219](https://github.com/Chain-Frost/ryan-tools/issues/219) input health;
[#223](https://github.com/Chain-Frost/ryan-tools/issues/223) lifecycle;
[#220](https://github.com/Chain-Frost/ryan-tools/issues/220) culvert filters.
**Parent:** draft [#251](https://github.com/Chain-Frost/ryan-tools/pull/251), not `main`.
**Parent base at start:** `dc6d74455e092d3611b975f0fc3975a977578ecc`.
**Parent integration reconciliations:** `e62e19b47c0e1f1185170d5d378ea851eb8fc8b9`, `47e360d97c37704a5d929a7660d25107ba878341`, and `741803bc58c03e01e549bb217ed884fe55f01eea` (active work packs and shared indexes retained; no #251/main merge).

## Delivered implementation under review

- Lifecycle: clear `df`, `raw_df`, success, applied filters and diagnostics at each
  invocation in the selected maintained concrete processors. A TimeSeries inner
  read no longer marks success before subclass transform and final validation.
  Incomplete/replaced TLF logs cannot retain previous finished rows.
- Selection: postpone maximums entity filtering until Cmx, Nmx and Chan have
  canonical `Chan ID`; preserve requested-only direct/collection outputs.
  A correctly parsed but unmatched selection remains `filtered_empty` for
  the existing SKIP classifier; this is not a recovered full dataset.
  Preserve pre-selection EOF alignment in `ProcessorCollection` rather than
  inventing fuzzy channel-ID recovery.
- Hydraulic values: shared sample classifier rejects missing/nonfinite/non-numeric
  values and explicitly handles `**********`. TUFLOW's `-99999` is masked for
  recognised structure water levels before PO `AbsValue`, POMM
  `AbsMax`/`SignedAbsMax`, timeseries H and PO QA.
  Raw PO `Value` and POMM `Max`/`Min` retain source dry-level sentinel
  rows/values for EG02 provenance, while derived `AbsValue`,
  `AbsMax`/`SignedAbsMax` explicitly mask their invalid numerical effects. Usable POMM maxima
  survive a missing minimum; wholly unusable extrema do not rank as zero.
  Unusable POMM rows retain NaN extrema and event/TP provenance for
  `pomm_utils` population counts. All-invalid POMM files can remain
  structurally processed for aggregation but are classified as workflow FAIL
  by `input_health="INVALID"`.
  Negative discharge, negative level datums and real zero remain valid.
- QA: report rejected-sample counts/reasons, partial/invalid input health and
  separate hydraulic heuristic status. Reject malformed-width CSV records,
  nonfinite/reversed/duplicate timestamps and invalid warning/tolerance
  settings before ordinary OK/FLAT. The stability heuristic is whole-record
  significant-sign-change counting, not an implemented rolling window.
- Independent synthetic regressions for direct factory filtering, Q retries
  and invalid timestamps, POMM actual maxima and zero/negative preservation,
  PO masking, TLF finished/incomplete/recovery, selected HU QA, malformed
  records and tolerance configuration. Existing mocked reader tests now
  simulate a genuinely fresh read after state reset.

## Boundaries and known remaining acceptance

WP04 does not take ownership of WP09 closure population (#215), WP05 ccA
storage/cache processing, WP12 reporting-point adapters, WP10 Parquet plotting,
or general network connectivity (#221). Existing EOF truncated-ID alignment
requires complete unfiltered channel references in the collection; validate
direct EOF selection and wrapper ordering before claiming all of #220 complete.
WP12 [#269](https://github.com/Chain-Frost/ryan-tools/pull/269)
already carries its own HU/HD adapter masking; compare its integration semantics
without overwriting that branch.

This work record is **not evidence that tests have run**. The GitHub-API delivery
environment has no Windows Python 3.14, no checked-out Git worktree, and cannot
perform original PyTuflow/SMB/GDAL/QGIS licensed solver campaigns. Hosted checks,
regression snapshots, Module04 and EG02 source values, ten-TP mean/median,
independent malformed tail/parity, Windows/copy-wheel and Codex review still
require verification at the final child head. Do not claim #219/#220/#223
closed, complete or integrated until the acceptance results are recorded.

## Focused local agent handoff

Use an isolated Windows Python 3.14 checkout/worktree of the **current draft
WP04 child head**, with current vendored PyTuflow wheel and latest CI profile.
Do not modify `main`, merge, release or regenerate source snapshots without
independent expected-value review.

```powershell
python -m pytest tests/processors/tuflow/test_wp04_processor_contracts.py tests/processors/tuflow/test_collection_channel_selection.py tests/processors/tuflow/test_timeseries_processor.py tests/processors/tuflow/test_base_processor.py tests/processors/tuflow/test_max_data_processor.py tests/processors/tuflow/maximums_1d tests/processors/tuflow/other_processors tests/functions/tuflow/test_po_timeseries_checks.py -q
python -m ruff check ryan_library/processors/tuflow ryan_library/functions/tuflow/sample_validity.py ryan_library/functions/tuflow/po_timeseries_checks.py tests/processors/tuflow/test_wp04_processor_contracts.py
python -m ruff format --check ryan_library/processors/tuflow ryan_library/functions/tuflow/sample_validity.py ryan_library/functions/tuflow/po_timeseries_checks.py tests/processors/tuflow/test_wp04_processor_contracts.py
```

Run strict Pyright **only on changed Python files** using the repository's
current command, the Loguru formatting check if logging calls changed, the
docs/link checks, and installed-wheel/copied-wrapper behaviour for the affected
QA and culvert wrappers. Validate actual Module04 and EG02 POMM/PO values,
HU/HD stability outcomes, 10-TP mean/median counts/provenance, EOF alignment
with two selected channels and no-match, direct Chan/CF/H/Q/V/ccA selection,
and 1D Q malformed/truncated rows under source and wheel. Record exact SHA,
number of passes/fails/skips and environment-specific limitations in PR;
request fresh Codex review and resolve actionable findings.


## 2026-11-08 targeted CI preflight repair

Reviewed failed run 42476665887 at 8d4f6c30. Formatted processor_collection.py
and test_wp04_processor_contracts.py with the branch Ruff policy. Both initial
format repairs are AST-equivalent. Strict validation also exposed Unknown key/value
types after narrowing rejection metadata to dict: an explicit dict[object, object]
cast retains every existing key/value/count validation without changing the data.

Windows Python 3.14.6: Ruff lint/format and strict two-file Pyright pass; **33 focused
WP04/HDF tests pass**, zero skips with PyTables 3.11.1 and the accepted parser wheel.
No-bump 26.11.8.4 wheel build/verification passes. This is a narrow CI correction,
not acceptance of every native WP04 case. Source/installed parity and other review
requirements stay attached to their own evidence. No parent/main merge or release.


## 2026-11-08 EG02 derived no-data regression contract

After the preflight correction at 0e1be2bb, hosted run 42477372985 reached tests
and found the PO golden comparison still expected AbsValue=99999 for dry level
samples. A complete focused EG02 run reproduced PO and POMM failures only
(11 passes / 2 failures). This is not dependency/download infrastructure.

The WP04 implementation deliberately retains raw -99999 provenance while masking
its derived numeric effect, as specified above. The regression now explicitly
amends only the legacy snapshot's 88 upstream/downstream structure-level PO
AbsValue cells and the two POMM rows whose dry Min previously dominated a valid
Max. The fixture Parquet files are unchanged. Exact-count assertions prevent
scope expansion. Every raw Value/Max/Min, valid sample, non-structure type,
column/dtype and remaining cell still uses the strict frozen comparison. No
processor calculation was weakened or changed to recover the old sentinel bug.

Windows focused acceptance: **46 passes / zero skips** across all 13 EG02 cases,
WP04 contracts and HDF persistence. Strict modified-test Pyright, Ruff and format
pass. Confirm fresh hosted CI and review on the resulting child head before
integration. No main/parent merge, release or issue closure.

## 8 November 2026 — final invalid PO selection and storage reconciliation

The remaining P2 finding concerned an existing PO source location whose
readings were wholly rejected (e.g. `inf`). After unfiltered processing,
`ProcessorCollection.filter_locations()` could silently remove the location
and report SKIP as though it had never been present. WP04 now retains
source-header evidence, reclassifies known all-rejected selections as
INVALID/FAIL, and keeps an empty selected processor for explicit diagnostics.
No invalid readings are converted into valid hydraulic values. Tests cover
public filtering, selected data-type subcollections, genuinely absent
locations, valid locations, combined selection and repeated file processing.

The `rejected_samples` counter now counts rejected **source readings**,
not an additional derived POMM `all_missing_extrema` row. That row count
remains available in `rejection_reasons` for TP population diagnostics.

The separate unused HDF5 snapshot implementation was removed by #291
with an upgrade notice pointing to published PyPI `26.11.8.3` through
31 March 2027. WP04 no longer exercises retired HDF5 persistence:
its validation uses native `ryan-tools` processors and in-memory
collection selection/copy. Future directory-based Parquet import/export
belongs to draft #293, not this work pack. Parent reconciliation preserves
new WP12 source-format processors, WAL/ccA protections and other packs.

**Acceptance caveat:** original local Windows 10/Python 3.14 source/wheel,
copied-wrapper, genuine ten-TP provenance and live SMB acceptance
have not been independently provided for this reconciled head.
Hosted CI and Codex must be reported with the exact SHA. No `main` merge,
automatic release or claim of native acceptance is authorised.

## Source-confirmed nonfinite 1D channels (review follow-up)

After PO's invalid-location correction, exact-head Codex review found the
same source-presence gap for Q/H time-series processors. The shared
`TimeSeriesProcessor` now captures canonical channel IDs from CSV column
headers before numeric cleaning or H pivoting drops all-invalid series.
Public collection-level filtering retains an INVALID/FAIL diagnostic
processor for a requested source-confirmed unusable channel, while a
genuine absent channel still SKIPs and a usable C2 remains OK.
Tests include direct Q/H raw CSV, public/type selection and repeated
processing reset, without replacing the native ryan-tools TUFLOW readers.

## 8 November 2026 — filter-history versus rejected-source distinction

Follow-up to Codex P2 on successive location filters: header presence alone
does not establish failure after a prior filter has legitimately excluded a
usable channel. TimeSeriesProcessor now records usable channel IDs after
normalisation but **before** any direct entity selection. Only source-confirmed
channels that had no usable rows are invalid when selected into an empty
collection. The same pre-selection usability distinction is applied to PO,
where dry-level sentinel values do not count as usable `AbsValue`.

New synthetic regressions cover Q zero/negative flows, H dual-level values,
valid source channels removed by successive public filters and direct entity
filters, and PO valid/dry-sentinel location handling. The preflight Ruff
format correction is included. Focused tests, strict Pyright, hosted CI,
fresh Codex review, genuine Module04/EG02 and wheel parity still require
independent verification at the pushed head.

## 8 November 2026 — all-invalid source with absent direct selection

Codex identified a second outcome corner: an entirely rejected but well-formed
file previously failed even when the requested entity did not exist. An empty
PO result now uses the recorded canonical source locations to classify a truly
absent direct selection as SKIP, without treating malformed files (which do
not provide validated source IDs) as valid. Timeseries handling similarly
recognises an empty H pivot or Q/H/CF/V normalisation only when the parser
has reliable source IDs; existing entirely invalid selections remain FAIL.
Added focused all-invalid PO/Q/H missing-versus-present and malformed-time
regressions. Source and installed-wheel hydraulic parity, exact-head CI,
strict Pyright and Codex review are pending.

## 8 November 2026 — malformed width and H quality before pivot

Follow-up exact-head Codex review identified two remaining P2 cases. An
all-invalid direct no-match must only SKIP when the original CSV record widths
are consistent; pandas otherwise pads truncated records with NaN and masks
source corruption. Shared, fail-closed CSV width inspection is invoked only
on this rare absent-selection shortcut to avoid rescanning normal large
TUFLOW files. Existing QA record-width validation remains independent.

H channel samples are now classified on the source-shaped table before
`pivot_table()` discards wholly blank channels. This preserves exact
`missing`, `no_data` and `nonfinite` rejection counts without
double-counting during the post-pivot process. New regression cases cover
Q/H/PO truncated rows, explicit blank H values, nonfinite H values,
present-invalid vs absent selection and retained quality evidence after
public collection filtering.

No new dependency, output format or merge authorisation. Exact-head
Ruff, strict Pyright, Windows hosted tests, fresh Codex and native
Module04/EG02 installed-wheel fidelity are independent acceptance gates.

## 2026-11-08 — one-sided H level dtype correction

After classifying H source samples before pivoting, a new Codex P2 found
that a completely absent upstream/downstream pivot column was reconstructed
as object-typed `pd.NA` and failed the downstream `float64` conversion.
The pivot helper now creates missing endpoint columns using numeric
`float("nan")` without changing original rejection counts. Focused
processor regressions cover wholly absent upstream and downstream readings
with valid data on the other end, PARTIAL health, exact rejection counts
and preserved usable levels. Existing original H sample cleaning remains
before the pivot; no extra post-pivot rejection accounting.

## 9 November 2026 — feature integration and model-scope filter regression

Reviewed WP04 PR #284 was squash-integrated into draft #251 at `ff9e0ed323fc7bf397e0015967c813e694dbdb27`, without touching `main`.
The child `4a8cd631` passed six exact-head hosted checks (3,208 Windows tests), strict Pyright and no-findings Codex review; all 18 review threads were resolved.

Combined-head Codex identified that the new canonical-filter validation required `MBHistoryProcessor` to record a matching model/domain scope as an applied entity filter before `validate_data()`. The local correction invokes `apply_entity_filter()` on the canonical `Location` column for matched scopes, preserving an unmatched scope as `filtered_empty`/SKIP; both MB history and MB node readers now use the shared per-invocation reset. Added real EG02 regression cases for MB/MB1D/MB_HPC matching scopes, unmatched scope and protected model report export.

The combined-head CI and review of this correction remain independent gates. The original #219/#220/#223 engineering acceptance (Windows source/wheel, Module04/EG02, genuine ten-TP provenance and live UNC/SMB) is not represented as passed by the hosted suite. Do not merge #251 into `main`, release or close those issues based on this feature-branch integration.


## 2026-11-09 ? final Windows native acceptance and corrective follow-up

Baseline: live draft #251 `63c7da77676b9ad44e20e7f1729b738985d400cc`.
Separate worktree `E:/Github/ryan-tools-wp04-final-acceptance`, branch
`fix/wp04-native-acceptance-20261109`. Existing dirty worktrees and all unrelated
submodule state are preserved. Test-data stays pinned at
`f3c49e867cd8a165e031217a148293a8968c2d88`; no tutorial bytes or snapshots change.

### Defects reproduced and corrected

1. Q/PO with one good record and a subsequent shortened CSV record returned
   workflow OK/PARTIAL because pandas padded the missing field. When parsed
   frames contain missing cells, verify original record widths before numerical
   cleaning. Explicit blank fields remain supported partial observations;
   truncated records fail. Complete numeric frames incur no extra file scan.
2. Direct EOF and ccA unmatched channel selections returned FAIL instead of SKIP.
   Set the existing `filtered_empty` flag after a successfully applied no-match.
3. ccA omitted the shared invocation reset and could retain `processed=True`
   after later failure. Apply the same reset as other maintained processors.

Four new regression cases cover shortened Q/PO records versus explicit blanks,
recovery with valid zero/negative samples, direct EOF no-match/reselection and
real ccA DBF no-match/success/failure/recovery. No WAL/GPKG staging or hydraulic
formula changes. This follows existing processor ownership; no new MCP tool is
needed because existing inspection exposes the corrected statuses.

### Current local acceptance

Windows 10 build 19045; normal Python 3.14.6, pandas 3.0.5, NumPy 2.5.3,
PyShp 3.1.0, PyArrow 24.0.0, openpyxl 3.1.5, pytest 9.1.1.
The initial workstation parser was 1.2.9; those runs are superseded. Installed
and verified the repository's accepted PyTuflow **1.3.0**, source
`887534ea8c15b80d185a38a7ead1d42ab42a7167`, wheel SHA-256
`4bfcd24734349f9e0dcf60776ee06a2e17953ac342f40e3611b0e2aae33b30e3`.
All current counts below were refreshed with that exact dependency.

- Requested focused processor/QA suite plus EG02, culvert selection, POMM
  outcomes/populations/provenance and export failure regressions: **369 passed,
  zero failed/skipped**. Installed-wheel subset: **121 passed, zero failed/skipped**.
- Ruff lint/format, strict Pyright on all seven changed Python files, Loguru
  policy and documentation/default-index checks pass.
- Built and verified package **26.11.9.2**, retained wheel SHA-256
  `d00012fbd503c09f42c12e124c5354583653b4c8aec718fbacb0de447c2fc308`.
  Installed it into an isolated target using the normal interpreter, without
  changing the normal ryan-tools installation or creating a virtual environment.
  Source/wheel physical import origins are recorded independently.
- **176 original native checks per environment per storage path**: local NTFS,
  actual mapped T: drive and actual UNC to the same existing SMB server/share,
  each using a unique disposable directory. **1,056 passed checks total**.
  Fourteen authentic files per path; exact processed-row/numerical fingerprints,
  health/rejection outcomes and input hashes match source versus wheel.
- Factory/direct, public collection and MCP filtering exercised Cmx/Nmx/Chan/
  EOF/ccA/CF/H/Q/V with one/two/absent/whitespace selections. Existing focused
  tests additionally verify numeric-text IDs, repeated filters, rejected-source
  presence, EOF unambiguous/ambiguous alignment and model/domain MB scopes.
- Actual completed EG02 TLF replacement with initialising bytes and recovery,
  shortened/malformed Q/PO, missing files, invalid times and real Windows
  deny-sharing read locks pass on NTFS/mapped/UNC under source and wheel.
- Copied maintained maximums, peak-QA and stability-QA wrapper entry points:
  **18 successful executions**, source/wheel across three paths. Four exported
  tables per paired run match exactly after path columns are excluded; selected
  maximums/raw tables contain only requested FC01.01. Native no-clobber exporter
  checks preserve pre-existing report bytes on all six runs.
- All original selected input hashes and directory inventories remain unchanged;
  only disposable output/failure-fixture directories receive writes.

Independent Module04 HU/HD maxima: **40.9482 / 40.7525 m**, with **7 / 1** dry
PO source samples. EG02_011 HU/HD maxima: **40.7877 / 40.6995 m**, with **44 / 44**
dry PO samples. Raw sentinel provenance survives; derived extrema exclude it.
Selected peak and stability QA explicitly use verbose structure-level measurement
names and retain PARTIAL health, separate from hydraulic status. Source values
are independently read with Python csv/finite-value arithmetic, not approved by
snapshot regeneration. Ordinary Flow-only behavior remains covered.

Ten authentic distinct TP simulations are unavailable in this selected tutorial
set. Ten explicitly synthetic TP labels carry each authentic source population;
mean/median reproduce the four maxima above (roundoff below 1e-12), contributing
counts are 10, median selects TP06 and mean selects TP01 for equal-valued ties.
This validates aggregation/provenance logic, **not genuine ten-TP native data**.
Partial/all-invalid population evidence is also checked by the focused suite.

### Limits, issue verdicts and delivery

**#219 / #220 / #223: PASS WITH LIMITATIONS on this local corrective child;
parent closure acceptance remains incomplete.** The fixes must be reviewed,
pass exact-head hosted checks and be incorporated into #251 before claiming the
parent satisfies them. Do not add closing keywords until the maintainer accepts
remaining limitations, including the synthetic-TP evidence boundary.

The optional PyTuflow h5py import emits a native Windows 0xc0000139 diagnostic
and NumPy ABI warning in the broad focused run, which nevertheless exits with
369 passes; raw diagnostics are retained. No HDF/XMDF acceptance is inferred,
no binary warning is suppressed and no optional-package environment repair is
included. No licensed solver, topology or scalability campaign ran.

Reproduction artifacts (commands, XML/logs, independent native and copied-wrapper
runners, JSON tables/hashes, wheel and environment inventory) are retained under
`validation/wp04-final/` in the isolated worktree. Required acceptance commands
include the focused handoff suite above plus EG02/related workflow paths,
`python validation/wp04-final/run_native.py` and
`python validation/wp04-final/run_wrappers.py`. Native scripts are local evidence,
not a new supported repository API. Initial harness failures (missing isolated
user-site dependency path, spawn guard and short-name versus verbose HU/HD
selection) were corrected; final runs assert nonempty QA exports.

Delivery: one scoped checked batch to a child of #251; no parent/main merge,
release, auto-merge or manual issue closure. Next: review exact child CI/Codex,
obtain explicit limitation disposition and integrate only with authorisation.
Next review remains 2026-11-15 or on review completion.
