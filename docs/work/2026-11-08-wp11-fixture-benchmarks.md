# WP11 — fixture contracts and measured PO scalability

| Field | Value |
| --- | --- |
| Status | First-stage native acceptance PASS; reconciled child pending exact-head CI and review; broader #228/#229 open |
| Owner | WP11 implementation / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | #251 feature branch fix/tlf-hyphenated-variables-239 at dc6d74455e092d3611b975f0fc3975a977578ecc |
| Tracking | [WP11 in #240](https://github.com/Chain-Frost/ryan-tools/issues/240), [fixtures #228](https://github.com/Chain-Frost/ryan-tools/issues/228), [scalability #229](https://github.com/Chain-Frost/ryan-tools/issues/229) |

## Delivery scope and ownership

This first WP11 delivery implements a bounded, executable support/fixture matrix and a deliberately safe reproducible
PO performance harness. It does **not** modify library processors, selection logic, plot code, raw data, dependency
pins, CI jobs or the current parent PR. Follow the [processor development contract](../../ryan_library/processors/tuflow/README.md)
and [benchmark usage guide](../../repo-scripts/benchmarks/README.md).

Fixture-contract cases are in tests/processors/tuflow/fixture_contract_wp11.json with four mutually exclusive outcomes:
**supported** (must process and provide independent expected rows/columns/values); **empty** (known empty source,
must not be counted as processed); **unsupported** (known outside a documented model/format convention, must not be
reported as successfully processed); and **malformed** (known bad or missing required records, reject safely).
Unlisted fixtures are not newly accepted or excluded by this bounded contract. The former broad robustness smoke check
still tolerates warnings for some unclassified failures; its complete replacement is open under #228.
Every manifest entry has a reason, exact basename, provenance from the pinned example-model or prior issue evidence.

The previously permissive TLF snapshot test now calls the actual production BaseProcessor for each main .tlf and
requires exact membership against the pinned snapshot. It does not regenerate an observed baseline, copy a separate
parser or silently ignore newly added files. This tests regression against pinned output, not independent proof that
all hydraulic fields are correct. The PO synthetic regression exercises real processor factory → per-location filter
→ ProcessorCollection.po_combine → ExcelExporter and file readback, including signed and zero discharge, hour timestamps,
variable identity and raw run-code preservation; optional Parquet also receives a source-value roundtrip.

## Bounded format/consumer matrix

| Product / consumer | Selected first-stage contract | Not demonstrated or separately owned |
| --- | --- | --- |
| Main TLF log, production log processor | Complete pinned main-log snapshot membership and expected numerical/status fields | Other header encodings/truncation, all workflow wrappers and independent source fidelity |
| 1D Q, H culvert files / public processors | Pinned EG11_001 Q/H result processing, field membership; channel and full run-code conventions | N1/N2 general network interpretation (issue #221), missing/invalid sample and lifecycle (WP04 #219/#223) |
| PO point-output CSV / processor, collection and exporter | Real two-line header, H/Q/V, hourly Time, independent numeric values and L001 location filtering | PO QA and processor error classification (WP04); plotting and distinct Parquet plot cache (WP10 #225) |
| POMM / selected TP summaries | Existing separate WP03 delivery within #251, source-specific maxima and TP provenance contract | Broader missing/dry populations and closure stats (WP09 #215) |
| Selected reporting points/lines/regions | WP12 _RLP_H/_RLP_Hmx, _RLL_Q, _RLR_Vol/_RLR_Volmx and their independent source times | Unselected SQ/SHmx and native container families; no claimed read-I/O or peak-RSS saving |
| GPKG/DBF ccA; EOF | Specific explicitly empty or missing-section examples remain unsupported/invalid | Blanket support for every suffix or all native formats (WP05 coordination) |
| Performance against caches and alternate libraries | No new cache or reader implementation in WP11 | HDF (WP05), plotting Parquet (WP10), Dask (defer until measured) |

Neither #228 nor #229 is complete. These matrix rows do not certify all data variants or consumers.

## Reproducible workload, outputs and safeguards

The full generator creates 5 AEPs × 6 durations × 10 TPs = **300 distinct runs** with **100 locations** and H/Q/V.
One-minute timestamps cover 0 through 2,880 inclusive, **2,881** samples. The corresponding tidy PO record totals are
**259,290,000** for all locations and **51,858,000** for the selected 20 locations.
Time, Value and AbsValue as three float64 columns represent only a theoretical numeric payload lower bound of
**6,222,960,000** and **1,244,592,000 bytes**, excluding pandas indices, object/category metadata, extra columns and
temporaries. Do not treat this as a safe peak-RAM allocation.

repo-scripts/benchmarks/benchmark_po_workload.py defaults to **two runs, six locations, 121 timestamps** and a two-location
selection. It generates real PO-style CSVs with deterministic values, zeros and signed discharge; uses actual POProcessor;
compares full versus selected and retained-series versus summary products for serial and process-pool execution. Summaries
retain one exact per-run absolute-peak row per location/variable, not an ensemble/closure statistic. The serial retained
series calls ProcessorCollection.po_combine; other cases are explicitly identified as pandas concat or DataFrame IPC.
The manifest records environment/pandas and the installed ryan-tools distribution identity, physical processor
import paths and source Git revision where available, actual generated/export bytes, planned retained-output disk
reservation, fixture-generation plus processing/aggregation/export/total wall time, row counts, and optionally 50ms
**sampled** parent/worker/combined RSS. After each measured export, the benchmark independently reads the generated
source CSV (not the POProcessor DataFrame), compares exact per-run/location/type absolute-peak values and timestamps
for summaries, selected keyed signed-value/time samples for retained series, and streams the CSV/Parquet export back
to check row counts, missing columns, duplicate keys and numerics. Validation time is recorded **outside** the measured
parse/aggregation/export stages, with an explicit per-case fidelity PASS or failure.
These samples are **not** OS-confirmed peak or working-set maxima. No controlled cold filesystem cache, UNC/SMB I/O
counter, selected-reader byte reduction, partitioned-memory baseline, optional Dask, exact peak-time parity or solver
run is claimed.

The full 300-run profile requires **both** PROFILE="full" and ALLOW_FULL_WORKLOAD=True and available disk. Before
generating fixtures, an intentionally conservative allocation includes wide input files and every enabled retained
summary/series export for both serial and parallel cases (512 estimated bytes per long row, plus safety factor).
Another free-space guard runs immediately before each individual export. Estimates do **not** guarantee actual
filesystem capacity; actual sizes and safe failure are still audited. Full-series cases whose three-column numeric
lower bound exceeds MAX_FULL_SERIES_NUMERIC_GIB are explicitly **NOT RUN**. The generated
source and report destinations are new and never silently overwritten. Full workload generation still requires an
independently approved RAM/disk/time budget; do not run it by default.

## Validation and delivery (2026-11-08)

Historical first-delivery isolated Linux scratch worktree (Python 3.13.5, **not** Windows 3.14):
- PASS: pytest -q tests/benchmarks/test_benchmark_po_workload.py — **7 passed** (exact 300-grid dimensions,
  inclusive timesteps, source values, zero/negative discharge, unique names, overwrite refusal, RAM gate, source-key
  export/peak-time validation with deliberately corrupted values/time, header drift and full retained-output disk budget).
- PASS: Python compilation of new script and tests, and JSON syntax validation of the fixture manifest.
- NOT RUN **at this historical stage**: pinned real processor fixtures, end-to-end benchmark, Ruff,
  strict Pyright, documentation checker, installed wheel, native Windows/SMB and CI. The container has no
  Python 3.14, source checkout, pinned test-data submodule or network Git remote; source files were authored on an
  isolated local scratch worktree and delivered through the GitHub connector into a **separate child branch**.
- No licensed engineering solver was executed. The synthetic generator tests are not representative end-to-end
  benchmark results and do not justify performance promises.

## Exact Windows 10 / Python 3.14 local-agent handoff

Use a **separate worktree at the exact child PR head**, initialise **only** tests/test_data, and use the repository's
normal Windows Python 3.14 environment, reduced CI configuration and accepted vendored PyTuflow wheel. Do not touch
main or the consolidation branch. Never initialise or inspect unsorted and never enable a solver or full 300-run profile.
Record Python, pandas, NumPy, psutil, PyArrow, pinned submodule SHA, OS and filesystem.

~~~powershell
git submodule update --init tests/test_data
py -3.14 -m pytest -q tests/benchmarks/test_benchmark_po_workload.py tests/processors/tuflow/test_fixture_contract_wp11.py tests/functions/test_parse_tlf_real_world.py
ruff check repo-scripts/benchmarks/benchmark_po_workload.py tests/benchmarks/test_benchmark_po_workload.py tests/processors/tuflow/test_fixture_contract_wp11.py tests/functions/test_parse_tlf_real_world.py
ruff format --check repo-scripts/benchmarks/benchmark_po_workload.py tests/benchmarks/test_benchmark_po_workload.py tests/processors/tuflow/test_fixture_contract_wp11.py tests/functions/test_parse_tlf_real_world.py
pyright --level error --pythonversion 3.14 repo-scripts/benchmarks/benchmark_po_workload.py tests/benchmarks/test_benchmark_po_workload.py tests/processors/tuflow/test_fixture_contract_wp11.py tests/functions/test_parse_tlf_real_world.py
py -3.14 repo-scripts/benchmarks/benchmark_po_workload.py
py -3.14 repo-scripts/check_documentation.py --links-only docs/work/2026-11-08-wp11-fixture-benchmarks.md repo-scripts/benchmarks/README.md docs/README.md docs/work/README.md
~~~

The benchmark writes to wp11-po-benchmark-output; choose an unused disposable local directory or safely remove only
owned outputs after review. Check the manifest per-case counts, numerical/time/provenance assertions, output bytes,
sampled RSS and first exception stack. Separately run the same **scaled** code against an installed wheel/copyable
wrapper only where relevant; distinguish source from wheel. Report failed, skipped and not-run cases exactly. If the
real TLF/PO suite disagrees with a pinned fixture, diagnose reader vs snapshot vs fixture—**do not rewrite the expected
snapshot to make it pass**. Do not run the multi-GB full profile or native UNC/SMB campaign without owner approval.

## Next action

1. Fix any exact-head hosted Windows Python3.14 formatter/typing/fixture incompatibilities, rerun only affected focused
   checks and preserve existing independently printed expectations.
2. Request/reconcile Codex review, post precise evidence to #240/#228/#229, and keep child PR draft until relevant gates
   are complete. Do not close either issue or integrate into #251 without explicit instruction.
3. With separate approval, collect representative full/selected native storage measurements; compare bounded pandas/
   partitioned Parquet with the baseline before deciding if Dask adds value. Coordinate PO validity with WP04, TP
   statistics with WP09, HDF with WP05, plot cache with WP10 and native PyTuflow data with WP12.


## Independent Windows acceptance — 2026-11-08

Original child head `6faa6a53cc272c3d9020a54056e5c8bf42f26604`; separate detached worktree.
Windows 10 19045, normal Python 3.14.6, pandas 3.0.5, NumPy 2.5.3, psutil 7.2.2, PyArrow 24.0.0;
NTFS. Only tests/test_data initialised, pinned d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58.
Accepted PyTuflow 1.3.0 wheel from immutable source 887534ea8c15b80d185a38a7ead1d42ab42a7167
verified with repo-scripts/vendor_pytuflow_wheel.py, SHA256
4bfcd24734349f9e0dcf60776ee06a2e17953ac342f40e3611b0e2aae33b30e3; installed into an external
temporary target for these checks, without replacing the global environment.

Requested focused pytest: 18 passed, zero skips, including genuine fixtures and exact TLF snapshot
membership/values. No expectations or snapshots changed. Initial lint/format/Pyright failures were
confined to new benchmark/test code. Removed unnecessary pandas casts, typed the test observations,
narrowed PyArrow's incompletely typed batch boundary, expressed damaged-time test as a scalar update,
used yield from, formatted one assignment, and documented narrow complexity allowances to keep timed
stage guards and the independent validation pass together. Ruff and strict Pyright now pass.

Default invocation first used installed wheel 26.11.8.8 (its manifest reports that origin accurately).
Separate source-tree runs explicitly import this worktree, with accepted PyTuflow target, and copied
benchmark runs import installed 26.11.8.8 from site-packages outside checkout. Both CSV and Parquet:
eight cases each, all fidelity PASS (32 cases total across source and installed comparisons).
Two runs, six/all or two/selected locations, three variables, 121 timestamps; series counts
4,356/all and 1,452/selected; per-run peak counts 36/all and 12/selected. Actual generated input
54,082 bytes per run set; source independently checked values and exact peak times outside timing.
Manifests contain per-case output bytes, timing components and 50 ms sampled RSS. No speedup,
read-I/O reduction, true high-water RSS or controlled cold-cache claim is made. The full grid is
NOT RUN; full-grid dimension tests do not generate that workload. No native UNC/SMB campaign.

Evidence, copied runner and source/installed CSV/Parquet manifests: E:/Github/wp11-native-evidence/.
Publish scoped repairs and this record together, then verify final-head CI/review. #228/#229 remain
open and broader support/scalability acceptance remains incomplete. No solver, dependency-policy
change, snapshot rebaseline, parent/main merge or release.

## Parent reconciliation — 2026-11-08

Accepted native Windows source and installed-wheel evidence: [PR #285 report](https://github.com/Chain-Frost/ryan-tools/pull/285#issuecomment-6722107028), original child `c04e288b5b6ad9384081e4d17accfb0eeb30dd51`.
Reconciled with parent #251 `2a2530dd18f5d8e3146a911ab2ac2d9be01b401f`, preserving unrelated parent changes. In
`tests/functions/test_parse_tlf_real_world.py`, the WP11 public `BaseProcessor` and full pinned
snapshot membership gate remains strict; WP01 #231's **twelve explicitly listed** new diagnostic fields
and #224's `EG05_018.tlf` archived `Model_Start_Time=-72.0` are retained as the sole expected
extra-field allowances. All existing pinned fields must match and other unexpected keys fail.
No source snapshot or fixture expectations were rewritten.

The work indexes use entries distinct from the independent WP10 insertion to avoid overwriting
concurrent plotter documentation. Exact parent-reconciled head CI and review remain required.
The complete 300-run profile, cold-cache I/O, OS peak RSS, optional Dask, partitioned data and
SMB/UNC acceptance are **NOT RUN**. Both #228 and #229 remain open pending broader scoped work.
