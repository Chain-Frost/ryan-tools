# Raster ensemble performance

| Field | Value |
| --- | --- |
| Status | Needs review |
| Owner | ChatGPT |
| Created | 2026-11-18 |
| Updated | 2026-11-18 |
| Next review | 2026-11-25 |
| Baseline | `main` at `fc33c109d1fbf62b37b0142576355b94966b1d7d`; branch `perf/raster-ensemble-throughput` |

## Scope

Improve the maintained temporal-pattern mean/median then duration-maximum raster workflows, keeping source rasters and
original-input CSV legends enabled by default. The reported workload contains ten 16,201 by 11,401 rasters per group,
twelve concurrent groups, and group times above three minutes. Actual user hardware and project data are unavailable.

The source review identified a Rasterio cache-unit error (`GDAL_CACHEMAX=512` is 512 bytes), unnecessary data/mask
passes and array copies, uncoordinated all-CPU compression, and a second full pass to compose final source IDs.
PR [#207](https://github.com/Chain-Frost/ryan-tools/pull/207) independently changes incomplete-TP handling in this workflow;
this work starts from current main and does not adopt or resolve that separate scope.

## Implementation and acceptance

- Correct cache units and make cache, chunk memory, GDAL threads, file caching and total worker-memory planning explicit.
- Read necessary masks with their values; retain independent mask semantics and avoid redundant NoData-derived reads.
- Bound windows using raster blocks, preserving striped TUFLOW-compatible outputs.
- Reduce temporary arrays while preserving Float64 sums, source ties, all-NoData cells and mean/median conventions.
- Write original temporal-pattern provenance during the final maximum, retaining legacy composition as a public helper.
- Add phase timings including output close/flush, retaining source-enabled defaults and API compatibility.
- Verify values, masks, source IDs, legend counts and metadata, then build and smoke-test the installed wheel.
- Record bounded synthetic performance separately from genuine Windows/full-resolution/SMB acceptance.

## Current state

Implementation and local acceptance are complete. Source output remains enabled by default in both maintained wrappers.
The checked batch is delivered on
[`perf/raster-ensemble-throughput`](https://github.com/Chain-Frost/ryan-tools/tree/perf/raster-ensemble-throughput).
The associated PR records its final commit and hosted CI outcomes; no merge is authorised.

The implementation fixes cache units, avoids redundant mask reads and array stacks, processes bounded block-aware
windows, coordinates process/thread/memory budgets and writes original TP provenance during the final maximum. The
new timing report includes final compression/close and distinguishes elapsed stage time from summed worker time.
The maximum-only workflow also retains its integer failure result when resource admission rejects a job.

Numerical behaviour remains unchanged: Float64 accumulation, existing reduction order (including single-pixel edge
windows), mean selection, stable upper-median rank, independent masks, all-NoData cells, signed-zero ties, source IDs
and legend counts. The intermediate value rasters still determine duration maxima at their stored precision.

Independent kernel and integration reviews found one maximum-workflow exception-handling issue; it was corrected and
covered by a focused regression. Integration collection also identified duplicate new test basenames; the orchestrator
test was renamed before the final passing run.

MCP discovery was reassessed. Both wrappers remain packaged but uncatalogued because scenario/result/glob/TP and output
configuration still require project-specific constants. The new execution flags do not change the exclusion rationale
in the [original workflow record](2026-09-09-tuflow-stat-then-maximum.md); no new MCP tool or catalogue entry is needed.

## Validation and delivery

### Source and installed package

Validated on 2026-11-18 using Linux, CPython 3.14.10, NumPy 2.5.5, Rasterio 1.5.2 (bundled GDAL 3.12.2),
and native OSGeo GDAL 3.10.3. Task-local dependencies and installation targets were used; no project input data were
available. Windows remains the primary acceptance environment and Linux spawn checks do not substitute for it.

- Focused regression selection: **143 passed**, no skips, in 6.17 s. It includes exact values/IDs/counts, Float32/64,
  all policies and mean methods, explicit masks, signed zeros, stable duplicate medians, fused/legacy provenance,
  cache units, bounded windows, source-free bypass, resource refusal and real spawned-worker success/failure.
- A bounded additional 72-case baseline/candidate differential experiment passed exact raster bits, IDs and complete
  legend rows; 168 independent window coverage/area cases also passed.
- Ruff 0.16.6 check and format: all 15 modified Python files passed. Strict Pyright 1.1.414: 0 errors/warnings.
- Wrapper compilation and both `--help` checks passed. Documentation links/index, Loguru formatting, installed-script
  inventory documentation (162 tracked / 84 selected) and whitespace checks passed.
- `python repo-scripts/build_library.py --skip-pip --version 26.11.18.1` built and verified the wheel, including package
  source, metadata, script allowlist and QGIS resources. `dist/` remains generated/ignored under repository policy.
  Wheel: `ryan_tools-26.11.18.1-py3-none-any.whl`, 1,306,334 bytes,
  SHA256 `29754a55f3ca9ccee24f9b26a2775785abd9f729a29a7ff49c722437a6318399`.
- Both installed-wheel wrappers were copied outside the checkout and run with real `spawn` processes and two workers,
  leaving source and phase-report defaults on. Across two durations by ten TPs for each wrapper, six value TIFFs,
  six original-TP source TIFFs and six legends matched independent expectations exactly; compression, strips, CRS,
  NoData, phase reporting and 40 unchanged input hashes passed. Imported modules came from the installed target.
- Only the pinned `tests/test_data` and `qgis-resources` submodules needed for tests/build were initialised; their
  independent revisions were preserved. No unrelated worktree changes were included.

Focused test command:

```bash
python -m pytest -q \
  tests/functions/tuflow/test_asc_to_asc_raster_operations.py \
  tests/functions/tuflow/test_asc_to_asc_raster_statistics.py \
  tests/functions/tuflow/test_asc_to_asc_runner.py \
  tests/functions/tuflow/test_asc_to_asc_performance.py \
  tests/orchestrators/tuflow/test_asc2asc_mean_then_max_by_search.py \
  tests/orchestrators/tuflow/test_asc2asc_stat_then_max_by_search.py \
  tests/orchestrators/tuflow/test_asc_to_asc_resource_planning.py \
  tests/scripts/tuflow/test_asc2asc_raster_wrappers.py
```

### Source-enabled pipeline benchmark

The retained benchmark creates two durations by ten TP rasters, each 16,201 by 512 cells, with Float32 DEFLATE inputs,
true zero values and spatial NoData. It times both per-duration means and the final maximum, with source rasters,
legends, output writes and close/flush included. The original pipeline includes its separate source composition pass;
the candidate fuses it. Output verification is outside the timed region. Each layout used three alternating
baseline/candidate runs. Candidate settings: one GDAL thread, 128 MiB decoded cache, 256 MiB chunk target and no VSI cache.
Baseline settings are unchanged, including all-CPU compression. Groups run sequentially on local storage; OS cache state
is uncontrolled. Approximately 406 MB of compressed input was generated for each layout.

| Input layout | Original median | Candidate median | Throughput ratio | Elapsed-time reduction |
| --- | --- | --- | --- | --- |
| One-row strips | 12.646 s | 8.091 s | 1.56x | 36.0% |
| 256 by 256 tiles | 29.645 s | 8.421 s | 3.52x | 71.6% |

Individual total samples (seconds): strips original `[15.624, 12.306, 12.646]`, candidate `[8.091, 7.401, 8.457]`;
tiles original `[36.587, 27.891, 29.645]`, candidate `[10.060, 8.421, 8.060]`.
Every run matched exact value/source bits, masks, legends and metadata. Combined raster-value parity SHA256:
`40d3a0d3aa5d919630609869cabc76c565021414ddf378742b5daf8f1af3f4ef`.
Candidate kernel source SHA256: `e7061f779e8beb53c1f07d77357826dd6bdc1f83279ff70a04f2813dd2ebe953`.

```powershell
python repo-scripts/benchmarks/benchmark_raster_statistics.py --width 16201 --height 512 --layout strips --repeats 3
python repo-scripts/benchmarks/benchmark_raster_statistics.py --width 16201 --height 512 --layout tiles --repeats 3
```

These measurements establish a bounded source-enabled improvement. They do **not** establish the improvement on
16,201 by 11,401 project rasters, twelve concurrent groups, native Windows or SMB. The resource admission model is an
estimate, not an RSS cap, and block layout follows the first input. No TUFLOW executable comparison was rerun because
the preserved native implementation is the performance baseline.

### Earlier diagnostic evidence

The initial review's Linux read-only experiment used ten 16,201 by 512 Float32 DEFLATE rasters, 66-row windows and three
alternating warm-cache measurements. One-row strips took 2.805 s with a 512-byte GDAL cache versus 1.776 s with 128 MiB;
256 by 256 tiles took 11.142 s with 512 bytes versus 2.043 s with 512 MiB. These measurements exclude statistics,
source selection, writing and concurrent groups. They establish the cache problem, not a full-workflow speedup.

## Next action

Review the associated PR and final-commit hosted CI. On the user's normal Python 3.14/Windows installation, install the
matching library/wrapper and run the real source-enabled workload with `--workers 12 --report-phases --no-pause`.
Compare elapsed throughput, effective worker/thread plan, phase times, input block layout and memory use before
adjusting the defaults. A merge requires a separate user instruction.

The register also contains unrelated overdue reviews dated 2026-11-15 through 2026-11-17. This performance task does
not reopen those work fronts or advance their dates.
