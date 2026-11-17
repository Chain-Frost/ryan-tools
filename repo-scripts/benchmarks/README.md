# Maintenance benchmarks

These scripts measure candidate implementation choices and regressions; they are not production interfaces or
repeatable performance claims. Record the command, Python and package versions, data source, storage location and result
when using their output to support a decision.

## Source-enabled raster statistics

`benchmark_raster_statistics.py` compares the maintained source-enabled pipeline
with the raster implementation at a trusted local Git revision. It creates
synthetic Float32 DEFLATE inputs, runs per-duration ASC_to_ASC-style means and
the final maximum, and includes source rasters, original-input legend counts,
output writing and closing in the timed work. The baseline includes its separate
provenance rewrite; the candidate composes original TP IDs during maximum
processing. Values, masks, source IDs, legends and metadata are compared exactly
outside the measured interval.

```powershell
py -3.14 repo-scripts\benchmarks\benchmark_raster_statistics.py
py -3.14 repo-scripts\benchmarks\benchmark_raster_statistics.py --width 16201 --height 512 --layout strips --repeats 3
py -3.14 repo-scripts\benchmarks\benchmark_raster_statistics.py --width 16201 --height 512 --layout tiles --repeats 3
```

Defaults are two durations, ten TPs each, 1,024 by 512 cells, three alternating
baseline/candidate runs, one candidate GDAL thread, 128 MiB decoded cache and
256 MiB chunk memory. Every CLI option has an editable top-level setting.
The default baseline is `fc33c109d1fbf62b37b0142576355b94966b1d7d`;
`--baseline-ref` selects another **trusted** existing local revision. The script
executes that revision's Python module and never fetches it from the network.
Ensure the candidate `ryan_library` is importable from the intended checkout or
matching installation; the JSON report records the imported module path and
source SHA256 so stale installed packages can be detected.

Inputs and outputs are temporary local files and are removed afterwards.
Progress goes to stderr; stdout contains JSON with dependency/platform details,
configuration, input size, individual timings, medians, candidate phase times
and exact output-parity hashes. Runs are sequential, input data are freshly
generated, and operating-system cache state is uncontrolled. Results do not
establish full-resolution, twelve-process, Windows, network-storage or actual
project performance. Use the real wrapper's phase report for those measurements.
See the [raster performance work record](../../docs/work/2026-11-18-raster-ensemble-performance.md).

## WP11 PO workload baseline

The new benchmark_po_workload.py generates correctly named synthetic TUFLOW PO runs, then measures the real processor,
location selection, per-run peak reduction, collection/concatenation and CSV or optional Parquet export. It records actual input/export bytes, dependency and imported-processor identities/source SHA, wall time and 50 ms
**sampled** parent/worker RSS (when psutil is installed). After recording timings it checks exported
CSV/Parquet against independent generated-source values and peak times, outside the timed stages; it also reserves disk
space for every enabled retained export. These RSS samples are not high-water marks, and cold/warm-cache behavior
is not controlled.

~~~powershell
py -3.14 repo-scripts\benchmarks\benchmark_po_workload.py
~~~

The safe default is two runs, six locations and 121 inclusive one-minute samples. A full 300-run × 100-location ×
48-hour, 3-variable run requires editing both PROFILE and ALLOW_FULL_WORKLOAD, available disk, and separate approval
of the high resource usage. Full-series cases above the configured numeric RAM limit are marked NOT RUN, rather than
loading unbounded data. The 20-location case filters the same 100-location source files.
No Dask or bounded full-series/SMB performance is claimed. See the
[WP11 work record](../../docs/work/2026-11-08-wp11-fixture-benchmarks.md).

## File collection

`benchmark_file_collection.py` compares `find_files_parallel()` and TUFLOW `collect_files()` from the installed package
and the current working tree. Missing roots are skipped, which allows the local and network paths to be measured in one
run:

```powershell
py -3.14 repo-scripts\benchmarks\benchmark_file_collection.py --runs 5
py -3.14 repo-scripts\benchmarks\benchmark_file_collection.py --local-root D:\results --network-root \\server\share\results
```

The installed comparison is meaningful only when the installed distribution is distinct from the editable checkout.

## DataFrame backends

`benchmark_dataframe_backends.py` measures concatenation and grouping using synthetic pandas frames. It can also try
Polars and PyArrow when installed, or load existing CSV/Parquet files with repeated `--input-glob` options:

```powershell
py -3.14 repo-scripts\benchmarks\benchmark_dataframe_backends.py --num-frames 200 --rows-per-frame 1000
py -3.14 repo-scripts\benchmarks\benchmark_dataframe_backends.py --input-glob "results\*.csv" --limit-frames 100
```

Backend timings include different conversion and feature scopes, so compare like-for-like actions and validate output
semantics before choosing an implementation.

## TUFLOW GPU polling

`benchmark_tuflow_gpu_polling.py` measures read-only local TUFLOW process inspection, CodeMeter WebAdmin snapshots and
their combined cost. It records individual wall-time, CPU-time and RSS-delta samples plus median, 95th percentile and
maximum summaries:

```powershell
py -3.14 repo-scripts\benchmarks\benchmark_tuflow_gpu_polling.py --scenario "GPU 0 occupied" --samples 30
```

Run it only alongside independently observed hardware states. CodeMeter allocations are licence evidence, not physical
GPU IDs. The complete two-GPU procedure and interpretation rules are in
[`TUFLOW_GPU_CODEMETER_VALIDATION.md`](../../docs/TUFLOW_GPU_CODEMETER_VALIDATION.md).
