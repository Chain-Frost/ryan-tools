# Maintenance benchmarks

These scripts measure candidate implementation choices and regressions; they are not production interfaces or
repeatable performance claims. Record the command, Python and package versions, data source, storage location and result
when using their output to support a decision.

## WP11 PO workload baseline

The new benchmark_po_workload.py generates correctly named synthetic TUFLOW PO runs, then measures the real processor,
location selection, per-run peak reduction, collection/concatenation and CSV or optional Parquet export. It records actual
input/export bytes, dependency versions, wall time and 50 ms **sampled** parent/worker RSS (when psutil is installed).
These are not high-water marks and cold/warm-cache behavior is not controlled.

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
