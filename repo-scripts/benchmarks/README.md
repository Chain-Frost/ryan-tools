# Maintenance benchmarks

These scripts measure candidate implementation choices and regressions; they are not production interfaces or
repeatable performance claims. Record the command, Python and package versions, data source, storage location and result
when using their output to support a decision.

## Windows Policy dependencies

`benchmark_policy_dependencies.py` implements the bounded experiment in
[issue #299](https://github.com/Chain-Frost/ryan-tools/issues/299). It compares the canonical Policy profile with a
scratch candidate omitting only `build`, `ruff` and `pytest-cov` from the current development extra. It preserves
runtime/MCP requirements and the accepted parser wheel; it does not change production installation profiles.

The dedicated branch-scoped workflow runs Windows/Python 3.14.10 and fixes pip 26.3, package artifact identities and
build inputs. Eight clean environments cover both profiles, empty and populated pip download caches, and a
reversed-order repeat. The same strict file manifest and capability checks run in every environment. Fresh temporary
environments are specific to this isolated experiment; normal repository setup remains unchanged. The command needs
separate output and scratch directories and does not initialise any submodule.

Use `--plan-only` to inspect the metadata-derived profile and manifest without installing dependencies or running
the benchmark. Executed measurements require Windows and the fixed Python version. Preserve the JSON reports and
logs, and distinguish preparation, cache seeding, environment creation, GIS bootstrap, profile setup and typing
durations. A populated download cache is not an already-installed environment. Results go in #299; a smaller download
alone does not justify adopting a new profile. The workflow is scoped to `ci/policy-dependency-benchmark-299` and has
no schedule or pull-request trigger.

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
