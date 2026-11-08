# Maintenance benchmarks

These scripts measure candidate implementation choices and regressions; they are not production interfaces or
repeatable performance claims. Record the command, Python and package versions, data source, storage location and result
when using their output to support a decision.

## Windows Policy dependencies

**Completed on 2026-11-09: retain the canonical Policy profile.** The bounded experiment in
[issue #299](https://github.com/Chain-Frost/ryan-tools/issues/299) found no repeatable Windows saving from omitting
`build`, `ruff` and `pytest-cov`. The candidate was slower in three of four paired measurements; its only faster
result saved 0.701 seconds. All eight clean environments passed dependency, parser, capability and strict-typing
parity checks. Production installation profiles remain unchanged.

The [single successful run](https://github.com/Chain-Frost/ryan-tools/actions/runs/42516847229) used source
`92351854625dae81001c033306969f73eff4a7f7`, Windows/Python 3.14.10, pip 26.3, fixed package artifacts and isolated
build inputs. Its complete job took 789 seconds, including preparation and all eight samples. The measured Policy
stages replay a fixed 20-file typing selection, not the entire ordinary Policy job. Empty and populated pip caches
were compared in reversed-order repeats, with a fresh source and environment for every sample. Populated caches
still required some network downloads; OS/network/CDN caches were uncontrolled.

Compact evidence is archived here so artifact expiry does not require another hosted run. JSON values are
preserved; Windows line endings are normalised to LF:

- [Comparison](results/policy-299-20261109/comparison.json): all eight timing rows, paired deltas, versions, parity
  reconciliation, limitations and the full artifact's SHA-256.
- [Plan](results/policy-299-20261109/plan.json): exact source/tree, host, requirements, file hashes, manifest and schedule.
- [Preparation](results/policy-299-20261109/preparation.json): locked artifact identities and expected versions.
- [Raw results](results/policy-299-20261109/results.json): executed commands, exit statuses, cache observations and
  stage timings. Referenced detailed logs remain in the original Actions artifact, which expires on 2026-11-23
  Australia/Perth time.

The temporary workflow is retired. The harness remains an archived experiment, not a production installer or a
scheduled benchmark. Its original [execution commit](https://github.com/Chain-Frost/ryan-tools/commit/33c9c650a6df719b5cb7369b869c28ce26f5cd74)
preserves the workflow and invocation used for these measurements. No further run is needed for this candidate.

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
