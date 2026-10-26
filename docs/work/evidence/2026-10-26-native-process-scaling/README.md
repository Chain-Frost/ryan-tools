# One versus three CPU processes

Owner explicitly authorised multiprocessing for a separate diagnostic, overriding the earlier experiment restriction. No production code or solver is changed. The same frozen candidate parser and consumer run the same 480-context SSD workload.

**Measured:** one process median **21.672800 s**, three processes **10.169246 s**: **2.131x faster**, **53.078% less wall time**. Three worker processes make this workload faster, although only one and three were tested; the optimal count is unknown.

## Initialisation

These single-observation calibrations include fresh process launch and all imports. They use the ordered workload prefix, not a representative sample of every context type.

| Contexts, one process | Parent end-to-end s | Launch/import/setup to ready s | Model parse s | Context/projection s |
| --- | ---: | ---: | ---: | ---: |
| 1 | 2.016991 | 0.921853 | 0.776353 | 0.126460 |
| 10 | 2.376822 | 0.932420 | 0.786534 | 0.469234 |
| 160 | 8.658350 | 0.906170 | 0.804983 | 6.774311 |

First model initialisation costs approximately **0.8 s**; interpreter launch/import/setup costs approximately **0.9 s**. The first complete context adds approximately **0.13 s** of context/projection work. End-to-end one-context checking is **2.02 s**. In the full trials, three workers are all ready by a median **0.954 s** versus **0.906 s** for one worker: their setup overlaps rather than adding three times to wall time. Each then parses its own model and processes one 160-context chunk.

## Matched full-workload timings

Every timing includes worker startup, imports, model parsing, full preflight, private worker output and parent report assembly. This boundary is wider than the previous API-only timing; the new matched serial baseline is the comparator.

| Block | Order | First s | Second s | Third s | Fourth s |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | 1 / 3 / 3 / 1 | 21.716925 | 10.496153 | 10.409998 | 21.628675 |
| 2 | 1 / 3 / 3 / 1 | 21.720486 | 9.928493 | 9.825212 | 21.025402 |

| Metric, median across 4 trials | One process | Three processes |
| --- | ---: | ---: |
| End-to-end seconds | 21.672800 | 10.169246 |
| API/report CPU seconds, summed | 20.484375 | 26.609375 |
| Summed peak worker working set MiB | 207.513672 | 587.707031 |
| Fragment evaluations | 2494.000000 | 2671.000000 |
| Fragment reuses | 22001.000000 | 21854.000000 |
| CSV header reads | 24.000000 | 48.000000 |

All native trials cover all 480 request indices exactly once, preserve 50 unique physical dependencies, check 480 column selections, have zero issues/fallbacks and pass source/model/input stability guards. Three processes perform 48 total header reads for the same 24 unique headers because each inventory is independent. Their fragment evaluations rise by 177 (7.1%) and reuses fall by 147; no cross-process fragment cache is shared.

Three-worker API/report CPU totals are about 30% higher than serial, trading more CPU and memory for lower latency. The sum of peak working sets is roughly 588 MiB versus 208 MiB; this is a memory budget/upper bound, not synchronized peak process-tree RSS. CPU figures exclude startup/import and parent CPU. Repeated initialisation and reduced reuse explain some extra work, while CPU-frequency/cache contention effects are not isolated.

## Correctness and design implications

Before timing, three independent parser processes evaluated 160 contexts each. Complete public records exactly equal the saved 480-context serial result after restoring the outer request indices to their original global values. Deterministic UUIDs are used only during parity parsing; all nested UUIDs, flags, commands/ordinals/order, file/layer references, BC activations, database sources, CSV selectors and errors match. Normal UUID generation is used for all native timing. Raw records remain private.

For this 480-context job, **three workers are a defensible latency choice**. Give each worker a large chunk and initialise once per chunk; passing path/config/ranges avoids serialising a parsed model. Keep a serial path for tiny workloads. A tentative policy could require roughly 50 or more contexts per worker and cap at the available CPU budget, initially three. That is a heuristic, not a measured break-even threshold: small-job parallel runs, two/four workers and sustained server contention were not tested.

The measured startup-plus-parse cost is approximately 1.7 s per worker, with startup overlapping across workers. A rough cost model is `T(p,N) = startup_to_ready(p) + max_worker(parse + context_work(N/p) + output) + merge`; the 3-worker context rate must be calibrated separately because ideal division by p overpredicts this result. Use the observed 2.13x gain, not an assumed 3x. Persistent workers or parent-only inventory could reduce repeated work but require new correctness/cleanup contracts and are not implemented here.

## Files

- [Protocol and boundaries](protocol.json).
- [Every full-workload trial and worker stage](timings.json).
- [Initialisation calibrations](initialization.json).
- [Summary/ranges](summary.json).
- [Full-record parity result](parity.json).
- [Worker audit](worker.py.txt).
- [Controller audit](controller.py.txt).
- [Canonical LF SHA-256 checksums](artifact-hashes.json); UTF-8 BOM retained where present.

The worker reads private config fields `output`, `parser_source`, `consumer_source`, `tcf`, `runs_json` and `options`. The controller audit uses `PRIVATE_PROCESS_BENCHMARK_ROOT`, `PRIVATE_SSD_BENCHMARK_ROOT` and `PRIVATE_SOURCE_SNAPSHOT_ROOT` for existing private configuration/manifests. Never execute the model launcher. Outputs are private and outside source/model repositories.

[Authoritative status](../../2026-10-26-native-inactive-scope-benchmark.md) and [earlier single-process CPU evidence](../2026-10-26-native-smb-ssd-inactive-scope/README.md).
