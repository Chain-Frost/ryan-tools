# Native SMB and SSD inactive-scope evidence

Owner requested an SSD comparison and expanded data collection before pushing results. The bundle contains **32 native timings** (8 SMB, 24 SSD), complete four-mode provenance parity, 16 process-CPU/peak-memory measurements, all-site operation counters, and separate A/B SSD profiles. All native trials pass 480 contexts, 50 files, 24 CSV headers, 480 column selections, zero issues and zero fragment fallbacks.

**Finding:** the warm workload is CPU-bound. In the resource-measured SSD subset B uses a median 18.062500 CPU seconds in 18.083090 wall seconds; the median per-run CPU/wall ratio is 99.929%. Its median inventory/header interval is 0.007527 s, while 18 seconds of processing remains. This supports prioritising repeated scope construction, expansion and membership/equality work. It does not establish cold-SMB behaviour or isolate all filesystem time: inventory excludes initial control/database parsing.

## Sources and correctness

- A: `1ca131f98a307f8054d2cf658c733cfa987d66c5`.
- B: `36ad04bd7cb045c48f3b77e8794c48a956c90c06`.
- Consumer: `a710f24ffb1698e949937f56a0212b6ae599e54e`.

The batch and seven control/database files match byte-for-byte across storage. All 50 local reference patterns resolve physically to the SSD, including database Source CSVs. Full records match between storage locations after replacing only the mapped-drive/UNC model roots. Within each storage location, independent ordinary uncached, ordinary cached, A-fragment and B-fragment processes produce exactly identical full records. Input UUIDs, ordered commands, ordinals, file/layer references, activations, database sources, CSV selectors and errors are preserved; UUID generation is deterministic only during parity parsing and restored before evaluation. All native contexts select cell size 32; 13 other declarations are excluded. End Time has two active visits per context. Eight focused regression tests pass, including overlapping/default declarations and failed-run/fallback cases.

## Native timings

Boundary: preflight API through private CSV report completion, excluding imports/startup. Separate process-wall timing is retained in JSON. Context time includes parser evaluation and reference projection; inventory includes physical checks and CSV headers. Phase medians must not be added. All timed runs are fresh serial processes with one I/O worker and identical batch/files/header options. Profiling/tracing is absent from these trials.

| Storage / subset | Trials per revision | A median s | B median s | Median reduction |
| --- | ---: | ---: | ---: | ---: |
| SMB | 4 | 20.583592 | 19.845790 | 3.584% |
| SSD, initial | 4 | 19.130097 | 18.564542 | 2.956% |
| SSD, CPU/memory subset | 8 | 18.296748 | 18.083090 | 1.168% |
| SSD, all | 12 | 18.433583 | 18.278605 | 0.841% |

Every individual elapsed time, in execution order:

| Storage | Block | Order | Position 1 s | Position 2 s | Position 3 s | Position 4 s |
| --- | ---: | --- | ---: | ---: | ---: | ---: |
| SMB | 1 | ABBA | 21.973760 | 19.743048 | 20.222496 | 20.129618 |
| SMB | 2 | ABBA | 19.847371 | 19.948533 | 19.586333 | 21.037567 |
| SSD | 1 | ABBA | 19.338640 | 18.452590 | 18.547935 | 19.240313 |
| SSD | 2 | ABBA | 19.019881 | 18.581149 | 19.269140 | 18.905304 |
| SSD | 3 | ABBA | 18.579734 | 18.106557 | 18.348124 | 18.392827 |
| SSD | 4 | BAAB | 18.217162 | 18.474340 | 18.251806 | 18.340047 |
| SSD | 5 | BAAB | 18.037123 | 18.261726 | 18.076287 | 18.059623 |
| SSD | 6 | ABBA | 18.161616 | 17.922273 | 17.819973 | 18.331771 |

The six SSD block means all favour B, with reductions of 4.091%, 0.197%, 1.401%, 0.460%, 0.664% and 2.058%. The small pooled-median effect depends on collection stratum and drift: initial SSD reduction 2.956%, supplementary subset 1.168%, pooled 0.841%. Do not substitute pooled independent samples for matched blocks. An exploratory sign-swap calculation across six block mean differences gives two-sided p=0.03125; the four supplementary blocks alone give p=0.125. Additional collection followed inspection of initial results, orders are fixed/counterbalanced, and caches/load were uncontrolled. These are exploratory results, not a preregistered randomized acceptance test.

SMB and SSD sessions were sequential, not interleaved. Their timing difference cannot be attributed entirely to storage. Cold caches, physical network requests and isolated active-input guard overhead were not measured. Native SMB acceptance remains unestablished; these results do not turn the fast path into a substantial solution for the remaining CPU cost. The original recommendation to withhold production performance acceptance remains; no production revert was performed.

## CPU and memory

CPU uses `time.process_time()` around the same API/report interval; memory uses one psutil query after timing. Peak working set includes imports/startup and is not an allocation count. Small CPU/wall ratios above one reflect timer granularity or brief multi-thread runtime activity.

| Metric, median of 8 per revision | A | B |
| --- | ---: | ---: |
| Wall seconds | 18.296748 | 18.083090 |
| CPU seconds | 18.296875 | 18.062500 |
| CPU / wall percent | 99.985107 | 99.928969 |
| Peak working set MiB | 211.277344 | 207.888672 |
| Inventory/header seconds | 0.007359 | 0.007527 |

## Separate CPU profiles

Profiles measure preflight/report after imports in fresh processes. Instrumentation expands elapsed time to about 54 seconds; never use those durations as native speed measurements. The following are **self** times. Inclusive stack times overlap and are retained separately in JSON.

| Function | A calls | B calls | A self s | B self s |
| --- | ---: | ---: | ---: | ---: |
| scope.Scope.__init__ | 5,891,004 | 5,877,564 | 7.736686 | 8.033143 |
| scope.Scope.explode | 5,532,600 | 5,511,000 | 4.015602 | 4.205323 |
| scope.Scope.__eq__ | 6,177,434 | 6,164,954 | 3.917786 | 3.938287 |
| scope.ScopeList.contains | 390,362 | 387,002 | 2.207800 | 2.208197 |
| scope.Scope.__new__ | 5,891,004 | 5,877,564 | 2.041180 | 2.049599 |

B still constructs **5,877,564 Scope objects**, expands scopes **5,511,000 times**, and executes **6,164,954 scope equality checks** in this profile. Constructor/expansion/equality work, repeated regex compilation-cache lookups/substitution and string case conversion dominate the remaining work. A large `_join` inclusive time includes lazy child evaluation and must not be presented as assembly self time. Code-defined constructor entries are identified by class; autogenerated dataclass constructors are not reliably separable by class in pstats.

The profile records 51 ordinary opens, 119 stat calls, one scandir, 34 SQLite connection calls and 13 connection execute calls for each revision. These are observed Python/native API calls within the profiled boundary, not physical disk/network request counts. Aggregate costs are in the profile JSON, including reference projection, context preparation and database-source projection.

## Avoided versus moved work

The separately observed SMB all-site counts show 47,051 inactive keys avoided, but membership evaluations decline only 389,468 to 386,108. Total scope calls increase 157,442 to 173,801. Input-state construction remains 2,494 in both revisions. Inside-key scope calls decrease while outside-key calls increase: the main expense moves before key construction. All-site group counts include preparation (125,280 membership evaluations in either revision), included controls (85,920 to 82,560), cell-size variables (53,284 unchanged), End Time (42,852 unchanged) and other settings (38,922 unchanged). Count shares are not timing shares.

Next measured optimisation targets: (1) reuse immutable expanded scope representations to avoid transient Scope construction; (2) compile membership/negation/Else If decisions against complete scenario/event/variable dependencies; (3) compare canonical scope representations and avoid repeated normalization/regex work. Preserve the ordinary evaluator and full-record/overlapping/default/failure tests as correctness guards. No production optimisation, parent/child split or worker tuning is implemented here.

## Artifacts and reproduction

- [Protocol and environment](protocol.json): exact revisions, options, boundaries, guards and limitations.
- [All 32 native timings](timings.json): phase timings, process walls, counters and resource fields where measured.
- [Statistical summaries](summary.json): ranges, sample SDs, matched block means and exploratory calculations.
- [Operation counters](operation-counts.json): anonymous all-site and command-group counts, key phases and selected diagnostic self times.
- [A/B SSD profile aggregates](profile-aggregates.json): selected ranked application/runtime functions, constructor counts, projection and filesystem calls.
- [Measurement runner audit](measure.py.txt): exact runner used for the additional 16 native trials and two SSD profiles.
- [Counter observer audit](counter-observer.py.txt): SMB observer with only private-root routing replaced by an environment variable.
- [Artifact checksums](artifact-hashes.json): SHA-256 checksums of canonical LF bytes (UTF-8 BOM retained where present), excluding the checksum file itself. This avoids checkout line-ending differences.

The measurement runner reads a private JSON configuration with `output`, `parser_sources` (A/B snapshot directories), `consumer_source`, `tcf`, `runs_json`, expected run/path/header/selection counts and `options`. Use the exact options in the protocol. Run `python measure.py --config PRIVATE_CONFIG.json --revision A --name UNIQUE_TRIAL` in a fresh normal Python process; follow the documented block orders. Add `--profile` only for separate diagnostics. The context JSON retains exact supplied scenario/event flags and order; launcher-only `-b` is excluded. Output must remain outside repositories and model directories.

The observer requires `PRIVATE_BENCHMARK_ROOT` containing A/B source snapshots plus private `workload.json` and `runs.json`; it invokes the existing diagnostic harness with private file/key-phase attribution. Models, command lists, path maps, manifests, raw full records, report CSVs and full profiler outputs are intentionally absent from this bundle. Source/model/input guards pass, including before/after every supplementary native trial.

[Authoritative status record](../../2026-10-26-native-inactive-scope-benchmark.md). No merge or release is authorised.
