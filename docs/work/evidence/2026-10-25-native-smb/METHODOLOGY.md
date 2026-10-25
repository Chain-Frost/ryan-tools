# Native SMB benchmark methodology: independent review required

Date: 2026-10-25. **Collection completed; performance conclusions are provisional.**
The owner requested closure because the campaign took too long and its methodology
needs review. No additional measurements, profiles or counter passes will run.

The main coordinator completed in **89.544 minutes**, excluding initial preparation.
I expanded the experiment beyond what was necessary for an initial performance
comparison and did not provide an adequate runtime estimate before starting it.

## Review bundle

- [Every timed observation: trials.csv](trials.csv), 64 rows, full numeric precision.
- [Descriptive summaries](summary.json), including every underlying timing value.
- [Declared protocol](protocol.json).
- [Environment and stage durations](environment.json).
- [Correctness summary and limitations](correctness-summary.json).
- [Actual worker source, archived as text](driver.py.txt).
- [Actual coordinator source, archived as text](orchestrate.py.txt).
- [Actual aggregation source, archived as text](summarize.py.txt).
- [Public artifact checksums](artifact-hashes.json).
- [Authoritative work record](../../2026-10-25-tuflow-native-combined-benchmark.md).

The source-text files are audit artifacts, not maintained executable entry points.
They contain no private model paths or input flags. The private configuration,
full identities, manifests and logs are intentionally excluded from this bundle.

For another agent on the same machine, use the existing review checkout
`E:/Github/ryan-tools-review-161`. Its ignored
`.validation/active_native_campaign.txt` points to the complete private directory.
Start there with `CLOSEOUT.md`, `progress.json`, `completed.json`,
`experiment-freeze-final.json`, `cross-revision-parity.json`, and `reports/`.
**Do not rerun the campaign or publish private evidence merely to review it.**

## Actual workload and interpretation of “runs”

The owner supplied one BAT on an SMB-mapped drive. It was read as text using
`ryan_library.functions.tuflow.command_plan.read_command_plan()` from #160.
The initial directory was explicitly assumed to be the BAT's parent. The owner
did not separately specify the shell launch directory; review this assumption.
The effective selected TCF existed, and there were no CD/PUSHD statements. The planner found:

- 480 ordered commands and 480 distinct complete event/scenario flag dictionaries;
- one effective TCF and one effective working directory;
- two SET statements, 480 START statements and a final PAUSE.

The independent legacy `read_run_commands()` extraction agreed on all 480 ordered
flag dictionaries. Neither the BAT nor TUFLOW was executed. These are proposed-run
dependency evaluations, **not 480 solver simulations**.

## Sources and environment

| Role | Exact Git revision |
| --- | --- |
| Common parser integration base | `fad6dae3038a9381c268b308ef4aae216682aba2` |
| P6 alone | `05d132e45176f117b1c75a176fcc9d5027c429a1` |
| P7 alone | `1d8b59b538654443434399e596938f4c9fe73a0f` |
| Combined PyTUFLOW #39 | `ee44350a9312f365316498708c3f958036eebc3a` |
| Frozen diagnostic and ordinary consumer | `f1f7958b79f3ffe84ba8ac0b04658f27c311460a` |
| Experimental callback/probe consumer | `01e3e46c76e07602afba187284aa539652398fff` |

The common base is an ancestor of all parser candidates. It is the integration
base, not a claim about the latest moving master. Clean normal local clones were
used. No new Git worktree or virtual environment was created. The historical
accepted benchmark and frozen executable files were not modified.

Python was normal Windows CPython **3.14.6**, with the GIL enabled, on 16 logical
CPUs. Model inputs were on confirmed SMB storage; sources and reports were on
local NVMe. Exact dependency versions were recorded privately and checked around
invocations. Relevant installed versions included:

| Library | Version | Role |
| --- | --- | --- |
| psutil | 7.2.2 | RSS sampling and one subsequent CPU observation |
| pandas | 3.0.5 | PyTUFLOW/database dependencies; not the benchmark aggregation implementation |
| NumPy | 2.5.3 | Parser/GIS dependency |
| GeoPandas | 1.1.3 | GIS dependency |
| pyogrio | 0.12.1 | GIS dependency |
| rasterio | 1.5.2 | Raster dependency |
| Shapely | 2.1.2 | GIS dependency |
| pyproj | 3.7.2 | CRS dependency |
| h5py | 3.16.0 | Installed PyTUFLOW dependency |
| netCDF4 | 1.7.4 | Installed PyTUFLOW dependency |
| Loguru | 0.7.3 | Existing consumer logging |

Git revisions and actual parser/consumer import locations were verified; the
package's nominal version number alone was not treated as source provenance.
The inventory is not a byte-level fingerprint of every loaded native DLL.

## Matrix actually executed

| Stage | Worker invocations | Work inside each invocation | Logical run-context evaluations |
| --- | ---: | --- | ---: |
| Full correctness gates | 4 | Four diagnostic modes plus individual and batch public preflight, each over 480 runs | 11,520 |
| Full-authoritative production timing | 16 | Four revisions times four trials; one 480-run batch preflight | 7,680 |
| Graph diagnostic timing | 32 | Four revisions times two graph policies times four trials | 15,360 |
| Callback/probe timing | 16 | Four conditions times four trials; three conditions perform two collections | 13,440 |
| Main total | **68** | **64 timed conditions plus four full gate invocations** | **48,000** |

Preparation also performed four representative gates: three selected runs in
each of four modes for each revision, adding 48 logical run evaluations.

The main matrix instantiated approximately **100 TCF models**: 24 in the full
gates, 16 in production timing, 32 in graph timing and 28 in callback timing.
The representative gates added 16 model instantiations. These are counts of
logical evaluations/model loads, not measured counts of individual Python class
constructors: caching can avoid some internal construction.

The large matrix explains most of the elapsed time. A smaller initial study
could have answered the primary source-comparison question much sooner.

## Measurement boundaries

Each condition ran in a fresh Python subprocess. Conditions ran sequentially.
Imports, source/dependency/input guards, report JSON serialization and process
startup/shutdown were outside the reported `wall_seconds` window.

### Full-authoritative production timing

The worker called:

```python
report = check_simulation_inputs(tcf, requests, context_mode="batch")
report.write_csv(new_private_report_path)
```

`wall_seconds` spans the checker call and CSV writing. The public check includes
model loading, context/dependency collection, supported materialisation and final
authoritative inventory. Batch mode uses the parser's default graph policy.
**This is explicit developmental batch mode, not the maintained wrappers'
default individual mode.** The public checker exposes no graph on/off switch.

The recorded consumer parse/context/inventory times are named internal phase
measurements. Their medians must not be added and presented as an exact median
total or as a complete CPU/I/O attribution.

### Graph diagnostic timing

The worker called the unchanged frozen `run_checks.collect()` with either
`batch_graph_off` or `batch_graph_on`. This includes parser work, diagnostic
identity/provenance construction, materialisation and its inventory checks.
It does not have the identical boundary as the full public preflight above.

**Some JSON-based semantic hashing is inside this diagnostic timing**, through
the frozen provenance implementation. Final result-file serialization and
comparison against saved gate data are outside the timer. Its hashing overhead
was not isolated, and it must not be assumed to be zero.

### Callback/probe timing: important limitation

All four conditions used combined #39 and the same probe consumer revision:

| Condition | Preliminary work | Final work |
| --- | --- | --- |
| authoritative_only | None | Full public batch preflight and CSV |
| plain | Parse a model and collect all batch dependencies | Parse again in full public batch preflight and write CSV |
| noop | Same preliminary collection with a no-I/O callback | Parse again in full public batch preflight and write CSV |
| probe | Parse and run the bounded read-ahead probe; consume its adapter views | Parse again in full public batch preflight and write CSV |

The no-I/O callback appends completed projections to a list, then checks their
identity/order against returned cases; it is not literally an empty function.
The probe uses one I/O worker and at most eight pending futures.

**The latter three conditions deliberately perform two model collections.**
They measure the current separate pilot followed by unchanged authoritative
validation. They do not measure an integrated one-pass implementation that safely
reuses the projected dependencies or speculative inventory results. A slower
probe result here cannot establish that properly integrated parallel checking
would be slower. The comparison is limited to this explicitly duplicated pipeline.

## Trial order and conditions

Production revision order rotated through all four positions:

1. base, P6, P7, combined;
2. P6, P7, combined, base;
3. P7, combined, base, P6;
4. combined, base, P6, P7.

Graph timing used the same revision rotations and alternated off/on versus on/off
order between trials. Callback order rotated the four conditions through all
positions. Order was deterministic, not randomized.

There was no cache flush, CPU affinity control or power-state control. Client and
server caches and background/server load were uncontrolled. Accessibility checks,
correctness gates and input hashing preceded timing and warmed some data/metadata.
There is no cold-cache claim or claim of complete environmental isolation.

## Correctness and input/source guards

The frozen helper returns complete run/reference/file/layer identity sets and
ordered command/reference semantic hashes. Gates compared sorted complete sets
and hashes, not just equal counts. Fresh UUID byte values were not compared across
independent parses; ownership/lineage was validated within each projection.

All four revisions passed all four full modes with 12,480 diagnostic identities,
49 diagnostic files and zero findings. Individual and batch public preflights
agreed on 50 checked path spellings. Cross-revision identities, command/reference
provenance and public outcomes agreed.

Timed production/callback results had to pass, cover 480 runs and reproduce saved
checked paths. Timed graph results checked complete identity equality and complete
provenance status; their semantic hashes are also retained in the private result
files for independent comparison. The exact worker text is authoritative about
which checks ran at each point.

Model/BAT content hashes were checked around invocations. Known input size/mtime,
source cleanliness and installed dependency-version guards also ran around timed
workers. Initial content hashes cover 62 known input spellings, including selected
GIS/raster sidecars. This is not a complete OS-level trace of every file read.
**A final full input-content hash recheck was not performed before closeout.**

The private worker was refined during untimed preparation. Final worker and plan
hashes were captured before elapsed trials; a later audit confirmed those frozen
files after the production phase. The earliest untimed gates predate some added
metadata/import-location guards.

## Libraries and logic used for aggregation

Measurement/orchestration used standard Python `subprocess`, `time.perf_counter`,
`threading`, `pathlib`, `json`, `hashlib`, `dataclasses`, `gc`, and
`importlib`, plus psutil. Statistical aggregation used **lists/dictionaries and
the standard `statistics` module**, not pandas, NumPy statistics, SciPy or a
statistical model. PyTUFLOW may use pandas internally during the measured work.

There are **64 timing rows**, not thousands of timing samples. The thousands of
identity rows are correctness data. They were handled as sets, sorted lists and
semantic hashes. No whole TCF/class-instance graphs were persisted with pickle.

The aggregation selects rows by `(stage, variant, condition)`, sorts them by
zero-based trial index, and computes:

```python
median = statistics.median(values)
mean = statistics.mean(values)
minimum, maximum = min(values), max(values)
sample_sd = statistics.stdev(values)  # n-1; four observations per group
median_change_percent = 100 * (candidate_median / base_median - 1)
paired_change_percent = [100 * (candidate[i] / base[i] - 1) for i in range(4)]
```

Positive change means slower. Ratio of medians and median/mean of paired ratios
are different statistics and must not be interchanged. No outliers were removed,
no failed timed run was retried automatically, and no p-values, confidence
intervals, bootstrap estimates or formal causal model were calculated.

Final JSON summary generation took **0.182 seconds**, including its subprocess
startup. This timing does not quantify semantic JSON hashing inside diagnostic
collection. Pandas is convenient for reviewing the flat CSV:

```python
import pandas as pd
df = pd.read_csv("trials.csv")
df.groupby(["stage", "variant", "condition"])["wall_seconds"].agg(
    ["count", "median", "mean", "std", "min", "max"]
)
```

That pandas example is a suggested independent analysis, not a claim that it was
used by the executed aggregation script. Blank CSV phase fields mean unavailable
for that boundary, not zero.

## JSON, pickle, copying and constructor cost

Pickle can be more efficient for compatible Python-object snapshots. JSON was
used here for normalized records and canonical semantic hashing. A pickle of a
whole object graph is a different representation, potentially including mutable
state, UUIDs, handles and version-specific class relationships. Serialization
format, copying strategy and avoiding constructor/evaluator work are separate
questions that have not been benchmarked against one another here.

P7 already uses guarded `copy.copy()` and detaches mutable per-run pieces. Its
measured improvement does not isolate raw allocation time from constructor logic,
scope/event evaluation, translation, copying or cache-proof overhead.

## What was not collected / what remains uncertain

- No physical SMB operation/byte trace or complete filesystem-call census.
- No constructor/copy/event/scope profile or full public parser-counter dump.
- Prepared helpers `observe_io.py`, `collect_metrics.py`, and
  `profile_contexts.py` were **not executed**. Their existence is not evidence.
- RSS was sampled every 20 ms during work. This is extra observation overhead,
  not literally zero instrumentation, and not an exact lifetime peak.
- One separate CPU observation saw a worker at 100% of one core, or 6.25% of the
  16-logical-CPU machine, with about 49.7 CPU seconds in 50.5 elapsed seconds.
  It supports investigating CPU work, not a universal claim about every phase.
- Four trials, one host and one physical workload support descriptive results;
  they do not establish general performance or complete causal attribution.
- Library/parser correctness checks are not TUFLOW-engine or hydraulic acceptance.

## Issues and questions for independent review

1. The matrix was excessive for the initial question. Review whether the primary
   decision needs only bounded correctness plus paired base/P7/combined timing.
2. Inspect the captured sources and timing boundaries before accepting rankings.
   Production, diagnostic and double-collection pilot timings are different work.
3. Verify the frozen source/plan and source import identities against private
   evidence; assess input-manifest coverage and the absent final content rehash.
4. Assess cache/order/background-load effects, sample size and retained outliers.
5. Determine whether provenance hashing belongs outside the diagnostic performance
   window, while retaining correctness validation separately.
6. Do not attribute the remaining context-phase time to constructors alone without
   profiling. Evaluate immutable templates, isolated run state, event/scope reuse
   and possible bounded process partitioning as separate future hypotheses.
7. Do not interpret this pilot comparison as a benchmark of integrated parallel
   authoritative validation. That implementation was not present or measured.

Two preparation errors are retained in the private logs: a collision with an
installed `tests` package was worked around by loading the unchanged frozen helper
by filename; a setup helper's final status print lacked `import json` after writing
the runner files. The shell continued into the coordinator. The actual runner
files subsequently compiled and completed their gates. This was a fail-fast
tooling mistake; review the executed artifacts and logs rather than treating the
later success messages as sufficient methodology validation.

The next action is **independent methodology/data review and a smaller declared
testing process**, not another automatic benchmark, merge or release.
