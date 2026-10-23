# TUFLOW preflight regression harness

The **data** is maintained in the dedicated
[ryan-tools-test-data repository](https://github.com/Chain-Frost/ryan-tools-test-data/tree/feat/tuflow-preflight-example-fixture/tuflow/example_input_preflight)
and accessed via the existing `tests/test_data` submodule.

This directory contains **supported repository-only regression tooling** for
Windows/Python 3.14. `run_checks` verifies the public archive and compares complete
eager, individual deferred and optional cached-batch dependency inventories, with
an explicit graph-off/on comparison. Its focused
tests and strict Windows replay workflows remain part of the regression boundary.
This lifecycle decision supersedes the earlier instruction to remove the temporary
runner before merge. The permanent ZIP and manifest remain in the test-data repository.

The harness is excluded from the package wheel and installed-wrapper catalogue;
use the maintained `check_tuflow_inputs.py` wrapper for ordinary project checks.
It deliberately shares internal consumer collection/materialisation helpers so
regressions exercise the production inspection stages. These imports are a
repository test contract, not an additional public API. Full parity requires a
compatible PyTUFLOW development build with the APIs pinned by the dedicated CI
workflows; normal package dependency metadata is unchanged.

`run_local_checks` is the optional private-model diagnostic using the same
comparison. Run it only on an authorised workstation with access to the inputs.
It is not a CI requirement for every consumer change or a guarantee of performance
on another model or storage server.

## Data and provenance

`tests/test_data/tuflow/example_input_preflight/` contains:

- `examples.zip`: 1,464 source input files (15,137,122 compressed bytes).
- `manifest.json`: original relative paths, sizes and SHA-256 hashes.
- `baseline.csv`: 161 TCFs / 174 batch-defined run combinations.

Archive SHA-256:
`e374c8e072679fc6c4dc11e63b270caa1101b6aebb84d70060872cdd1b0aa7d6`

The original input file bytes were verified during the transfer to
[ryan-tools-test-data PR #1](https://github.com/Chain-Frost/ryan-tools-test-data/pull/1).
No general simulation results, solver executables, generated caches or logs
are contained in the archive. One required restart input remains.

A local checkout must contain the pinned test-data submodule commit:

```powershell
git submodule update --init tests/test_data
```

## Run read-only checks

Use Python 3.14 locally or the supported Windows pinned-fork CI environment.
From the `ryan-tools` repository root, verify the archive without
importing PyTUFLOW:

```powershell
python -m tests.fixtures.tuflow_example_repro.run_checks --verify-only
```

Run all input combinations against your PyTUFLOW source checkout:

```powershell
python -m tests.fixtures.tuflow_example_repro.run_checks `
  --pytuflow-checkout E:\Github\PyTuflow `
  --output "$env:TEMP\tuflow-example-reports"
```

Select a smaller subset:

```powershell
python -m tests.fixtures.tuflow_example_repro.run_checks `
  --pytuflow-checkout E:\Github\PyTuflow `
  --tcf EG03_011.tcf --tcf EG03_012.tcf
```

The runner verifies the ZIP and each member, extracts once into a local
temporary directory, never launches model batches/TUFLOW and writes
incremental `results.json` and per-TCF issue CSVs outside the data
checkout. It compares (run, owning input command, file, layer) records and
does not claim parity for partial attribute/database materialisation.
A nonzero exit means incomplete parity and must fail the acceptance check.

## Private native-model handoff

For the owner's newly offered model on a different SMB server, treat the results
as a new workload. Record the exact parser and consumer Git heads before running,
use that same source pair throughout, and replace the model/count placeholders
below. The earlier M480/N42 trials remain historical evidence and need not be
repeated as a gate for the bounded consumer release. Keep private model data out
of both repositories.

```powershell
git rev-parse HEAD
git -C E:\Github\PyTuflow rev-parse HEAD
py -3.14 -m tests.fixtures.tuflow_example_repro.run_local_checks `
  --tcf "<model directory>\runs\<model>.tcf" `
  --commands-file "<model directory>\run_tuflow_simulations_commands.txt" `
  --expect-runs "<actual run count>" --label NEW_SMB `
  --pytuflow-checkout E:\Github\PyTuflow `
  --output "<private report directory>\baseline" `
  --batch --graph-compare --repeats 4 `
  --storage-and-cache-conditions "New SMB server; sequential warm-cache trials; no cache flush"
```

The command export must contain only the selected TCF's exact event/scenario
combinations. It is read as text and never executed. For other models, use their
actual count with `--expect-runs`. Alternatively, `--runs-json`
accepts a private JSON list of string event/scenario dictionaries. Project launchers
are never imported or executed by this runner.

The runner checks first/middle/last runs before full comparisons, rotates mode
order across repetitions, loads a fresh model per mode, checks every physical
file/layer/command/run identity, then runs public preflight. Nonzero exit or any
missing, extra, collection or inventory issue fails acceptance. Private differences,
logs and CSVs can disclose model paths: keep them outside the repository and share
only anonymised counts and summaries. No solver is launched and no model input is edited.
It verifies that PyTUFLOW was imported physically from the requested checkout,
requires `pytuflow>=1.2,<2`, and writes **both actual Git heads and dirty states**,
the source/Python versions, relevant parser-module hashes, mode selection, run and
distinct-flag counts, and the supplied storage/cache description to private
`provenance.json`. Git and actual parser/consumer checkouts are required;
distribution metadata alone cannot identify a modified fork. Keep detailed
reports private, including Git status paths. The flags remain in their original
order, including duplicate rows. The command export is not checked for matching
TCF paths: export only the intended combinations for the selected model.

`--batch --graph-compare` selects these four diagnostic modes:

| Report mode | Public parser call |
| --- | --- |
| `eager` | `model.context(flags, file_resolution="eager")` |
| `deferred` | `model.context(flags, file_resolution="deferred")` |
| `batch_graph_off` | `model.collect_batch_dependencies(flags, cache_scopes=True, compile_tree=False)` |
| `batch_graph_on` | `model.collect_batch_dependencies(flags, cache_scopes=True, compile_tree=True)` |

Both batch modes retain the other batch caches. Each collection loads a fresh
TCF and makes one ordered public batch call. The diagnostic uses the existing
strict `BatchModelView` and full materialisation/inventory stages. It compares
every selected mode with eager and graph-off directly with graph-on. Bounded
per-run hashes compare the complete ordered command ownership, source lines and
command text, plus the public deferred references and their inspection flags.
UUID joins are validated within each batch projection; fresh parses do not need
equal random UUIDs. Missing lineage, failed modes and partial results fail.

Graph comparison requires `--batch`, an explicit
`--storage-and-cache-conditions` description, and a positive multiple of four
full trials (default **4**). Mode order rotates through a complete cycle so
each mode occupies each position equally. Run sequentially on the same storage
without cache flushing or competing benchmarks. The representative gate is
additional to the full trials. State the observed cache conditions accurately;
the runner does not certify that a server cache is warm.

The original two-mode invocation remains available without `--batch`, and
`--batch` alone retains the three-mode/default-parser comparison. Their default
repeat count remains 1. `--representative-only` stops after first/middle/last;
it can return 0 for a successful smoke but `summary.json` explicitly records
`full_workload_validated=false` and `accepted_for_benchmark=false`.

Public individual and **default-batch** preflight remain a separate untimed gate,
including raster-header checks and identical checked paths/issues. `preflight.json`
records `timed=false`, `batch_graph_policy="parser_default"` and
`graph_comparison=false`. The consumer API has no graph switch, so this is not
graph-off/on public-preflight timing or validation. Both diagnostic graph modes
still perform complete materialisation and physical inventory.

For a known problematic model, `--continue-on-findings` completes the diagnostic
instead of stopping at the gate. Findings remain in every report, acceptance
remains false and the final exit is still 1. It does not permit launch or certify
a workload, and such timings must not be presented as a successful-model speedup.

Timings include parse, context creation or the **actual public batch call**, and
remaining collection/provenance/materialisation/inventory work, including
physical-path identity normalisation, excluding process
startup and report writing. Raster-header validation is checked by the separate,
untimed public individual/batch preflight comparison. These are diagnostic timings,
not complete launcher wall times. Public parser counters and bounded fallback
reasons are retained under each batch mode. Constructor attempts/elisions and
failed-run work are distinct from eligibility/cache-hit counts. The parser's key,
clone, certification and constructor timings can overlap other phases: do not
sum them into a total or subtract them as exclusive phases. Only the diagnostic's
exclusive parse/context/batch timers are subtracted from its collection total.
Python audit counters
record `open`, `os.scandir`, `os.listdir`, `sqlite3.connect` and `pathlib.Path.glob`
by phase. They do **not** count every stat or native GIS/OS read and do not measure
exact memory peaks. A background sampler records process RSS every 20 ms, start
RSS and sampled peak/delta for each mode, including native allocations. Rotating
mode order and collecting garbage between modes reduces allocator/order effects;
this is a sampled process observation, not a memory-allocation proof. Retain the
separate zero-I/O spy tests; an empty audit count alone cannot
prove zero native I/O. State the actual storage and cache conditions when reporting
timings; local SSD results do not establish network performance.

`parity.json` retains each gate/trial, while `summary.json` states final acceptance.
Reports are initialised as incomplete before a run so a failed representative
gate cannot leave an older accepted summary in a reused directory. Use a separate
private directory for each parser revision. Compare parser baseline `e162c04`,
append-accounting PR #14 and literal-setting PR #15 separately with the same
diagnostic revision and workload before testing any combined candidate. Share
only anonymised counts, timings, parity, Git heads and relevant batch metrics.

Identity comparison follows physical aliases deliberately, while inventory I/O
retains the supplied path spelling. Converting a mapped drive to a UNC SQLite
`file://host` URI can otherwise create artificial unresolved layer findings.
Physical identity is resolved once per exact path spelling within each `collect()`
call (one TCF and mode), then reused across that call's run/command records. Each
call starts with an empty cache so later calls observe changed link targets;
resolution failures still fail the comparison. Archive containment and imported
module verification keep their separate link-aware checks.
GeoPackage wildcard/`all` selectors share one successful table-name enumeration per
exact path spelling per call. Each selector is filtered independently, read-only
enumeration connections close immediately, and query failures still fail the
comparison. Later calls re-read changed database contents. The final shared
inventory remains a separate verification stage. Wildcard expansion also retains
all reference inspection/provenance flags.

PyTUFLOW owns control-file parsing, scenario/event selection, variable expansion
and graph reuse. This consumer diagnostic only compares physical identities after
materialisation through the existing ryan-tools adapter and inspection stages;
the identity cache does not inspect PyTUFLOW's private state or duplicate its
evaluation logic.

**Performance:** ZIP extraction is a one-time setup step, not part of
the eager/deferred parse/context/inventory timings. For comparisons, use the
same already-extracted files on the same SSD. Do not unpack and commit the
1,464 files to Git; that would introduce unnecessary checkout and small-file
overhead.

## Acceptance and follow-ups

The full public archive gate requires all 161 TCFs / 174 runs to pass, with zero
missing or extra materialised identities, no collection/inventory issues and
matching public preflight results. A selected `--tcf` smoke is useful for debugging
but does not substitute for that complete gate. Keep the focused harness tests
and all strict workflow assertions when changing the comparator.

The Windows ordered-batch workflow pins parser baseline
`e162c04330d24c37bee3ade94479cc4602651060` and runs the full archive with
`run_checks --batch --graph-compare`, plus focused diagnostic/adapter tests,
Ruff and strict Pyright. The archive runner is a correctness gate, not a native
performance benchmark; its four collection modes are each run once per TCF.

Consumer PR #156 was integrated into parent #143 at `298fb13` after all five
workflows passed and independent review returned no findings. That evidence
belongs to that source and the workflow's exact parser pins. Final parser/consumer
pairing and any later source changes require their own hosted checks; no native
speedup is inferred. See the [preflight work record](../../../docs/work/2026-10-21-tuflow-input-preflight.md)
and [batch work record](../../../docs/work/2026-10-22-tuflow-batch-preflight-optimisation.md)
for current delivery and the separate optimisation/new-SMB-model follow-ups.

No solver execution, automatic preflight enforcement, PR merge or PyPI
publishing is authorised by this dataset's presence.
