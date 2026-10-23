# TUFLOW preflight example reproduction runner

The **data** is maintained in the dedicated
[ryan-tools-test-data repository](https://github.com/Chain-Frost/ryan-tools-test-data/tree/feat/tuflow-preflight-example-fixture/tuflow/example_input_preflight)
and accessed via the existing `tests/test_data` submodule.

This directory contains a **temporary, branch-only diagnostic runner**,
not the source inputs. Remove this runner and related temporary CI steps
before final merge unless they are converted into supported regression tests.
The permanent ZIP and manifest belong in the test-data repository.

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

Use the same comparator on the other workstation/network without copying private
model data into either repository. From this checkout, record both Git heads and run:

```powershell
git rev-parse HEAD
git -C E:\Github\PyTuflow rev-parse HEAD
python -m tests.fixtures.tuflow_example_repro.run_local_checks `
  --tcf "<model directory>\runs\<model>.tcf" `
  --commands-file "<model directory>\run_tuflow_simulations_commands.txt" `
  --expect-runs 440 --label R440 `
  --pytuflow-checkout E:\Github\PyTuflow `
  --output "$env:TEMP\tuflow-R440-check" --batch --repeats 3
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
requires `pytuflow>=1.2,<2`, and writes the source version, Python version and relevant
parser-module hashes to private `provenance.json`. Keep both Git heads
alongside this report; distribution metadata alone cannot identify a modified fork.

`--batch` adds a third mode through the existing opt-in adapter; its actual batch
call is included in timing and its scalar upstream counters/phase timings are
retained. Public individual and batch preflight must both pass and have identical
checked paths and issues. The default two-mode command remains available without
`--batch`. `--representative-only` stops after the first/middle/last gate.

For a known problematic model, `--continue-on-findings` completes the diagnostic
instead of stopping at the gate. Findings remain in every report, acceptance
remains false and the final exit is still 1. It does not permit launch or certify
a workload, and such timings must not be presented as a successful-model speedup.

Timings include parse, context creation and remaining collection/materialisation/
inventory work, including physical-path identity normalisation, excluding process
startup and report writing. Raster-header validation is checked by the separate,
untimed public individual/batch preflight comparison. These are diagnostic timings,
not complete launcher wall times. Python audit counters
record `open`, `os.scandir`, `os.listdir`, `sqlite3.connect` and `pathlib.Path.glob`
by phase. They do **not** count every stat or native GIS/OS read and do not measure
exact memory peaks. A background sampler records process RSS every 20 ms, start
RSS and sampled peak/delta for each mode, including native allocations. Rotating
mode order and collecting garbage between modes reduces allocator/order effects;
this is a sampled process observation, not a memory-allocation proof. Retain the
separate zero-I/O spy tests; an empty audit count alone cannot
prove zero native I/O. State the actual storage and cache conditions when reporting
timings; local SSD results do not establish network performance.

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

## Remaining work

The original baseline accepted 107 TCFs (119 runs). The local Windows/Python 3.14.6
replay on 2026-10-22 with coordinated eager secondary-source fixes accepts all
161 TCFs (174 runs), with zero missing/extra identities and public preflight failures.
The fixes are committed as consumer `09dfd502` and parser `22e2413`. Upstream
[Windows replay 39711771147](https://github.com/phantom-branch/PyTuflow/actions/runs/39711771147)
on parser/workflow head `fb103a1` now passes all 161 TCFs/174 runs with zero differences.
Both replay workflows enforce complete parity and pin the corrected implementation.
Native-model checks and final review remain distinct from that hosted example result. See
[the work record](../../../docs/work/2026-10-21-tuflow-input-preflight.md).

No solver execution, automatic preflight enforcement, PR merge or PyPI
publishing is authorised by this dataset's presence.
