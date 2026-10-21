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
A nonzero exit is expected while supported-input gaps remain.

**Performance:** ZIP extraction is a one-time setup step, not part of
the eager/deferred parse/context/inventory timings. For comparisons, use the
same already-extracted files on the same SSD. Do not unpack and commit the
1,464 files to Git; that would introduce unnecessary checkout and small-file
overhead.

## Remaining work

The baseline accepted 107 TCFs (119 runs), and the latest Windows
shared-GIS-reader sweep accepted 140 TCFs (153 runs). Outstanding examples
include table links, rainfall grids, AD/hydraulic BC databases and
pit/network identifiers. See
[the work record](../../../docs/work/2026-10-21-tuflow-input-preflight.md).

No solver execution, automatic preflight enforcement, PR merge or PyPI
publishing is authorised by this dataset's presence.
