# Temporary TUFLOW example reproduction bundle

**Branch-only diagnostic data: remove this entire directory before the PR's final merge.**
Keep durable synthetic regressions in the normal test suite instead. Also remove this README's link
from the central documentation index when removing the bundle. No automatic cleanup is performed.

This bundle contains only input files from the user-supplied TUFLOW Example Model Dataset.
It contains no confidential project data. The one restart file read by EG06_004 is retained as an input. Original input bytes and their folder relationships are
preserved, including real raster files: empty files with raster extensions would invalidate metadata
parsing. No TUFLOW executable, general simulation output, logs, generated `xf` caches, spreadsheets or copied
project Python scripts are included. The compressed archive is approximately 14.4 MiB; its 1,464
members total approximately 19.2 MiB. The source folder is not required after downloading this branch.

## Files

- `examples.zip`: input tree with `runs`, `model`, `bc_dbase` and `pit_dbase` folders.
- `manifest.json`: original relative paths, byte sizes and SHA-256 hashes for every member, plus the
  archive SHA-256. This records local provenance, not independent verification of an upstream release.
- `baseline.csv`: the original 161-TCF results matrix, covering 174 batch-defined runs.
- `run_checks.py`: read-only diagnostic runner; it extracts into a new temporary directory and writes
  reports to a separate directory. It never executes batch commands or starts TUFLOW.

Baseline consumer commit: `a60ba0cc11c06e51395f400a8d0c95100331f37c`.
Baseline PyTUFLOW fork commit: `65f24fa96bbb93f5e836c3391be8837fa0048092`.
Archive SHA-256: `e374c8e072679fc6c4dc11e63b270caa1101b6aebb84d70060872cdd1b0aa7d6`.

## Reproduce

Use normal repository development dependencies and Python 3.14. Run these commands from the
repository root so the module invocation imports this checkout's consumer code. The optional explicit
PyTUFLOW source-checkout argument selects the dependency under investigation without modifying the
global installation. Otherwise the runner uses installed PyTUFLOW, which must expose the deferred API.

Verify the archive without loading PyTUFLOW:

```powershell
python -m tests.fixtures.tuflow_example_repro.run_checks --verify-only
```

Run every example with the local updated dependency:

```powershell
python -m tests.fixtures.tuflow_example_repro.run_checks --pytuflow-checkout E:\Github\PyTuflow --output "$env:TEMP\tuflow-example-reports"
```

Select individual cases (repeat `--tcf` as needed):

```powershell
python -m tests.fixtures.tuflow_example_repro.run_checks --pytuflow-checkout E:\Github\PyTuflow --tcf EG03_011.tcf --tcf EG03_012.tcf
```

Exit zero means all selected cases pass. **Exit one is expected for the complete baseline**:
107 TCFs / 119 runs pass, while 54 TCFs / 55 runs remain unresolved. Read `results.json` and the
per-TCF issue CSVs in the reported output directory. Reports are written incrementally. Input
extraction is cleaned up on exit; the report files remain available. Reusing an explicit output
directory replaces reports with the same names, so use a fresh directory when retaining old evidence.

The harness compares event/scenario context, originating command name, physical file and GeoPackage
layer. It checks actual file/layer existence. Attribute-linked cases remain unresolved until their
complete secondary dependencies can be materialised; matching partial inventories never pass.
The runner deliberately uses the consumer's private reference collector for branch diagnostics;
it is not a supported installed-package command, packaged workflow or automatic CI test.

## Investigate these groups

| Group | Representative cases | Baseline behaviour |
| --- | --- | --- |
| Shapefile attribute sources and table links | EG02_013/014, EG04_004/005/006 | Deferred cannot inspect all secondary inputs. |
| Rainfall grids | EG03_011/012 | Deferred rejects unsupported grid dependencies; eager also raises parser exceptions. |
| Network/pit curve identifiers | EG11_006, EG15_007/008 | Eager exposes identifier `aa` as a path; deferred refuses unsupported Shapefile attribute inspection. |
| Hydraulic BC versus AD BC databases | EG17_001/002/003, EG18_002 | Deferred sees ambiguous BC inputs; eager also cannot resolve the active BC database. |

The original full sweep produced 112 unsupported Shapefile attribute findings, 18 unsupported
database-linked findings (12 table links, two rainfall grids, four AD BC databases), and four
ambiguous-BC findings. These counts overlap models. See
[the authoritative work record](../../../docs/work/2026-10-21-tuflow-input-preflight.md)
for the validation boundaries. Successful input preflight is not hydraulic or engine acceptance.
