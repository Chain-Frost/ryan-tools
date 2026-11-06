# POMM mean peak coverage findings

| Field | Value |
| --- | --- |
| Status | Needs review |
| Owner | Unassigned |
| Created | 2026-11-05 |
| Updated | 2026-11-05 |
| Next review | 2026-11-12 |
| Baseline | Primary NAS checkout; existing unpublished raster edits preserved |

## Outcome

The mean report adds tested duration bounds, sampled and missing duration lists, explicit peak-boundary,
ensemble-size and duration-coverage warnings, and an overall findings status to `aep-mean-max`.
Every check is scoped to AEP/location/type/run. Original selected peak values remain unchanged.
The wrapper exposes editable expected ensemble size and duration schedule; the Excel data dictionary
records the effective settings and describes the new columns. Details and duration-list provenance are in
the [TUFLOW scripts guide](../../ryan-scripts/TUFLOW-python/README.md#mean-peak-coverage-findings).

## Validation

Windows/Python 3.14, 2026-11-05, Python source on NAS; scratch data and real outputs on local disk.

```powershell
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'
python -m pytest tests/functions/tuflow/test_pomm_coverage_findings.py tests/functions/tuflow/test_pomm_utils.py tests/functions/tuflow/test_pomm_utils_coverage.py tests/functions/tuflow/test_pomm_metadata_provenance.py tests/scripts/test_pomm_max_items.py tests/scripts/test_pomm_peak_report.py -q -p no:cacheprovider --basetemp C:/Temp/pomm-findings-tests
python repo-scripts/build_library.py --no-bump --skip-pip
python repo-scripts/check_loguru_formatting.py
```

47 tests passed, including actual workbook round-trip coverage. The disabled cache plugin produces one
benign unknown-cache-dir warning. Build and wheel verification passed for checkout version 26.11.5.9;
the existing installed version 26.11.5.10 was not downgraded or replaced. Ruff and strict Pyright executable
launches were denied by Windows application control; these checks remain pending.

A real source-checkout run processed 110 local POMM files. All original columns of all 16 peak-report rows
exactly match the previous workbook. All bins contained 10 records; the findings identified two lower-bound
peaks and missing intermediate 45/540-minute durations. The separate local `pomm_coverage_review` directory
holds the workbook; a project-local NAS validation script retains the comparison assertions.

## Delivery and next action

On 2026-11-06, the user authorised GitHub delivery. Changes are isolated on
`feat/tuflow-reporting-findings-20261106` from current main, excluding unrelated RORB and PyTUFLOW work. The updated
wrapper requires the updated shared library: run with the NAS checkout on process-local `PYTHONPATH` for
development, or install a newly versioned build after reconciling the newer installed package. Do not replace
the installed 26.11.5.10 with this verification build of 26.11.5.9. Next: run lint/strict typing in an allowed
environment and prepare the matching package update before ordinary installed-package wrapper use.

### Related timeseries validation

The existing NAS peak-timing and stability wrappers were also exercised on local results with installed
26.11.5.10. Peak timing: 1,888 readable PO flow series passed, with at least 5.9 hours between peak and
recorded end and maximum residual 1.12% of the peak excursion. Three files were empty at that scan.
Stability: 1,783 series flagged, 1,352 flat, 877 OK and four empty-file rows. The scans occurred while
results were changing, so their file counts differ. These are screening results, not solver completion
or numerical-stability certification: the default stability rule flags more than two significant slope
sign reversals across the whole hydrograph, which can include physical multi-peaked responses.
No script changes were needed. Proprietary outputs, paths and location identifiers remain local.

MCP assessment: this extends the existing report export contract, with no separate tool or duplicate CLI
needed. The canonical script guide documents the new inputs and output semantics.

The register contains overdue unrelated vendor integration, installed-script and release-automation reviews;
these were not advanced or resumed. Concurrent RORB/MCP changes were preserved.

## Progress

### 2026-11-05

Implemented and tested all requested findings. Duration gaps are checked only inside sampled bounds;
non-standard or unrecognised durations are explicit warnings. Counts refer to input records, not distinct
TP identities. No statistical selection rules were changed.
