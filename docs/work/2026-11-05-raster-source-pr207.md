# Raster source output defaults and PR 207 validation

| Field | Value |
| --- | --- |
| Status | Needs review |
| Owner | Unassigned |
| Created | 2026-11-05 |
| Updated | 2026-11-05 |
| Next review | 2026-11-12 |
| Baseline | PR #207 detached head `81d98a30f0e9061b280671d9f2742c7371f97860`; isolated NAS worktree |

## Outcome and scope

Tested [PR #207](https://github.com/Chain-Frost/ryan-tools/pull/207) using local raster data
and Python files on the NAS. Enabled source rasters and CSV legends by default in both maintained
mean/median-then-maximum wrappers. `--no-source` disables output; `--source` overrides an edited false
constant. Generic library defaults are unchanged.

## Current state and next action

Implementation and runtime validation are finished. Run Ruff and strict Pyright in an allowed environment
before incorporating these unstaged changes into a PR. Windows application control denied both executables.
The user authorised GitHub delivery on 2026-11-06. The reporting follow-up branch
`feat/tuflow-reporting-findings-20261106` contains the wrapper defaults and tests; PR #207 retains
the separate provenance fixes. No merge or package installation was performed. The primary checkout contains
unrelated RORB/MCP work, preserved throughout. Only wrapper defaults/tests/documentation were copied
back to the primary checkout; the PR library implementation remains in the isolated checkout.

## Validation and delivery

Windows, Python 3.14, 2026-11-05. All Python files were executed from NAS paths; generated test rasters
and real project outputs were local. PR test-data submodule was pinned at
`d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58`; its worktree path and long-path checkout were repaired.
An initial fixture-discovery failure was resolved by restoring the missing long-path fixtures.

```powershell
$env:PYTEST_DISABLE_PLUGIN_AUTOLOAD='1'
python -m pytest tests/functions/tuflow/test_asc_to_asc_raster_operations.py tests/orchestrators/tuflow/test_asc2asc_stat_then_max_by_search.py tests/scripts/tuflow/test_asc2asc_raster_wrappers.py -q -p no:cacheprovider --basetemp C:/Temp/pr207-focused-tests
```

Result: 31 passed. The warning about `cache_dir` follows from disabling the cache plugin.
After copying the defaults to the primary NAS checkout, its wrapper suite also passed all 11 tests
with `--basetemp C:/Temp/pr207-primary-wrapper-tests`. `python repo-scripts/check_documentation.py`,
explicit `--links-only` checks of all four touched documentation files, and `git diff --check` passed.
The real project run processed 330 inputs into 33 means and 3 maxima with source output left at its
new default. All 36 source TIFFs and legends were generated. Blockwise validation verified exact value
and validity-mask parity with the earlier run, DEFLATE source compression, int32 source IDs, zero
NoData IDs, matching grid/CRS, valid original-input legend paths, and CSV cell counts equal to complete
source-raster histograms. Final legends identify 110 original inputs each. Synthetic tests cover
all-input-NoData behavior under the zero policy.

Local results and machine-readable evidence are in the project's `ensemble_statistics_pr207` output
directory (`validation_report.json`). Project-specific validation scripts remain in the isolated NAS
worktree and are not intended for public delivery. Existing result files were preserved.

## Completion criteria

- Default source output, explicit opt-out and explicit opt-in tests pass: met.
- Real local raster run and provenance/compression/value checks pass: met.
- Ruff and strict Pyright validation: pending allowed environment.

## Progress

### 2026-11-05

Tested the pinned PR and delivered wrapper defaults locally. No PR publication was requested.
The register also contains overdue unrelated review items (including vendor integration, installed
scripts and release automation); those were left unchanged and are outside this raster task.
