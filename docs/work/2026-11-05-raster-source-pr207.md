# Raster source output defaults and PR 207 validation

| Field | Value |
| --- | --- |
| Status | Needs review |
| Owner | ChatGPT / maintainer |
| Created | 2026-11-05 |
| Updated | 2026-11-06 |
| Next review | 2026-11-13 |
| Baseline | PR #207 `81d98a30`; PR #238 `dee8e82f`, consolidated by merge `8550b511` on `fix/tuflow-src-profile-cell-counts` |

## Outcome and scope

Tested [PR #207](https://github.com/Chain-Frost/ryan-tools/pull/207) using local raster data
and Python files on the NAS. Enabled source rasters and CSV legends by default in both maintained
mean/median-then-maximum wrappers. `--no-source` disables output; `--source` overrides an edited false
constant. Generic library defaults are unchanged.

## Current state and next action

The user authorised consolidation of #238 into #207 and a default-off incomplete-TP override on 2026-11-06.
#238 is merged into #207's branch; #207 now contains the source defaults, provenance fixes and POMM findings.
Both raster wrappers expose `ALLOW_INCOMPLETE_TP_BINS = False` and matching positive/negative CLI flags.
The [native raster guide](../ASC_TO_ASC_NATIVE_OPERATIONS.md#incomplete-temporal-pattern-bins) owns the
current behaviour: zero-byte inputs count as missing; ordinary runs withhold affected maxima, while an
explicit override calculates from available inputs and warns. Unaffected result groups can finish.
Strict mode conflicts with the override; duplicate/unexpected TPs and processing failures remain errors.

Local implementation, strict typing, focused validation and copied-wrapper wheel checks pass. Delivery is
through `fix/tuflow-src-profile-cell-counts` / [PR #207](https://github.com/Chain-Frost/ryan-tools/pull/207).
Next: review the integrated branch's hosted checks and validate against the user's real data in separate
output folders. #207 remains open against main. Existing NAS/real-data evidence below is historical;
the new override has been tested with synthetic rasters, not the proprietary project ensemble.

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
- Ruff and strict Pyright validation: passed on the integrated change on 2026-11-06.
- Incomplete-bin values, provenance, zero-byte handling and CLI precedence: passed synthetic validation.
- Integrated hosted validation and real-data override acceptance: review on PR #207 / local handoff.

## Progress

### 2026-11-06: Consolidation and incomplete TP override

- Preserved #238's complete reporting work and raster source defaults while integrating into #207.
- Added an explicit incomplete-bin opt-in to both wrappers and the shared orchestrator. A wholly unusable
  bin remains a finding and can only be omitted with the override; no usable jobs still fails. Existing
  withheld maxima are preserved with a warning that they were not refreshed.
- Linux Python 3.14.10, real GDAL/Rasterio: **117 focused tests passed** (70 raster and 47 POMM), including
  actual raster processing, unequal TP-bin provenance, source cell counts and workbook round trips.
  Tests selected Windows-style `spawn`; this environment disallows POSIX forkserver sockets.
- Ruff 0.16.6 formatting/lint and strict Pyright passed for the changed Python files. Corrected inherited
  #238 lint/type issues without changing peak selection. Documentation, Loguru policy and whitespace pass.
- `python repo-scripts/build_library.py --skip-pip` built and verified **26.11.6.3**; wheel SHA-256
  `2fca993c39b089ffc0ae142a3ded032152101ed35ada7a1a373fb47a3fc6fcb1`.
- Installed that wheel into a separate validation target and ran unmodified copied mean/median wrappers
  outside the checkout. Both exposed both flags; default dry-run failed on 2/10 inputs; override runs
  succeeded with mean 6 / upper median 10 for inputs 2 and 10. Final values, TP02 provenance, two-row
  original-input legends, cell counts and DEFLATE source compression all passed. The ordinary installed
  package was not replaced.
- Independent implementation review found no blocking defect; added explicit stale-maximum wording from
  that review. Both wrappers already belong to the installed-script inventory, so no duplicate MCP tool
  or workflow entry is needed; their help and canonical documentation expose the new setting.
- Unrelated overdue work-register entries remain outside this task and were not advanced.

### 2026-11-06: GitHub delivery

Uploaded code, tests and anonymised evidence in [draft PR #238](https://github.com/Chain-Frost/ryan-tools/pull/238).
Raster validation is also posted on [PR #207](https://github.com/Chain-Frost/ryan-tools/pull/207#issuecomment-6675605193).
On the current-main branch, 37 library tests and 10 orchestrator/report tests passed. Initial report-test
failures were caused by the new worktree lacking tutorial fixtures; copying the bounded Module_01 fixture
restored all 10 passes. Documentation index/links and whitespace passed. Hosted lint/type validation remains
pending. No merge, release, package installation or project-data upload occurred; PyTUFLOW is excluded.

### 2026-11-05

Tested the pinned PR and delivered wrapper defaults locally. No PR publication was requested.
The register also contains overdue unrelated review items (including vendor integration, installed
scripts and release automation); those were left unchanged and are outside this raster task.
