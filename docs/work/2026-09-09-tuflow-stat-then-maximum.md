# TUFLOW statistic-then-maximum raster workflow

| Field | Value |
| --- | --- |
| Status | Complete |
| Owner | Unassigned |
| Created | 2026-09-09 |
| Updated | 2026-09-10 |
| Next review | — |
| Baseline | `main` at `13f02c6`; interrupted staged implementation recovered with unrelated `vendor/run_hy8` changes preserved |

## Outcome and scope

The temporal-pattern mean then duration-maximum workflow now uses a generic
mean-or-median first stage. A maintained median wrapper exposes the same project
settings and CLI controls as the existing mean wrapper. When source output is
enabled, the duration-maximum source raster and legend resolve directly to the
original temporal-pattern inputs.

## Current state

Implementation, documentation, compatibility forwarding, focused tests and the
package build are complete. The old mean orchestrator module and its callable
interfaces remain as thin compatibility APIs through 2026-12-31. The workflows
retain their existing overwrite behavior and currently recalculate prepared
outputs rather than validating and skipping existing results.

The mean workflow exposes `asc_to_asc`, `closest_source` and `arithmetic`
value/source methods. Its default is `asc_to_asc`: write the arithmetic mean
while selecting the lowest original TP value at or above it for provenance.

The new wrapper was assessed for MCP discovery. It remains uncatalogued because
its scenario, result-type, input-glob and TP-set configuration is intentionally
edited in a project wrapper rather than exposed as a bounded agent-facing CLI.

## Next action

Maintainer review and Git delivery; no implementation follow-up is required for
the requested scope.

## Completion criteria

- Mean and upper-median temporal-pattern aggregation share one orchestrator.
- Maximum jobs retain deterministic duration ordering and project-aware NoData policies.
- Median outputs use `TPMedian` and `TPMedian-DurMax` naming.
- Final source provenance points directly to original TP rasters.
- Existing mean imports, functions and wrapper behavior remain supported.
- Mean value/source selection is configurable and defaults to ASC_to_ASC behavior.
- Focused raster tests, strict typing, wrapper checks, documentation checks and package build pass.

## Validation and delivery

Validated on Windows with Python 3.14 on 2026-09-09:

- Black: seven modified Python files unchanged.
- Strict Pyright: `0 errors, 0 warnings, 0 informations` for all modified Python files.
- Loguru formatting check: passed.
- Focused tests: `18 passed`.
- Broader ASC_to_ASC raster suite: `53 passed, 4 skipped`; executable parity cases skipped because `ASC_TO_ASC_EXE` was not set.
- Full non-slow suite: attempted, but exceeded the five-minute command limit before pytest emitted a result.
- Both wrapper `--help` checks passed; median missing-directory behavior returned exit code `1` without pausing.
- Package build passed and produced `dist/ryan_functions-26.9.10.1-py3-none-any.whl`.

Additional validation on Windows with Python 3.14 on 2026-09-10:

- Mean-method focused raster and wrapper tests: `42 passed`.
- The default ASC_to_ASC regression verified that inputs `1`, `4` and `10`
  write arithmetic mean `5` while source provenance selects the `10` raster.
- Strict Pyright, Black, Loguru formatting, wrapper `--help` and documentation checks passed.
- Package version `26.09.10.1` built successfully.

Pytest used an explicit local `--basetemp` and ignored inaccessible
`pytest-cache-files-*` directories created on the network checkout. The usual
Rasterio pending-deprecation warnings and an environment-level unknown
`cache_dir` warning remained non-failing.

Changes are uncommitted and partly staged from the interrupted session. The
package version, replacement wheel and documentation updates are unstaged.
The unrelated modified `vendor/run_hy8` submodule was not changed. A new
unrelated `.gitignore` modification appeared during the 2026-09-10 work and was
also preserved without alteration.

## Progress

### 2026-09-09

Recovered the interrupted implementation, corrected statistic-neutral labels
and failure output, preserved constructor compatibility fields, added the
median provenance regression, documented current overwrite behavior, and
completed the required validation and package rebuild.

### 2026-09-10

Exposed the mean value/source method through the generic and compatibility APIs
and the maintained mean wrapper. Set the workflow default to `asc_to_asc`,
while retaining `closest_source` and `arithmetic` as explicit alternatives.
