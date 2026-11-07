# WP02 — Compatibility lifecycle and import evidence (issue #98)

| Field | Value |
| --- | --- |
| Status | Needs review — baseline evidence gathered; draft PR and dependency-absence verification outstanding |
| Owner | ChatGPT / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-20 |
| Baseline | `main` `5ad632cc9a17b302ad3f4f2b55315a1e2e8d1d51` (2026-11-07), separate from WP02 [#234](https://github.com/Chain-Frost/ryan-tools/issues/234) |
| Delivery | Documentation-only branch `docs/wp02-compatibility-review-98`; no main merge or release authorised |

## Outcome and bounded scope

Reconcile [issue #98](https://github.com/Chain-Frost/ryan-tools/issues/98) and the
[historical August lifecycle audit](../audits/2026-08-08-ryan-library-lifecycle-plan.md)
with actual current namespaces and supported workflows. The [compatibility policy](../COMPATIBILITY_POLICY.md)
owns the authoritative removals, replacement paths, dates and retained deprecated APIs.
[Issue #240](https://github.com/Chain-Frost/ryan-tools/issues/240) delegates this work as WP02.
Do not reopen accepted HY-8 numerical testing, broaden the [script selection audit](../audits/2026-10-19-installed-scripts-inventory.md),
restore removed shims, or change import/export behaviour.

## Reconciled lifecycle and callers

| Historical classification (2026-08-08) | Current `main` classification and evidence |
| --- | --- |
| `ryan_library/scripts` compatibility-only until 2026-12-31; two GDAL shims similarly dated | **Removed 2026-10-18** under explicit accelerated maintainer approval; [PR #120](https://github.com/Chain-Frost/ryan-tools/pull/120) merged `274f09d7`. Do not recreate these imports. |
| `ryan_functions` alias / eager package imports | Alias and old `ryan_library.__getattr__` script-forwarding aliases **removed**. [Package root](../../ryan_library/__init__.py) now has only a docstring; [functions package](../../ryan_library/functions/__init__.py) does not eagerly import processors/engines. Use explicit `ryan_library` imports. |
| `functions/data_processing.py` and `tkinter_utils.py` removal candidates | Both absent. The unfinished `safe_apply` has no direct replacement; for supported filename metadata use `TuflowStringParser`. |
| `functions/tlf_missing_runs.py` removal candidate | **Maintained**, called by [missing-run orchestrator](../../ryan_library/orchestrators/tuflow/tlf_missing_runs.py) and [headless wrapper](../../ryan-scripts/tuflow/find_missing_tlf_runs.py). Original table input and the current filename-inferred campaign workflow are documented in [MISSING_RUNS.md](../../ryan-scripts/tuflow/MISSING_RUNS.md). |
| `functions/hy8` experimental | **Maintained bridge**, used by [`tuflow_to_hy8.py`](../../ryan-scripts/tuflow/tuflow_to_hy8.py) and documented in the [engine guide](../TUFLOW_CULVERT_ENGINES.md). `run-hy8` and `ryan-culverts` are independent required PyPI dependencies, not local gitlink packages. |
| Deprecated POMM report wrappers | `pomm_max_items.run_mean_peak_report` and `run_median_peak_report` remain present, warn with `DeprecationWarning` and forward to corresponding `export_*` functions. **No removal deadline or approval recorded** for these two APIs. |
| POMM helpers in `wrapper_utils` and old GDAL/data-processing modules | Removed as explicitly authorised in #120. General `wrapper_utils`, `loguru_helpers`, maintained TUFLOW parser and direct GDAL processing APIs remain. |

**Caller evidence:** the maintained HY-8 and missing-run wrappers are known production entry points, not
unused test-only modules. The processor parser and notebook APIs also have documented direct-user entry
points, so a repository search finding zero internal callers is not grounds for deletion. Historical
static review found no **maintained internal** callers for explicitly retired shims; unknown outside
projects, old copied scripts and installed third-party callers cannot be disproved by that search.
No published API is scheduled for removal from this review.

## Separate import and packaging evidence

| Context | What was verified on current main | Limitation |
| --- | --- | --- |
| **Source checkout** | [Windows Python CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/42388182051), MCP smoke job, **passed** at `5ad632cc`. Checkout with Python 3.14, editable install, focused tests and `import ryan_library.mcp.server` plus MCP tool registrations passed. Package-root and functions package initializers were also inspected directly. | Not a standalone dependency-absence test; the hosted source environment installs dependencies. |
| **Installed wheel outside checkout** | Same CI's Package job **passed**: built and verified `ryan_tools-26.11.7.13-py3-none-any.whl` (SHA-256 `028e2942e1a88fe87d8798c6525e56bb853076f05f30ecf74b750deacf30f577`), installed in an isolated target, and ran `smoke_test_installed_wheel.py --expected-root`. The test checked physical origins of `ryan_library`, `run_hy8`, `culvert_solver` and vendored imports under that target. Copied log-summary and six TUFLOW result-wrapper smoke scripts also passed with the installed wheel. | Target was given required dependencies; selected help checks skip when optional GIS imports are missing. It is **not** proof of an all-dependencies-absent import. |
| **Published artifact** | [v26.11.7.14](https://github.com/Chain-Frost/ryan-tools/releases/tag/v26.11.7.14) includes a wheel; [PyPI publishing workflow](https://github.com/Chain-Frost/ryan-tools/actions/runs/42388524975) and release-asset workflow succeeded on 2026-11-07. | The `26.11.7.14` release wheel was not downloaded or independently import-smoke-tested by this issue review; the package job verified `26.11.7.13`. |

The CI Package job installs the wheel to a separate directory and deliberately exercises copied
scripts outside the source checkout; a successful source editable import by itself does not establish
wheel correctness. Both types of evidence are retained and named rather than conflated.

## Optional-dependency isolation

The root [`ryan_library/__init__.py`](../../ryan_library/__init__.py),
[`functions/__init__.py`](../../ryan_library/functions/__init__.py) and
[`functions/gdal/__init__.py`](../../ryan_library/functions/gdal/__init__.py)
have no eager GDAL, QGIS or HY-8 imports. The optional external `osgeo` binding is imported when
[`functions.gdal.raster_processing`](../../ryan_library/functions/gdal/raster_processing.py)
is explicitly imported. The [HY-8 bridge package](../../ryan_library/functions/hy8/__init__.py)
does eagerly import `run_hy8` **when that bridge is selected**; this is expected because `run-hy8`
is an unconditional `ryan-tools` requirement in `pyproject.toml`. The MCP SDK is an explicit
`mcp` extra. Importing the package root must not require optional GIS or MCP tooling.

**Verification not completed in this review:** a fresh Python 3.14 source checkout and a separate
installed-wheel process that deliberately blocks `osgeo`, QGIS and the MCP SDK have not been run.
A focused runtime check, run from the respective *normal* environments without a virtual environment,
can use:

```powershell
py -3.14 -c "import sys; sys.modules.update({'osgeo': None, 'qgis': None, 'mcp': None}); import ryan_library, ryan_library.functions, ryan_library.functions.gdal; print(ryan_library.__file__)"
```

For installed-wheel confirmation, execute from **outside the repository** and inspect the printed
package path; for source-checkout confirmation, execute from the checkout and inspect the distinct
source path. A successful probe establishes only the package-initializer boundary, not optional GIS
workflow functionality. Do not report this command as already executed.

## HY-8 and missing-run current acceptance

- **HY-8:** the selected `tuflow_to_hy8.py` and two engine-specific culvert wrappers remain
  available. The bridge exposes the documented TUFLOW Maximums conversion. The
  [AB-023 decision](../audits/2026-10-22-script-remediation-backlog.md) accepted the existing
  comparison evidence on 2026-11-01; no new numerical rerun, external executable trial or
  project-data comparison was performed here.
- **Missing runs:** [#250](https://github.com/Chain-Frost/ryan-tools/pull/250) merged
  filename-ensemble inference, read-only log scanning, optional finished-marker verification
  and standard log-summary export into main at `5ad632cc`. Its
  [dated work record](2026-11-07-missing-tlf-ensembles.md) reports 33 focused local cases,
  installed-wheel smoke and a 1,468-log mapped-drive campaign scan; those are previously
  recorded evidence, not newly repeated by WP02. No native model was launched.

## Next action, validation and delivery

1. Review the new issue #98 draft PR, check its hosted documentation validation at the exact head
   and confirm that no unrelated implementation or package changes entered its diff.
2. When a Python 3.14 source checkout and an installed-wheel installation are available,
   run the separately identified optional-dependency-absence probe in both contexts.
   If it fails, open a **separate bounded implementation issue**; do not silently expand #98
   into code refactoring or remove a public API.
3. Obtain maintainer acceptance before closing #98; keep broader package/MCP source-inventory
   approval on [PR #137](https://github.com/Chain-Frost/ryan-tools/pull/137).

**2026-11-08 validation:** inspected source package initializers, relevant wrappers,
orchestrators, policy, August audit and the current `pyproject.toml`/distribution on GitHub;
reviewed exact-main [CI job steps and package smoke logs](https://github.com/Chain-Frost/ryan-tools/actions/runs/42388182051).
Only Markdown/policy/register/status-record changes are planned. New local Python 3.14 tests,
native HY-8/TUFLOW tests and a local `check_documentation.py` run were **not performed** in this
connector-only environment. No simulation runs, data writes, submodule changes or merge occurred.

## Progress

### 2026-11-08

Recorded current import/lifecycle state, reconciled the merged #120 and subsequent published
release, distinguished source from installed-wheel evidence, identified the remaining
optional-dependency test limitation, and retained separately owned distribution and native
software acceptance gates.
