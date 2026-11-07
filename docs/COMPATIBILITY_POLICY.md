# Compatibility Policy & Inventory

This document serves as the authoritative inventory for all compatibility-only modules and legacy imports in `ryan-tools`. It defines explicit replacements, support deadlines, and the required checklist for their eventual removal.

## Early removal delivery — 2026-10-18

| Field | Value |
| --- | --- |
| Status | Merged in [PR #120](https://github.com/Chain-Frost/ryan-tools/pull/120) on 2026-10-18; subsequently released |
| Owner | Unassigned |
| Updated | 2026-11-08 (bounded lifecycle review: [issue #98](https://github.com/Chain-Frost/ryan-tools/issues/98)) |
| Next review | None for the completed early removals; reassess only if another API retirement is proposed |
| Original policy baseline | `main` / `f7118e2` (historical) |
| Review baseline | `main` / `5ad632cc` (2026-11-07) |

On 18 October 2026, the maintainer explicitly approved bringing forward removal of the
`ryan_library.scripts` namespace and other December-dated deprecated APIs after checking
maintained callers. [PR #120](https://github.com/Chain-Frost/ryan-tools/pull/120) merged as
`274f09d7` on that date. The subsequent published [v26.11.7.14 GitHub release](https://github.com/Chain-Frost/ryan-tools/releases/tag/v26.11.7.14)
contains a `ryan-tools` wheel; the prior [main Windows package job](https://github.com/Chain-Frost/ryan-tools/actions/runs/42388182051)
separately verified and smoke-tested an isolated `26.11.7.13` wheel.
These are **completed breaking changes**, not pending December removals.
Old copied scripts using the retired imports must migrate; external callers remain unknown.
The [dated lifecycle evidence](work/2026-11-08-wp02-compatibility-lifecycle.md) distinguishes source
and installed-wheel verification from tests not performed.

## Removed `ryan_functions` import namespace — 2026-10-18

The maintainer explicitly authorised removal of `ryan_functions/__init__.py` with the installed-script packaging change.
The import alias and its import hook are removed; use `ryan_library.functions` and its maintained submodules instead.
No active internal Python callers were found. External users must update old import paths before using the current distribution.
The PyPI distribution is renamed from `ryan_functions` to `ryan-tools`; Python imports remain under `ryan_library`.
The PyPI and local-wheel installers automatically remove `ryan_functions` before installing `ryan-tools`, because both
distributions own the same `ryan_library` files. Users installing directly with pip must uninstall the old distribution first. The new-name project is already published; no future Trusted Publisher setup is implied by this historical migration.
The separate `ryan_library.scripts` namespace and the remaining unused December compatibility
APIs are also removed early under subsequent explicit maintainer decisions.

## Removed `ryan_library.scripts` import namespace — 2026-10-18

The maintainer brought forward the 31 December 2026 deadline to **18 October 2026**.
PR #120 removes all 20 deprecated compatibility files, including the TUFLOW, GDAL, RORB,
and wrapper forwarding modules, as well as the six aliases formerly exposed through
`ryan_library.__getattr__`. Maintained internal code uses direct imports and does not need
this namespace. External users must update imports to the corresponding
`ryan_library.orchestrators.*` or `ryan_library.functions.*` modules, or use the
maintained entry points in `ryan-scripts`. The change is intentionally breaking for
legacy imports, and is already effective in the current distribution.

The additional unused GDAL/data-processing modules and the POMM wrapper-only compatibility helpers
were also retired early in the same PR following the maintainer's explicit authorisation.
The general-purpose `ryan_library.functions.wrapper_utils` module remains supported.

## Remaining December compatibility APIs removed early — 2026-10-18

A current code search found no maintained repository imports of `gdal_environment`,
`gdal_runners`, or the unfinished `data_processing` module. The latter was used only
by its own tests. Likewise, no maintained wrapper imports the deprecated
`PeakReportExporter`, `PommPeakWrapperDefaults`, or `run_pomm_peak_report_wrapper`
from `wrapper_utils`; only the documentation referenced them. Those obsolete helpers
and the three corresponding legacy modules have been removed. The general wrapper
utilities, TUFLOW parser, and in-process GDAL raster APIs are unchanged.

**Breaking API changes:** import modern raster functions from
`ryan_library.functions.gdal.raster_processing`; for POMM reports use
`ryan_library.orchestrators.tuflow.pomm_max_items` or the current wrappers.
There is no direct replacement for the unfinished `data_processing.safe_apply` API;
maintained string metadata parsing uses `TuflowStringParser`. External scripts relying
on these legacy names must migrate. These changes are already effective in the current distribution.

## Compatibility Inventory

| Legacy Module / Import Path | Supported Replacement | Status / warning | Removal date | Known callers |
| --- | --- | --- | --- | --- |
| `ryan_functions` import alias and hook | Import from `ryan_library.functions` (or the relevant maintained `ryan_library` module) | **REMOVED**; no runtime deprecation warning | **18 October 2026** | No maintained internal callers; unknown external scripts |
| Six old `ryan_library.__getattr__` script-forwarding aliases | Direct maintained imports or current entry points | **REMOVED**; no runtime deprecation warning | **18 October 2026** | No maintained internal callers; unknown external scripts |
| `ryan_library.scripts.*` (entire namespace) | Direct orchestrator/function imports or maintained `ryan-scripts` wrappers | **REMOVED** | **18 October 2026** (brought forward) | None internal; external callers may need migration |
| `ryan_library.functions.gdal.gdal_environment` | Installed Python GDAL environment directly | **REMOVED** | **18 October 2026** (brought forward) | None internal; external callers may need migration |
| `ryan_library.functions.gdal.gdal_runners` | `ryan_library.functions.gdal.raster_processing` | **REMOVED** | **18 October 2026** (brought forward) | None internal; external callers may need migration |
| `ryan_library.functions.data_processing` | Supported TUFLOW parser APIs (`TuflowStringParser`); no `safe_apply` replacement | **REMOVED** | **18 October 2026** (brought forward) | Tests only; external callers unknown |
| Legacy POMM helpers in `ryan_library.functions.wrapper_utils` | Maintained `pomm_max_items` orchestrator and wrappers | **REMOVED** | **18 October 2026** (brought forward) | No maintained callers; legacy copied wrappers unknown |
| `ryan_library.functions.misc_functions.setup_logging` | `ryan_library.functions.loguru_helpers` | **REMOVED** | Previously removed (not part of the December acceleration) | No maintained callers |
| `ryan_library.orchestrators.tuflow.pomm_max_items.run_mean_peak_report` and `run_median_peak_report` | `export_mean_peak_report` and `export_median_peak_report` in the same module | **RETAINED**; `DeprecationWarning` on invocation | **No removal deadline specified** | Existing external calls unknown; do not infer removal approval |

> [!WARNING]
> The REMOVED rows document already-shipped intentional API breaks. The RETAINED POMM forwarding functions are
> separate deprecated APIs: lack of a known caller is not authority to remove them. Historical 31 December 2026
> deadlines were brought forward **only for the explicitly authorised rows above**. Other compatibility APIs
> require their own usage review, decision and support deadline.

## Deprecation & Removal Checklist

Use this checklist **for a future proposed removal**; it is not a list of still-pending actions
for already-merged PR #120. The original source/cleanup and packaging work has shipped, and the
current Windows main package job verifies an isolated wheel. For another API, assess documented
external entry points and source/installed-wheel imports before setting a deadline:

- [ ] **Migrate Callers**: Verify and update any known callers (including external repository references or documentation examples) to use the supported replacement.
- [ ] **Update Documentation**: Remove references to the legacy import path from README files, tutorials, and examples.
- [ ] **Remove Code**: Delete the deprecated Python modules, forwarding shims, and package-level aliases.
- [ ] **Remove Tests**: Remove any tests specifically testing the compatibility layer/shims.
- [ ] **Verify Wheel Content**: After building, verify the built wheel `.whl` no longer contains the removed modules/paths.
- [ ] **Release Notes**: Publish release notes stating the breaking removal and documenting the supported replacement.
