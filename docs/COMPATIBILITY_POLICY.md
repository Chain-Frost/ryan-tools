# Compatibility Policy & Inventory

This document serves as the authoritative inventory for all compatibility-only modules and legacy imports in `ryan-tools`. It defines explicit replacements, support deadlines, and the required checklist for their eventual removal.

## Early removal handoff — 2026-10-18

| Field | Value |
| --- | --- |
| Status | Implemented in PR #120; awaiting merge and release |
| Owner | Unassigned |
| Updated | 2026-10-18 |
| Next review | PR #120 acceptance and release |
| Baseline for registration | `main` / `f7118e2` |

Tracked in the [work register](work/README.md). On 18 October 2026, the maintainer explicitly
approved bringing forward removal of the `ryan_library.scripts` namespace and the other
December-dated deprecated APIs when no maintained production callers were found. All changes
are implemented in PR #120, not yet released. Old external import paths may break; update
copied wrappers and scripts before upgrading.

## Removed `ryan_functions` import namespace — 2026-10-18

The maintainer explicitly authorised removal of `ryan_functions/__init__.py` with the installed-script packaging change.
The import alias and its import hook are removed; use `ryan_library.functions` and its maintained submodules instead.
No active internal Python callers were found. External users must update imports before upgrading.
The PyPI distribution is renamed from `ryan_functions` to `ryan-tools`; Python imports remain under `ryan_library`.
The PyPI and local-wheel installers automatically remove `ryan_functions` before installing `ryan-tools`, because both
distributions own the same `ryan_library` files. Users installing directly with pip must uninstall the old distribution first. The new PyPI project needs its own Trusted Publisher configuration before publication.
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
legacy imports, and is effective in the next published distribution.

The additional unused GDAL/data-processing modules and the POMM wrapper-only compatibility helpers
were also retired early in the same PR following the maintainer's explicit authorisation.
The general-purpose `ryan_library.functions.wrapper_utils` module remains supported.

## Remaining December compatibility APIs removed early — 2026-10-18

A current code search found no maintained repository imports of `gdal_environment`,
`gdal_runners`, or the unfinished `data_processing` module. The latter was used only
by its own tests. Likewise, no maintained wrapper imports the deprecated
`PeakReportExporter`, `PommPeakWrapperDefaults`, or `run_pomm_peak_report_wrapper`
from `wrapper_utils`; only the documentation referenced them. All six symbols
and the three obsolete module files have been removed. The general wrapper
utilities, TUFLOW parser, and in-process GDAL raster APIs are unchanged.

**Breaking API changes:** import modern raster functions from
`ryan_library.functions.gdal.raster_processing`; for POMM reports use
`ryan_library.orchestrators.tuflow.pomm_max_items` or the current wrappers.
There is no direct replacement for the unfinished `data_processing.safe_apply` API;
maintained string metadata parsing uses `TuflowStringParser`. External scripts relying
on these legacy names must migrate. These changes take effect in the next distribution.

## Compatibility Inventory

| Legacy Module / Import Path | Supported Replacement | Warning Category | Support Deadline | Known Callers |
| ----------------------------- | ----------------------- | ------------------ | ------------------ | --------------- |
| `ryan_library.scripts.*` (entire namespace) | Direct orchestrator/function imports or maintained `ryan-scripts` wrappers | **REMOVED** | **18 October 2026** (brought forward) | None internal; external callers may need migration |
| `ryan_library.functions.gdal.gdal_environment` | Installed Python GDAL environment directly | **REMOVED** | **18 October 2026** (brought forward) | None internal; external callers may need migration |
| `ryan_library.functions.gdal.gdal_runners` | `ryan_library.functions.gdal.raster_processing` | **REMOVED** | **18 October 2026** (brought forward) | None internal; external callers may need migration |
| `ryan_library.functions.data_processing` | Supported TUFLOW parser APIs (`TuflowStringParser`); no `safe_apply` replacement | **REMOVED** | **18 October 2026** (brought forward) | Tests only; external callers unknown |
| Legacy POMM helpers in `ryan_library.functions.wrapper_utils` | Maintained `pomm_max_items` orchestrator and wrappers | **REMOVED** | **18 October 2026** (brought forward) | No maintained callers; legacy copied wrappers unknown |
| `ryan_library.functions.misc_functions.setup_logging` | **REMOVED** (Use `loguru_helpers.py` for logging configuration) | N/A | Removed | None |

> [!WARNING]
> All removals listed above are intentional API breaks in the next release; unrelated maintained modules remain supported.

## Deprecation & Removal Checklist

Use this checklist when retiring additional compatibility APIs. For the removals in PR #120,
source, docs, test cleanup and wheel exclusion checks are implemented; verify CI and document
breaking changes when the release is published:

- [ ] **Migrate Callers**: Verify and update any known callers (including external repository references or documentation examples) to use the supported replacement.
- [ ] **Update Documentation**: Remove references to the legacy import path from README files, tutorials, and examples.
- [ ] **Remove Code**: Delete the deprecated Python modules, forwarding shims, and package-level aliases.
- [ ] **Remove Tests**: Remove any tests specifically testing the compatibility layer/shims.
- [ ] **Verify Wheel Content**: After building, verify the built wheel `.whl` no longer contains the removed modules/paths.
- [ ] **Release Notes**: Publish release notes stating the breaking removal and documenting the supported replacement.
