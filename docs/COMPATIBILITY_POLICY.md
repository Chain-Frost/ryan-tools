# Compatibility Policy & Inventory

This document serves as the authoritative inventory for all compatibility-only modules and legacy imports in `ryan-tools`. It defines explicit replacements, support deadlines, and the required checklist for their eventual removal.

## Scheduled removal handoff — 2026-09-06

| Field | Value |
| --- | --- |
| Status | Deferred |
| Owner | Unassigned |
| Updated | 2026-09-06 |
| Next review | 2027-01-04 |
| Baseline for registration | `main` / `f7118e2` |

Tracked in the [work register](work/README.md). The original 2026-12-31 retirement date remains
in force for the separate deprecated GDAL and data-processing modules. On 2026-10-18, the
maintainer explicitly brought forward the entire `ryan_library.scripts` retirement for PR #120,
accepting that external copies of legacy imports may break. The 2027-01-04 review date for the
other compatibility APIs is unchanged.

## Removed `ryan_functions` import namespace — 2026-10-18

The maintainer explicitly authorised removal of `ryan_functions/__init__.py` with the installed-script packaging change.
The import alias and its import hook are removed; use `ryan_library.functions` and its maintained submodules instead.
No active internal Python callers were found. External users must update imports before upgrading.
The PyPI distribution is renamed from `ryan_functions` to `ryan-tools`; Python imports remain under `ryan_library`.
For an existing installation, uninstall `ryan_functions` before installing `ryan-tools`, because both distributions
own the same `ryan_library` files. The new PyPI project needs its own Trusted Publisher configuration before publication.
The separate `ryan_library.scripts` namespace is also removed early following a distinct maintainer decision.
Other deprecated functions retain their previous deadlines.

## Removed `ryan_library.scripts` import namespace — 2026-10-18

The maintainer brought forward the 31 December 2026 deadline to **18 October 2026**.
PR #120 removes all 20 deprecated compatibility files, including the TUFLOW, GDAL, RORB,
and wrapper forwarding modules, as well as the six aliases formerly exposed through
`ryan_library.__getattr__`. Maintained internal code uses direct imports and does not need
this namespace. External users must update imports to the corresponding
`ryan_library.orchestrators.*` or `ryan_library.functions.*` modules, or use the
maintained entry points in `ryan-scripts`. The change is intentionally breaking for
legacy imports, and is effective in the next published distribution.

This earlier removal **does not** bring forward retirement of the separate deprecated
GDAL/data-processing functions or legacy POMM wrapper helpers; their December 2026
end dates remain unchanged.

## Compatibility Inventory

| Legacy Module / Import Path | Supported Replacement | Warning Category | Support Deadline | Known Callers |
| ----------------------------- | ----------------------- | ------------------ | ------------------ | --------------- |
| `ryan_library.scripts.*` (entire namespace) | Direct orchestrator/function imports or maintained `ryan-scripts` wrappers | **REMOVED** | **18 October 2026** (brought forward) | None internal; external callers may need migration |
| `ryan_library.functions.gdal.gdal_environment` | Installed Python GDAL environment directly | `DeprecationWarning` | 31 December 2026 | None internal; external callers unknown |
| `ryan_library.functions.gdal.gdal_runners` | `ryan_library.functions.gdal.raster_processing` | `DeprecationWarning` | 31 December 2026 | None internal; external callers unknown |
| `ryan_library.functions.data_processing` | No replacement; unfinished API | `DeprecationWarning` | 31 December 2026 | None internal; external callers unknown |
| `ryan_library.functions.misc_functions.setup_logging` | **REMOVED** (Use `loguru_helpers.py` for logging configuration) | N/A | Removed | None |

> [!WARNING]
> Remaining compatibility modules must strictly forward calls to maintained code and must **not** contain any independent workflow logic.

## Deprecation & Removal Checklist

When a support deadline is reached, the following checklist must be followed to completely remove the compatibility layer:

- [ ] **Migrate Callers**: Verify and update any known callers (including external repository references or documentation examples) to use the supported replacement.
- [ ] **Update Documentation**: Remove references to the legacy import path from README files, tutorials, and examples.
- [ ] **Remove Code**: Delete the deprecated Python modules, forwarding shims, and package-level aliases.
- [ ] **Remove Tests**: Remove any tests specifically testing the compatibility layer/shims.
- [ ] **Verify Wheel Content**: After building, verify the built wheel `.whl` no longer contains the removed modules/paths.
- [ ] **Release Notes**: Publish release notes stating the breaking removal and documenting the supported replacement.
