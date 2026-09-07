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

Tracked in the [work register](work/README.md). Removal waits until support through 2026-12-31 ends. At review, verify
current callers and inventory accuracy, then select removals using the checklist below. The checklist defines completion;
unknown external use must be resolved rather than inferred from static absence. Other namespaces need their own explicit
support decision. Registration did not run compatibility tests or change API deadlines. This handoff is uncommitted
documentation only; no implementation or publishing work is claimed.

## Compatibility Inventory

| Legacy Module / Import Path | Supported Replacement | Warning Category | Support Deadline | Known Callers |
| ----------------------------- | ----------------------- | ------------------ | ------------------ | --------------- |
| `ryan_library.scripts.*` (entire namespace) | Direct module imports (e.g. `ryan_library.orchestrators.*`) or new wrappers | `DeprecationWarning` | 31 December 2026 | None internal; external callers unknown |
| `ryan_library.functions.gdal.gdal_environment` | Installed Python GDAL environment directly | `DeprecationWarning` | 31 December 2026 | None internal; external callers unknown |
| `ryan_library.functions.gdal.gdal_runners` | `ryan_library.functions.gdal.raster_processing` | `DeprecationWarning` | 31 December 2026 | None internal; external callers unknown |
| `ryan_library.functions.data_processing` | No replacement; unfinished API | `DeprecationWarning` | 31 December 2026 | None internal; external callers unknown |
| `ryan_library.functions.misc_functions.setup_logging` | **REMOVED** (Use `loguru_helpers.py` for logging configuration) | N/A | Removed | None |

> [!WARNING]
> Compatibility modules must strictly forward calls to maintained code and must **not** contain any independent workflow logic.

## Deprecation & Removal Checklist

When a support deadline is reached, the following checklist must be followed to completely remove the compatibility layer:

- [ ] **Migrate Callers**: Verify and update any known callers (including external repository references or documentation examples) to use the supported replacement.
- [ ] **Update Documentation**: Remove references to the legacy import path from README files, tutorials, and examples.
- [ ] **Remove Code**: Delete the deprecated Python modules, forwarding shims, and package-level aliases.
- [ ] **Remove Tests**: Remove any tests specifically testing the compatibility layer/shims.
- [ ] **Verify Wheel Content**: After building, verify the built wheel `.whl` no longer contains the removed modules/paths.
- [ ] **Release Notes**: Publish release notes stating the breaking removal and documenting the supported replacement.
