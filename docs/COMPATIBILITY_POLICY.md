# Compatibility Policy & Inventory

This document serves as the authoritative inventory for all compatibility-only modules and legacy imports in `ryan-tools`. It defines explicit replacements, support deadlines, and the required checklist for their eventual removal.

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
