> **Historical source documentation.** The initialiser migration is merged into `ryan-tools` through [#204](https://github.com/Chain-Frost/ryan-tools/pull/204). Current delivery and accepted evidence are in the [living record](README.md); [#312](https://github.com/Chain-Frost/ryan-tools/issues/312) owns continuing development/engineering acceptance and [#247](https://github.com/Chain-Frost/ryan-tools/issues/247) owns source retirement. Earlier installation paths, issue numbers and pending/merge statements below are retained as history.

# Migration from ryan-tools

The independent `project_setup.py` and maintained TUFLOW control templates were copied from `ryan_library/orchestrators/tuflow/project_setup.py` and `ryan_library/resources/tuflow_templates`. License copied from `ryan-tools`.

Differences: no default copying of utilities; optional explicit source; initial working GIS `2d_code`, `2d_loc`, `2d_rf` only; `2d_bc` reference commented out.

Original TUFLOW `2d_loc_qa_check.qml` referenced in ryan-tools is missing in current source; it is optional and deliberately not invented.

**Removal from ryan-tools is a separate future PR after testing**: update any importers, package includes, wrapper, tests and documentation and validate standalone setup on actual installed TUFLOW before removing originals.
