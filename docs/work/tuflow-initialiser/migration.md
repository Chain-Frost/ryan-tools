> **Historical record from [tuflow-initialiser](https://github.com/Chain-Frost/tuflow-initialiser).** Pending or proposed work is tracked in ryan-tools migration PR #246 and issues #242–#245; old issue numbers below are retained for provenance.

# Migration from ryan-tools

The independent `project_setup.py` and maintained TUFLOW control templates were copied from `ryan_library/orchestrators/tuflow/project_setup.py` and `ryan_library/resources/tuflow_templates`. License copied from `ryan-tools`.

Differences: no default copying of utilities; optional explicit source; initial working GIS `2d_code`, `2d_loc`, `2d_rf` only; `2d_bc` reference commented out.

Original TUFLOW `2d_loc_qa_check.qml` referenced in ryan-tools is missing in current source; it is optional and deliberately not invented.

**Removal from ryan-tools is a separate future PR after testing**: update any importers, package includes, wrapper, tests and documentation and validate standalone setup on actual installed TUFLOW before removing originals.
