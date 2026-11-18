# TUFLOW rainfall and loss input audit: 18 November 2026

- Issues: [#323](https://github.com/Chain-Frost/ryan-tools/issues/323) base,
  [#324](https://github.com/Chain-Frost/ryan-tools/issues/324) stacked material × soil grouping.
- Current delivery: source-only initial implementation in a draft PR; no
  merge or TUFLOW execution authorised.
- Initial implementation: inert BAT/TXT plan reuse, selected TCF contexts,
  BC-adjusted RF series totals, material/soil definition inventories,
  factor-only grouping and non-overwriting JSON.
- Known limitations: no certified effective rainfall after RF GIS/global
  adjustments, no native TUFLOW validation, no verified spatial activity
  without paired model-cell check grids; nonzero status on unresolved runs.
- Validation: synthetic Python tests for defined data and factor grouping.
  Windows Python 3.14, installed wheel, native model/SMB and PyTUFLOW
  integration checks not performed in this environment.
- Next: review source, validate against genuine model data; complete #324
  on a stacked PR; identify required public PyTUFLOW APIs for unresolved
  rainfall factors and native check-grid verification. Keep #319 separate.
- Next review: 2026-11-23.


## 18 November: stacked material/soil grouping

- #324 implementation derives observed cell-centre pairs from externally supplied
  aligned raster check grids (plus optional active mask) and groups by effective
  loss configurations, omitting neutral dimensions rather than multiplying
  all defined material and soil IDs.
- A zero material IL/CL does not suppress material imperviousness when soil
  infiltration remains active; unsupported methods/unknown IDs fail closed.
- Six focused tests passed in local portable Python 3.13.5; Windows/Python
  3.14, installed-wheel/PyTUFLOW and genuine native TUFLOW check-grid acceptance
  remain outstanding. This does not certify rainfall or infiltration physics.
- Stacked draft PR #324 targets the #323 implementation branch. No merge or
  release is authorised; next review 2026-11-23.
