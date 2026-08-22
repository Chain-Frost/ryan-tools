# README Improvement Plan

Status: implemented on 8 August 2026; validation results are recorded in the task handoff.

## Implementation outcome

- The root README was reduced from 489 to approximately 250 lines and now delegates architecture and environment
  detail to the new documents under `docs/`.
- The docs index preserves the concurrently added development, environment, roadmap and dated-audit material and now
  also indexes MCP setup.
- The legacy GDAL table now describes only the seven BAT files that remain, while the maintained GDAL README covers
  grouped VRT building, overwrite intent and in-place NoData changes.
- The TUFLOW processor README now documents current lifecycle conditions, return types, combination behavior,
  filtering, metadata compaction, HDF caching and extension steps.
- The general script guide now identifies `other` and states the legacy status of BAT folders. The TUFLOW wrapper
  README was intentionally left unchanged after source verification.
- `repo-scripts/check_documentation.py` now checks the seven repository-owned READMEs without traversing submodule
  content and supports links-only checks for prose-heavy plans and audits.

## Scope and conclusion

This plan reconciles two independent README audits against the current checkout. It covers the seven
repository-owned README files and excludes all content inside Git submodules.

The READMEs do not need a wholesale rewrite, but they are not uniformly current. The highest-priority work is to
correct factual contradictions in the root, legacy GDAL, and TUFLOW processor READMEs. Smaller discoverability and
safety improvements should follow. The root README would benefit from a measured reduction in duplicated inventories,
but that should preserve its useful setup and introductory examples.

All explicit relative Markdown links in the seven READMEs currently resolve. The main drift comes from prose and
copied file/API inventories rather than broken Markdown links.

## Reconciliation of the other audit

### Accepted recommendations

1. **Remove the AI-draft disclaimer from the root README.**
   It is an internal drafting note, adds no user value, and accurately signals that the document still needs editorial
   consolidation.

2. **Replace the root README's GDAL batch-tool section.**
   The section presents deleted legacy BAT files as useful current entry points. Current users should be directed to
   `ryan-scripts/gdal-python/README.md`; the BAT README should be retained only as migration guidance for the seven
   BAT files that still exist.

3. **Clarify that root TUFLOW orchestrator and wrapper lists are representative.**
   The other audit correctly identified omitted current entry points such as `tuflow_logsummary_append`,
   `log_processing/append_log_summary_to_master_workbook.py`, closure-duration reporting, and the mean/median wrappers. Because the existing
   headings say “Useful” and “Common,” omission does not make the current text false. Expanding another manually
   maintained inventory would create more drift; explicitly label the lists as representative and link to the
   TUFLOW wrapper README or folder instead.

4. **Add human-facing coverage for `build_VRT.py`.**
   It is absent from the GDAL Python README. It is not absent “everywhere”—it is already described in
   `gdal_cli_tools.json`—but a brief human-readable recipe or catalogue pointer is appropriate.

5. **Leave the TUFLOW wrapper README substantially unchanged.**
   Its wrapper-standard, banner, shared CLI, pause, and POMM compatibility descriptions agree with the current
   implementation.

### Rejected or revised recommendations

1. **Do not remove `setup.py` or label it as a legacy shim.**
   Although project metadata lives in `pyproject.toml`, `setup.py` is an active setuptools build hook. Its custom
   `build_py` command copies pinned QGIS TUFLOW styles into wheel builds and raises an error when those resources are
   unavailable. It was also updated recently. Keep it in the repository map and describe it as the wheel resource
   staging hook if clarification is needed.

2. **Do not remove `TUFLOW-bat` or `misc-bat` as phantom directories.**
   Both directories exist and contain tracked BAT files: three under `TUFLOW-bat` and one under `misc-bat`. The other
   audit's filesystem conclusion was incorrect.

3. **Do update `docs/README.md`.**
   `docs/MCP_SETUP.md` already exists but is missing from the index. This is a present discoverability defect, not
   merely a possible future concern.

4. **Do update `ryan-scripts/gdal-bat/README.md`.**
   Its legacy status is correct, but its migration table is not: twelve of the nineteen named BAT files have already
   been deleted. The README should distinguish the seven remaining files from removed historical variants, or omit
   deleted names entirely.

5. **Do update `ryan_library/processors/tuflow/README.md`.**
   The statement that both `get_processors_by_data_type()` and `check_duplicates()` return a new
   `ProcessorCollection` is false. `check_duplicates()` returns
   `dict[tuple[str, str], list[BaseProcessor]]`. The README also predates current collection features such as EOF
   channel-ID alignment, metadata compaction/reattachment, collection-wide raw-dataframe disposal, filtering, and HDF
   persistence.

## Final implementation plan

### Phase 1: Correct factual errors

#### 1. Root `README.md`

- Remove the AI-draft disclaimer and tighten the surrounding status prose.
- Replace “GDAL batch tools” with a short migration notice:
  - current workflows: `ryan-scripts/gdal-python/README.md`;
  - remaining legacy mappings: `ryan-scripts/gdal-bat/README.md`.
- Correct the VS Code interpreter claim. `.vscode/settings.json` currently leaves
  `python.defaultInterpreterPath` empty; the `python:ensure-venv` and `python:stdin` tasks implement the `.venv`,
  `RYAN_TOOLS_PYTHON`, and `python` fallback logic.
- Keep `setup.py` in the repository map and, if needed, label it “setuptools wheel resource staging hook.”
- Mark the TUFLOW orchestrator and wrapper lists as representative and link to the detailed wrapper README instead of
  trying to enumerate every entry point in the root document.
- Rename or qualify “currently configured TUFLOW data types” so it is clear that the table lists types backed by
  `BaseProcessor` implementations. The suffix registry also contains raster-classification types that are not
  `BaseProcessor` data types.
- Add a concise testing section that identifies `repo-scripts\run_tests.bat` as the complete Windows runner and
  recommends targeted pytest for focused changes. Mention the repository-local pytest cache/base-temp and the bundled
  HY-8 import path only to the extent needed to explain why the runner is preferred.

Justification: the root README is the first source users encounter. It currently contains deleted entry points and a
configuration claim contradicted by the checked-in VS Code settings, while omitting the repository's usable test
runner.

#### 2. `ryan-scripts/gdal-bat/README.md`

- Retain the clear “legacy; do not use for new work” message.
- Rebuild the migration table from the seven BAT files that remain in the current tree.
- If deleted variants are historically useful, put them in a separately labelled “already removed” note; do not say
  that those files remain temporarily.
- Continue directing all new work to the maintained Python wrappers and `gdal_cli_tools.json`.

Justification: the deprecation policy is current, but the file inventory is already stale and directly contradicts the
filesystem.

#### 3. `ryan_library/processors/tuflow/README.md`

- Correct the return-value description for `check_duplicates()` and show a small dictionary-oriented usage example.
- State that `add_processor()` requires both `processor.processed` and a non-empty processed DataFrame; an empty
  processed result is skipped.
- Review the combination table against current `ProcessorCollection` behavior, including EOF/Chan enrichment.
- Add concise sections for current collection operations:
  - location filtering;
  - EOF channel-ID alignment;
  - path-metadata compaction and reattachment;
  - raw-dataframe disposal;
  - HDF save/load.
- Keep processor-extension instructions focused on actual hooks and configuration fields; verify every claimed return
  type and success condition against source signatures.

Justification: this is developer-facing API documentation, and its current return-type claim could lead directly to
incorrect calling code.

#### 4. `docs/README.md`

- Add `MCP_SETUP.md` under an appropriate setup/integration heading.
- Retain the repository improvement roadmap under maintenance documents.

Justification: the index currently omits one of only two substantive documents in the directory.

### Phase 2: Improve safety and discoverability

#### 5. `ryan-scripts/gdal-python/README.md`

- Add a `build_VRT.py` recipe or a direct pointer to its `build_grouped_tuflow_mosaics` catalogue entry.
- Add a visible warning immediately before the `gdal_set_nodata.py` example that it modifies raster metadata in place.
- Mention that overwrite flags should be used only when replacement is intended, consistent with
  `gdal_cli_tools.json`.
- Keep the JSON catalogue authoritative for complete argument arrays, defaults, mutation classifications, and
  automation discovery.

Justification: the current examples are mostly accurate, but the human README omits one maintained wrapper and does
not surface the most important mutation risk beside its command.

#### 6. `ryan-scripts/README.md`

- Keep the existing CLI/editable-wrapper/folder-driven guidance and safety section.
- Add `other` to the folder guide, or explicitly label the table as a selected-folder guide.
- Retain `TUFLOW-bat` and `misc-bat`; they are real tracked directories. Make their legacy status explicit if the
  combined row is not sufficiently clear.

Justification: this README is recent and broadly accurate. Only small scope and discoverability clarifications are
needed.

#### 7. `ryan-scripts/TUFLOW-python/README.md`

- No substantive correction is currently required.
- Optionally add a compact maintained-wrapper index only if it can be generated or checked automatically; otherwise
  prefer the existing description plus `--help` guidance.

Justification: avoiding another hand-maintained inventory reduces future drift.

### Phase 3: Refocus the root README without losing practical guidance

- Keep a concise project purpose, supported Python version, setup, build workflow, repository map, and a few canonical
  examples.
- Preserve practical user paths into TUFLOW, GDAL, RORB, 12D, Excel, and QGIS workflows, but link to focused documents
  instead of duplicating long inventories.
- Move detailed maintainer/agent setup to `docs/` if it remains useful, and link it from the root README.
- Do not remove working examples merely to reduce line count. Consolidate only content that duplicates a more
  authoritative source.

Justification: the root README is 489 lines and mixes onboarding, API reference, resource catalogues, maintenance
automation, and agent setup. A measured split will make it easier to keep current without discarding useful guidance.

## Drift-prevention follow-up

- Add a lightweight documentation check that verifies relative Markdown links and backtick-quoted repository paths
  intended to name actual files.
- Prefer links to `gdal_cli_tools.json`, the TUFLOW suffix registry, and wrapper `--help` output over copied exhaustive
  inventories.
- Add a README review item to wrapper deletion/rename and build-workflow checklists.
- Where a table must mirror structured configuration, generate it or add a small validation script rather than relying
  on manual comparison.

## Validation for the eventual documentation change

No package build or pytest run is required for a README-only implementation. Validate it with:

1. `git diff --check`.
2. A relative Markdown-link and referenced-path check across the seven repository-owned READMEs, still excluding
   submodules.
3. JSON parsing of `ryan-scripts/gdal-python/gdal_cli_tools.json` if its documented catalogue entries are touched.
4. Representative `--help` checks for commands whose usage examples change.
5. A final comparison of API claims with current function signatures.
6. `git status` verification so unrelated submodule changes and the pre-existing index state are preserved.

## Intended order of implementation

1. Correct the processor API claim and both GDAL legacy contradictions.
2. Correct root setup/testing claims and remove the drafting note.
3. Update the docs index and GDAL safety/discoverability guidance.
4. Make the small `ryan-scripts` clarification.
5. Refocus the root README in a separate, reviewable documentation pass.
6. Add drift-prevention automation only as a separate task, because it is repository tooling rather than README
   editing.
