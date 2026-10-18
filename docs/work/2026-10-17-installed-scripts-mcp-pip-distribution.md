# Installed scripts, MCP workflow distribution and package identity

| Field | Value |
| --- | --- |
| Status | In validation — implementation reconciliation for PR #130 |
| Owner | ChatGPT / maintainer review |
| Created | 2026-10-17 |
| Updated | 2026-10-19 |
| Next review | After exact-head CI and wheel checks |
| Branch | `planning/pip-scripts-mcp-distribution-20261017` / PR #130 |
| Delivery | Multiple focused implementation commits; **no publishing or direct MCP execution in this PR** |

## Current decision and scope

A normal `pip install ryan-tools` supplies the maintained `ryan_library` implementation, an intentionally
**broad** engineering dependency set, a curated installed script collection and an MCP discovery server that
locates those scripts without requiring a Git checkout. The repository name remains
`Chain-Frost/ryan-tools`. Python imports remain `ryan_library`, not `ryan_tools`.

The earlier 17 October proposal was overtaken by merged implementation in PRs #100 and #120,
and by later explicit maintainer decisions. This record supersedes the deferred-only instructions
and unchecked historical action lists in the original draft.

**PRs #118 and #119 are not prerequisites to merging #130.** Those RORB and TUFLOW raster
refactors are separately reviewable, and this PR does not depend on their implementation.
Do not merge either indirectly or treat their open status as a blocking gate.

## Reconciliation with the approved requirements

| Requirement | Status as of 19 October | Reviewed decision / outstanding item |
| --- | --- | --- |
| Keep GitHub repository named `ryan-tools` | Implemented | No further repository split. |
| Broad default engineering dependencies | Implemented | Preserve the existing default `numpy`, `pandas`, GIS, hydraulic, and reporting environment; do not substitute narrow extras. |
| Install source-identical maintained wrappers | Implemented in #120; hardened in #130 | The wheel places `ryan-scripts` beside `ryan_library`. PR #130 replaces blanket copying with a reviewed explicit manifest and byte-for-byte wheel verification. |
| No-checkout MCP discovery | Implemented in #120 | Locate sibling installed scripts; development checkout is an explicit or discovered alternative. MCP provides bounded listings and details, not arbitrary execution. |
| Curated MCP exposure and mutation profiles | Implemented | Existing workflow JSONs retain `core`, `analysis`, `create`, `privileged` and explicit approval metadata. Installation does **not** grant execution consent. |
| Copy human-editable wrappers into project folders | Added in #130 | `ryan-scripts list` and `ryan-scripts copy` operate on the manifest, preserve source bytes and refuse accidental overwrite; an existing destination directory is required. |
| Migrate `run-hy8` / `ryan-culverts` to pip | Implemented in main | Both are unconditional dependencies. Their former hydraulic submodule entries and bundled solver source were removed; existing wheel/installed tests exercise imports and basic operations. Engineering parity is tracked independently. |
| PyPI distribution `ryan-tools` | Implemented in #120 | Prior conditional proposal is obsolete. Existing PyPI/local-wheel installer migration handles the former distribution. Stable `ryan_library` imports remain; removed legacy aliases are intentional breaking changes documented in the compatibility policy. |
| Python minimum | **Revised by later maintainer decision** | Retain **Python 3.13+** including `ryan-culverts`; Python 3.14 remains the preferred development baseline. The original planned November 3.14-only switch is superseded unless explicitly reauthorised. |
| No unconditional direct MCP execution | Retained | `run_workflow` requires a distinct design/review; never infer approval from a discovered command. |
| Windows, QGIS, mapped/UNC paths | Existing policy retained | Preserve `Path.absolute()` default and application-specific Python/executable boundaries; installed-wheel CI does not prove external software interoperability. |

## Curated script inventory and wheel boundary

The manifest at
[`ryan_library/resources/mcp/installed_scripts.json`](../../ryan_library/resources/mcp/installed_scripts.json)
is the authoritative explicit list of **83 selected files** at this update, including original source scripts,
package `__init__` companions and local documentation. These files are distributed without rewriting editable
defaults. MCP exposure is separately determined by the existing workflow catalogues. All 33 distinct currently
catalogued script paths are covered; module/console targets are independently checked by the registry.

Selected groups: TUFLOW Python wrappers, TUFLOW model/culvert launchers, GDAL Python wrappers,
hydraulic entry points, selected RORB/file-management/Word helpers, the QGIS audit script
and maintained wrapper documentation. Scripts are **installed-only** unless specifically catalogued.

The **63 repository-only files** include 12D/AutoCAD and CAD experiments, versioned snapshots, legacy
GDAL/TUFLOW BAT files, miscellaneous PDF/point-cloud/hydrology utilities and unpolished/unsorted scripts.
This classification is conservative: it does not mean those utilities have no value. Review one
individually before adding it to the install manifest or MCP catalogue. Existing privileged MCP targets
(including the Excel-protection and deletion utilities) stay catalogued with their approval metadata;
do not silently drop them from the wheel.

Build invariants:
- `setup.py` reads the manifest and removes stale build-output script copies before staging selected source files.
- `repo-scripts/verify_wheel.py` rejects missing, unexpected and non-byte-identical installed script files,
  and checks the installed manifest as package data. `MANIFEST.in` retains source files in an sdist;
  a built wheel only installs the explicit selection.
- `ryan-scripts copy` copies a single listed source wrapper; it does not execute the script, install
  external tools or copy an arbitrary directory. Users must still install matching library dependencies
  and provide any project-specific inputs or external executables.
- `ryan-mcp` continues to report availability, command prefix, help, wrapper version, mutation profile
  and scenarios. Explicit checkout selection remains useful for development.

## Validation status and acceptance gates

Acceptance already recorded in [the PR #120 delivery record](2026-10-18-installed-scripts.md) includes
an installed wheel with no checkout override, real stdio MCP handshake/listing, all 35 catalogue workflows,
and reported Python 3.13/3.14 compatibility decisions. These are historical results **at the earlier head**;
they are not substitutes for validation of the new manifest and copy helper.

PR #130 acceptance requires:
- [ ] Five required GitHub checks pass at the **final PR head**, including Python 3.13 compatibility,
  Ruff/whitespace, policy, hosted tests and package verification.
- [ ] Build an exact wheel and verify **only** allowlisted script members, byte identity and preserved
  Python 3.13+ / hydraulic dependency metadata.
- [ ] Run an isolated no-checkout installed-wheel smoke that enumerates all MCP catalogue scripts and
  copies an installed wrapper to a temporary project folder.
- [ ] Confirm that `ryan-scripts list/copy` help, safe dry run, overwrite refusal and errors pass focused tests.
- [ ] Run documentation/link checks and update the PR body to reflect implementation (not deferred-only work).
- [ ] Verify mergeability and obtain maintainer acceptance for release/publishing impact; **do not**
  force bypass failing checks or automatically publish from this review.

External acceptance not claimed by hosted tests: GUI QGIS 4, real TUFLOW runs,
HY-8 executable/numerical parity, RORB datasets, network/mapped/UNC Windows paths and
long-path manual tests. These require separately tracked on-machine cases as those applications
and fixtures are available; they are not preconditions for the strictly bounded wheel-manifest change.

## Post-merge work (separate from #130)

1. Finish and validate PRs #118 and #119 independently, including their RORB/TUFLOW data checks.
2. Reassess additional **individual** repository-only scripts for wheel inclusion only after wrapper/CLI
   headless, import-safety and external dependency audits. Avoid indiscriminate auto-discovery.
3. Retain independent HY-8 and culvert engineering validation, numerical parity and publication evidence
   under the existing hydraulic follow-up issues (#102/#103 as relevant).
4. Consider direct typed MCP execution only in a new explicitly approved design with safety, process
   isolation, bounded logs and mutation approvals. Not part of this PR.
5. Review any future Python minimum change as a separate policy decision. Python 3.13 remains supported.

## Progress record

- **17 October:** Original architecture proposal drafted; installation and publishing changes deferred.
- **18 October:** PRs #100, #120 and #129 merged; subsequent installed package and MCP work recorded in the
  [implementation handoff](2026-10-18-installed-scripts.md).
- **19 October:** Merged `main` into #130, reconciled completed status rows and reclassified this as
  an implementation/reconciliation PR. Added the curated manifest, wheel enforcement, installed-script
  list/copy command and focused acceptance tests. Final CI and maintainer review remain required.
