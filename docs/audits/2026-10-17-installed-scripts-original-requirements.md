# Original PR #130 requirements — preserved baseline

This is the **full original requirements record**, preserved from commit
[`3db9ee7`](https://github.com/Chain-Frost/ryan-tools/blob/3db9ee7d35f06fddd23ac045c443945f4f8968f6/docs/work/2026-10-17-installed-scripts-mcp-pip-distribution.md).
The text below is retained verbatim as requirements evidence. Its 17 October implementation/PR status
and dependency sequencing are historical, **not current progress**.

All original functional and validation requirements remain outstanding until verified or expressly
superseded by a documented maintainer decision. Use the
[living requirement traceability record](../work/2026-10-17-installed-scripts-mcp-pip-distribution.md)
for the current status; neither a green CI nor a merged PR silently cancels an item.

---

# Installed scripts, MCP workflow distribution and package identity

| Field | Value |
| --- | --- |
| Status | Deferred — planning only; **not approved for implementation or merge** |
| Owner | Unassigned |
| Created | 2026-10-17 |
| Updated | 2026-10-17 |
| Next review | 2026-11-17 (review only, **not** a merge deadline; defer again if PR backlog remains) |
| Baseline | `main` as of 2026-10-17; planning branch `planning/pip-scripts-mcp-distribution-20261017` |
| Delivery | Documentation-only draft PR; no runtime, dependency, wheel, or publication changes |

## Outcome and intent

Make a standard pip installation of `ryan-tools` provide (1) the maintained Python library, (2) the intentionally **comprehensive** engineering/scientific/GIS dependency environment, (3) selected maintained human-facing scripts, and (4) a useful, safety-aware MCP catalogue whose installed workflows are runnable **without a Git checkout**.

Keep the GitHub repository **`Chain-Frost/ryan-tools`**. Do **not** create more repositories merely to separate script categories. A future **PyPI distribution rename** is permitted, subject to name availability and deliberate migration; no package or import rename is authorised by this planning PR.

The goal is not to expose every Python file or to turn the MCP server into an unrestricted shell. Retain the existing library / processor / orchestrator / wrapper architecture and reuse the wrapper as the *one authoritative CLI implementation*.

## Current evidence (re-check at implementation)

- Current distribution name is `ryan_functions` (PyPI normalises it to `ryan-functions`); maintained imports use `ryan_library`, with the `ryan_functions` legacy import namespace.
- `pyproject.toml` packages `ryan_library`, selected `ryan-scripts/TUFLOW-python` module directories, and source submodules `vendor/run_hy8` and `vendor/ryan_culverts`. Other maintained `ryan-scripts` directories, notably GDAL, file management and the root hydraulic wrappers, are not installed as equivalent CLI entry points.
- Only `ryan-mcp` is currently declared under `[project.scripts]`.
- `ryan_library/mcp/registry.py` supports explicit `script`, `module`, and `console_script` targets. Most catalogue workflows still resolve `script` relative to `RYAN_TOOLS_REPOSITORY_ROOT` / a checkout. Absent a checkout, the metadata can be examined but many commands are unavailable.
- `ryan_library/resources/mcp/workflows.json` contains 23 general workflow records and `gdal_cli_tools.json` contains 8 curated GDAL workflow records at this baseline. Counts are informational; re-inventory before implementation.
- MCP direct read-only helpers (TUFLOW parsing, collection inspection, raster metadata) already work from the installed package. `list_workflows` / `get_workflow` are **discovery** operations; the server intentionally does **not** run catalogued CLI commands.
- The current comprehensive runtime dependency list includes `numpy`, `pandas`, `geopandas`, `fiona`, `rasterio`, `laspy`, `matplotlib`, `openpyxl`, `ezdxf` and other general-use engineering packages.

Primary references: [development policy](../DEVELOPMENT_GUIDE.md), [wrapper standard](../../ryan-scripts/WRAPPER_STANDARD.md), [MCP setup](../MCP_SETUP.md), [MCP architecture handoff](../mcp-handoff/README.md), [PyPI publishing](../PYPI_PUBLISHING.md), and [compatibility inventory](../COMPATIBILITY_POLICY.md).

## Decisions and non-negotiable requirements

1. **Repository remains `ryan-tools`.** Do not rename the GitHub repository or invent further domain repositories.
2. **Keep the comprehensive default pip installation.** This package also bootstraps a new engineering user's general Python working environment. Do **not** replace the default dependency set with a minimal core and opt-in `[gis]`, `[hydraulics]`, `[documents]`, or `[all]` profiles. Existing `dev` / `mcp` extras may remain appropriate for development and server-specific dependencies; they are not a reason to remove the user's broad runtime dependencies.
3. **Plan Python 3.14+ from November 2026.** The user plans to stop supporting 3.13 in November. Coordinate metadata, tooling, CI, installers and docs at that point; do not claim that the support change has happened in this planning PR.
4. **Package selected maintained scripts as well as the library.** Preserve the original `ryan-scripts` source layout, embedded editable defaults, wrapper versions, CLI flags, help, exit codes, banners and optional pauses. Users must still be able to copy wrappers into project directories. Never duplicate the underlying processing implementation just to make a package entry point.
5. **MCP catalogue exposure is curated, not automatic.** Packaging a script does not automatically make it an MCP tool. Preserve the existing `core` / `analysis` / `create` / `privileged` capability profiles and explicit metadata for mutation/approval.
6. **Hydraulic packages are independent pip dependencies once migration is proven.** Replace the `vendor/run_hy8` and `vendor/ryan_culverts` submodules/bundled top-level packages with compatible published `run-hy8` and `ryan-culverts` dependencies only **after** ongoing culvert integrations have been settled and exact-version wheel/installed tests pass. Keep the existing QGIS/Excel resource and test-data submodules for now. Preserve the explicitly excluded `unsorted` submodule boundary.
7. **PyPI rename is a separate decision.** Candidate name is `ryan-tools` (availability/ownership unverified); keep `ryan_library` imports and the existing `ryan_functions` distribution working through an explicitly planned transition. Do not assume PyPI can rename a project in place.
8. **Windows engineering usage is first class.** Python 3.14, GDAL/QGIS 4 environments, TUFLOW/HY-8 external executable boundaries, long paths, mapped drives, UNC paths, folder names with spaces and `Path.absolute()` by default. Use `resolve()` only when physical identity or symlink-aware containment is required. No mandatory venv or installation solely on C:.
9. **No behavioural, dependency, packaging or release change in this draft PR.** The plan is a deferred backlog handoff while other PRs are finished.

## Workstream A — inventory, lifecycle and packaging policy

- [ ] Re-inventory `ryan-scripts` by domain (TUFLOW, GDAL, RORB, hydraulic/floodway, CAD/12D, file management, documents, point-cloud and miscellaneous). Record source path, owner, maintained/legacy/experimental status, import dependencies, invocation, `--help`, headless `--no-pause`, mutation semantics and external application requirements.
- [ ] For each file classify independently as **packaged maintained wrapper**, **packaged but not MCP-catalogued**, or **repository-only**. Do not assume versioned snapshots or experimental scripts are safe to distribute.
- [ ] Prioritise already-catalogued maintained workflows and representative useful scripts: TUFLOW combination/log/raster/model workflows, GDAL conversion and recompression, culvert/floodway front ends, RORB converters, common archive/check-file utilities.
- [ ] Keep `repo-scripts`, obsolete BAT snapshots, experimental `python-not-polished`, unverified scripts and project-specific experiments out of ordinary runtime distribution.
- [ ] Resolve import-time side effects, hard-coded locations, accidental execution on import, and dependencies on source checkout-relative `__file__` paths *before* classifying a wrapper as installed-runnable.
- [ ] Preserve the option of directly running or copying the original `.py` file with editable defaults at its top; library-backed wrappers must continue to print wrapper **and installed library** versions.

## Workstream B — wheel layout and installed invocation

- [ ] Choose one stable wheel strategy for portable original scripts: installed package resources / deliberate script bundle with explicit metadata and safe extraction or executable installation where justified. Retain original filenames, especially for user copying. Avoid relying on invalid dotted module paths through hyphenated directory names.
- [ ] Package only allowlisted maintained wrappers; don't indiscriminately use every `*.py` below `ryan-scripts`. Verify resource data and any sibling assets are available to installed wrappers.
- [ ] Add a stable resolver for packaged wrapper locations, independent of current directory, repository checkout and `PYTHONPATH`. A development checkout can remain a documented override, but installed-wheels should be a first-class default.
- [ ] Support source-checkout development without accidentally calling a **different** installed wheel version. Return explicit executable path, script source, `WRAPPER_VERSION`, installed distribution version and environment diagnostics.
- [ ] Preserve `--help`, `--no-pause`, CLI error codes, editable wrapper defaults and no implicit destructive action.
- [ ] Where helpful, provide a deliberate `ryan-tools scripts list` / `ryan-tools scripts copy` style command or equivalent to locate/copy maintained installed wrappers into engineering project folders; avoid placing a huge uncontrolled set of individual executables on PATH.
- [ ] Update `build_library.py`, `verify_wheel.py`, `smoke_test_installed_wheel.py`, setuptools configuration, package manifest / resource inclusion, README, installation and release documentation only during a later implementation PR.

## Workstream C — MCP discovery and execution-target reconciliation

- [ ] Reuse `ryan_library/mcp/server.py`, `registry.py`, `models.py` and the existing packaged JSON catalogues, instead of adding another server or one endpoint per script.
- [ ] Resolve installed packaged script targets first for normal pip users. Keep explicit repository-script selection for development, with deterministic precedence and diagnostics rather than silently choosing a stale copy.
- [ ] Use existing `module` / `script` / `console_script` execution kinds where possible. Extend the registry model only as needed to represent packaged resources unambiguously.
- [ ] Return reliable command prefix, `--help` command, available scenarios, resolved script location, installed package + wrapper version, external prerequisite availability and mutation/approval metadata from `get_workflow`.
- [ ] Retain a small curated MCP tool surface: bounded read-only helpers and `list_workflows` / `get_workflow`. Do not automatically publish every packaged wrapper as a direct executable MCP tool.
- [ ] Keep read-only audits in `analysis`, new-file/report creation in `create`, and overwriting, deleting, moving, replacing or external-executable workflows in `privileged` as appropriate. Require explicit approval for sensitive actions, including when command-line flags can elevate a nominally safer workflow.
- [ ] Maintain documented safe defaults, dry-run/preview, destructive confirmation, path checks, useful errors and agent-visible return codes/output paths. Do not treat installing a script as permission to run it.
- [ ] Update the 23 general and 8 curated GDAL catalogue entries against the actual maintained files; reject dead entries or classify them clearly as unavailable rather than pretending that packaging automatically makes them operational.
- [ ] Update `check_repo_health` to distinguish installed script availability, repository-only fallback, absent external programs, missing environment capabilities and installed distribution versions.
- [ ] **Future separate scope only:** assess controlled `run_workflow` direct MCP execution with typed validation, explicit approvals, sandbox/boundaries, safe argument construction, bounded log/progress handling and audit trail. The current MCP server should remain discovery-only until such a tool has a separately reviewed design and tests.

## Workstream D — hydraulic pip migration

Dependencies: settle and merge relevant work in [ryan-tools PR #100](https://github.com/Chain-Frost/ryan-tools/pull/100) and the dependent [run-hy8 PR #6](https://github.com/Chain-Frost/run-hy8/pull/6) before altering ownership and submodule layout. Re-check current state at implementation.

- [ ] Verify corresponding PyPI releases exist, are owned/trusted and have versions compatible with the tested hydraulic applications.
- [ ] Replace source-embedded `culvert_solver` and `run_hy8` wheel contents with dependency declarations, keeping the comprehensive default install.
- [ ] Remove `vendor/run_hy8` and `vendor/ryan_culverts` Git submodules only when installed and source-checkout functional parity passes. Remove stale `.gitmodules` entries, package-find `vendor/` paths, `extraPaths` and obsolete update utilities.
- [ ] Update submodule pin/provenance strategy to explicit package version requirements or constraints and a reproducible test record. Avoid uncontrolled breaking hydraulic upgrades; use tested lower/upper bounds or a release constraints file where justified.
- [ ] Adapt `verify_wheel.py`: *assert hydraulic packages are dependencies and independently importable* rather than expecting their source files inside `ryan_functions`.
- [ ] Preserve engineering acceptance, numerical parity and external Windows HY-8 checks as separate gates; an import-success smoke test is insufficient proof of hydraulic calculations.

## Workstream E — package identity and supported Python

- [ ] Keep repository `Chain-Frost/ryan-tools` and retain `ryan-mcp` unless there is a real user-facing reason to change its command.
- [ ] Evaluate `ryan-tools` as the PyPI distribution name; confirm availability, registration/control and whether normalised naming collides with any existing project. The current `ryan_functions` is normalised to `ryan-functions` on PyPI.
- [ ] Make the distribution-name change as a **separate, explicitly approved PR/release**. Update Trusted Publishing (OIDC identity), release/tag guards, dependency URLs, `importlib.metadata`, docs, artifact names, user upgrades and package verifier. Publishing a new name does not transfer existing installations automatically.
- [ ] **Do not conflate the distribution name with import names.** `ryan_library` may remain the stable import namespace initially. A future `ryan_tools` namespace requires a separate compatibility/forwarder policy and consumer inventory; do not break copied wrappers or retired namespaces prematurely.
- [ ] When the November 2026 decision is enacted, change `requires-python` to `>=3.14` and update classifiers, strict Pyright baseline, CI policy/jobs, wheel smoke, bootstrap scripts and documentation consistently.
- [ ] Retain the existing wide dependency set in the default install, rather than adopting proposed minimal/optional domain installation groups. Document that the package intentionally sets up a broad engineering Python environment.

## Workstream F — validation and rollout gates

- [ ] Add inventory / manifest tests verifying *exactly* which maintained scripts are included and which legacy/experimental scripts are excluded.
- [ ] Build a verified wheel, install from its exact bytes in a neutral Windows Python 3.14 environment with **no repository checkout**, and show `ryan-mcp`, catalogue listing and selected scripts' `--help` work without `RYAN_TOOLS_REPOSITORY_ROOT`.
- [ ] Exercise harmless read-only workflows and output-creating workflows with synthetic fixtures; cover safe failures when input/external binaries are missing.
- [ ] Validate wrapped GDAL/QGIS/TUFLOW/HY-8 workflows in their specific environments where executable integration is needed; mark hosted CI tests as capability-limited, not as complete engineering acceptance.
- [ ] Verify copied wrappers on mapped/UNC/network paths, with whitespace/long names, compatible installed library version and expected stdout/progress/exit behaviour.
- [ ] Regression-test `core/analysis/create/privileged` filtering, approval requirements, no unreviewed destructive execution and no implicit MCP execute action.
- [ ] Run Ruff, strict Pyright, focused pytest, docs/link checks, the package verifier and applicable hosted CI **at the exact final PR head**, documenting non-runnable external engineering checks separately.
- [ ] Explicitly decide migration order, version constraints, publication cutover, old-PyPI-name handling and support/deprecation timelines. Do not publish, delete historical releases or merge a rename without sign-off.

## Sequencing / active blockers

**No implementation should begin under this PR until the current outstanding changes are stabilised.** As of this planning record, the relevant open ryan-tools PRs are [#100](https://github.com/Chain-Frost/ryan-tools/pull/100), [#118](https://github.com/Chain-Frost/ryan-tools/pull/118), [#119](https://github.com/Chain-Frost/ryan-tools/pull/119) and [#120](https://github.com/Chain-Frost/ryan-tools/pull/120). Prioritise their review/merge/closure and verify dependent `run-hy8` and hydraulic package versions first.

Proposed *future* sequence, each in small bounded follow-up PRs:
1. Classify and inventory wrappers; agree exact packaged/MCP allowlists.
2. Package selected maintained wrappers without changing their source interfaces.
3. Update MCP registry to find wheel-installed scripts; verify no-checkout operation and approval profiles.
4. Migrate the two hydraulic source submodules to tested pip requirements.
5. Apply Python 3.14+ support floor in November, when authorised.
6. Separately decide/execute a possible PyPI distribution rename, preserving import compatibility.
7. If desired, design direct MCP execution as a separately reviewed feature.

Project setup overlaps with `tuflow-initialiser`; finish that separate, independently tracked migration only after verifying callers and actual TUFLOW installation. Do not widen this plan into a speculative rewrite of that repository.

## Completion criteria

The *planning PR* is ready to retain as a draft when this file is indexed and the work register links to it. **That does not satisfy the implementation scope.**

Implementation can be considered complete only after the approved allowlist works from the pip-installed distribution without a checkout, MCP discovery correctly advertises actual runnability and safety, copied wrappers work, expected external integrations pass appropriate checks, bundled hydraulic source is replaced safely with tested pip dependencies, and release/upgrade instructions are verified. Renaming PyPI and direct MCP execution are independent approvals rather than hidden merge requirements.

## Next action

**Leave this PR as a draft.** Finish current open PRs first. On the next requested review after those changes, rebase/re-inventory against current `main`, confirm scope and package naming decision, then split implementation into small reviewable PRs. Do not merge the planning branch merely because its documentation checks are green.

## Validation and delivery

- **2026-10-17:** Documentation-only proposal created on branch `planning/pip-scripts-mcp-distribution-20261017`.
- Current code was inspected via GitHub; no package build, scripts, commands, tests, publication, PyPI rename, or submodule removal performed.
- Local Python documentation checker and hosted CI: **not run** as part of drafting; validate if/when the PR is updated or implemented.
- Delivery: draft PR only, intentionally left open.

## Progress

### 2026-10-17

Recorded user decisions (keep `ryan-tools` repo name, comprehensive default dependencies, planned Python 3.14+ minimum from November, pip-installed scripts for MCP) and deferred related architectural options with explicit safety and release gates.
