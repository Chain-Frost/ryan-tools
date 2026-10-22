# Batch-aware TUFLOW preflight optimisation — stacked PR scope

**Status:** Scoping complete; production changes **not implemented**. New draft PR branch `feat/tuflow-batch-preflight-optimisation` targets `feat/tuflow-run-preflight` (ryan-tools PR #143), **not `main`**.

**Date:** 2026-10-22. **Owner:** ChatGPT. **Next review:** after PyTUFLOW PR #5's batch API and outstanding PR #143 feature-parity gates progress.

## Why this is separate from PR #143

PR #143 owns the read-only event/scenario-sensitive dependency checker and its existing shared filesystem/GIS inventory. It must finish correctness and exact eager/deferred parity before optimisation is considered safe. Do not make the optimiser a prerequisite for fixing #143 or silently merge changes into its branch.

Existing same-fork local SSD observation: Model B 240 runs spent 41.835 seconds constructing deferred contexts; the deduplicated final physical inventory spent approximately 0.029 seconds (one ordered run pair). This locates the likely bottleneck, but does not establish an optimised performance result. Current 174-run example parity remains incomplete.

**Dependency:** PyTUFLOW PR #5, initially based on PyTUFLOW #2, supplies the opt-in batch dependency API. PyTUFLOW owns command parsing, conditional scopes, variable lineage, safe memoisation and reusable compiled expressions. ryan-tools must *not* implement a second TUFLOW parser or repeat that context-resolution logic.

## Work package A — baseline, accounting and contract tests

1. Establish exact-SHA baselines for per-run deferred `TCF.context()`, eager resolution and unique filesystem/GIS materialisation, using the same flags and same PyTUFLOW fork build.
2. Instrument parse, contexts, batch-plan compilation, batch evaluation, database-row projection, attribute/GIS scans, wildcard expansion, file inventory and result routing separately.
3. Count unique physical paths, path patterns, directories listed, individual stats, GeoPackage opens/queries, attribute-bearing layers, secondary database sources and cache hits/misses. Include per-run issue counts and maximum memory use.
4. Compare exact provenance identities (TCF/run flags/owning control and command/path/layer/database row/secondary source) including both extra and missing items. A matching partial subset is a failure, not a pass.
5. Benchmark same-file shared-TCF 240, 440 and synthetic 500 contexts. Use three sequential warm-cache trials per mode on the same local SSD; separately validate Windows SMB/network drive, avoiding mixed-storage speedup claims.

## Work package B — consume the PyTUFLOW batch API

1. Introduce a small typed optional batch interface/protocol in `ryan_library/orchestrators/tuflow/input_preflight.py`, without exposing unstable fork internals through public wrappers.
2. Use an opt-in batch strategy, tentatively `auto`/`batch`/`individual`. Do not change default launcher behaviour or the published package dependency while the fork is under validation. Select the final parameter names once the upstream API is stable.
3. Pass the already-deduplicated list of `RunRequest` flag combinations for a single TCF to PyTUFLOW once; preserve each original run index, description, flags, owning command/control file and issue provenance in the returned model.
4. Flatten the resulting immutable batch references into the existing `_FileReference` / shared checking stages without reconstructing 500 full PyTUFLOW contexts. Preserve event-specific BC/AD BC/rainfall, materials/soils, cross-section, GIS table-link and raster metadata semantics.
5. If the new API is absent, deliberately select the **existing supported** individual-context path. Do not catch a real parser/semantic error and silently retry as if the batch passed. Strict batch mode should fail clearly if unsupported.
6. Maintain a hard fail-closed boundary for unknown database/source classes and incomplete GIS attribute inspection. Output-only destinations remain excluded; real READ consumers of variables must still be checked.

## Work package C — one shared physical inspection session

1. Retain the existing `dependency_index.py` two-phase design, but share bounded nonrecursive parent-directory snapshots across primary wildcard expansion, attribute wildcard expansion and final existence inventory when semantically safe.
2. Deduplicate GeoPackage layer enumeration and applicable attribute reads across all runs and all equivalent reference requests; reuse a read-only SQLite connection during a single inspection where practical and close it explicitly. Do not load geometry BLOBs unnecessarily.
3. Keep a central per-session map keyed by the appropriate *source identity plus layer/inspection requirement*, and a separate reverse index mapping unique discovered results to every run/owning command. Do not lose provenance when deduplicating.
4. Preserve lexical mapped-drive/UNC and `..` semantics; only deduplicate *physical* aliases when safe/explicit and without changing the paths displayed to users. Respect Windows case-insensitive file/layer semantics and test Linux case handling separately.
5. Benchmark the 8-path `os.scandir` threshold versus targeted `stat` at realistic network-directory sizes. Do not replace targeted checks with unbounded recursive scans. Keep 50,000-entry safety cap or a measured safer policy.
6. Consider a small bounded thread pool only if instrumented Windows SMB latency proves a benefit; keep SQLite and GIS access safe, report failures as unresolved and avoid nondeterministic issue output.

## Work package D — regression and installed consumer validation

- Synthetic 500-run single-TCF model: mostly shared static TGC/GIS/raster/materials, differing event, scenario and TEF source values; assert exact identities and substantially fewer command evaluations.
- Existing P (4) and B (240) models; unchanged original 42-run model; Higginsville 440-run model; repository 161 TCF/174-run example reproduction (all supported items, not only previously accepted cases).
- Same Shapefile/GeoPackage element rules for network M/Q/QO/P, Ignore flags, pit IDs, cross-section/table links, active/ambiguous database kinds and attribute-derived secondary sources.
- Wildcards, `>> all` and implicit layers, rainfall CSV index/NetCDF, raster metadata, soils/materials, AD BC and output-root variables.
- Mock batch API, fallback/unsupported API, per-run issue propagation, path aliases, permission errors, missing parent directories, large directories, partial inspection failures and no repeated underlying GIS opens.
- Windows Python 3.14 focused tests, Ruff format/check, strict Pyright on changed files, no-bump package/wheel smoke and copied wrapper `--help` / `--preflight-only` / `--no-pause` checks. Use only approved local/model locations; no TUFLOW executable, project edits or publishing.

## Expected files and architectural boundaries

- `ryan_library/orchestrators/tuflow/input_preflight.py` — batch adapter and orchestration only.
- `ryan_library/functions/tuflow/dependency_index.py` — bounded session-scoped physical inventory, patterns and path probes.
- `ryan_library/functions/tuflow/gis_attribute_dependencies.py` — shared GPKG/SHP attribute/secondary inspection.
- `tests/orchestrators/tuflow/test_input_preflight.py` and focussed `tests/functions/tuflow/*` — parity, invalidation, fallback, I/O-count regression tests.
- `docs/TUFLOW_INPUT_PREFLIGHT.md` and this record — consumer selection, support policy and benchmark evidence.
- Avoid altering `run_tuflow_simulations.py` / `check_tuflow_inputs.py` unless needed for a deliberate, backwards-compatible *opt-in* mode. Normal launch/preflight defaults remain unchanged.
- No new MCP tool unless the existing documented interface changes meaningfully; do not expose unstable batch helper as a separate MCP entry.

## Explicit acceptance gates

1. PR #143 correctness gaps are resolved or clearly remain blockers; do not suppress any finding to improve timing.
2. Exact per-run materialised dependency parity, including command provenance, GeoPackage layer and GIS/database/raster-derived inputs; no additional unresolved cases caused by caching.
3. No per-run filesystem/GIS operations reintroduced by batch consumption; one unique required file/layer inspection per session where semantically possible.
4. Measured improvement in context resolution/total elapsed on 240/440/500-run workloads with identical work and no material memory regression. Quantitative improvement gate follows profiling; never claim a speedup from incomplete work.
5. All relevant exact-head CI/Windows/GIS tests pass; local-model evidence documents environment and source SHAs.
6. The new PR stays a draft based on #143. Do not merge into #143 or `main`, publish to PyPI, or launch TUFLOW without explicit approval.

## Stacking and retargeting

- PyTUFLOW: #2 -> #1; new #5 -> #2. Merge #2 into #1 and retarget #3 to #1; merge #1 into `master` and retarget #3 to `master`.
- ryan-tools: new optimisation PR -> #143. After upstream #5 is available and parity/benchmark gates pass, decide whether to merge optimisation into #143 (single release) or keep it stacked until #143 itself merges; no automatic merge.
- Do not repin `pyproject.toml` to a development fork by default, bypass published dependency policy, or modify the global Python installation to conceal incompatibilities. Keep isolated CI/local source injection clearly identified by commit.
- Do not delete temporary reproduction fixtures from #143's own branch as part of this PR; coordinate their required removal with #143's existing clean-up gate.

## Work status and immediate next action

- Planning-only branch and draft PR; **no optimiser production code exists yet**.
- Next: profile individual contexts and establish the batch API contract with the PyTUFLOW #5 author; implement adapter only after the upstream shape is stable.
- Existing release/default-on preflight remains blocked by #143 parity and native validation; do not interpret this supporting PR as release authorisation.


## Upstream implementation update — 22 October 2026

PyTUFLOW #2 has merged into #1. The upstream optimisation is now [PyTUFLOW #5](https://github.com/phantom-branch/PyTuflow/pull/5), targeting #1; the ryan-tools consumer remains this separate draft stacked on #143.

PyTUFLOW #5 now offers an experimental opt-in `TCF.collect_batch_dependencies(...)` interface. It returns immutable per-run file references, database-activation records and source-row candidates. Crucially, `BatchInputReference` preserves input UUID, ordered command index, owning control file and line number, so a future consumer can join GIS/BC inputs to the correct active database declaration without flattening identity. The adapter **must not** materialise all candidate database rows as required input files when the active GIS row identifiers select only a subset.

Upstream synthetic Windows/Python 3.14 three-trial medians ([CI 39708797831](https://github.com/phantom-branch/PyTuflow/actions/runs/39708797831)) show about 47% lower time for 500 distinct-flag syntactic projections (2.3273s to 1.2271s) and about 45% for a 120-run/20-static-input workload (4.0646s to 2.2282s). **These exclude real-model GIS, filesystem and raster inspections, and cannot be treated as validated overall preflight speedups.**

The current normal `_collect_references()` downstream path independently constructs contexts and calls `deferred_database_sources()`. Integrating the provisional batch output without respecting active-activation UUID, per-input owner, table-link scans and secondary GIS materialisation risks falsely claiming parity. Accordingly, this branch remains an **implementation-isolation boundary**, not a replacement for the current #143 validation route.

**Next implementation gate:** an adapter that preserves the full `BatchInputReference` provenance, reproduces existing per-run missing/unresolved issue routing and selects candidate Source rows only where the active database/feature identity establishes a real dependency. Require exact eager / per-context deferred / batch identity comparison, both missing and extra records, on Windows prior to switching even the opt-in consumer. Keep defaults, published PyTUFLOW dependency and main branch unchanged.


## Implementation checkpoint — 22 October 2026

**Implemented on the stacked draft branch (not #143 or main):**

- Added `ryan_library/orchestrators/tuflow/batch_preflight_adapter.py`, a read-only `BatchModelView` that consumes the upstream opt-in `TCF.collect_batch_dependencies()` once for the entire ordered proposed run set. It does not implement an independent TUFLOW syntax parser.
- Added `check_simulation_inputs(..., context_mode="batch")`; default remains `"individual"`. The batch view supplies immutable input references to the **existing** `_collect_references`, wildcard, attribute-link, raster-metadata, GeoPackage and shared filesystem inventory stages, preserving per-run issue routing.
- Enforced exact run count, index and full flag string; complete ordered `input_references` matching `file_references`; nonempty per-input UUID/command positions; correctly owned database Source rows; and conservative rejection of ambiguous multiple active stateful databases. A batch request against a published PyTUFLOW without the new method is an explicit incompatibility, **never a silent fallback or accepted partial inventory**. Errors for an individual run do not suppress findings for other runs.
- Added fast mock-based parity and failure-contract tests, including a 500-run **single batch API invocation** with no per-run `model.context()`, and a run-specific unresolved finding.
- Added separate [Windows fork batch smoke workflow](https://github.com/Chain-Frost/ryan-tools/blob/feat/tuflow-batch-preflight-optimisation/.github/workflows/tuflow-batch-smoke.yml), pinned to PyTUFLOW #5 commit `093b17ad`, not the public PyPI package. It includes an opt-in real parsed-TCF example verifying complete individual-versus-batch issue and path inventories.

**Remaining:** Exact-head strict Ruff/format/Pyright/focused/real-fork CI is not yet accepted here; check the new workflow and its result, fix failures, then compare *fully materialised* eager/deferred/batch identities over the 161-TCF/174-run archive and P/B/Higginsville native projects; the separate 42-run gate was withdrawn by the owner. Full BC feature-to-activation mapping is **not implemented**; multiple active stateful BC database declarations deliberately remain unresolved rather than passing. No benchmark can be described as end-to-end production acceleration until these parity gates pass.

The default launcher, standalone wrapper and `pyproject.toml` are unchanged. There is no merge, release or production dependency re-pin.


## Full Windows archive batch-parity evidence (22 October)

The dedicated fork-smoke GitHub Actions [run 39716802481](https://github.com/Chain-Frost/ryan-tools/actions/runs/39716802481) **passes** using pinned PyTUFLOW #5 commit `093b17ad` (code under test: ryan-tools #148 `a33a2ae`). The optional three-mode archive differential explicitly verifies the complete **161 TCFs / 174 simulation contexts**, with no missing/extra materialised eager-versus-deferred or eager-versus-batch `(run, owner command, physical path, GeoPackage layer)` identities and matching checked-path and issue sets for individual versus batch public preflight. The runner retains the original eager/deferred contract and only runs batch when `--batch` is explicit. No model simulation was launched; extracted fixtures and reports are disposable and the original test-data submodule is unchanged. [Archive differential reports](https://github.com/Chain-Frost/ryan-tools/actions/runs/39716802481).

**CI correction:** Before the successful run, one diagnostic-only run incorrectly used `len(identities)==2` to trigger eager/deferred comparison; when all three modes succeeded, it omitted the original comparison. Fixed this condition to compare whenever eager and deferred are both present, and reran the full strict replay. No production input logic was weakened.

**Outstanding gates:** The base #143 single-run route remains the production default. #148 is still draft and lacks materialised native P/B/Higginsville batch-vs-individual benchmarking and full GIS BC feature-level database activation selection. Synthetic 500-run test guarantees one public batch API call and per-run fail-closed routing; real TCF collection speedups are not yet verified. The exact-head Policy CI must pass strict Pyright; it first found only missing annotations and an unused protocol in the new test/facade files (the archived differential, Ruff, installed package and fork smoke passed). Annotations have been revised in a follow-up and require a new CI result. No merge, production PyPI repin or release.
