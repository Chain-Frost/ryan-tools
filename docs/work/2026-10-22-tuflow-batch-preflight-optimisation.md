# Batch-aware TUFLOW preflight optimisation — stacked PR scope

**Status:** Active final paired validation with the consumer release scope fixed. Child #156 is merged into
parent #143 (`feat/tuflow-run-preflight`) at `298fb13`, after all five workflows passed and final independent
review returned no findings. The parent retains the diagnostic identity/layer memoisation and the ordered
input/reference-count adapter. The archive harness is now supported repository regression tooling.
Parent #143 remains unmerged to `main`; normal package dependencies and launcher defaults are unchanged.

PyTUFLOW #12 retains the existing scenario/variable group proof and isolation work. A narrow original-constructor
safety correction and the independent reference-count contract are complete at
`126ec64e221908f5327ee9dc9d372c8084afaf12`, with 263 focused tests passing on Python 3.12 and 3.14.
Exact-source paired Windows checks remain pending. Broader literal-setting reuse and `Inputs.append`
optimisation are separate follow-up PRs, not additions to the bounded consumer release.
Earlier source `84f1f38` passed 245 focused tests on Python 3.12/3.14 and all eight active hosted checks;
that is historical evidence, not validation of subsequent source changes.

**Date:** 2026-10-23. **Owner:** ChatGPT. **Next review:** 2026-10-30.

**Historical completed benchmark revisions:** PyTUFLOW `a9f654c1ec0812fc70035e227fb2f9f12290f449`;
consumer diagnostic `2e03c0b110a272d9c6a05cc6da3bc08817130164`.
The earlier pinned hosted batch workflow passed strict eager/deferred/batch materialisation parity for all
161 TCFs / 174 runs. The preceding parser b1c9cfd also passed the local strict archive replay.
Native rotated three-mode evaluations are complete; see the final results below. The small SMB model has 42 commands.

Native nested two-BC model confirmation is explicitly deferred by the owner. The next offered benchmark is a
new model on a different SMB server, using a recorded exact parser/consumer pair. Old M480/N42 reruns are not
required to finish the bounded release. Broader non-READ/BC/include reuse remains outside the current parser
proof; new code must be validated independently of the historical benchmark.
Historical planning/checkpoints below describe earlier states; this summary and the latest evaluation entry
supersede their delivery/acceptance statements.

## Current parser and consumer ownership

| Repository / PR | Owns | Boundary |
| --- | --- | --- |
| PyTUFLOW #12 | Control-file parsing, scenario/event and variable semantics, compiled dependencies, safe state reuse, isolation and batch API diagnostics | Proves reuse inside the parser; does not implement the consumer's inventory/reporting workflow |
| ryan-tools #143 | Public PyTUFLOW API adaptation, physical GIS/database/file materialisation, unique inventory, provenance and diagnostic identity comparisons | Does not duplicate TUFLOW parsing, infer parser reuse from equal file sets, or inspect private graph/cache state |

The diagnostic optimisation is confined to `tests/fixtures/tuflow_example_repro/`: cache successful
`Path.resolve()` results and read-only GeoPackage table-name lists by **exact supplied path spelling** for
one `collect()` invocation. Preserve physical alias equivalence and each run/command record, but retain
original spellings for inventory and SQLite I/O. New invocations start fresh; exceptions still fail the
comparison. Archive-containment checks and installed-module verification retain their separate deliberate
link resolution. Production preflight and parser dependency pins are unchanged.

The parser's [recorded handoff at 84f1f38](https://github.com/phantom-branch/PyTuflow/blob/84f1f38767c446ab58c64fd478b383d97d516a73/agent_workstreams/P5_graph_group_reuse.md)
describes that graph implementation and validation. Its [original benchmark report at 2b8ddb9](https://github.com/phantom-branch/PyTuflow/blob/2b8ddb9/agent_workstreams/P5_benchmark_results_2026-10-23.md)
remains immutable historical evidence. The new SMB workload belongs to the designated workstation and must
record the final parser/consumer source pair. No raw native data or identifying paths are uploaded.

## Historical planning: why optimisation was separate from PR #143

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

**Remaining:** Exact-head strict Ruff/format/Pyright/focused/real-fork CI is not yet accepted here; check the new workflow and its result, fix failures, then compare *fully materialised* eager/deferred/batch identities over the 161-TCF/174-run archive and P/B/Higginsville native projects. Full BC feature-to-activation mapping is **not implemented**; multiple active stateful BC database declarations deliberately remain unresolved rather than passing. No benchmark can be described as end-to-end production acceleration until these parity gates pass.

The default launcher, standalone wrapper and `pyproject.toml` are unchanged. There is no merge, release or production dependency re-pin.


## Full Windows archive batch-parity evidence (22 October)

The dedicated fork-smoke GitHub Actions [run 39716802481](https://github.com/Chain-Frost/ryan-tools/actions/runs/39716802481) **passes** using pinned PyTUFLOW #5 commit `093b17ad` (code under test: ryan-tools #148 `a33a2ae`). The optional three-mode archive differential explicitly verifies the complete **161 TCFs / 174 simulation contexts**, with no missing/extra materialised eager-versus-deferred or eager-versus-batch `(run, owner command, physical path, GeoPackage layer)` identities and matching checked-path and issue sets for individual versus batch public preflight. The runner retains the original eager/deferred contract and only runs batch when `--batch` is explicit. No model simulation was launched; extracted fixtures and reports are disposable and the original test-data submodule is unchanged. [Archive differential reports](https://github.com/Chain-Frost/ryan-tools/actions/runs/39716802481).

**CI correction:** Before the successful run, one diagnostic-only run incorrectly used `len(identities)==2` to trigger eager/deferred comparison; when all three modes succeeded, it omitted the original comparison. Fixed this condition to compare whenever eager and deferred are both present, and reran the full strict replay. No production input logic was weakened.

**Outstanding gates:** The base #143 single-run route remains the production default. #148 is still draft and lacks materialised native P/B/Higginsville batch-vs-individual benchmarking and full GIS BC feature-level database activation selection. Synthetic 500-run test guarantees one public batch API call and per-run fail-closed routing; real TCF collection speedups are not yet verified. The exact-head Policy CI must pass strict Pyright; it first found only missing annotations and an unused protocol in the new test/facade files (the archived differential, Ruff, installed package and fork smoke passed). Annotations have been revised in a follow-up and require a new CI result. No merge, production PyPI repin or release.

## Rebased upstream fork acceptance

PyTUFLOW #5 commit `7a718f51` incorporates current #1 commit `d10970ae` and passes [Windows Python 3.12/3.14 regression CI](https://github.com/phantom-branch/PyTuflow/actions/runs/39717728914) and [strict full example parity](https://github.com/phantom-branch/PyTuflow/actions/runs/39717728819). The isolated batch smoke workflow has been re-pinned to the exact revision. ryan-tools #148's prior full 161/174 eager/deferred/batch parity green evidence was collected using the older PR #5 head `093b17ad`; the new pin requires another exact-head replay before treating it as final. ryan-tools Policy strict Pyright passed on [CI 39717812548](https://github.com/Chain-Frost/ryan-tools/actions/runs/39717812548) (the subsequent pin/documentation changes are non-Python). No published package dependency is modified.


## Native evaluation compatibility fixes - 22 October 2026

Source `a9f654c` repairs two existing eager/shared defects independently of deferred functionality:

- `Context._create_list` previously enumerated slots 0-8, omitting `-s9` / `-e9` from conditional scopes even
  though placeholder translation accepted them. Slot 9 now selects its actual branch in all three modes.
- Materials CSV blank separator and whole-line comment rows previously reached integer-ID conversion and
  prevented initial database loading. These non-record rows are excluded; nonempty rows without IDs still fail.

Both are separately documented in the upstream `eager_source_completeness_fixes.md`; no private fixture data,
model edits, flag remapping or ignored parser errors were used. The affected upstream regression selection
passes **223 tests**. The new consumer diagnostic records Context/materials source hashes as well as the
reference/database/batch modules. The production dependency and default individual mode remain unchanged.

The earlier consumer `4ad6f14` dedicated deferred and batch workflows pass; its general Python CI remains red
on an unrelated Nmx results snapshot after a failed-job rerun (1380 passed, 6 skipped, 7 deselected on the
first attempt). Do not treat the dedicated parser parity evidence as an all-checks-green claim.


### Exact-source hosted evidence

- PyTUFLOW source a9f654c: [reference/context regression workflow](https://github.com/phantom-branch/PyTuflow/actions/runs/39791230176)
  and [strict archive workflow](https://github.com/phantom-branch/PyTuflow/actions/runs/39791230068) pass.
- Consumer source 2e03c0b: [dedicated batch smoke](https://github.com/Chain-Frost/ryan-tools/actions/runs/39791564359)
  passes 9 adapter/diagnostic tests and strict full 161/174 eager/deferred/batch replay using the a9f654c pin.
- [Dedicated deferred consumer](https://github.com/Chain-Frost/ryan-tools/actions/runs/39791564413)
  passes 59 focused tests and its strict archive replay.
- [General Python CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/39791564396)
  has 1380 passed, 6 skipped, 7 deselected and one Nmx snapshot failure: column-axis pandas StringDtype
  has NA-value `<NA>` versus `nan`. Package, Python 3.13 compatibility/MCP and Policy jobs pass.
  This is recorded separately and has not been fixed or suppressed in the deferred evaluation work.


### Completed current-source local SSD trials

Windows Python 3.14, PyTUFLOW 1.2.6 source a9f654c, diagnostic 2e03c0b; three sequential warm-cache trials per model,
rotating eager/deferred/batch order. These timings include fresh parsing, reference materialisation and
shared physical inventory, including diagnostic physical-path identity normalisation. Raster-header validation
is covered by the separately repeated public individual/batch preflight. Process startup, report writing and
that public comparison are outside the timed interval, so these are diagnostic timings, not complete launcher
wall times. Compared native identities are `(run index, flags, normalised command, physical path, layer)`;
stronger upstream input UUID/control-line provenance is covered by synthetic cache/uncached tests. All three modes have exact per-run/command/path/layer identity
parity, zero collection/inventory findings, and matching successful individual/batch public path/issue sets.

| Model | Runs | Eager median (range), s | Deferred median (range), s | Batch median (range), s |
| --- | ---: | ---: | ---: | ---: |
| P | 4 | 1.234 (1.229-1.264) | 0.852 (0.847-0.887) | 0.688 (0.685-0.689) |
| B | 240 | 62.640 (62.504-66.919) | 40.876 (40.779-44.050) | 26.285 (25.781-26.549) |
| Replacement SSD | 480 | 118.843 (114.569-122.171) | 65.007 (64.672-67.559) | 45.223 (43.873-45.557) |

The replacement SSD model's last batch trial still constructs 480 contexts and 28,327 commands, with 3,353
command-plan/template reuse hits. Of 43.708 s in the batch call, 34.597 s is context construction,
2.936 s reference collection and 4.480 s database projection. These last-trial counters substantiate the
remaining graph optimisation opportunity; they do not imply that all context work can be safely removed.
Phase categories differ across modes: individual source projection is in residual collection time, whereas
batch projection is inside the timed batch call. Python audit counters omit native/stat operations and
must not be treated as a complete I/O census. RSS is sampled at 20 ms in the same reused process with GC
between modes; it is an observed sample, not an exact peak or an isolated memory-regression guarantee.


## Interim native benchmark results (2026-10-22)

PyTUFLOW **1.2.6**, source `a9f654c1ec0812fc70035e227fb2f9f12290f449`; consumer diagnostic
`2e03c0b110a272d9c6a05cc6da3bc08817130164`, Windows Python 3.14. Three sequential warm-cache
trials per model, rotating eager/deferred/batch order. The 480-run local and SMB tests use identical saved
command-file bytes and flags. The owner intentionally removed irrelevant files from the network copy.

| Model | Storage | Runs | Eager median, s | Deferred median, s | Batch median, s |
| --- | --- | ---: | ---: | ---: | ---: |
| P | SSD | 4 | 1.234 | 0.852 | 0.688 |
| B | SSD | 240 | 62.640 | 40.876 | 26.285 |
| M480 | SSD | 480 | 118.843 | 65.007 | 45.223 |
| M480 | SMB | 480 | 229.062 | 91.844 | 70.014 |

All listed full trials have identical materialised `(run, flags, normalised command, physical path, layer)`
sets across modes and **zero collection/inventory findings**. P/B and M480 SSD also pass the separate full
public individual/batch preflight with matching checked paths and issue sets. **Still running:** M480 SMB's
final public comparison, followed by the newly supplied 42-command network model. Do not describe those
pending gates as passed.

Timings include parsing, reference materialisation, diagnostic physical-path identity normalisation and
shared inventory. They exclude process startup, report writing and the separately repeated public-preflight
comparison (which validates raster headers). These are diagnostic timings, not complete launcher wall times.
No native data or identifying P/B paths are committed; no model simulation is launched.

Source fixes are split explicitly: `a9f654c` repairs existing eager/shared ninth-slot conditional scopes and
materials CSV blank/comment loading; `b1c9cfd` repairs the new shared cached-source accessor (including eager
callers) and deferred/batch output-folder compatibility. The UNC/mapped-drive inventory fix is diagnostic-only.
The affected upstream selection passes **223 tests**; dedicated consumer checks pass **59 deferred + 9
batch/diagnostic tests** and strict **161-TCF / 174-run** replay. The general Python workflow separately fails
one Nmx snapshot column-axis pandas StringDtype comparison (`<NA>` versus `nan`); do not claim all CI green.

Batch still constructs one context per run (480 for M480). The broader immutable control-file graph remains
future optimisation. The owner explicitly deferred the two-BC model. The incomplete 792-run model was replaced;
its direct expected PO-file existence check found GD08 393/396 and GD14 394/396 absent, not a valid benchmark.


## Final native benchmark results (2026-10-22)

All supplied native workloads pass **three full rotated trials**, with zero missing/unresolved input findings,
identical eager/deferred/batch materialised identity sets, and matching successful public individual/batch
checked-path and issue sets. The 480-run network copy retains all inputs needed by this benchmark after the
owner's removal of irrelevant files. No TUFLOW simulation was launched.

PyTUFLOW **1.2.6**, parser source `a9f654c1ec0812fc70035e227fb2f9f12290f449`; consumer diagnostic
`2e03c0b110a272d9c6a05cc6da3bc08817130164`. Windows Python 3.14. Model names P/B and all native paths
remain private. The M480 command-file bytes and 480 flag dictionaries match between SSD and SMB copies.

| Model | Storage | Runs | Eager median (min-max), s | Deferred median (min-max), s | Batch median (min-max), s |
| --- | --- | ---: | ---: | ---: | ---: |
| P | SSD | 4 | 1.234 (1.229-1.264) | 0.852 (0.847-0.887) | 0.688 (0.685-0.689) |
| B | SSD | 240 | 62.640 (62.504-66.919) | 40.876 (40.779-44.050) | 26.285 (25.781-26.549) |
| M480 | SSD | 480 | 118.843 (114.569-122.171) | 65.007 (64.672-67.559) | 45.223 (43.873-45.557) |
| M480 | SMB | 480 | 229.062 (228.122-230.345) | 91.844 (90.437-96.858) | 70.014 (69.540-71.085) |
| N42 | SMB | 42 | 80.892 (80.308-82.257) | 40.232 (39.470-43.397) | 16.818 (16.817-19.501) |

For M480, batch reduces diagnostic elapsed time versus individual deferred by **30.4% on SSD** and **23.8%
on SMB**. Network elapsed remains higher in every mode. These are sequential warm-cache observations,
not cold-cache guarantees. Each repeat rotates mode order; no timed model workloads run concurrently.

Timings include fresh parsing, reference materialisation, physical-identity normalisation and shared inventory.
They exclude process startup, report writing and the separate public-preflight replay (including raster-header
validation). They are **diagnostic timings**, not complete launcher wall times. Native identity keys compare
`(run index, flags, normalised command, physical path, layer)`; upstream UUID/control-line provenance has
separate synthetic cached/uncached tests. Required-input inventory, not equality of all project files, gates
acceptance. Large copied binaries were size-checked, not exhaustively byte-hashed.

M480 retains **12,480 compared identities / 49 distinct physical paths** per copy in every mode;
N42 retains **1,922 identities / 196 paths**. Median timing phases for the matched 480-run copies:

| Storage / mode | Parse, s | Context or batch call, s | Remaining materialisation/identity/inventory, s |
| --- | ---: | ---: | ---: |
| SSD / eager | 0.572 | 95.865 | 22.406 |
| SSD / deferred | 0.556 | 56.850 | 7.601 |
| SSD / batch | 0.560 | 43.708 | 0.928 |
| SMB / eager | 0.701 | 181.811 | 46.741 |
| SMB / deferred | 0.716 | 58.676 | 32.458 |
| SMB / batch | 0.710 | 43.595 | 25.946 |

Column medians are computed separately and need not sum to the median total. Last-trial M480 eager-context
audit counts are 6,253 SQLite connects, 13,454 glob events and 1,440 scandir events in each storage case;
individual deferred contexts and the batch call record none of these selected events. This does not measure
all native I/O: in particular, physical path identity normalisation remains in the residual diagnostic phase,
which is noticeably more expensive on SMB. Do not misattribute that entire residual to the production parser.

### Accounting and limits

Process RSS is sampled every 20 ms with GC between modes; it includes native allocations but is not an exact
peak or an isolated allocator regression proof. Maximum sampled RSS across the three full trials:

| Model / storage | Eager MiB | Deferred MiB | Batch MiB |
| --- | ---: | ---: | ---: |
| P / SSD | 263.4 | 263.4 | 263.4 |
| B / SSD | 332.7 | 331.2 | 332.1 |
| M480 / SSD | 344.3 | 341.9 | 341.5 |
| M480 / SMB | 342.1 | 339.5 | 341.0 |
| N42 / SMB | 353.1 | 353.7 | 350.4 |

The M480 SSD last batch trial still constructs **480 contexts / 28,327 commands**, reusing 3,353 command
templates. Its 43.708 s batch call includes 34.597 s context construction, 2.936 s reference collection and
4.480 s database projection. A broader immutable control-file graph remains future optimisation, not work
completed by this evaluation. Phase categories differ across modes; individual database projection is part
of residual collection, whereas batch projection is inside the batch call. Python audit events count selected
open/directory/SQLite/glob calls, not all native/stat I/O; retain independent no-I/O spy tests.

### Delivery and remaining scope

- Parser source a9f654c passes 223 affected local tests, exact-head reference CI and strict archive CI.
  Consumer code 2e03c0b passes **68 focused local tests** against a9f654c with both real fork integration
  gates enabled. Hosted batch smoke passes 9 tests and strict 161-TCF / 174-run three-mode replay on a9f654c.
  The earlier hosted 59-test dedicated deferred job used its older 8e7bc30 pin; that workflow is now aligned
  to a9f654c as well. Its new hosted result must be checked separately rather than attributing the old green
  result to the new parser revision.
- Existing eager/shared fixes are recorded separately upstream: ninth-slot conditional scopes and materials
  CSV blank/comment loading (a9f654c). New shared cached-source accessor repair, deferred/batch output-folder
  compatibility (b1c9cfd), and diagnostic mapped-drive SQLite handling remain separately identified.
- The replaced 792-run workload is not accepted: direct PO-file existence checks found GD08 393/396 and
  GD14 394/396 absent. This is not a reason to suppress findings or silently skip both catchments.
- N42 is the 42-command network benchmark. The historical external 440-run evidence is revision-specific
  and was not rerun on this machine.
- The owner explicitly postponed the two-BC-model investigation. No new two-BC functionality is claimed.
- General Python CI separately fails the Nmx snapshot column-axis StringDtype NA-value comparison
  (`<NA>` versus `nan`): 1380 passed, 6 skipped, 7 deselected, one failure on source 2e03c0b.
  Dedicated parser checks are green; the whole PR is not all-checks-green or ready for automatic merge.
- No solver run, default-on switch, production dependency repin, merge or release. Raw native data and reports
  remain outside Git; only anonymised findings and benchmark information are published.


**Delivery:** The interim records are pushed as consumer `38cdc66` and parser-documentation `9b39b43`,
with interim comments on PR #143 and PyTUFLOW #5. Final results and the aligned deferred workflow pin are
published in the subsequent final-results commits. The benchmark source is parser a9f654c and consumer diagnostic 2e03c0b; later source changes are
tracked separately below. New documentation/workflow-head checks are distinct from the source CI cited
above. Next action: review the final evidence and resolve the independent Nmx snapshot CI issue; keep the
two-BC investigation deferred and scope the broader immutable graph separately. Next review: 2026-10-23.


### Benchmark revision boundary after publication

Concurrent follow-up commits `37a69b3` (host filesystem case handling for cached database paths) and
`f3325b8` (POSIX case-sensitive selection regressions) landed in PyTUFLOW during publication and are
preserved. **The native benchmark evidence above is for a9f654c, not those later source changes.**
All five recorded native source snapshots match retained a9f654c Git contents with normal checkout
LF/CRLF conversion accounted for. A retained a9f654c source extraction independently passes all 68
focused consumer tests with both real-fork gates enabled. The later case-handling source was not rerun
through the native performance matrix. Benchmark figures must remain associated with their recorded
source rather than being relabelled as current branch-head timings.

## PR #7/#10 repair and benchmark review - 2026-10-23

PyTUFLOW PR #7 was based on an older PR #9, causing the native GitHub stack
to reject PR #10 even after a clean local merge. Rebased PR #7 onto current
PR #9 `204c9e0`, then PR #10 onto PR #7. Preserved local backup branches and
used explicit lease-protected pushes. Both now report mergeable: PR #7
`66872d5`, PR #10 source `5b11415`; parser documentation follow-up `099434f`.
No PR merge, release, production dependency repin or engine execution.

The integration fix distinguishes scenario-only conditional READs from DEFINE
and event scopes for the new whole-model proof. General conditional command
reuse remains disabled; dynamic paths and DEFINE scopes retain conservative
bypass. **This repairs new batch optimisation, not original eager behaviour.**
Local Windows/Python 3.14.6 validation passes **266 parser/integration tests
and 68 focused consumer tests**, compileall and whitespace checks.

Retained parser benchmark snapshot `5053a07` has exactly the same Git tree as
source `5b11415`: `53f09c3d7f97d356c4f1ca3f258c5657afbceb8f`.
Consumer diagnostic head is `bd77a3924d561eff56ae9cc3fda0c10915e8f1a5`.
Source history changes do not relabel or replace the retained source manifest.

### Process and completed public checks

- Verified public archive: 1,464 members, SHA-256
  `e374c8e072679fc6c4dc11e63b270caa1101b6aebb84d70060872cdd1b0aa7d6`.
- Full **161 TCFs / 174 documented runs** passes the P4 four-mode identity
  comparison and P3 three rotated trials: individual deferred, uncached batch,
  cached batch without graph, cached batch with graph. No fabricated extra runs.
- Separate consumer eager/deferred/batch **materialised** replay passes all
  161/174. The syntactic comparison alone is not the materialisation gate.
- Sums of model median parser times are 7.643 / 7.577 / 5.746 / 5.794 seconds
  in that mode order. These are sums of per-model medians, not a single suite
  wall-clock measurement. The graph adds about 0.8% on this small public set;
  this does not demonstrate a graph speedup or predict native SMB behaviour.
- Native M480 (480 distinct runs) and N42 (42 distinct runs) execute sequentially
  on SMB, four modes and three rotating trials. Parsing is reported separately;
  mode timings include evaluation and reference projection. Sampled RSS and
  actual constructed/elided run-state counters are retained. A separate full
  materialised consumer replay follows each parser workload, untimed for
  consumer optimisation claims. No native inputs are copied or uploaded.
- The old three-mode consumer diagnostic cannot isolate the graph's effect.
  Current graph comparisons use the same parser revision with graph off/on.
  Consumer production optimisation work is separate from this parser benchmark.

Private raw native reports stay on the network validation area; only anonymised
aggregates belong in Git/PRs. Public reports are retained outside Git under the
local temporary `pytuflow-review-5053a07` directory. Hosted exact-head checks are
queued, not yet accepted. Next action: finish native measurements, publish the
aggregates here and on PR #10/#143, and check terminal hosted results.

**Delivery:** Parser repair and handoff pushed; interim comment posted on
PyTUFLOW PR #10 and consumer PR #143. Consumer record pushed as `0e01f5a`.
Broader invariant graph
coverage and the owner's deferred two-BC investigation are separate follow-ups.

### Current native results and graph coverage

M480 passes all three four-mode parser trials with 480 distinct flag signatures,
zero missing/extra ordered identities and zero failed runs. Its separate full
480-run eager/deferred/batch materialised replay and both public preflight modes
pass, with identical checked paths and issues. N42 also passes all parser trials, the 42-run materialised comparison and both
public-preflight modes, with zero issues and identical checked paths.

| SMB model | Runs | Individual deferred median, s | Uncached batch median, s | Cached, graph off median, s | Cached, graph on median, s |
| --- | ---: | ---: | ---: | ---: | ---: |
| M480 | 480 | 73.466 | 67.279 | 43.455 | 45.844 |
| N42 | 42 | 32.653 | 32.988 | 10.457 | 10.766 |

M480 parse time is 0.751 s, outside those mode totals. Graph-on trials are
43.516 / 52.255 / 45.844 s; graph-off trials are 41.995 / 43.455 / 45.755 s.
The graph adds **5.5% median overhead** here. Existing batch caching improves
on uncached batch; that benefit must not be attributed to the new graph.

**The graph implementation is partial.** M480 records 181 compiled graph nodes,
7 eligible reads, 20 bypassed reads and 3,360 graph lookups, but still constructs
480 contexts and elides **zero input run-states**. It is consulted without
avoiding the dominant repeated construction on this model. Broader immutable
command-group reuse with sufficient scope/dependency proofs remains unfinished;
correctness and clean CI do not establish that optimisation as complete.

N42 parse time is 2.091 s, outside mode totals. Its graph-on trials are
11.240 / 10.766 / 10.490 s, graph-off 11.257 / 10.457 / 10.456 s: **3.0%
median overhead**. It records 551 graph nodes, 11 eligible reads, 202 bypassed
reads and 462 graph lookups, still constructing 42 contexts and eliding zero
input run-states. These observations justify a follow-up optimisation PR;
they do not justify weakening the graph's conservative proof conditions.

At parser documentation head `099434f`, all eight active hosted checks pass:
Windows Python 3.12/3.14 integration and reference contracts, tutorial contracts
on both versions, full 161-TCF graph replay and example parity. The optional
second full-public tutorial job is skipped. Both PR #7/#10 report clean.
See [integration CI](https://github.com/phantom-branch/PyTuflow/actions/runs/39814801852).


Final native acceptance: M480 retains 12,480 identities / 49 physical paths;
N42 retains 1,922 / 196, equal in all materialised modes. Both return exit 0.
Maximum sampled parser RSS (individual / uncached / cached graph-off / graph-on)
is 440.4 / 440.9 / 440.4 / 438.8 MiB for M480 and
430.1 / 429.8 / 429.8 / 430.0 MiB for N42. Sampling is every 20 ms and
includes allocations retained within the shared process; these are not isolated
per-model allocator peaks. No cold-cache or full-launcher wall-time claim.

**Merge instruction:** After these completed benchmarks the owner explicitly
authorised squash-merging the parser stack and opening a new graph-improvement
handoff PR. Consumer PR #143 is separate and is not authorised for merge here.

### Authorised merge and graph handoff delivery

After the full native pipeline returned exit 0, the five parser PRs were
squash-merged atomically into `master`: #6 `2a6aab5`, #8 `2a40276`,
#9 `124ff1a`, #7 `970f43e`, #10 `3de5cd9`. Git comparison confirms the final
merged tree exactly matches validated `099434f`. No package was released.

Opened [draft graph-improvement PR #12](https://github.com/phantom-branch/PyTuflow/pull/12)
from merged master, branch `docs/graph-invariant-group-handoff`, commit `0aa911c`.
Its [P5 handoff](https://github.com/phantom-branch/PyTuflow/blob/0aa911c/agent_workstreams/P5_graph_group_reuse.md)
contains the benchmark baseline and counters, proposed partial-group reuse,
provenance/scope constraints, validation and explicit separation of eager fixes.
The draft contains instructions and a discovery link only; P5 implementation
is not started or accepted. Next action: implementing agent works on PR #12,
then obtains workstation native reruns. Consumer PR #143 remains separate/open;
its production pins and earlier independent CI limitation are unchanged.

**Delivery:** Benchmark Markdown pushed to consumer PR #143; final aggregate
comment on parser PR #10. Parser merge confirmed by GitHub API for all five
PRs. New draft handoff pushed and labelled enhancement. No native input data
or detailed native reports were published. Review follow-up on 2026-10-30.

### Ordered batch consumer integration — supporting PR (22 October 2026)

A separate child PR of #143 now consumes the immutable `BatchDependencyRun.ordered_inputs` introduced in PyTUFLOW #6.
Its context facade retains **all active command ordinals**, including file-free Set Variable/output/declaration commands,
and validates UUID, source owner control-file/line and reference ordering before reconstructing the existing collector
surface. Non-reference commands remain in the sequence but never become false required paths.

With complete ordered descriptors, the existing guarded multi-`BC Database` selection in #143 can now call
`deferred_database_activations()` and `deferred_database_sources_for_activation(uuid)` on the batch facade.
The adapter never combines multiple BC Source tables into one active table. Invalid activation sequences,
duplicate UUIDs or missing owner links fail closed. The old single-BC/ref-only projection is retained for
older PyTUFLOW batch builds; it must not claim multi-BC completeness.

**Validation:** synthetic ordered/orphan/ownership and two-declaration collector tests have been added,
plus isolated Windows/Python 3.14 CI pinned to the merged PyTUFLOW #6–#10 stack `3de5cd9`, with the complete public
161-TCF/174-run three-mode materialised parity replay. The existing deferred/batch production defaults
and published dependency are unchanged. The first PR #156 head `c6102a0` passed its dedicated Windows ordered-input contract and full 161-TCF/174-run materialised parity workflow. Post-parent-sync CI is required before final acceptance.
A representative nested/multi-declaration real project and any SMB timings remain deliberately deferred to
the sole SMB-capable agent; do not upload native model data or claim production speedups from synthetic runs.

**Final PR #156 source-head acceptance (23 October 2026):** Merged parent #143 head
`f63bc02` (including #155's Nmx regression fix) into child #156 without
losing concurrent parent documentation. Updated isolated Windows contracts to
the validated merged PyTUFLOW #6–#10 source commit `3de5cd9`, not the published
production dependency. Exact source head `5303fe9` passes all four workflows:
Fast PR checks (39882687988), ordered batch Windows contracts and full public
161-TCF/174-run materialised replay (39882687996), Python CI including the
Nmx regression (39882688013), and batch optimisation smoke (39882687991).
There were no unresolved inline review threads; a fresh Codex review was
requested and had not yet completed when this was recorded. Native SMB and
real nested two-BC project confirmation remain independently deferred. No
solver, production dependency update, child-into-parent merge, main merge,
or package release was performed. Next action: close any new Codex findings,
then consider squash-merging child #156 into parent #143. Next review:
2026-10-30.

### PR #156 Codex correction and final source acceptance (23 October 2026)

Codex identified a P2 error in the multi-BC activation Source row mapping:
relative `Source` paths were anchored to the process working directory.
Fixed in `1e8ccc4` with Ruff formatting finalised in `41fdabb`. The
selected BC Source path is now relative to `source.owner_control_file.parent`,
or `source.database_path.parent` when the control file is absent, matching
the existing aggregate deferred materialisation semantics and preserving the
mapped-drive path with `Path.absolute()`. The two-activation regression tests
both ownership routes, including nested model paths and separate BC directories.

At `41fdabb`, all five exact-head Windows/hosted workflows pass: Fast PR
checks `39883774509`, deferred fork consumer `39883774462`, batch smoke
`39883774524`, ordered batch contract/public 161-TCF/174-run parity
`39883774515`, and full Python CI `39883774395`. The original Codex P2
thread is replied to and resolved. A fresh Codex review on this source head
was requested and was still in progress at this handoff. No new SMB native
validation, solver execution, production dependency pin change, or PR merge.
**Next:** review any new Codex findings and then optionally squash-merge
#156 into parent #143 (not `main`); #143 retains separate owner gates.
Next review: 2026-10-30.

### PR #156 ordered-reference completeness correction (23 October 2026)

A second Codex P2 review identified a fail-open case where a file-consuming
command's references could be missing from **both** flattened batch lists.
The consumer cannot derive completeness from command text or the two mutually
incomplete lists. The fix therefore introduces an immutable, independent
per-command `reference_count` in [PyTUFLOW PR #13](https://github.com/phantom-branch/PyTuflow/pull/13),
computed from each original active input. The optional batch consumer now
requires a valid nonnegative integer count and equality with that command's
owned reference projection; absent metadata or coordinated loss fails closed.
Legitimate zero-reference commands remain accepted, and older batches without
`ordered_inputs` retain the previously bounded compatibility path.

Upstream source branch `fix/batch-ordered-reference-count` at `d4a47d1`
includes the new contract, parity tests, and existing activation-index and
tutorial-benchmark constructor updates. The isolated Windows ordered contract
workflow in this child PR is pinned to this exact upstream **development** SHA;
published `pytuflow>=1.2,<2` remains unchanged. Focused regressions cover
reference loss from both flattened tuples, unavailable descriptor counts and
intentionally file-free commands. Upstream #13 and consumer #156 require
exact-head CI and independent review before merge; they are not yet production
features. No native SMB work, TUFLOW solver invocation or release.

**Next:** verify both PRs' Windows checks and Codex review, resolve P2 thread,
and only then consider merging upstream #13 and child #156 into parent #143.
PR #143 has its own separate release and native-model validation gates.
Next review: 2026-10-30.

## P5 rerun and file-sharing investigation - 2026-10-23

The owner requested new benchmarks, GitHub delivery and recommendations, plus
per-run M480 file/parameter sets and an actual-checks/graph comparison on the
existing SSD copy. The supplied benchmark and diagnostic work is complete.

Retained parser source `c305a6d2d5425ad72f388ef386ace375b4e53f99` (PyTUFLOW
1.2.6, Windows/Python 3.14.6) has identical parser and benchmark code to later
CI/documentation head `eebbb16`. Consumer diagnostic source is `5685fda8`;
later `f63bc029` only changes an unrelated regression test. Library/diagnostic
code is identical; consumer child PR #156 was not substituted.

**273 parser/integration tests, 67 consumer tests and 2 CI-comparator tests pass**,
as do compilation, full public 161-TCF/174-run parser/materialised replays and
native M480/N42 parser/materialised/public-preflight gates. Eight active hosted
checks pass at eebbb16, with no new P5 TMF regressions against pre-P5. This is
not zero absolute upstream failures: both fork versions have 39 failed / 530
passed / 11 skipped on hosted Python 3.14; three canonical-upstream differences
predate P5 and remain separate eager/shared-compatibility follow-ups.

Three rotated SMB parser medians: M480 graph-off **42.895 s**, graph-on
**42.790 s**, eliding **1,916 of 31,680 constructors**; N42 graph-off
**11.310 s**, graph-on **11.367 s**, eliding **0 of 4,148**. Differences of
0.24% faster / 0.50% slower do not establish a meaningful graph speedup.
M480 accounts for all eligible visits: four cold constructions plus 1,916
reuses. Existing batch-cache gains remain separate.

The private SSD trace captures all 480 flag/final-variable/event-variable records,
graph decisions, required files and observed Python I/O. Each run needs **25
distinct normalised paths, 23 common to all runs**; the union is 49. Two BC
database paths and 24 BC source CSVs account for variation. All graph and final
materialised path sets match; geometry/extent-related references stay constant.
Observed post-parse batch and reference-collection Python I/O is zero. Shared
inventory uses **26 stat calls, one scan and 13 SQLite connections**; shared
raster metadata opens six sources. One same-file TIF alias causes 50 original
spellings for 49 normalised locations. These are API requests, not physical
disk/SMB counts; native library internal operations are not fully traced.

The temporary parity diagnostic separately makes **12,480 Path.resolve calls**
for 49 normalised identity targets. A direct check confirms mapped network paths
become UNC with resolve, while absolute preserves spelling. The graph/cache does
not resolve paths. Recommend invocation-local physical-identity memoisation in
the diagnostic, preserving deliberate alias/security checks and original I/O
paths, plus broader proved fixed-scenario group reuse and cost profiling.

[Full P5 results and recommendations](https://github.com/phantom-branch/PyTuflow/blob/docs/graph-invariant-group-handoff/agent_workstreams/P5_benchmark_results_2026-10-23.md)
owns trial values, phases, RSS, counters, source hashes, CI and artifact boundaries.
SSD/network flags, 49 required file sizes and four control hashes match; large
binary contents were not exhaustively hashed. Raw native CSVs/reports/parameter
values stay private; only anonymised aggregates are published.

**Delivery:** Results committed/pushed to PyTUFLOW #12 as `2b8ddb9`; its description
and final benchmark comment are updated. This consumer #143 work record mirrors
the outcome; delivery commits are linked from the PR comments. Both remain unmerged; no production repin, solver,
launcher or release. Next action: review the measured coverage and select the
next bounded optimisation from the report. Next review: 2026-10-30. Other overdue
register fronts were not resumed by this benchmark task.

## Benchmark recommendations implemented - 2026-10-23

The owner explicitly requested the diagnostic efficiency fix in consumer PR #143 and clear ownership between
the two repositories. The earlier planning restriction on bringing the separate production optimiser into
#143 does not prevent this authorised diagnostic-only correction. Child PR #156 remains separate.

**PyTUFLOW delivery:** PR #12 commit `84f1f38767c446ab58c64fd478b383d97d516a73` expands the supported
GIS Code / Grid Zpts group proof to scenario-conditioned and proven variable-derived literal inputs. It
includes branch/scenario slot, configuration, source and relevant variable/declaration identities; detaches
mutable initial seeds and clones; fixes the remaining Grid `cf` alias; skips caches without useful candidates;
and exposes candidate, bypass, reuse, failed-run and cost accounting. The 500-context correctness fixture
elides 997 constructors versus 499 before the change, with full ordered parity. This is a constructor count,
not a measured native speedup. Unsupported command groups still use normal parsing.

All eight active hosted checks pass at that parser commit, including
[Windows 3.12/3.14 integration and the full public graph differential](https://github.com/phantom-branch/PyTuflow/actions/runs/39892457442).
The focused contract set passes 245 tests on each version, both locally and on Windows. The TMF comparison
reports no new P5 failures; pre-P5 and current fork results remain 39 failed / 530 passed / 11 skipped, with
three already documented differences from canonical upstream. These existing failures are not claimed fixed.

**Consumer implementation:** Only the temporary archive diagnostic and its regression tests/README change.
Its `collect()` caches successful physical identities by exact path spelling, and caches successful complete
GeoPackage table-name tuples by the same spelling. Each layer selector and run/command provenance record is
still evaluated independently. Original supplied paths are used for SQLite and inventory operations, aliases
remain comparable, and enumeration connections close explicitly on success or failure. The next collection
starts fresh; resolution/query failures propagate. Containment/module verification stays separate.

**Local consumer validation (Linux, Python 3.14.8):**

- `python -m pytest -q tests/fixtures/tuflow_example_repro/test_run_checks.py`: **6 passed**. The tests cover
  exact spelling counts, mapped/UNC alias handling, run/command provenance, changed link targets and database
  contents across calls, failed resolution/query retry and closed enumeration connections.
- Ruff 0.16.6 check and format verification pass for `run_checks.py` and `test_run_checks.py`; strict Pyright
  on those two files reports **0 errors / 0 warnings**. Compilation and whitespace validation pass.
- The layer fixture uses three runs, two path spellings and three selectors: **two enumeration connections**
  plus one independent final inventory connection, while retaining every expected identity.
- Documentation validation covers the default repository README set and all three changed Markdown files.
  The existing Windows batch-smoke workflow already selects the modified tests and complete archive replay;
  its development-only parser pin is retained. No package build is needed for this diagnostic-only change.

**Git delivery and next action:** This consumer patch is delivered on `feat/tuflow-run-preflight` to existing
draft PR #143; the PR records its exact commit and hosted validation. Production preflight, launcher defaults,
package/development dependency pins and submodule gitlinks are unchanged. No merge, release or simulation is
authorised by this change. Review both current PRs, then have the designated workstation run the native
M480/N42 protocol against the new parser/consumer source pair before making a timing claim. Broader parser
group support, `Inputs.append` bookkeeping and pre-existing upstream differences remain separately scoped
follow-ups. Next review: **2026-10-30**.

## PR #156 parent synchronisation and merge authorisation - 2026-10-23

The owner explicitly requested resolving the conflicts and merging #156 if complete. Its target remains
`feat/tuflow-run-preflight` (#143). Parent source `144e1e4` is integrated into the child while retaining both
the ordered-input/reference-count history and the newer graph/diagnostic benchmark record. The two conflicts
were documentation-only; the parent's diagnostic memoisation and tests merged without source conflicts.

Before this synchronisation, all five consumer workflows passed at `9a4c693`, including full public
161-TCF/174-run materialised parity, and Codex reported no findings. The required independent count contract
in PyTUFLOW #13 at `d4a47d1` passed all four upstream workflows and review. The outstanding second P2 thread
is implemented and validated; it needs its final evidence reply/resolution. The synchronised child must pass
its fresh hosted checks before the authorised merge into #143. The PR records the resulting source and merge
commits; the production dependency remains unchanged.

Local synchronisation checks: 24 focused adapter/diagnostic tests pass on Python 3.14.8; one real-fork gate
is intentionally left to the dedicated Windows workflow. Default documentation/index validation and links
in the three integrated Markdown files pass. The source merge introduced no Python conflict resolutions.

PyTUFLOW #12 must also adopt the same reference-count contract before it is tested with this consumer: ordered
descriptors without the count are intentionally rejected. Retain that fail-closed check. Native SMB and real
nested two-BC validation stay deferred to the designated workstation. Next review: **2026-10-30**.

## Bounded release, child integration and next workload — 2026-10-23

[PR #156](https://github.com/Chain-Frost/ryan-tools/pull/156) is now closed/merged into parent #143 at
`298fb13`, after its refreshed five workflows passed and the
[final review](https://github.com/Chain-Frost/ryan-tools/pull/156#issuecomment-6357553345) returned no findings.
The guarded parent fast-forward preserved the ordered-input/reference-count adapter, diagnostic memoisation
and both work-record histories. Parent #143 remains unmerged to `main`.

PyTUFLOW #12 source `126ec64e221908f5327ee9dc9d372c8084afaf12` includes the narrow constructor-hook safety
correction and the independent reference-count contract required by this consumer. The 263-test focused
selection passes on Python 3.12 and 3.14. Fresh paired Windows checks are pending against that exact parser
pin and the final consumer cleanup source. Those results must be recorded separately from the previous
`84f1f38` and #156 hosted evidence. Do not weaken the consumer's count or full-archive acceptance checks.
Literal-setting reuse and `Inputs.append` optimisation remain separate parser follow-up PRs.

The former temporary archive runner is now supported repository-only Windows regression tooling, with
focused tests and full 161-TCF/174-run replay retained. Its lifecycle/documentation cleanup changes only
docstrings, comments, workflow labels and documentation. The package wheel, normal published PyTUFLOW
dependency, installed wrapper catalogue and opt-in launch defaults remain unchanged. The
[preflight record](2026-10-21-tuflow-input-preflight.md) owns the release validation and delivery status.

The owner has offered a new model on a different SMB server. Treat it as a new workload: record both exact
Git heads, the private model/run-list provenance and the expected run count before executing the existing
`run_local_checks --batch` protocol on the designated workstation. That protocol compares eager, individual
deferred and batch collection and validates the representative/full inventories and public preflight.
It does not expose graph compilation on/off; a same-revision graph comparison requires a separate bounded
consumer diagnostic PR and validation before timing. Old M480/N42 trials remain historical and need not be
repeated as a release gate. No new native speedup is claimed from the hosted archive or constructor counts.

**Local validation and delivery:** Ruff lint/format and strict Pyright pass for the docstring-only Python
change on Linux/Python 3.14.8; executable AST structure matches `HEAD`. Default documentation/index coverage,
all six touched Markdown link checks and `git diff --check` pass. The preflight record above contains the
exact commands and environment. Edits are unstaged on `feat/tuflow-run-preflight` at `298fb13`; no commit or
push was made by this documentation update.
The separately owned batch-smoke matrix edit is preserved. No solver, private-model benchmark, release or
production dependency repin occurred. Next action: finish exact-source hosted validation for the bounded
release, then use separate PRs for each selected optimisation and the new-workload diagnostic extension.
Next review: **2026-10-30**.
