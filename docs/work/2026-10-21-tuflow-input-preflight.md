# TUFLOW proposed-run input preflight

## Scope

Implement a read-only Python checker in ryan-tools for every event/scenario combination proposed by the maintained
TUFLOW simulation launcher. It must verify scenario- and event-dependent control-file inputs and event-specific
BC database Source files without starting TUFLOW. Provide a standalone wrapper and an opt-in integrated check.
Preserve existing GPU scheduling, run ordering and batch execution defaults.

## State

In validation. Owner: ChatGPT. Updated: 2026-10-22. Next review: 2026-10-23; example reproduction bundle is temporary branch-only data; resolve the recorded feature gaps and remove the bundle before final merge.

## Decisions

- PyTUFLOW is a required runtime dependency of ryan-tools, installed with the normal package; no optional extra.
- Reuse the launcher's exact deduplicated Simulation objects for integrated checks.
- Extract reusable run-flag parsing into ryan_library, while retaining the launcher scheduling/dashboard logic.
- Keep checks read-only; write only a finding CSV; fail closed on unresolved references.
- Enable integrated input checks only when explicitly requested until real-model validation passes.
- Warn by default at each simulation launch if a live local TUFLOW process is confirmed to use an overlapping GPU.
  Unknown device assignments are reported as uncertain, not silently assumed free; warnings do not block launches.

## Acceptance criteria

- No TUFLOW process started from preflight-only or standalone checker.
- Missing BC Source CSV for one event is reported only for its affected proposed runs.
- Recursively inspect model input references; check GeoPackage layer names when enabled.
- First gather all run-to-file dependencies, then deduplicate files and use bounded shared-directory scans for dense inputs.
- Exclude output directories and write-only commands from required-file checks.
- Return non-zero and prevent batch launch for missing/unresolved dependencies.
- Warn on confirmed overlapping -puN GPU assignments (external and same-launcher processes) without blocking or changing GPU schedules.
- Do not falsely classify a running TUFLOW process with no explicit GPU flag as using a known GPU.
- Provide --no-pause and --help on both wrappers.
- Document limitations, install and regression testing; verify current Python 3.14 wheel.

## Validation and delivery

New focused tests accompany the implementation. GitHub hosted CI and native Windows/PyTUFLOW installation,
standard package dependency installation and additional HPC control-file edge cases remain to be verified.
Do not merge until validation and review are complete.

On 2026-10-21, a representative Q-drive model supplied through its existing launcher and 440-command export passed
the corrected checkout with 63 unique inputs and no issues. PyTUFLOW 1.2.6 took 1.06 seconds to parse the model,
160.40 seconds to resolve all run contexts and 0.06 seconds for the deduplicated inventory (two directory scans and
11 direct file checks). The benchmark exposed that PyTUFLOW presents `BC Event Source` variable assignments as
pipe-joined pseudo-paths; these are now excluded from file checks, while resolved BC database `Source` files remain
validated separately. A focused regression test covers that distinction.

A second Q-drive model check against a supplied 42-run critical-storm batch file found two additional output-command
edge cases. All 42 active runs failed context construction when PyTUFLOW 1.2.6 expanded the nested
`Set Variable OutputRoot == ..\<<OutputBase>>` assignment but retained the closing `>>`, which it then interpreted as
a database layer delimiter on a directory path. A temporary sibling TCF that replaced the nested output roots with
inert static destinations allowed context resolution, but the inventory then incorrectly treated `Set Variable
OutputBase` and `Set Variable XMDFBase` values as required inputs. Commenting only those output-only assignments in
the temporary copy produced a successful input-only check: 42 runs, 202 unique inputs, eight directory scans, 135
direct checks and no issues; parse/context/inventory times were 4.57/113.77/0.73 seconds. The real TCF and batch file
were not modified, and the temporary TCF was removed. This smoke check used the available Python 3.13.14 interpreter
with PyTUFLOW 1.2.6 isolated in a temporary target because Python 3.14 and PyTUFLOW were not installed locally.

Focused validation after the fix passed 28 preflight/launcher tests, the Loguru policy check and documentation link
checks. A no-bump build produced and verified the 26.10.21.1 wheel (SHA-256
`2d009ee77bfc6b4dcc417099b2af81588329e1e63bfe4677256f8c5eb1e6d4e4`); reinstalling that wheel and checking one
representative run found 20 unique inputs and no issues. Ruff and strict Pyright could not be rerun locally because
the corporate network-share execution policy rejected both executables with Windows error 5; exact-head CI remains
the authoritative pending static-analysis check.

## Next action

Exclude output-path variable assignments before PyTUFLOW file iteration and handle nested output-root variables during
context construction, with focused regressions for both observed forms. Then rerun this 42-run model without a TCF
workaround, run exact-head CI and complete the remaining Python 3.14 dependency/wheel and input-edge-case checks;
address any review comments before requesting merge.

**Performance validation required:** PyTUFLOW retains its initial control-file/BC database build state,
but each `TCF.context()` creates run-state inputs that can glob the same files and open GIS layers again.
The two-phase inventory reduces repeated checks *after* context resolution; it does not eliminate
PyTUFLOW's internal network access. Inspect phase timings on 500 representative runs, and do not claim
single-pass filesystem I/O until run-state activity is measured or an upstream supported cache is introduced.
Avoid intrusive monkeypatches of PyTUFLOW's private internals.


## Deferred PyTUFLOW integration — 21 October 2026

Integration commenced against [phantom-branch/PyTuflow PR #1](https://github.com/phantom-branch/PyTuflow/pull/1).
The consumer now detects whether PyTUFLOW exposes the `file_resolution` keyword,
uses `context(flags, file_resolution="deferred")` and immutable `file_references`
when available, and preserves the published-package eager fallback. A bounded,
nonrecursive, shared wildcard scan and wildcard/ALL GeoPackage-layer selection are
included. Pending GIS attribute scans and non-BC database-linked dependencies are
reported unresolved instead of being silently ignored.

**Validation status:** focused mock-based integration tests and a Windows fork-consumer smoke workflow added. The smoke workflow installs pinned PyTUFLOW commit `6784162da7ed0389ae8efafa6af138d10deecaeb` independently of normal PyPI packaging. Exact-head CI,
native installed-fork tests, eager/deferred dependency parity, 440-run Windows
network-drive benchmark and complete attribute/database bulk inspection remain.
Keep this PR open and preflight opt-in until those checks are satisfied.
Normal dependency remains `pytuflow>=1.2,<2`; no GitHub fork URL is published
in package metadata. Next action: inspect CI failures, then add integration
tests using a pinned fork commit and verify real-model coverage.


## 440-run evidence and deferred database follow-up — 21 October 2026

Local Higginsville Windows/Python 3.14.5 validation on mapped `Q:` storage
(compare [PR #143 discussion](https://github.com/Chain-Frost/ryan-tools/pull/143#issuecomment-6310509970)):
for 440 runs, eager dependency generation took **269.356 s** versus
**26.011 s** deferred (about **10.35× faster**). The earlier fork revision
`6784162` reported **880 unresolved findings**: 440 material-database
dependencies and 440 unavailable active BC databases. Its nine raw deferred
path tokens could not yet be compared to the 63 materialised eager paths.

The new upstream [PyTUFLOW PR #1](https://github.com/phantom-branch/PyTuflow/pull/1)
API provides `deferred_database_sources()` with event-resolved secondary
`bc`/`materials` references from cached, parsed database rows, explicitly
rather than rebuilding eager per-run database contexts. The ryan-tools
consumer uses the new method when present, leaves older forks fail-closed
and keeps all resulting source files in the shared deduplicated inventory.
The validation workflow is pinned to `1c300a7013522f177de07e68f27f3824a3ba8177`.
Python tests now cover per-run BC source differences, numeric-only material
databases, unavailable API behaviour, Windows-legal literal wildcard-like
folder names, and an opt-in real-fork end-to-end fixture.

**Open gates:** confirm exact-head CI and real-model BC/material parity, then
rerun the actual 440-run inventory and compare materialised file/layer
dependencies per run; complete GIS attribute-linked sources and unsupported
database schemas (fail closed until then). No TUFLOW executable launch, merge,
or PyPI dependency replacement is authorised.


## Nested control-file completeness — 21 October 2026

New local Higginsville parity evidence is recorded on PyTUFLOW PR #1, comment
6311680970. Representative runs 1, 221 and 440 showed 57 eager versus 30
deferred run/path/layer entries, with nine nested GIS/DEM dependencies omitted
from every run (21 versus 12 physical unique paths). The BC and materials
source API behaved correctly, but the preflight falsely passed with incomplete
nested TGC/TBC/ECF traversal. Therefore the 440-run parity gate remains blocked.

Upstream PyTUFLOW now keeps an in-memory original-path index for parsed nested
controls, compares those paths against context-specific references (including
mapped-drive/UNC aliases), and raises for any unmatched or ambiguous cached
child instead of silently omitting it. Deferred GeoPackage references without
an explicit layer now include the conventional stem-named default layer.
Regression tests cover TGC/TBC/ECF nested GIS and raster input traversal,
alias paths, missing parsed children, duplicate literal matches and wrong
scenario folder matches.

The current upstream pin is `1c300a7013522f177de07e68f27f3824a3ba8177`.
Its deferred test workflow passed Linux Python 3.12/3.13 and Windows
Python 3.12. This is *synthetic validation*, not confirmation that the nine
real Higginsville references are now restored. The consumer has a gated
end-to-end nested-tree test as well. Recheck runs 1, 221 and 440 against
materialised eager/deferred file-and-layer identities on Windows Python 3.14,
then run all 440 only if the representative parity gate passes. Preflight
remains opt-in; do not merge or switch the production dependency.


## Higginsville all-440 parity achieved; GIS attributes bulk phase (21 October 2026)

Local Windows Python 3.14.5 results in PR #143 comment 6312917098 and
PyTUFLOW PR #1 comment 6312929632 confirm **0 eager/deferred path+layer
differences across all 440 runs**: 8,360 materialised run/path/layer
entries, 62 physical unique files in both modes. The historical
63rd eager token is the same BC database through UNC and mapped Q: aliases.
The corrected deferred generation took 93.513s versus 174.103s eager
(1.86x) while avoiding 3,969 repeated GIS/pyogrio/SQLite operations of
each kind. The previously missing nine nested TGC/TBC/ECF inputs per
run are recovered.

The only outstanding preflight issues were exactly 440 Read GIS Network
attribute-linked cases. PR #143 now adds a read-only GeoPackage attribute
bulk scan in ryan_library/functions/tuflow/gis_attribute_dependencies.py.
It selects only fields which can refer to secondary sources, groups all
simulation requests by database/layer, scans each distinct GeoPackage
once, adds discovered secondary files (and filename wildcard expansions)
to the normal shared inventory, and reports per-run missing files.
Mixed/unknown geometry, unknown layers, unsupported Shapefile/MIF sources,
missing attribute fields and unexpanded source variables remain UNRESOLVED.
Tests include 440 synthetic runs sharing one GeoPackage, M/Q network
source examples, linked-file deletion, invalid schemas and wildcard layers.

**Remaining validation:** The new scanner has not been run on the local
Higginsville data. Repeat representative runs and then all 440 on the
current PR head; verify the 440 previous Read GIS Network findings are
now either resolved with checked secondary files or remain documented
schema-specific exceptions. Do not claim the overall preflight passes
until the local report has no missing/unresolved requirements.
Run the separate 42-run output-variable model unchanged. No merge or
automatic enforcement until local and CI gates complete.

## P and B Windows smoke checks (2026-10-21)

Tested consumer head `a550e86304f752f31932c73821e8a3c801db3431` with normal Windows Python 3.14.6.
The supplied `E:\Github\PyTuflow` checkout is on master at `cf908cc`, without the deferred API.
Its local Git object store also contains the workflow-pinned fork commit
`1c300a7013522f177de07e68f27f3824a3ba8177`; a `git archive` of that exact commit was extracted
under the temporary validation directory and imported only in the test process. Neither the checkout,
installed packages nor project files were changed. This is source-import evidence, not installed-fork wheel evidence.

The run-variable dictionaries and TCF paths were extracted using Python AST from the supplied launchers;
their parameter products produced four P and 240 B requests. Neither launcher's main function,
simulation execution, command export nor post-run hooks were called. Representative indices were 1/3/4
and 1/121/240 respectively, followed by the full products to count affected runs. The full deferred checks
were diagnostic runs despite the representative failures; they do not establish the parity acceptance gate.

| Model / mode | Runs | Unique paths | Findings | Parse / contexts / inventory seconds |
| --- | --- | --- | --- | --- |
| P / pinned deferred | 4 | 16 | 8 unresolved across all runs | 0.200 / 0.307 / 0.002 |
| B / pinned deferred | 240 | 20 | 480 unresolved across all runs | 0.229 / 23.776 / 0.005 |
| P / installed eager 1.2.6 (rerun) | 4 | 18 | 0 | 0.261 / 0.640 / 0.001 |
| B / installed eager 1.2.6 (rerun) | 240 | 22 | 240 missing-path findings | 0.257 / 35.300 / 0.004 |

Each deferred run reports unsupported `Read Soils File` database inspection and a material database
error for the numeric list `0.05, 0.1, 0.1, 0.05`. The latter aborts secondary database collection;
do not infer complete BC coverage or compare these incomplete inventories as dependency parity.
Both models require upstream schema support before deferred acceptance. No attribute GeoPackage scans
were requested by these models, so this does not validate the outstanding Higginsville attribute gate.
P additionally emitted a projection-reading warning for its PRJ input.

The original eager comparison used PyTUFLOW 1.1.10 and is superseded by a full rerun with verified
installed **PyTUFLOW 1.2.6**, satisfying `pytuflow>=1.2,<2`. The table reports the corrected rerun.
B's reported missing BC source still retains literal `~AEP~` and `~DUR~` placeholders in 1.2.6.
This demonstrates unresolved substitution, rather than proving the intended event-specific rainfall file
is absent. No project edits or workarounds were used. P passes all four runs; B fails closed on all 240.

Focused verification:

- A process-local fork import with `RYAN_TEST_DEFERRED_PYTUFLOW=1` and
  `pytest -q -ra tests/orchestrators/tuflow/test_input_preflight.py tests/functions/tuflow/test_gis_attribute_dependencies.py`:
  **28 passed, 1 warning**. Native imports also emitted Windows exception diagnostic `0xc0000139` involving
  h5py and a NumPy binary-size warning, although pytest exited zero. Treat native dependency health as unresolved.
- `python -m pytest -q -ra tests/functions/tuflow/test_gpu_occupancy.py tests/scripts/test_run_tuflow_simulations.py tests/scripts/tuflow/test_rename_geopackage_layer_to_filename.py`:
  **25 passed**.
- Direct checker `--help` failed with `ModuleNotFoundError` for the new `run_flags` module in the existing
  installed ryan-tools. The source tests import this checkout; this failure confirms the installed package
  is not a matching PR build. No global installation was changed to conceal that gap.

Local reports, JSON summaries and reproduction scripts are in
`C:\Users\Ryan\AppData\Local\Temp\ryan-preflight-20261021` (`validate.py`, `eager.py`, `eager-all.py`,
`focused.py`); these are temporary, untracked artifacts and include project-specific paths.
No TUFLOW executable was started. No commit, push, merge or PR comment was requested or performed.
Only this status record and its register row were edited; unrelated submodule state was preserved.
A concurrent `pyproject.toml` version change from `26.10.21.1` to `26.10.21.2` appeared during validation;
this session did not make that change and preserved it. The recorded smoke results apply to the consumer
source at the stated head, not to a newly built wheel.

**Next action:** address numeric material lists and soils support in the upstream deferred API, rerun
the representative models with the required supported PyTUFLOW version, then compare materialised
per-run path/layer inventories. Matching installed-wheel/dependency and copied-wrapper smoke,
native-library health, mapped-drive copied-GeoPackage exercise, Higginsville attribute validation,
the separate 42-run model and exact-head CI remain open. These local smoke results do not approve merge.

### Correct-version eager rerun (2026-10-21)

Verified installed PyTUFLOW 1.2.6 and ryan-tools 26.10.21.2; consumer source head remains `a550e86`.
Ran `python "$env:TEMP/ryan-preflight-20261021/eager-1.2.6.py"` on Windows Python 3.14.6 for
all four P and 240 B runs. The reproduction script asserts PyTUFLOW's distribution version before
importing project data, uses the installed eager parser, and imports the current consumer checkout.
Reports are `P-eager-1.2.6-all.csv`, `B-eager-1.2.6-all.csv` and `eager-1.2.6-all-summary.json` beside
that script. No TUFLOW process was launched. The corrected results supersede the earlier 1.1.10 evidence.

`python -m pytest -q -ra tests/orchestrators/tuflow/test_input_preflight.py tests/functions/tuflow/test_gis_attribute_dependencies.py`
with the published 1.2.6 package: **24 passed, 4 skipped** (the four real deferred-fork tests).
Direct checker `--help` now passes, superseding the earlier missing-module failure. A copy of the checker
in the temporary directory also passes `--help` from that directory without a checkout path override.
This verifies installed-module discovery, not a complete installed-wheel model smoke or eager/deferred parity.
The earlier fork native-import warnings and deferred schema blockers remain separate open findings.

### Reconcile local and remote PR branch (2026-10-21)

Resolved the interrupted merge of `65ab0c1` into local `473f168`, preserving the local P/B evidence
and incoming pit/depth database regression. Consolidated the duplicate preflight register entries into
one dated row retaining both validation fronts. Then merged remote `8b32955`, including its database
scan guard precedence correction. Local merge commits are `5c5a604` and `7da687e`.
Applied Ruff formatting to the two integrated Python files; no runtime behaviour was changed locally.
Focused preflight/GIS tests: **29 passed, 4 deferred-fork tests skipped**. Ruff lint and format checks
and default documentation validation pass. No package rebuild, native model rerun or hosted CI was
performed for this merge reconciliation. The branch contains the fetched remote head and retains
the local testing commit. Changes are committed locally; no push or PR merge is authorised by this record.

### P/B rerun after project corrections (2026-10-21)

At consumer head `c5d61c8`, reran the complete current launcher parameter products with installed
PyTUFLOW 1.2.6 on Windows Python 3.14.6. Both projects pass the eager file preflight:

| Project | Runs | Unique files | Findings | Parse / contexts / inventory seconds |
| --- | --- | --- | --- | --- |
| P | 4 | 18 | 0 | 0.463 / 0.683 / 0.001 |
| B | 240 | 33 | 0 | 0.677 / 50.613 / 0.006 |

B now uses the recognised `Event File` command. Its AEP/duration source substitution no longer
produces the earlier 240 missing-path findings. This supersedes the earlier eager failure for B;
the earlier observations describe the previous project configuration, not a substitution defect
established in published PyTUFLOW. The separate upstream synthetic investigation also identifies
the previous `BC Event File` spelling as unrecognised by the tested parser.

PyTUFLOW still warns `Event "EnsembleILCL" not found in event database` for B, despite a textual
definition in its event file; this warning is not included in the consumer finding count. A passing
file preflight therefore does not certify complete event recognition or engine validity. Investigate
that discrepancy separately. P retains its projection-reading warning. No GIS attribute databases
were scanned, and the deferred schema/parity and other real-model gates remain open.

Reproduction: `python "$env:TEMP/ryan-preflight-20261021/recheck.py"`; reports are
`P-eager-1.2.6-recheck.csv`, `B-eager-1.2.6-recheck.csv` and
`eager-1.2.6-recheck-summary.json` in the same temporary directory.
Focused preflight/GIS tests at this head: **30 passed, 4 deferred-fork tests skipped**.
No TUFLOW process was started and no project files were edited by this session.
Only the status record and register were updated, left uncommitted; no build, push or merge performed.

### B event warning diagnosed (2026-10-21)

The `EnsembleILCL` definition in B's event file contains `BC BC Event Source`, with a duplicated
`BC`. PyTUFLOW 1.2.6 ignores that unrecognised command. Its model event database is populated from
recognised event-source assignments; therefore this definition contributes no recognised event entry
and `Context.load_events()` warns that the selected event is absent.

Direct inspection of the run-state BC database for runs 1/121/240 confirms that source file paths
resolve, but the selected rainfall columns remain `TP01_~CC~`, `TP01_~CC~` and `TP10_~CC~`.
The earlier zero-finding file preflight does not validate these column selections. Its documented
scope excludes time-series column/content validation; it must not be reported as full model acceptance.

A synthetic TCF/TEF/BC reproduction with PyTUFLOW 1.2.6 confirms that changing only
`BC BC Event Source` to `BC Event Source` removes the event warning and expands the selected
column from `TP01_~CC~` to `TP01_test`. The fixture includes another valid event definition so the
event database is nonempty, matching the real-model warning path. Without any recognised events,
PyTUFLOW skips event loading and leaves the token unresolved without that warning.
Reproduction command: `python "$env:TEMP/ryan-preflight-20261021/reproduce_event_typo.py"`.
Real-model diagnostic scripts `investigate_event.py` and `investigate_columns.py` are alongside it.

No real project or PyTUFLOW source files were edited. Next action: correct the duplicated keyword
in B's event file, then verify resolved rainfall column names against the actual CSV headers for
the intended events and rerun preflight. Deferred and other PR validation gates remain open.

### Corrected B event assignment verified (2026-10-21)

The project owner corrected B's duplicated keyword. With installed PyTUFLOW 1.2.6, all 240 B
contexts now resolve without the unknown-event warning. All **480 selected time/rainfall column
checks** across **12 distinct CSV files** match the actual time-series headers, including the resolved
climate-change suffix. ARR CSV files contain metadata before the header; verification explicitly
located the `Time (hour)` header rather than treating the first metadata row as column names.

Repeated complete eager preflight: **P: 4 runs, 18 files, zero findings; B: 240 runs, 33 files,
zero findings**. B parse/context/inventory times: 0.672/48.618/0.004 seconds. P's projection warning
remains separate. No TUFLOW execution or simulation-data/engine acceptance is claimed.
Temporary evidence: `corrected_event.py`, `verify_columns.py`, `verify_arr_headers.py`,
`B-column-recheck-corrected.json` and `eager-1.2.6-corrected-event-summary.json` in the previously
recorded temporary validation directory. This session did not edit project files. Status updates
remain uncommitted; deferred schema/parity, other model and package/CI gates remain open.

### Local SSD benchmark (2026-10-21)

Storage is **local SSD, no network**, as specified by the project owner. These measurements must
not be presented as mapped-drive/network performance. Windows 10 build 19045, Python 3.14.6,
16 logical CPUs; consumer head `c5d61c8a1394133ae995086ce4f316c0ec067e46`.
Eager uses installed published PyTUFLOW 1.2.6. Deferred uses the isolated source archive of the
workflow-pinned fork `1c300a7013522f177de07e68f27f3824a3ba8177`, with shared installed dependencies.

Three trials per mode, each in a fresh Python process, sequentially with alternating mode order:
eager/deferred, deferred/eager, eager/deferred. Each process checks P then B using the exact current
launcher parameter products. Existing OS/filesystem caches were retained; this is a warm-cache
benchmark, not a cold-storage test. The timed call includes model parsing, all run contexts and
the deduplicated inventory; Python startup, package import and CSV report writing are excluded.
No TUFLOW execution, project changes, private upstream monkeypatches or global package changes.

| Project / mode | Runs | Median total seconds | Total range seconds | Median parse / contexts / inventory seconds | Unique files | Findings |
| --- | --- | --- | --- | --- | --- | --- |
| P / published eager | 4 | 1.224 | 1.219–1.244 | 0.494 / 0.728 / 0.00145 | 18 | 0 |
| B / published eager | 240 | 57.933 | 57.847–58.458 | 0.693 / 57.227 / 0.00455 | 33 | 0 |
| P / pinned deferred, incomplete | 4 | 0.952 | 0.924–0.960 | 0.483 / 0.470 / 0.00198 | 16 | 8 unresolved |
| B / pinned deferred, incomplete | 240 | 45.221 | 44.980–46.527 | 0.669 / 44.528 / 0.00496 | 20 | 480 unresolved |

Each phase median is computed independently, so phase medians need not sum exactly to the median total.
Inventoried path sets, run counts and finding counts were stable across all three repetitions in each
mode. **Dependency parity fails:** deferred omits two unique inventoried files for P and 13 for B,
with no additional inventoried files relative to eager. Every deferred run reports unsupported soils
inspection and the numeric material-list exception, aborting complete secondary database collection.
These incomplete timings do not establish an equivalent-workload speedup. Per-run/layer parity is
not claimed; it must be checked after those schema limitations are addressed. The published eager
baseline passes all 244 runs. B's context construction accounts for about 98.8% of its eager time;
the final shared file inventory takes only milliseconds on this SSD. No I/O call counting or cache
flushing was performed. P's known projection-reading warning persists.

Reproduction and full local evidence are under
`C:\Users\Ryan\AppData\Local\Temp\ryan-preflight-20261021\ssd-benchmark`:
`run.py`, `worker.py`, `summarise.py`, `environment.json`, `summary.json`, six trial logs/JSON files
and per-project issue CSVs. Commands:

```powershell
python "$env:TEMP/ryan-preflight-20261021/ssd-benchmark/run.py"
python "$env:TEMP/ryan-preflight-20261021/ssd-benchmark/summarise.py"
```

Temporary trial JSON/logs and reproduction scripts contain confidential project paths; do not add
them to the repository. This record retains only anonymised results. Documentation/link and whitespace
checks pass; no source/test changes, package rebuild or hosted CI were needed for this measurement.
The record/register changes remain uncommitted. Next action: resolve the deferred schema blockers,
prove equivalent per-run file/layer coverage, then repeat the same local benchmark before comparing speed.


## Local SSD P/B materials and soils parity gate

New local Windows Python 3.14 measurements on an SSD (three warm-cache trials
per eager/deferred mode) show P: 4 runs, eager median **1.22 s**, deferred
**0.95 s**; B: 240 runs, eager **57.93 s**, deferred **45.22 s**.
**Both deferred outputs are incomplete** due to materials/soils and must not
be reported as performance gains. In B, roughly **98.8%** of eager time
was spent in context resolution, not final file checks. The local record
has 49 lines of uncommitted changes that the local agent should reconcile
and commit separately without overwriting their working tree.

In PyTUFLOW PR #1, deferred cached-row materialisation now recognises
scalar values, inline depth/Manning's-n pairs and external materials
sources, and checks soil methods NONE, ILCL, GA, HO, CO and SCS with
event/scenario variable expansion. Unsupported/malformed cases remain
fail-closed rather than silently skipping a potential dependency.
The ryan-tools collector recognises soils as a deliberately inspected
database type and always retains the primary materials/soils file
checks. Tests cover valid and malformed forms, an external material
source, a soil method matrix, and downstream preflight.

**Overall contract:** deferred mode must discover *every* input that
eager PyTUFLOW supports, not merely the three benchmark models.
Comparison keys must include run/event/scenario, originating command,
physical path, GeoPackage layer and attribute-derived secondary inputs.
Keep outstanding database/GIS schemas unresolved until their deferred
resolver and parity regressions exist. Model P/B workarounds or suppressing
diagnostics are not acceptable. No merge, PyPI change or claimed benchmark
speedup until complete materialised parity and exact-head CI.

## Updated local fork: P/B parity restored (2026-10-21)

Consumer/PR head `a60ba0cc11c06e51395f400a8d0c95100331f37c`; clean local PyTUFLOW checkout
`65f24fa96bbb93f5e836c3391be8837fa0048092`. Imported the supplied checkout explicitly in isolated
test processes; no global installation, source or project files changed. The consumer CI workflow
pins `f965314c2a5df68a3f5ada016b02607513d783c1`, so local source evidence and CI dependency identity
are recorded separately. Both checkouts report PyTUFLOW 1.2.6; commit provenance distinguishes the fork.

The material-list and soils errors are resolved on these models. Full public deferred preflight:
**P: 4 runs, 17 unique files, zero findings; B: 240 runs, 32 unique files, zero findings**.
Physical-identity comparison follows links deliberately and deduplicates aliases; these figures
must not be interpreted as missing files solely by comparison with older lexical-path counts.

Representative gates (P runs 1/3/4; B 1/121/240) passed before full comparison. Then compared
every run using the same updated fork in explicit eager and deferred modes. Keys contain run index,
event/scenario flags, originating command name, canonical physical path and GeoPackage layer.
Command names are compared without their RHS because eager string rendering includes the assignment,
whereas deferred reference origins expose its LHS. Wildcard filenames/layers are materialised before
comparison. No attribute-linked requests occur in P/B; the harness refuses parity if such inputs
appear without complete secondary materialisation.

| Model | Runs | Matching run/command/path/layer entries | Physical files | Missing / extra |
| --- | --- | --- | --- | --- |
| P | 4 | 68 | 17 | 0 / 0 |
| B | 240 | 4,560 | 32 | 0 / 0 |

All modes report zero collection issues. The earlier incomplete deferred P/B inventory result is
superseded for this updated dependency and consumer. This is P/B parity, not proof that all eager
features or the remaining GIS/database schemas are supported across arbitrary models.

### Same-fork local SSD timing

After the concurrent correctness checks finished, ran **one sequential eager/deferred pair per
model** against this same fork, on the same local SSD with existing filesystem caches. Fresh model
objects per mode, P then B, eager before deferred. Reported totals sum timed parse, context/reference
collection and unique physical-file/layer inventory phases. They exclude Python/package startup,
report writing and the separate parity-key/wildcard materialisation work. Public preflight was also
run separately above. These phase totals are not interchangeable with the older three-trial full-call
medians, and one ordered pair does not establish a stable performance gain.

| Model / mode | Parse seconds | Contexts seconds | Inventory seconds | Measured phase total seconds |
| --- | --- | --- | --- | --- |
| P / eager | 0.471 | 0.706 | 0.00163 | 1.178 |
| P / deferred | 0.445 | 0.451 | 0.00164 | 0.898 |
| B / eager | 0.666 | 52.482 | 0.02881 | 53.177 |
| B / deferred | 0.642 | 41.835 | 0.02897 | 42.507 |

Both inventories pass and the benchmark pair independently repeats zero missing/extra parity.
No network data access, cold-cache measurement or I/O-operation instrumentation is claimed.
P retains the known projection-reading warning.

### Validation, artifacts and delivery

- Current local fork imported before pytest, with `RYAN_TEST_DEFERRED_PYTUFLOW=1`:
  `tests/orchestrators/tuflow/test_input_preflight.py` and
  `tests/functions/tuflow/test_gis_attribute_dependencies.py`: **35 passed, zero skipped**.
- Exact PR head `a60ba0c`: six hosted checks completed successfully, verified with `gh pr view`:
  fork-consumer-smoke, Ruff and whitespace, Policy, Python 3.13 compatibility and MCP, Hosted tests,
  Package. [Python CI run](https://github.com/Chain-Frost/ryan-tools/actions/runs/39626059450),
  [fork smoke run](https://github.com/Chain-Frost/ryan-tools/actions/runs/39626059430).
- Reproduction scripts in the existing temporary validation directory: `updated_parity.py`,
  `updated_preflight.py`, `updated_benchmark.py`. Evidence: `updated-local/parity.json`,
  per-project difference JSON files, `updated-local/benchmark-single.json`,
  `updated-local-preflight-summary.json` and issue CSVs. All private paths remain outside the repository.
- Only this record/register changed, left uncommitted. No package rebuild, global install, push,
  PR comment or merge performed. Installed-fork wheel smoke, the other model/GIS attribute gates,
  and broad eager-feature parity remain open; successful P/B validation does not authorise merge.

## Tutorial-set eager/deferred check (2026-10-21)

Checked the existing `tests/test_data/tuflow/tutorials` checkout, fixture commit
`b604b20ea62a61f1aa8e4837dc7e4ebd155430a4`, without changing its files or Git state.
Consumer `a60ba0c`, PyTUFLOW source checkout `65f24fa`, Windows Python 3.14.6.
The 11 `_run_Mxx_HPC.bat` files specify **21 simulation TCFs and 33 exact run combinations**.
Most TCFs use an empty run context; Module 8 supplies nine scenario combinations and Module 9
supplies six event combinations. Batch commands were read, never executed. The additional
`Create_Empties.tcf` is setup-only and was explicitly excluded from simulation parity.

**Fixture completeness blocks acceptance:** all 11 module folders lack their referenced `model`
directories, and required BC databases are also absent. The fixture contains run controls/results,
not a complete runnable tutorial input set. Both eager and deferred inventories find missing inputs
in every one of the 33 proposed runs. Deferred full preflight fails all runs with **306 findings:
273 missing dependencies and 33 unresolved active BC databases**. No simulation was launched.

The eager collector produces zero collection exceptions but its independent file/layer inventory
still fails every case. Its lack of collection exceptions is not a pass. Raw partial dependency
sets differ across modes for all 21 TCFs; missing nested controls, spatial databases and BC tables
prevent materialising their actual dependencies, so neither real input parity nor a deferred
regression can be established from this incomplete fixture. No performance claim is made.

Temporary reproduction: `python "$env:TEMP/ryan-preflight-20261021/tutorial_check.py"`.
The diagnostic harness exits zero after recording all cases; acceptance is carried by the JSON/CSV
results, which are failures. Evidence: `tutorial-check/results.json`, per-TCF difference JSON and
preflight CSV files, plus `tutorial-check.log` in the existing temporary directory. The harness
checks physical file/layer identities in both modes and runs the public deferred preflight.

Next action: populate a complete tutorial input set containing the referenced model controls,
GIS/DEM layers, material files, BC databases and source data, then repeat the same 33 cases.
No automatic download, submodule update, fixture repair, source change or package build was performed.
Only this record/register were updated and remain uncommitted. P/B's previously verified parity
remains separate; this tutorial attempt does not close the broader feature-parity gate.

## Complete example-model dataset sweep (2026-10-21)

Tested `E:\temp\TUFLOW_Example_Model_Dataset\TUFLOW`, which includes model/GIS and BC inputs,
using consumer `a60ba0c` and local PyTUFLOW `65f24fa`, Windows Python 3.14.6. Read all example
batch files, ignored comments/SET lines, deduplicated their TCF/flag combinations and included every
remaining unparameterised TCF with an empty context. No batch file or TUFLOW executable was run.
Coverage: **161 TCFs, 174 distinct runs**; parameters are supplied only where defined in the batches.
The SHA-256 over sorted relative names and raw bytes of the 183 TCF/batch files is
`c48d1783cf94dfa16abdc71a17f13f71f440a9b257ee26470c37edf65d0103e8` (NUL separators).

**107 TCFs / 119 runs pass** public deferred preflight, both independent physical file/layer
inventories, and exact eager/deferred run/flags/command/path/layer dependency comparison.
**54 TCFs / 55 runs remain unresolved**. No case was accepted merely because two partial sets
matched. Attribute-bearing cases are explicitly excluded from accepted parity until their secondary
dependencies are completely materialised; no passing case requested an attribute scan.

The [per-TCF results matrix](2026-10-21-example-model-parity.csv) records all 161 cases, run counts,
collection/inventory issues, comparison counts and full public preflight reasons. A blank comparison
means parity was deliberately not attempted past an unsupported attribute source, not zero differences.
The 134 public deferred findings break down as follows (overlapping TCFs):

| Limitation | Finding count | Detail |
| --- | --- | --- |
| Attribute-linked sources outside GeoPackage | 112 | All observed sources are Shapefiles; the current shared attribute scanner supports GeoPackage only. |
| Unsupported deferred database-linked command | 18 | 12 `Read GIS Table Links`, two `Read Grid RF`, four `AD BC Database`. |
| Multiple BC database inputs | 4 | EG17_001/002/003 and EG18_002: the resolver reports ambiguous BC inputs rather than choosing a database. |

The eager baseline has its own failures and is not universal acceptance evidence:

- EG03_011 fails context construction with `'NoneType' object has no attribute 'resolve'`.
- EG03_012 fails context construction with `Length of indexer and values mismatch`.
- EG11_006 and EG15_007/008 expose the bare network/pit identifier `aa` as a missing filesystem
  path; do not conclude those are genuine missing project input files without identifier lookup.
- EG17_001/002/003 and EG18_002 also report that the active BC database cannot be resolved.
- The other 152 TCFs have no eager collection or file/layer inventory finding. That alone does not
  make their unsupported deferred counterparts pass.

Next implementation fronts are Shapefile attribute-linked discovery with the same row/geometry and
identifier semantics as GeoPackage, deferred table-link/rainfall-grid resolution, and distinguishing
AD BC databases from hydraulic BC databases. Add supported resolvers and parity regressions rather
than suppressing diagnostics. Investigate the two eager rainfall-grid exceptions independently.
This dataset adds broad coverage but does not close the overall eager-feature parity gate.

Temporary reproduction: `python "$env:TEMP/ryan-preflight-20261021/example_check.py"`.
Evidence under the existing temporary directory: `example-check/results.json`, `summary.json`,
per-TCF difference JSON and issue CSV files, `example-check.log`; diagnostic script
`example_eager_diagnostics.py` confirms the three `aa` path reports. The harness exits zero after
recording all cases, while individual acceptance remains explicit in the matrix. No timing claim
is made for this correctness sweep. Input files were only read; neither dataset nor submodules were
changed. Status/register and the new results matrix remain uncommitted, with no source/package,
push or merge changes. Documentation and whitespace checks pass.

## Temporary branch-only example input bundle (2026-10-22)

At the user's explicit request, packaged a self-contained reproduction set in
[`tests/fixtures/tuflow_example_repro`](../../tests/fixtures/tuflow_example_repro/README.md),
outside the independently maintained test-data submodule. It includes only the user-supplied
TUFLOW example dataset, with no confidential project inputs. Archive: **1,464 files, 19.2 MiB
uncompressed / 14.4 MiB compressed**. The archive preserves original source bytes for run controls,
GIS, grids, BC and pit databases, plus the one restart file required as input by EG06_004.
General results/logs, generated `xf` caches, executables, spreadsheets and copied project scripts
are excluded. Real rasters are retained because empty text files would invalidate metadata parsing.
The archive and each member have SHA-256 checksums; candidate input bytes were checked for the
confidential identifiers before packaging. The manifest records source provenance without claiming
independent verification of an upstream dataset release.

The bundle contains a per-case baseline, manifest, README and read-only module runner. Run from
the repository root:

```powershell
python -m tests.fixtures.tuflow_example_repro.run_checks --verify-only
python -m tests.fixtures.tuflow_example_repro.run_checks --pytuflow-checkout E:\Github\PyTuflow --output "$env:TEMP\tuflow-example-reports"
```

Use repeated `--tcf` arguments to select specific examples. Extraction uses a fresh temporary
directory and enforces physical containment; reports are retained separately. No batch command
or TUFLOW executable is ever run. The complete baseline intentionally returns exit one because
unsupported cases remain unresolved. This diagnostic runner uses the consumer's private collector
and is not a new supported library API or MCP workflow.

Validation: archive/member integrity, module help and compilation, Ruff lint/format, strict Pyright
on the runner, documentation links/index and whitespace checks pass. Full extracted-data sweep
matches the original matrix exactly: 161 TCFs / 174 runs, 107 TCFs / 119 runs accepted and
54 TCFs / 55 runs unresolved, with zero per-case acceptance differences. Exit one is expected for
the unresolved baseline, rather than an extraction or harness failure. No package
build is needed because package source/metadata are unchanged. The source dataset and test-data
submodule are unmodified. This fixture commit also retains the preceding local validation notes
and complete example matrix from the same work front.

**Removal requirement:** delete the entire temporary fixture directory and its central documentation
index row before final PR merge; retain appropriate synthetic regressions and durable validation
notes. This is a temporary upload to `feat/tuflow-run-preflight`, not merge authorisation. The user
explicitly authorises committing and pushing this reproduction bundle on that existing branch.


## Shared Shapefile and GeoPackage attribute discovery (2026-10-22)

Agent 1's changes in PR #143 now route both Shapefile DBF and GeoPackage
SQLite attributes through the **same TUFLOW element classifier**. Supported
element families are `1d_nwk`, `1d_pit`, `1d_xs`, `1d_tab`, `1d_xz`,
`1d_bg`, `1d_lc`, `1d_cs`, `1d_hw`, and `1d_na`. The shared classifier
handles M/QO matrix CSVs, Q/P and pit curve identifiers, network Ignore=Y/T,
blank required fields and unsupported schemas consistently across containers.
Shapefile inspection reads only DBF records via pyshp and SHP header geometry
type, not per-feature geometries. GeoPackages continue to use read-only SQLite.
Unique files are scanned only once across run contexts, and missing linked
files are routed back to every affected simulation. The console read count
now says **attribute GIS-source opens**, not GeoPackage-only connections.
A narrow local pyshp typing surface preserves strict Pyright checks.

The temporary fixture's **EG02_014** was checked on the Windows pinned-fork
CI consumer (run 39637338154): full eager/deferred materialised parity and
read-only preflight passed, 31 unique input files and 0 issues. Previously
its three Shapefile attribute references were unsupported. **EG04_004**
now inspects its Shapefile attributes but remains UNRESOLVED because
`Read GIS Table Links` has no complete upstream deferred resolver.
The temporary harness now reuses the production GIS-attribute materialiser,
retains original owning-command provenance for derived references and
tolerates the Windows GDAL temporary-file lock during archive cleanup.
Its full 161-TCF/174-run read-only replay is provisionally enabled in
the pinned-fork Windows workflow, with per-model diagnostic JSON/CSV as an
artifact. This step and the entire example bundle must be removed before merge.

**Remaining gates:** verify exact-head CI (including strict Pyright and
new Shapefile tests), analyse the new full example sweep against the original
119 accepted runs, correct any remaining reader/schema defects, and
coordinate table-link/rainfall-grid/AD BC parser work with Agent 2 on
PyTUFLOW PR #1. A matching partial inventory is never an accepted pass.
Do not merge, publish or launch TUFLOW simulations.


## Verified Shapefile/GeoPackage full example sweep (2026-10-22)

The read-only Windows/Python 3.13 CI replay on [run 39638190458](https://github.com/Chain-Frost/ryan-tools/actions/runs/39638190458)
used the pinned deferred fork, verified archive checksums and shared
TUFLOW element-type GIS attribute classifier. Complete per-case
results.json and 161 per-TCF issue CSVs are attached as GitHub Actions
artifact tuflow-example-full-replay (artifact ID 12363572383).

**Full matrix: 161 TCFs, 174 exact batch-defined runs.**

- **140 TCFs / 153 runs accepted**, up from 107 TCFs / 119 runs:
  **33 additional TCFs and 34 runs** now meet fully materialised
  eager/deferred parity and read-only preflight.
- **21 TCFs / 21 runs unresolved**, down from 54 TCFs / 55 runs.
- None of the previous 112 unsupported-Shapefile-format findings remain.
  Both Shapefile DBF and GeoPackage SQLite use identical recognised TUFLOW
  element/field logic, including matrix CSVs, network/pit curve identifiers,
  and network Ignore=Y/T handling.
- **17 remaining TCFs** have 18 database-linked findings (12 table links,
  two rainfall grids, four AD BC databases); four overlapping cases
  additionally report ambiguous active BC inputs.
- Four further cases fail strict eager/deferred provenance comparison
  without a deferred preflight finding: EG11_006 (1 missing, 1 extra),
  EG11_009 (0 missing, 1 extra), EG15_007 and EG15_008
  (1 missing, 1 extra each). Eager's erroneous interpretation of the
  curve identifier aa as a filename contributes to three cases;
  do not create a fictitious file to satisfy parity.

The real EG02_014 case now passes eager/deferred identity comparison and
public read-only preflight: 31 unique inputs, zero issues. EG04_004 now
scans its Shapefile attributes but remains unresolved for upstream
Read GIS Table Links. No TUFLOW simulation was launched.

The temporary diagnostic harness now materialises secondary GIS inputs
through the same production code, including owning-command origin. The
Windows pinned-fork consumer focused tests passed 41 cases on run
39637338154. The full results in the attached Actions artifact are the
authority for currently accepted and unresolved examples.

**Ownership and next actions:** Agent 1 owns ryan-tools format-shared
GIS attribute inspection, deduplicated inventory and fixture harness.
Agent 2 owns PyTUFLOW PR #1's table-link, rainfall-grid and AD BC
classification/handling, plus erroneous eager identifier resolution.
See the upstream coordination comment 6321743535.

Before PR #143 merges, restore parity in the remaining supported-input
cases and repeat real/CI regression checks. Then delete the entire
branch-only tests/fixtures/tuflow_example_repro directory, related
workflow replay steps and its central docs-index row. Keep durable
synthetic regressions and this anonymised validation record.
No merge, default-on preflight, PyPI release or TUFLOW engine launch is authorised.


## Dataset transferred to dedicated test-data repository (2026-10-22)

The earlier **branch-only archive location is superseded** by the user's
instruction to store reusable test inputs in
[ryan-tools-test-data PR #1](https://github.com/Chain-Frost/ryan-tools-test-data/pull/1).
The ZIP, SHA-256 member manifest, and original 161-case baseline now live
at `tests/test_data/tuflow/example_input_preflight/` through the already
supported test-data submodule. The ryan-tools PR #143 submodule gitlink was
updated to test-data commit `3589290cd41974e240b555f8e3ceb2a51aec2951`
and the three duplicates were deleted from the ryan-tools branch.
The Python read-only diagnostic runner remains temporarily at
`tests/fixtures/tuflow_example_repro/run_checks.py`, but now reads
the bundle from the shared test-data directory.

The transferred archive Git blob is byte-identical to the source
(`b29f38ff4d6179ca3e2dbba92b807a44427ffe5d`); its SHA-256
is `e374c8e072679fc6c4dc11e63b270caa1101b6aebb84d70060872cdd1b0aa7d6`.
[Test-data transfer CI](https://github.com/Chain-Frost/ryan-tools-test-data/actions/runs/39656816536)
recomputed **all 1,464 member checksums** and measured **0.198 s
one-time extraction on a hosted Linux SSD** (this is not a Windows or
network-drive benchmark). ZIP size is 15,137,122 bytes; extracted input
bytes are 20,134,068. There is no runtime decompression per run;
the harness extracts once before contexts are constructed. Keep the
archive zipped in Git to avoid 1,464 checkout metadata/file operations.

The test-data PR must be merged to `main` before or alongside ryan-tools
PR #143, and the ryan-tools submodule gitlink must then be updated to
the resulting permanent test-data commit (especially if PR #1 is squash-merged
and its original SHA is no longer reachable). The short-lived diagnostic
runner/CI sweep may still be removed before ryan-tools merge, but **do
not delete the permanent archive in ryan-tools-test-data**.
