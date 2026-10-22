# TUFLOW proposed-run input preflight

## Scope

Implement a read-only Python checker in ryan-tools for every event/scenario combination proposed by the maintained
TUFLOW simulation launcher. It must verify scenario- and event-dependent control-file inputs and event-specific
BC database Source files without starting TUFLOW. Provide a standalone wrapper and an opt-in integrated check.
Preserve existing GPU scheduling, run ordering and batch execution defaults.

## State

In validation. Owner: ChatGPT. Updated: 2026-10-22. Next review: 2026-10-23; local eager secondary-source fixes accept all 161 TCFs / 174 runs. Changes span both repositories and remain uncommitted; commit upstream and update the isolated CI pin before hosted verification. Preserve permanent test data.

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
initially updated to test-data feature commit `3589290cd41974e240b555f8e3ceb2a51aec2951`;
after [test-data PR #1 was squash-merged](https://github.com/Chain-Frost/ryan-tools-test-data/pull/1),
it was repinned to permanent `main` commit `d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58`
in ryan-tools commit `d7e18809fdc10fa0d63fd166e5839a1c222b4b6d`.
The three duplicates were deleted from the ryan-tools branch.
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

**Test-data PR #1 is merged and the gitlink points to its final `main`
commit.** The short-lived diagnostic runner/CI sweep may still be removed
before ryan-tools merge, but **do not delete the permanent archive in
ryan-tools-test-data**. Verify the final ryan-tools CI uses the permanent
submodule commit before merging PR #143.


## Typed upstream source integration and Windows example sweep (22 October 2026)

Agent 1 integrated PyTUFLOW supporting PR #2 typed cached source API into
ryan-tools PR #143 on the opt-in Windows/Python 3.13 consumer workflow.
Production PyPI dependency stays unchanged. The pinned test revision was
PyTUFLOW `336efdea7f9c9d8ca27515284b042f48804f2bac`.

**Read-only CI [run 39663958589](https://github.com/Chain-Frost/ryan-tools/actions/runs/39663958589)** checked all 161 TCFs, 174 run combinations and their archive hashes
using the permanent `tests/test_data` submodule. Exact identity parity plus
public preflight accepted **151 TCFs / 164 runs**; **10 TCFs / 10 runs**
remained unresolved. This supersedes the previous
140 TCFs /153 accepted runs (21 unresolved). The Windows CI saved
`tuflow-example-full-replay` artifact `12375333756`, including complete
per-case `results.json` and issue CSVs. No TUFLOW executable ran.

The ten cases are:
- EG03_011: eager/deferred primary identities match, but Rasterio sees a
  valid NetCDF **container without bands**; a subsequent change checks
  GDAL subdataset metadata without reading pixels, pending exact-head CI.
- EG03_012: all **41 rainfall grid files** match physically; only origin
  `Read Grid RF` versus `rainfall source: time` differs. The downstream
  source's actual owning command now provides the origin; pending retest.
- EG11_006 and EG11_009: deferred-only **active Q/P depth-discharge
  curve Source files**; verify eager completeness, do not delete real sources.
- EG15_007 and EG15_008: deferred-only active pit-inlet `aa` curve Source;
  verify whether eager omits valid secondary file dependencies.
- EG17_001, EG17_002, EG17_003 and EG18_002: deferred-only AD BC Source
  records (and an AD Global GIS source in EG18_002); review whether eager
  omits legitimate AD dependencies. These are not missing file reports.

The first integration attempt produced false deferred extras by adding
**every** pit-inlet database Source row, including those not referenced by
any GIS feature. Agent 1 changed this to only materialise active GIS IDs
using the matching pit-inlet or depth-discharge Name/Source database,
retaining per-run missing/ambiguous errors. The targeted
EG02_014/EG04_004 Windows parity replay now passes (run 39663958589).
The shared raster stage opens each distinct file once (plus GDAL NetCDF
subdataset metadata if required) after run-context construction; invalid
headers are unresolved, not silently accepted.

A precise [upstream handoff](https://github.com/phantom-branch/PyTuflow/pull/2#issuecomment-6325186159)
provides all ten cases and reasons. The next gate is a complete Windows
174-run retest against latest downstream and reviewed upstream SHAs,
followed by native P/B, 42-run and Higginsville parity. Do not claim an
actual speedup or merge upstream/downstream until complete supported-input
parity and CI are established.


## Final checked Windows 174-run result after NetCDF and rainfall origin corrections

Latest checked [Windows full example workflow 39666504925](https://github.com/Chain-Frost/ryan-tools/actions/runs/39666504925)
with pinned supporting PyTUFLOW PR #2 commit
`336efdea7f9c9d8ca27515284b042f48804f2bac`:
**153 of 161 TCFs / 166 of 174 runs accepted**, eight remaining
one-run cases. This supersedes the prior 151/161 TCF and 164/174 run
result. All focused pinned-fork/SHAPE/GPKG and 174-run diagnostics,
strict Pyright, Ruff, hosted tests, Python 3.13 compatibility and
wheel build passed. The archived results are in Actions artifact
`tuflow-example-full-replay` ID `12376903658`.

The two new accepted TCFs demonstrate:
- EG03_011: NetCDF root container with no standalone raster bands is
  valid if GDAL lists subdatasets and a subdataset has valid header
  dimensions and bands. Shared metadata checks read the header only;
  they do not read arrays.
- EG03_012: all 41 secondary rainfall FLT paths already matched eager.
  Normalising the deferred source's owner to its actual
  `Read Grid RF` command fixes the extraneous row-named origins.

All eight remaining mismatches are **deferred-only** (zero eager-only):
EG11_006 and EG11_009 each have one Q/P depth-discharge curve CSV;
EG15_007/008 each have one active pit inlet curve CSV; EG17_001/002/003
each have four AD BC source entries; EG18_002 has two AD BC source
entries and one AD Global GIS source. Public preflight passes all eight,
but the strict eager/deferred identity oracle finds additions.
**Do not remove physically required files merely to satisfy eager**:
confirm upstream eager completeness, active GIS identifier/BC declaration
semantics and genuine source ownership against TUFLOW documentation.
The [precise upstream handoff](https://github.com/phantom-branch/PyTuflow/pull/2#issuecomment-6325405340)
tracks each case.

No results validate the native P (4), B (240), 42-run output-variable
or Higginsville (440) models on their current revisions. No speedup,
merger, published dependency upgrade or solver invocation is authorised.


## Consolidated upstream branch and Windows handoff (22 October 2026)

- PyTUFLOW supporting [PR #2](https://github.com/phantom-branch/PyTuflow/pull/2)
  is merged **into the PR #1 development branch**, not `master`; resulting
  merge SHA `4228a805687cfd80d98a27e2ebcb62c234b40059`.
  [PR #1](https://github.com/phantom-branch/PyTuflow/pull/1) remains **draft**.
  Linux is tracked separately in draft [PR #4](https://github.com/phantom-branch/PyTuflow/pull/4).
- The isolated consumer CI workflow is now pinned to that merged SHA
  instead of the old PR #2 commit; the **production `pytuflow` PyPI dependency
  and application defaults remain unchanged**.
- [Windows replay 39706593237](https://github.com/Chain-Frost/ryan-tools/actions/runs/39706593237)
  **passed** on the consolidated pin with full 1,464-file ZIP integrity
  verification, focused regression checks and 161 TCFs / 174 run
  combinations. **153 TCFs / 166 runs accepted, eight TCFs / eight
  runs unresolved**. Diagnostic artifact `tuflow-example-full-replay`
  ID `12395795444` contains the exact reports.
- Those eight cases remain **zero eager-only**, with legitimate deferred
  additions: EG11_006/009 Q/P depth-discharge curves, EG15_007/008
  active pit-inlet curve sources, EG17_001/002/003 AD BC and EG18_002
  AD BC/AD Global sources. Their ordinary input preflight passes; do not
  suppress or discard physically real sources to improve the comparator.
- Updated the pinned consumer CI to **Windows Python 3.14** (previously
  Windows Python 3.13) to match the target engineering environment.
  Its exact-head results are a separate verification gate.
- Remaining Windows work: repair genuine eager source omissions and
  activation-to-GIS-feature selection upstream; rerun complete example
  parity, then P/B, 42-run and 440-run real-model checks with deduplicated
  filesystem/GIS operation counts and same-SSD warm-cache measurements.
  No network benchmark or speedup claim is yet verified. The read-only
  example runner is still a temporary diagnostic, while the source input
  ZIP remains permanently in the test-data submodule.

No TUFLOW process launch, production merge, release or change to
published package pin was authorised in this consolidation.

## Fresh local Windows validation after fixture move (2026-10-22)

Rechecked consumer head `86646b2f` on normal Windows Python **3.14.6**, NumPy **2.5.3** and
pandas **3.0.5**, without installing/replacing packages or creating a virtual environment.
The supplied local PyTUFLOW checkout is `546ab20`; its `pytuflow/` package tree has no differences
from workflow-pinned `4228a805687cfd80d98a27e2ebcb62c234b40059`. Tests explicitly import that source
checkout rather than the normal published package. This is source-import evidence, not a fresh
installed-wheel/dependency installation check. Permanent test-data pin is
`d2dae3ecf9a7ed306e1340de54a8d12ca9f53f58` and its worktree is unchanged.

The moved bundle at `tests/test_data/tuflow/example_input_preflight/` verifies successfully:
**all 1,464 member checksums and the archive checksum pass**. The runner extracts once outside
the checkout. Complete local replay: **161 TCFs / 174 runs**; ordinary public input preflight
passes **all 161 TCFs / 174 runs**, while strict materialised identity parity accepts
**153 TCFs / 166 runs**. Expected diagnostic exit **1** records eight unresolved parity cases,
not a harness failure. This independently reproduces the previously recorded hosted Windows outcome.

| Remaining TCF | Eager-only records | Deferred-only records | Public preflight |
| --- | --- | --- | --- |
| EG11_006 | 0 | 1 depth-discharge curve | Pass |
| EG11_009 | 0 | 1 depth-discharge curve | Pass |
| EG15_007 | 0 | 1 pit-inlet curve | Pass |
| EG15_008 | 0 | 1 pit-inlet curve | Pass |
| EG17_001 | 0 | 4 AD BC source records | Pass |
| EG17_002 | 0 | 4 AD BC source records | Pass |
| EG17_003 | 0 | 4 AD BC source records | Pass |
| EG18_002 | 0 | 2 AD BC and 1 AD Global source records | Pass |

Total: **zero eager-only and 19 deferred-only identity records**. These are not missing file
findings; do not delete real sources to satisfy the eager comparator. Active-feature selection,
BC declaration semantics and eager completeness remain the investigation targets.

Local checks:

- Focused consumer, shared GIS and raster-metadata tests with
  `RYAN_TEST_DEFERRED_PYTUFLOW=1`: **47 passed, zero skips**, 1.23 seconds. PyTUFLOW was imported
  before pytest, using its explicit local checkout.
- Ruff lint and format: **pass** on the four preflight modules, diagnostic runner and three
  focused test files (eight files total).
- Strict Pyright: **zero errors/warnings** on the four modules and diagnostic runner.
- Documentation/default index, touched-document links and whitespace checks: **pass**.

Reproduction:

```powershell
python -m tests.fixtures.tuflow_example_repro.run_checks --verify-only
python -m tests.fixtures.tuflow_example_repro.run_checks --pytuflow-checkout E:\Github\PyTuflow --output "$env:TEMP\ryan-preflight-20261022\86646b2f-local\reports"
python -c "import sys,os; sys.path.insert(0,'E:/Github/PyTuflow'); import pytuflow; os.environ['RYAN_TEST_DEFERRED_PYTUFLOW']='1'; import pytest; raise SystemExit(pytest.main(['-q','-ra','tests/orchestrators/tuflow/test_input_preflight.py','tests/functions/tuflow/test_gis_attribute_dependencies.py','tests/functions/tuflow/test_raster_metadata_dependencies.py']))"
```

Local evidence is retained under
`C:\Users\Ryan\AppData\Local\Temp\ryan-preflight-20261022\86646b2f-local`:
`environment.json`, `full-sweep.log`, `summary.json`, `reports/results.json` and issue CSVs.
No solver execution, benchmark, unrelated full suite, package rebuild, hosted CI rerun or native
project rerun was performed. Only status/runner documentation was updated and left uncommitted;
the pre-existing `qgis-resources` modification was preserved. No commit, push or merge performed.

### Windows Python 3.14 consumer replay — passed

The isolated pinned-fork integration [Actions run 39707415377](https://github.com/Chain-Frost/ryan-tools/actions/runs/39707415377)
passed on **Windows Python 3.14.8** using merged upstream commit
`4228a805687cfd80d98a27e2ebcb62c234b40059`. It reported **47
focused tests passed** and the complete verified **161 TCF / 174-run**
read-only replay returned **153 TCFs / 166 runs accepted, eight unresolved**,
consistent with the earlier Windows Python 3.13 run. Complete diagnostics:
`tuflow-example-full-replay` artifact **12396020763**.

The first full `pip install -e ".[dev]"` attempt on hosted Windows Python
3.14 failed while building Fiona from source; its other failures were
consequential missing packages. The isolated consumer workflow now installs
the upstream fork's GIS dependencies, the project in editable
`--no-deps` mode and the focused test requirements. **This establishes
preflight integration parity only, not full normal `ryan-tools`
Python 3.14 installation compatibility.** The Fiona/wheel and complete
dependency installation gate remains outstanding and must not be reported
as passing. No production dependency metadata was modified.

## Local/remote documentation merge reconciliation (2026-10-22)

Resolved the interrupted merge of remote `c498fbd4` into local testing commit `edfe539c`.
Retained both the complete local Python 3.14.6 evidence and hosted Python 3.14.8/Fiona limitation
notes. Preserved the existing local commit, including its already-recorded resource gitlink;
no resource checkout or package source was changed by this resolution. Default documentation,
touched-document link and whitespace checks pass. This merge changes documentation only relative
to the local testing commit, so the 47-test/174-run evidence remains attached to its original
tested source head rather than being relabelled as a new test run. User authorises completing
and pushing this existing branch merge and posting the eight-case remediation handoff on PR #143.
Production PR merge and publication remain unauthorised.

## Eager secondary-source completeness implemented locally (2026-10-22)

Implemented the requested eight-case remediation without removing dependencies or relaxing the
identity comparator. The missing records came from consumer use of eager `.files` plus hydraulic
BC rows only; that surface does not include AD source projections or database-key curve joins.
The existing upstream cached-row parser already supplies the required schema and path semantics.

PyTUFLOW changes (working tree based on `546ab20`, branch `feat/deferred-file-resolution`):

- Added `TCFRunState.database_sources()` as an immutable typed source projection usable in eager
  and deferred contexts. Existing `deferred_database_sources()` keeps its mode guard and delegates
  to the common projection. No default-mode or `Input.files` contract was changed.
- Source classification uses a deferred syntax configuration even when called from an eager
  context. Tests prohibit file opens, stat/glob and SQLite operations during the accessor itself.
- Preserved actual nested command owners, line/definition metadata and event metadata in both
  modes, keeping AD Global-relative source paths and per-entry provenance intact.
- Added independent synthetic expected-source assertions, shared-CSV/distinct-entry ownership,
  nested ownership and inactive scenario exclusion for hydraulic and AD databases.

ryan-tools changes (working tree based on merge `d1f1f0a0`):

- Eager reference collection requests the typed source projection when available and preserves
  all AD BC/Global entry records, including multiple owners of the same physical CSV.
- Eager primary metadata invokes the existing shared attribute/curve materialiser, so only curve
  identifiers from active GIS features add Source files. Unused curve-table rows remain excluded.
- Older eager parsers without the typed API fail closed for declared AD/curve database roles.
- Added missing-secondary/event routing, shared-file entry provenance and compatibility regressions;
  expanded the unused-curve/deletion regression to exercise both collection modes. Updated the local
  type stub and canonical preflight guide. MCP discovery remains unchanged: this fixes the existing
  catalogue workflow and adds no separate user-facing command or tool.

The documented [AD input contract](https://docs.tuflow.com/classic-hpc/manual/2026.0/AD-DataInput-2.html)
supports preserving the boundary/constituent entry and resolving Source relative to the AD Global
Database; [Q/P channel documentation](https://docs.tuflow.com/classic-hpc/manual/2026.0/Special-Channels-2.html)
confirms that the curve identifier belongs to a depth-discharge database. The preflight retains its
existing conservative inspection of rows in active database declarations; it does not claim to emulate
engine time-series selection, and scenario-inactive declarations remain excluded. No model data changed.

### Validation

- All eight formerly mismatching cases now pass, with zero missing/extra identity records.
- Full permanent example archive: **161/161 TCFs, 174/174 runs accepted**, diagnostic exit **0**;
  zero eager-only, zero deferred-only, and public preflight passes every case. The comparator itself
  was not modified. This supersedes the local eight-case gap for these working trees.
- Downstream focused preflight/GIS/raster plus launcher integration: **72 passed, zero skips**
  (53 preflight/GIS/raster tests alone). Ruff lint/format and strict Pyright on modified Python pass.
- Upstream deferred database/reference and affected TMF context/database/GIS tests: **181 passed**;
  changed modules/tests compile and whitespace checks pass. Neither unrelated full suite was run.
- `python repo-scripts/build_library.py --no-bump --skip-pip` built and verified
  `dist/ryan_tools-26.10.21.2-py3-none-any.whl` without changing package version/metadata.
  SHA-256: `2eb75d9d531361406e5ab78f9481ee0d15245d9962e2635bff4176a6acef0142`.
- Installed that wheel without dependencies into a new temporary target, verified consumer imports
  physically originate there, and replayed all eight formerly failing examples against the updated
  upstream source: **8/8 pass**. This shares host dependencies and is not a full clean dependency
  install or proof that published PyTUFLOW has the new API.

Local evidence under `C:\Users\Ryan\AppData\Local\Temp\ryan-preflight-20261022\eager-secondary-fix`:
`summary.json` includes source/wheel hashes; `eight`, `full` and `wheel-eight` contain results/issue
CSVs with corresponding logs. Full reproduction uses the existing runner and
`--pytuflow-checkout E:\Github\PyTuflow`; focused upstream commands cover
`tests/test_deferred_database_parity.py`, `tests/test_deferred_file_references.py` and affected
`tests/tmf/test_context.py`, `test_db_context.py`, `test_cf_context.py`, `test_gis.py`,
`test_gis_attr_driver.py`. Downstream focused commands add
`tests/scripts/test_run_tuflow_simulations.py` to the previously recorded 47-test command.

### Delivery and remaining gates

Changes are **unstaged/uncommitted in both repositories**; no commit, push, PR message or merge
was performed for this implementation. No release or production dependency metadata was changed.
The isolated CI still pins upstream `4228a80`, which does not contain the new shared accessor:
commit/review the upstream implementation, update that immutable CI pin, and rerun exact-head
hosted checks before claiming hosted parity for the fix. No P/B, 42-run or 440-run native model
rerun or new speedup claim was made. The permanent test-data submodule is unchanged; the temporary
runner/CI replay remains pending cleanup before final merge. Full normal Windows installation/Fiona
compatibility is still a separate gate.

### Separate eager/deferred review record (2026-10-22)

At the user's request, eager changes have their own upstream review record:
`E:\Github\PyTuflow\eager_source_completeness_fixes.md`, linked from that repository's
README and deferred implementation notes. Keep this distinction in subsequent PR
handoffs, commits and release notes:

| Scope | Classification |
| --- | --- |
| Upstream eager command reconstruction retaining nested ownership and definition/event metadata | Eager behaviour correction; deferred mode already retained this metadata |
| Upstream `database_sources()` plus syntax-only cached-row classification from eager contexts | Additive shared API and eager accessor correctness; no `.files` redefinition or default-mode change |
| Upstream `deferred_database_sources()` delegating to the common projection | Deferred implementation refactor |
| ryan-tools eager AD identities, active curve joins and legacy capability diagnostics | Downstream eager dependency-collection fix; not an upstream eager GIS join |

The 19 original differences describe the tested consumer/checkouts. They do not establish
the same defect in every published upstream release. Local 181-upstream/72-downstream
test results and 161-TCF/174-run replay remain the implementation evidence recorded above;
this documentation separation does not constitute another test run. Source changes remain
unstaged/uncommitted and the hosted pin still needs updating after upstream review/commit.
Documentation validation passes for the two touched downstream records and all five local
Markdown links in the upstream review documents; both repositories pass `git diff --check`.

The upstream eager record now also includes the original reconstruction statement,
verified in local `origin/master` commit `cf908cc922ca8448ed6002834877b0b377329eae`,
and explicit before/after effects. It separates this existing eager metadata defect
from the newly added shared API, that API's syntax-only projection, the deferred
delegation refactor and the consumer's collection gap. It records that nested AD
ownership/path behaviour is tested, while not every copied metadata field has an
independent assertion. This source comparison is not a fresh upstream fetch or
published-version validation; no implementation or test-result claims changed.

## Higginsville 440-run current-head validation (2026-10-22)

Reran the read-only preflight against the live mapped-drive model at
`Q:\26\RP26132 HIGGINSVILLE SURFACE WATER - WESRES\TUFLOW_Hig`, using the exact 440 commands from
`run_Higginsville_04_commands.txt` and TCF template
`runs\Higginsville_v04_~s1~_~e1~_~e2~_~e3~_~s4~.tcf`. The consumer was PR #143 head
`b68f4ada63f506ae68bdf9e00a5eb39ed1b1d399`; PyTUFLOW was imported from a temporary checkout of the
workflow-pinned commit `fb103a16ba778dbb5bf1e7b144c68334b26bc821`. Python 3.14.4 ran with bytecode writes disabled,
and no TUFLOW executable was launched.

The preflight **passed all 440 runs** with **zero issues and zero affected runs**. It checked 62 unique
physical input paths using two directory scans and ten direct checks, opened one shared GIS attribute
source and three raster headers, and reported 1.27 s parse, 28.77 s context/reference collection,
0.01 s GIS attributes, 0.09 s raster metadata and 0.11 s inventory. The temporary issue CSV contains
only its header. Model files were not changed; the only repository changes from this validation are
this status update and the corresponding register update, both unstaged. Remaining native-model gates
are the P/B and separate 42-run checks; exact-head hosted validation and upstream delivery gates remain
as previously recorded. Next review remains 2026-10-23.

### Matched eager/deferred performance benchmark

On the same consumer/upstream revisions, Python 3.14.4 and mapped `Q:` model, a matched single-process
benchmark ran eager first and deferred second across the same 440 run requests. Both modes used the same
consumer dependency collector, wildcard expansion and GIS-attribute materialisation. Mapped `Q:` and
`\\bgersgtnas05\waterways` spellings were normalised to one physical identity for the parity comparison.
The machine and network filesystem caches were already warm from the immediately preceding preflight;
this is one warm-cache observation, not a multi-sample statistical benchmark.

| Mode | Parse | Dependency generation | Raw references | Materialised run/path/layer entries | Physical unique paths | Issues |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Eager | 0.486 s | 110.359 s | 8,800 | 8,360 | 62 | 0 |
| Deferred | 0.392 s | 29.650 s | 8,360 | 8,360 | 62 | 0 |

Deferred dependency generation was **3.72x faster**, saving **80.709 s (about 73%)** in this run. The
materialised identity sets had **zero eager-only and zero deferred-only entries**, so the timing comparison
did not trade away dependency coverage. Each mode opened the one shared GIS attribute database once.
The timed generation phase ends after wildcard and GIS-attribute materialisation; it excludes the final
deduplicated file inventory and raster-header validation already measured by the passing preflight above.
No TUFLOW executable was launched and no model file was written. Reproduction script and JSON output are
under `C:\Users\Ryan.Brook\AppData\Local\Temp\ryan-preflight-higginsville-20261022-103632` as
`benchmark_eager_deferred.py` and `higginsville-440-eager-deferred.json`.
