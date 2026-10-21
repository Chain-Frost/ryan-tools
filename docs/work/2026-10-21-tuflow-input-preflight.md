# TUFLOW proposed-run input preflight

## Scope

Implement a read-only Python checker in ryan-tools for every event/scenario combination proposed by the maintained
TUFLOW simulation launcher. It must verify scenario- and event-dependent control-file inputs and event-specific
BC database Source files without starting TUFLOW. Provide a standalone wrapper and an opt-in integrated check.
Preserve existing GPU scheduling, run ordering and batch execution defaults.

## State

In validation. Owner: ChatGPT. Updated: 2026-10-21. Next review: 2026-10-22; resolve the P/B database findings and repeat parity checks.

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
