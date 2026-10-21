# TUFLOW proposed-run input preflight

## Scope

Implement a read-only Python checker in ryan-tools for every event/scenario combination proposed by the maintained
TUFLOW simulation launcher. It must verify scenario- and event-dependent control-file inputs and event-specific
BC database Source files without starting TUFLOW. Provide a standalone wrapper and an opt-in integrated check.
Preserve existing GPU scheduling, run ordering and batch execution defaults.

## State

In validation. Owner: ChatGPT. Updated: 2026-10-21. Next review: exact-head CI and Windows engineering smoke checks.

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
The validation workflow is pinned to `3a3cfa898955b413abfec92709758c3f26e55c77`.
Python tests now cover per-run BC source differences, numeric-only material
databases, unavailable API behaviour, Windows-legal literal wildcard-like
folder names, and an opt-in real-fork end-to-end fixture.

**Open gates:** confirm exact-head CI and real-model BC/material parity, then
rerun the actual 440-run inventory and compare materialised file/layer
dependencies per run; complete GIS attribute-linked sources and unsupported
database schemas (fail closed until then). No TUFLOW executable launch, merge,
or PyPI dependency replacement is authorised.
