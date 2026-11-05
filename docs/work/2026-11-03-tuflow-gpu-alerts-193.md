# TUFLOW GPU availability alerts and CodeMeter validation

| Field | Value |
| --- | --- |
| Status | Needs review; licensed hardware evidence complete within recorded scope; hosted validation tracked in PR |
| Owner | ChatGPT |
| Created | 2026-11-03 |
| Updated | 2026-11-05 |
| Next review | 2026-11-06 |
| Baseline | PR #193; scheduler tested at `b29f8c2b`, integrated remote `efeff949`; isolated `ryan-tools-pr193-two-gpu` worktree |

## Outcome and scope

Continue PR #193 with transition-based terminal GPU errors, a persistent reusable dashboard alert, synthetic scheduler
coverage, a non-destructive polling benchmark and a licensed two-GPU validation procedure. Record actual CodeMeter and
hardware observations separately from synthetic evidence. Do not merge or publish the package.

## Current state

The 2026-11-05 startup follow-up is on a new branch/PR,
`fix/codemeter-first-gpu-inspection`, based on current main after PR #205 merged.
It supersedes unconditional process enumeration: check the CodeMeter summary first;
zero or unverified usage skips enumeration, and unverified usage warns and continues.
Only confirmed active usage triggers inspection. Earlier hardware evidence below
does not validate this shortcut or licence-free/tutorial runs.

User clarification on 2026-11-04 supersedes conservative unknown-state blocking: helper failure or
uncertainty warns and continues. Only confirmed GPU conflicts wait. CUDA namespace checking remains
in place so unknown assignments are never misrepresented as known physical matches.

Licensed two-GPU execution on a second workstation is now recorded in
[the validation report](../TUFLOW_GPU_CODEMETER_VALIDATION.md#licensed-two-gpu-observations-on-2026-11-0304).
RTX 3090/3080 hardware, two available paid GPU licences, reversed TUFLOW/nvidia-smi numbering, concurrent runs,
external occupancy, real 60-second recovery, grouped placement, unknown assignment and cancellation were exercised.
CodeMeter endpoint failure was tested without stopping the licence service. All test solvers exited and allocations
returned to zero. The one-GPU observations below remain historical evidence from the earlier machine.

A strict benchmark JSON annotation was corrected without changing its output. Incoming remote history/report changes
through `efeff949` were fast-forwarded and preserved. Local Ruff remains blocked by Windows; hosted checks are required.
Native keyboard interrupt delivery, a real licence-service restart and interactive terminal visual inspection remain
explicit scope limits, rather than claimed results.

## Next action

Verify the CI-repair commit's hosted checks, then review the hardware evidence and retained scope limits.
Do not merge or publish without separate authorization.

## Completion criteria

- A configured unavailable GPU produces one transition `ERROR`, a persistent red dashboard alert and a recovery message.
- Other groups continue; uncertain inspection warns without stalling computation. Simulation outcome/Resume is unchanged.
- Synthetic tests cover alert state, scheduler transitions, interruption and disabled/non-execution paths.
- The benchmark and licensed two-GPU procedure are reproducible, with measured local limitations clearly separated.
- Required source, documentation, package and hosted checks are recorded; implementation is committed and pushed to PR #193.

## Validation and delivery

- 2026-11-03: focused dashboard, CodeMeter, GPU occupancy and runner suite: 101 passed, 2 skipped, 5 deselected. The five
  hardlink-alias cases were deselected after repeated host `OSError: [Errno 9] Bad file descriptor` reads of copied
  `run_tuflow_simulations.py` files. The wrapper suite similarly reached 70 setup errors before repository code because
  endpoint security denied the copied wrapper read; one non-fixture test passed. Hosted CI remains the wrapper gate.
- 2026-11-03: compileall, benchmark smoke, Loguru formatting policy, documentation/index validation and `git diff
  --check` passed.
- 2026-11-03: five local and five CodeMeter benchmark samples plus one unavailable-CodeMeter observation recorded.
- Ruff and Pyright executables: blocked by Windows `PermissionError [WinError 5]`; hosted Ruff identified one benchmark
  type-union ordering issue, which was corrected in `b29f8c2b`; exact-head hosted checks rerun after subsequent pushes.
- No-bump package build reached the QGIS style inventory and stopped because the isolated worktree lacked a materialized
  `qgis-resources` checkout. The optional resource clone was stopped and package verification dropped at user request.
- MCP exposure assessed: no catalogue change; this is an existing wrapper behavior change plus a maintainer benchmark.
- Implementation and prompt committed as `e0863cc4`; handoff record as `f3c8bf40`; benchmark lint correction as
  `b29f8c2b`. All were pushed to the existing PR #193 branch. No merge or publication.
- PR description updated with the two-GPU handoff and validation evidence. Exact-head hosted checks remain the delivery
  gate after the final report-order push.

## Progress

### 2026-11-03

Added structured GPU availability states, transition logging and a generic dashboard alert interface. Captured five
local and five CodeMeter measurements on the one-GPU workstation, plus CodeMeter-unavailable behavior. CodeMeter had
no physical GPU identifier; a separate TLF correlation reported GPU 0 for the shared PID. Two-GPU licensed scenarios
remain outstanding and are documented without fabricated results. Added a standalone copy-ready prompt for the future
two-GPU/two-licence agent session and prepared it for delivery in PR #193.

Committed and pushed the implementation, benchmark, evidence and future hardware prompt as `e0863cc4`. The partial
isolated `qgis-resources` clone remains outside the commit and has no parent-repository delivery effect.

Reordered the user-activity report so inferred log/TLF groups appear first, active CodeMeter processes follow, and the
authoritative current CodeMeter licence-utilisation table is the final output group. Focused activity-report validation
passed with 17 tests; compileall, documentation/index validation, Loguru policy and `git diff --check` also passed.

Changed runner execution-history `recorded_at` values from forced UTC to the local computer timezone with an explicit
UTC offset. Existing timezone-aware UTC rows remain readable. The focused execution-history suite passed with 22 tests;
compileall, documentation/index validation, Loguru policy and `git diff --check` also passed.

Reordered new runner history CSVs for operators: timestamp and event/result fields first, followed by model/process
context, with internal hashes and schema metadata last. Existing internal-first CSVs retain their header order and are
still readable and appendable. The focused history suite passed with 24 tests. Four copied-wrapper integration cases
were blocked during fixture import by the host's existing `OSError: [Errno 9] Bad file descriptor`, before repository
code executed.

### 2026-11-04: licensed two-GPU execution

- Hardware results and sanitized timing tables are in the validation report. Raw evidence is retained outside Git at
  `C:/Temp/ryan-tools-pr193-two-gpu-20261103`; no tutorial models, TLFs, operational identifiers or licence secrets are committed.
- `pytest --noconftest` focused GPU/CodeMeter/dashboard/runner tests: 101 passed, 2 symlink skips. Focused wrapper tests:
  71 passed. Incoming execution-history tests: 24 passed. The fixture-independent suites bypassed only the repository
  startup requirement for unrelated test-data materialization; no tests were deselected.
- Strict Pyright passed on all six selected production/benchmark files using explicit source-checkout extraPaths and
  explicit file arguments; this avoids resolving wrappers against the older globally installed package.
- Benchmark CLI smoke and Loguru formatting policy passed. Documentation/index, explicit changed-document links and whitespace checks passed.
- No-bump wheel build/verification and installed-wheel smoke passed on the hardware baseline. The final integrated
  wheel also built and verified: SHA-256 `5780af0f4d1bd7827936730239355c4875d6ffc5b82dfc027fec4653e54e1e19`. The normal Python installation is unchanged. The optional fully isolated
  dependency smoke could not run all wrapper behaviors because its dependency target lacked pandas; normal installed
  smoke passed with the existing dependencies.
- Submodule initialization in the new worktree exposed incorrect relative core.worktree paths. Only those new
  submodule configurations were corrected; their checkout used Git LFS skip-smudge. No existing submodule worktree was
  changed, and the excluded submodule was not inspected.
- Delivery: this record accompanies the source/documentation commit pushed to the existing PR #193 branch. The PR
  records the final commit SHA and exact-head hosted results. Only the four task files are staged/committed; generated
  wheel artifacts remain local and isolated submodule checkout state is excluded.
  No merge, publication or production-model mutation. Next review: 2026-11-05.

Hosted Ruff on `7c2e003f` found a missing blank line between standard-library and third-party benchmark imports.
Applied the reported import-spacing fix; this changes no behavior or packaged code. Exact-head checks rerun on the
follow-up commit, with the PR description retaining the authoritative delivery SHA and results.

Hosted lint passed on `006c491e`; formatting then identified four line-wrap changes across the scheduler and two
test files. Applied the exact formatter output; ASTs are unchanged. The affected runner/activity suites passed
101 tests with 2 symlink skips, strict Pyright passed on all three files, and the no-bump wheel rebuilt/verified
(SHA-256 `1d9864663d0b14c54a1a7ab952c2b2f156b16d9464b10191ee8965645d2f3243`). The preceding installed-wheel smoke remains behavioral evidence
for the identical ASTs. No repeat solver execution is needed for these formatting-only changes.

Final-head Ruff/whitespace passed at `b7bb9814`; Package, MCP smoke and POSIX security also passed. Hosted strict
Pyright then exposed intentional private-state access in the dashboard unit tests and overly narrow CSV key
inference in the legacy-history fixture. Documented the test-only private-access exception and annotated the
CSV writer with string keys. All 14 PR-changed Python files now pass local strict Pyright; 31 affected tests pass.
These test-only annotations do not require another wheel or solver run. Hosted checks rerun on the follow-up.

### 2026-11-04: GPU names and explicit device numbering

User requested GPU model names and an unambiguous `-puN` mapping in the runner and activity wrapper.
A shared read-only helper enumerates CUDA names once per executing batch; startup, launch messages,
dashboard rows and configured-slot alerts show names, with PCI identifiers for duplicate GPU models.
The map belongs to the launch environment, not external processes with potentially different CUDA settings.
Activity rows use their own companion HPC header and preserve the simulation machine; no current-machine
or NVIDIA-index guesses are applied to historic/remote logs. Unknown/stale/conflicting names stay explicit.
Companion freshness uses the recorded simulation start, retaining valid headers on long runs; without a
start timestamp, it requires modification times within two minutes.

Validation: 111 focused GPU/runner/activity tests passed, 2 Windows symlink skips; strict Pyright passed on
all seven changed Python files. Read-only hardware smoke matched `-pu0` RTX 3090 / `-pu1` RTX 3080 and
confirmed that a fresh process with CUDA_VISIBLE_DEVICES=1,0 reverses the labels. Both retained licensed
numbering logs produced the expected names. No solver was launched for this presentation change.
Loguru policy passed. Local Ruff remains blocked by WinError 5; hosted lint/format is the final gate.
MCP assessment: existing runner/activity workflows already expose this behavior; no extra tool is warranted.
Delivery: this follow-up is being committed/pushed to existing PR #193; exact-head CI and package evidence
are recorded in its description. Keep the PR open and unmerged; no publication. Next review 2026-11-05.

The no-bump wheel build and isolated-target installed GPU-name/log smoke passed. Hosted Ruff requested
an equivalent typed dictionary factory instead of a lambda; strict typing still passes. Companion freshness
now uses the existing parsed simulation start to retain names during long runs, with a regression case
that rejects an HPC log from a prior run. Final validation and delivery SHA are kept in the PR description.

Hosted lint passed after the default-factory correction; applied its three exact formatting edits.

### 2026-11-04: review correction for external CUDA numbering

Addressed review thread PRRT_kwDONDA8NM6zO3gf: external -pu ordinals are no longer compared until
CUDA_VISIBLE_DEVICES and CUDA_DEVICE_ORDER exactly match the runner environment. Different or unreadable
settings produce an unknown assignment and block all explicit slots; unrelated processes and owned
children are excluded before environment inspection. No environment contents are logged or persisted.
Equivalent-but-differently-spelled selectors are conservatively unknown rather than translated.

Validation: 100 focused occupancy/runner tests passed, two Windows symlink skips; strict Pyright passed
on all four changed Python files. Regression cases cover reversed integer/UUID lists, device-order changes,
missing versus empty selection, denied environment access, exited processes and scheduler integration.
An actual read-only Windows process-environment probe accepted a matching child and rejected a changed
child; both temporary Python processes were reaped. No solver execution or licence allocation was needed.
Package/check evidence and the final pushed SHA are recorded in PR #193. The existing MCP workflow needs
no new tool for this scheduling correction. Keep the PR open and unmerged; review again 2026-11-05.

Hosted Ruff requested instance-owned process-info dictionaries in three new test fixtures; applied the
fixture-only correction. The verified wheel and installed smoke are unaffected; exact-head CI reruns.

### 2026-11-04: prioritize computation over optional inspection

User explicitly prefers a possible computation failure over a batch stalled by a helper. Unknown GPU
assignments, inaccessible/different CUDA environments, incomplete inspection and query exceptions now
produce transition warnings and a yellow continuing alert, without blocking queued work. Confirmed
conflicts still block their own slots even in mixed known/unknown snapshots. GPU-name helper exceptions
also warn and continue. Dashboard capacity says eligible rather than claiming uncertain GPUs are free.

Validation: 105 focused runner/occupancy tests passed, 2 Windows symlink skips; strict Pyright passed
on the two changed Python files. Added actual launch-path regression cases for unknown, incomplete,
query failure, name-helper failure and mixed confirmed/unknown assignments. Hardware procedure and
wrapper docs updated; earlier paid tests remain historical, not claimed as validation of the new policy.
Build and current-head hosted evidence are recorded in PR #193. Existing MCP workflow unchanged.
Delivery: commit/push to the existing PR, no merge/publication. Next review: 2026-11-05.

### 2026-11-04: repair advisory test CI

Exact head `e936b30` failed only Hosted tests (1 failed, 2,012 passed, 14 skipped, 8 deselected).
The one-time startup advisory test inherited enabled GPU waiting and entered the scheduler instead;
its zero-argument snapshot stub rejected the scheduler's `exclude_pids` keyword. Explicitly disabling
waiting restores the intended advisory-only coverage, preserving all assertions and production behavior.

Python 3.14.6: `python -m pytest -q tests/scripts/test_tuflow_advisory_runner.py` passes all six tests.
Ruff 0.16.6 lint/format, strict Pyright on that file and `git diff --check` pass.
No library or package changes; no wheel rebuild required. Repair is being committed and pushed to
PR #193; final-head hosted checks remain pending. No solver execution, merge or publication.

### 2026-11-05: avoid slow unconditional startup process enumeration

An observed startup spent 49 seconds between CUDA-name reporting and the completed
local-process check. Read-only process probes reproduced slow enumeration. The runner
now requests only the CodeMeter monitoring summary, using IPv4 loopback to avoid
sequential localhost IPv6/IPv4 retries and a one-second request timeout. It does not
fetch product details or event history. A complete zero-usage summary skips process
inspection. Per the user's explicit clarification, unavailable, malformed, partial or
ambiguous usage also skips inspection and allows launches with a warning. Only active
usage triggers enumeration, with command lines read after executable-name filtering.
Every scheduling check obtains fresh usage; no idle result is cached across launches.

Validation: 199 focused tests passed, two Windows symlink tests skipped. Ruff lint and
format, strict Pyright on all six modified Python files, Loguru policy and documentation
links passed. Synthetic regressions cover zero/unknown usage in both guarded and
advisory launch paths, summary-only requests, malformed/ambiguous data and selective
command-line access. Existing CUDA namespace/conflict coverage remains passing.

Built and verified wheel `26.11.5.2` (SHA256
`9874efac363ae995e2a7f158adae68965f0eb2e11fcf501bd7056edf45af09a3`). An isolated
installation passed the four launch combinations and copied runner `--help`. A live
CodeMeter-unavailable probe returned in 1.021 seconds without enumerating processes.
The broad installed-wheel smoke stopped at missing pandas in its isolated environment;
it is not claimed as passed. No solver was launched or licence allocated. The normal
user-site installation remains unchanged. Existing MCP runner discovery is sufficient;
no new tool is needed for this internal scheduling correction.

The subsequent troubleshooting follow-up adds the WebAdmin URL and Windows Services
instructions to unverified-usage warnings in both guarded and advisory mode. The
exact Windows service name is `CmWebAdmin.exe` (display name `CmWebAdmin`); it is
separate from the licence runtime. Automatic startup is an optional user choice.
Multiline issue details are split into dashboard alert lines so height accounting
includes the added guidance. Zero usage does not print troubleshooting advice.
Validation: 161 focused tests passed / 2 Windows symlink skips, then all four affected
CodeMeter launch cases passed after the final wording edit. Ruff lint/format, strict
Pyright, Loguru policy, documentation links, a no-bump wheel build and an isolated
installed-wheel warning check passed. The user's existing local version edit to
26.11.5.3 is preserved and excluded from this follow-up commit; the verification
build uses that local version without changing metadata. No installed user package
or Windows service is changed. Delivery continues in PR #206.

Delivery: new branch/PR requested; changes are committed/pushed for review with hosted
CI tracked on the PR. No merge/publication is authorized. The separately reported live
dashboard repaint issue remains unresolved; passing logging tests do not reproduce the
user's terminal artifacts. Next review: 2026-11-06.

### 2026-11-05: enforce the user's one-second inspection budget

The user requested timing of the detailed path and set a maximum optional overhead
of one second. Three CodeMeter summaries took 0.191726–0.240229 seconds. The actual
detailed process scan exceeded a three-second probe cutoff; a separate one-second
probe also timed out. The previous name-filter optimization alone is therefore not
accepted as a fast launch path on this workstation.

The entire optional CodeMeter/GPU check now runs in one owned hidden Python worker
with a 0.9-second timeout, reserving 0.1 seconds for cancellation and reaping. This
covers imports/startup, HTTP and process inspection together. The summary request
uses a half-second socket timeout. Confirmed idle and failed usage still skip process
enumeration; only active usage enters it. Timeout/failure returns unverified status,
discards partial findings and continues launching. The worker is killed and reaped;
there is no late result or non-daemon thread holding shutdown open.

Three complete idle calls took 0.311939–0.330644 seconds. Three forced-active calls
using real process inspection timed out and were reaped in 0.939249–0.970314 seconds.
All three child PIDs were confirmed absent after return. The isolated installed
wheel repeated idle at 0.320773 seconds and detailed cancellation at 0.920239 seconds,
again confirming the worker exited. No solver was launched or licence allocated.
These observations do not guarantee latency on every OS/load or repeat licensed
multi-GPU acceptance of the deadline policy. Full timing samples and method are in
[the validation report](../TUFLOW_GPU_CODEMETER_VALIDATION.md#2026-11-05-bounded-timing-check).

Validation: 202 focused passes / 2 Windows symlink skips; Ruff lint/format, strict
Pyright on five modified Python files, Loguru policy, links and whitespace passed.
New regressions cover successful worker snapshots, timeout, launch failure, malformed
output, and continued guarded/advisory launches after timeout. Built and verified
the local no-bump 26.11.5.3 wheel and tested its actual subprocess boundary in an
isolated install. The user's version edit remains unstaged and excluded. Delivery
continues in PR #206; final-head hosted CI is recorded in the PR. No merge/release.
