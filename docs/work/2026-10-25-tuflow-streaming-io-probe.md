# TUFLOW streaming I/O overlap diagnostic — 25 October 2026

**Status:** Active; combined #39 integration passes locally, new-head hosted CI pending. Experimental only.
**Owner:** ChatGPT.
**Next review:** 1 November 2026, or after exact-head CI.
**Related producer:** [combined PyTUFLOW draft PR #39](https://github.com/phantom-branch/PyTuflow/pull/39); historical #28 is closed unmerged.
**Consumer scope:** independent draft PR in ryan-tools, based on main, not #158.

## Architecture decision

Existing PyTUFLOW model loading (TCF/TGC/TBC and referenced build-state
inputs), variable planning and graph compilation stay **sequential**. They
supply the stable parsed model and per-batch caches used by future runs.
The existing `collect_batch_dependencies()` returns its complete batch only
at the end. Merely putting ryan-tools' inventory in a different thread would
not overlap its I/O with earlier context construction.

PR #28 adds an **opt-in producer callback** after each complete immutable
per-run projection, before the next run starts. An opt-in ryan-tools consumer
can submit stable *literal, unambiguous* paths into one bounded I/O worker as
soon as the first run is completed. The callback is not used in normal calls.

## Consumer prototype

- `batch_preflight_adapter.BatchModelView(..., on_run=...)` forwards the new
  callback only when explicitly provided; legacy batch call untouched.
- `streaming_io_probe.probe_batch_io_overlap()` keeps original parser batching
  on the calling thread and uses **one** worker thread, a bounded outstanding
  future queue and stable path-spelling deduplication.
- Only ordinary fully literal references are eligible. Unresolved variables,
  wildcards, explicit GIS layers and references requiring attribute/database/
  raster/table-link inspection are excluded. Exclusions receive the existing
  authoritative sequential validation if the caller invokes ordinary preflight.
- The worker performs `Path.stat()` read-only, records sanitised aggregate
  counts and whether its first call began before the producer returned.
- The worker **does not** provide accepted file/inventory results, modify run
  references, or replace the final grouped filesystem/GeoPackage/raster/
  attribute inspection. This double-read pilot is deliberately not a speedup
  claim. Final preflight remains strictly unchanged.
- Worker exceptions propagate from the opt-in probe; source/consumer error
  paths do not become successful reports. The context manager drains/shuts
  down even on exceptions; no background worker is left running.
- No Windows network-drive auto-detection, run-level multiprocessing, solver,
  GPU handling, model mutation or native report transfer.

## Validation and acceptance

Synthetic tests cover actual worker overlap during later producer execution,
max outstanding queue size, path deduplication and ineligible selectors,
missing files, callback/worker failures, and unchanged legacy adapter calls.
No private SMB run has been launched. The prototype's workflow is a proof of
the scheduling boundary, **not evidence of elapsed-time improvement**.
Follow the repository's focused Ruff/strict Pyright, pytest and source
validation gates. Retain all pre-existing production paths and consumer
baseline commit `f1f7958b79f3ffe84ba8ac0b04658f27c311460a` unchanged.

Before any adoption into the public preflight or comparison harness:
1. prove independent full command/reference/activation/materialisation parity;
2. preserve directory-batched `os.scandir`, GeoPackage layer semantics,
   wildcard expansion and error details;
3. establish safe reuse or demonstrate a useful end-to-end gain despite
   speculative duplicate metadata reads;
4. compare local SSD and SMB conditions on authorised Windows with controlled
   sequencing; do not infer benefit from the presence of a mapped drive alone.

**Next action:** commit the local combined-producer workflow/tests separately,
then obtain push authorization and require all checks/review on that new head.
Resolve the two historical threads only with explicit GitHub-write authorization.

Do not merge, publish or launch a full native benchmark on this pilot alone.

## Cross-repository regression boundary

The independent `tuflow-streaming-io-probe.yml` Windows workflow pins producer
PR #28 commit `9329cc8de2acfd2db8af7a9a6b6dfd34450af46f`, installs it
without altering published dependencies, and exercises the actual callback
through `probe_batch_io_overlap` using a temporary synthetic TCF. Existing
ordered-batch and normal preflight CI retain their own historical parser pins;
this additional workflow does not replace the complete archive parity gates.

## Review corrections

The checked-in Windows smoke workflow now pins producer PR #28 at `9329cc8de2acfd2db8af7a9a6b6dfd34450af46f`, which moves callbacks for reused projections outside the parser exception handler. Only `tests/test_data` is initialised for this focused pytest gate. Speculative path filtering inspects the complete path after joining the reference and its base directory, so an inherited wildcard or unresolved token does not enter the worker. These corrections are pending full exact-head CI and refreshed review. No native measurement is authorised.

## 2026-10-25 — Policy repair and lifecycle regression checks

Current checkout and remote PR #161 head are `f97e7f69eba97c4041ce41c41ba768af0855d1d1`;
the initial parent worktree was clean. All nine hosted checks are terminal: eight pass,
including the pinned streaming Windows integration, and Policy fails with 11 strict
typing errors in the new test module. See [Python CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/40126940602)
and [streaming integration](https://github.com/Chain-Frost/ryan-tools/actions/runs/40126940632).
This evidence applies to the existing head, not the local changes below.

Local changes annotate the synthetic case collection and callback, and describe the
opt-in fork result through test-owned protocols rather than accessing a method absent
from the published `TCF` type. Added executable lifecycle checks prove that a producer
failure waits for worker completion while preserving the original producer exception,
and that a finished probe rejects submissions and repeated finalisation. Production
code, parser pins and dependency metadata are unchanged. Experimental diagnostics
remain unsuitable for MCP catalogue exposure or authoritative inventory reuse.

Validation on normal Windows Python **3.14.6**:

- `python -m ruff check tests/orchestrators/tuflow/test_streaming_io_probe.py`: passed.
- `python -m ruff format --check tests/orchestrators/tuflow/test_streaming_io_probe.py`: passed.
- `python -m pyright tests/orchestrators/tuflow/test_streaming_io_probe.py`: zero errors/warnings.
- `python -m pytest -q tests/orchestrators/tuflow/test_streaming_io_probe.py tests/orchestrators/tuflow/test_batch_preflight_adapter.py`:
  **14 passed, 2 skipped**. Both skips are explicitly opt-in fork integration tests;
  installed PyTUFLOW is the published **1.2.6**, not the pinned producer fork.
- `git diff --check`: passed before the handoff edits; repeat with documentation validation.

Delivery: test and handoff edits are **unstaged, uncommitted and unpushed** on
`test/tuflow-streaming-io-overlap`. No package build is required for test/documentation-only
changes. No fresh hosted run covers these local edits. Keep the PR draft; no merge,
publication, private model access or native measurement. Next review remains **2026-11-01**.


## 2026-10-25 - Exact-head status reconciliation, separate from runner delivery

The original checkout is clean at remote #161 head
`0cc67eaa6f1afe0858d9f7d65ad944e3a75a6132`. Its change from historical
`f97e7f6` includes the test typing/lifecycle correction and handoff; those edits
are no longer uncommitted/unpushed. Earlier failure and local-only statements
above remain historical evidence, superseded by this checkpoint.

All **nine jobs in five workflows** are terminal/success at the current head:

- [Python CI 40187717811](https://github.com/Chain-Frost/ryan-tools/actions/runs/40187717811):
  Policy, Hosted tests, Package, Python 3.13 compatibility and MCP.
- [Fast PR checks 40187717822](https://github.com/Chain-Frost/ryan-tools/actions/runs/40187717822):
  Ruff and whitespace.
- [Streaming integration 40187717857](https://github.com/Chain-Frost/ryan-tools/actions/runs/40187717857):
  windows-streaming-probe, with its own pinned producer integration.
- [Ordered contracts 40187717870](https://github.com/Chain-Frost/ryan-tools/actions/runs/40187717870):
  windows-ordered-batch.
- [Batch smoke 40187717781](https://github.com/Chain-Frost/ryan-tools/actions/runs/40187717781):
  established-api and graph-pr-12.

Live producer [PyTUFLOW #28](https://github.com/phantom-branch/PyTuflow/pull/28)
is closed **unmerged** at `28dbb261e88fac98a495e03360002704c0905410`.
Do not mistake closure for a published callback API or change this diagnostic's
historical pin implicitly. Producer successor integration needs its own source,
contract and adoption review within this experimental front.

This checkpoint reviews status evidence only, not production adoption or a new
performance result. Preserve complete authoritative sequential checks, pin-specific
integration, parity/materialisation, safe-reuse and local-SSD/SMB measurement gates.
#161 does not block #158, independent #160 or basic advisory runner #159. No streaming
runtime code, test, workflow, parser pin, benchmark baseline or dependency was changed.

Documentation corrections are unstaged/uncommitted in detached
`E:/Github/ryan-tools-review-161`; the original `E:/Github/ryan-tools` checkout is
unchanged. Documentation/index/link and whitespace validation is recorded below.
No runtime test rerun or package build is necessary for this status-only correction.
No staging, commit, push, merge, publication, GitHub write or native run was performed.
Next review remains 2026-11-01. Green remote-head checks do not cover this local patch.


### Combined-producer and unresolved-review checkpoint

Read the central PyTUFLOW #39 audit at exact head
`ee44350a9312f365316498708c3f958036eebc3a` (2026-10-25). It still records
consumer `f97e7f6` with 11 Policy errors; that part is stale against verified
consumer `0cc67eaa` and all nine passing jobs above. No central ledger edit was
posted or inferred to have been delivered. Keep #39's broader combined correctness
and native acceptance independent of this consumer status correction.

Static #39 source review confirms public `TCF.collect_batch_dependencies` still
accepts optional `on_run`, forwarding to the collector; callbacks on reused and
new complete projections remain outside the parser-error conversion path.
This is API inspection only. Existing green consumer integration pins producer
`9329cc8de2acfd2db8af7a9a6b6dfd34450af46f`, **not #39**. No current-head
combined-producer consumer integration result is claimed. Validate full callback,
duplicate, activation/reference, exception and legacy-no-hook contracts against
exact #39 before cross-repository integration acceptance.

Two historical review threads remain visibly **unresolved** on GitHub:
`PRRT_kwDONDA8NM6v7HY3` (test-data initialization) and
`PRRT_kwDONDA8NM6v7HY6` (wildcard parent exclusions). Current source verifies the
workflow explicitly initializes only `tests/test_data`, and eligibility inspects
the entire joined base/reference path. Current-head streaming CI passes, with
regressions retained. Source corrections are present; thread resolution and a
fresh Codex review request are separate GitHub actions requiring authorization.
No thread was resolved or review requested by this session.


### Documentation validation and delivery state

Normal Windows Python 3.14.6: `python repo-scripts/check_documentation.py`
passes all seven default documents and central-index coverage. Explicit
`--links-only` checks pass for every locally changed Markdown file in this
review worktree; `git diff --check` passes. All local changes are documentation
only, unstaged and uncommitted. No new remote head or hosted run exists for
these corrections. Revalidate the exact delivered head after authorized commit/push.


## 2026-10-25 - Authorized combined #39 consumer integration

The owner adopted the revised workflow and authorized local commits. The streaming
workflow now pins actual combined PyTUFLOW #39
`ee44350a9312f365316498708c3f958036eebc3a`, replacing obsolete #28 source
`9329cc8`. Published package dependencies, runtime adapter/probe, ordinary calls,
and frozen #158 benchmark code are unchanged. No production adoption is implied.

Used normal Windows Python **3.14.6**, with a no-dependency/no-build-isolation pip
installation of that exact source into local `.validation/producer39` on E:.
The package's `direct_url.json` verifies the requested/resolved commit above;
its imported module path is inside that target. No virtual environment or global
package replacement was used. Dependency build/temp files were redirected to E:;
an initial pip attempt using its default C: temp directory was stopped.

The exact combined producer passes **31 focused tests, zero skips**, covering
streaming overlap, bounded pending work, shutdown/error preservation, conservative
fully joined literal paths, unchanged no-hook adapter calls, duplicate flags,
callback ordering/object identity, callback exceptions including reused projections,
complete command/reference/database-source/activation parity, UUID ownership,
and authoritative individual/batch missing-file provenance. The new synthetic
multi-scenario BC fixture intentionally omits one file; both authoritative modes
report only that affected run. A first test draft used unsupported embedded scenario
filename expansion; it was corrected to the existing explicit IF Scenario syntax,
without weakening the expected affected-run assertion.

Command (with both `RYAN_TEST_STREAM_IO_PYTUFLOW=1` and
`RYAN_TEST_BATCH_PYTUFLOW=1`):
`python -m pytest -q tests/orchestrators/tuflow/test_streaming_io_probe.py tests/orchestrators/tuflow/test_streaming_combined_producer.py tests/orchestrators/tuflow/test_batch_preflight_adapter.py tests/orchestrators/tuflow/test_ordered_batch_adapter.py`.
The validation process imported the exact target producer before invoking pytest
from this consumer root. Ruff check/format and strict Pyright on the two streaming
test modules pass with zero errors/warnings. The dedicated Windows workflow now
runs this same expanded four-module gate against the new exact pin, retaining
explicit initialization of only `tests/test_data`.

The existing two unresolved GitHub threads have verified source fixes but remain
unresolved here; resolving them and requesting a fresh Codex review require separate
GitHub-write authorization. Earlier nine green jobs cover remote `0cc67eaa`, not
this new pin/test change. No push, merge, release, solver or native SMB benchmark.
The full authoritative inventory and independent production-adoption gates remain.
Next review: 2026-11-01, or after exact-new-head CI.


### Authorized local commit delivery

The owner subsequently authorized local commits. This work is retained separately
on local branch `review/pr161-combined-producer` in the review worktree above.
Local validation is complete; the completion report identifies the resulting commit.
No push or new hosted run exists. Earlier uncommitted/scaffold-only delivery
statements are historical checkpoints, superseded by this authorized local delivery.
Next action: obtain authorization to push this existing PR's changes, then require
CI/review on its exact delivered head. No merge, publication or GitHub discussion
mutation is authorized. Original `E:/Github/ryan-tools` remains unchanged.


The additional **full public Windows archive regression** also passes against
exact #39: **161/161 TCFs, 174 runs**, all accepted eager/individual-deferred/batch
materialised parity and separate public-preflight gates, exit 0. Command:
`python -m tests.fixtures.tuflow_example_repro.run_checks --pytuflow-checkout .validation/producer39 --batch --output .validation/archive39`.
Reports remain local in this worktree's `.validation/archive39`, with log
`.validation/archive39.log`; temporary extraction was redirected to E:.
This is a public correctness regression using the existing three-mode consumer
harness, not a repeat of the frozen four-mode native benchmark, a #39 graph
performance acceptance, or evidence of production speedup. No executable baseline
was changed. The new dedicated #39 streaming workflow still requires hosted CI.


### Current validation priority

The owner explicitly deferred hosted CI until local development has progressed.
Hosted checks are not on this work's current critical path. Continue local review,
implementation and proportionate tests; record exact local source and validation.
An authorized future push may run CI for later checking. Earlier statements making
new-head hosted checks an immediate development blocker are superseded by this
instruction. Push, merge and publication remain separate authorization boundaries.
Generated `.validation/` evidence is excluded locally and is not part of the commit.
