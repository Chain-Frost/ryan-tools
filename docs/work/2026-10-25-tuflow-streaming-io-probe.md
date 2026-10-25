# TUFLOW streaming I/O overlap diagnostic — 25 October 2026

**Status:** In validation; opt-in diagnostic prototype only.
**Owner:** ChatGPT.
**Next review:** 1 November 2026, or after exact-head CI.
**Related producer:** [PyTUFLOW draft PR #28](https://github.com/phantom-branch/PyTuflow/pull/28).
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

**Next action:** commit and push the local Policy typing correction when authorised,
then require all checks on that new head before assessing whether safe authoritative
inventory reuse is feasible without path/state staleness.
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
