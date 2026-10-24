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

**Next action:** finish exact-head CI and code review, then assess whether
safe authoritative inventory reuse is feasible without path/state staleness.
Do not merge, publish or launch a full native benchmark on this pilot alone.

## Cross-repository regression boundary

The independent `tuflow-streaming-io-probe.yml` Windows workflow pins producer
PR #28 commit `d99435dce741d80967df186ba6a28404358c8ca3`, installs it
without altering published dependencies, and exercises the actual callback
through `probe_batch_io_overlap` using a temporary synthetic TCF. Existing
ordered-batch and normal preflight CI retain their own historical parser pins;
this additional workflow does not replace the complete archive parity gates.
