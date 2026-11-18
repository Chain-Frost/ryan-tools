# TUFLOW preflight grouping and scalability — issue #320

| Field | Value |
| --- | --- |
| Status | Draft implementation; required validation outstanding |
| Owner | TUFLOW runner / maintainer |
| Created | 2026-11-17 |
| Updated | 2026-11-17 |
| Next review | 2026-11-19 |
| Baseline | ryan-tools main `fc33c109d1fbf62b37b0142576355b94966b1d7d`; upstream PyTUFLOW PR #17 open |

## Outcome and scope

[Issue #320](https://github.com/Chain-Frost/ryan-tools/issues/320)
requests faster input checks for a 1,040-run Windows/SMB batch, compact
presentation of repeated boundary-condition (BC) database missing-source
findings, and full per-run/per-boundary attribution. No second parser,
persistent filesystem cache, solver invocation or production-data mutation.

## Current state

This PR candidate implements only the **ryan-tools-owned reporting work**:

- Console groups repeated findings per original run, source spelling,
  status and kind. Groups report their repeated reference count and
  number of distinct details/column selections.
- No issue or warning is removed or reclassified in memory. The detailed
  no-overwrite CSV contains every original issue and advisory and captures
  source file/line, owner, layer, database, UUID, command index, source
  pattern, command text and exact required columns in JSON.
- Explicit runner strict preflight supports
  `PREFLIGHT_REPORT_CSV`/`--preflight-report`. Requested CSV failure
  invalidates strict preflight. Ordinary advisory checks do not write CSV.
- Runner logs completed check stage timings, input counter totals,
  checked runs, affected runs, blocking findings and advisories.
- Synthetic grouping and CSV provenance, report collision, runner options,
  conservative run/source separation and aggregation tests were added.
- 17 Nov additional request: add one blank console line between completed
  `Missing:` summaries and the next scenario table in
  `find_missing_tlf_runs.py`; coverage counts and summaries remain unchanged.
- Hosted CI at `61ceb9c` found a synthetic preflight mock missing the
  newly required inventory-counter fields. The test mock was corrected
  without weakening runtime validation. Hosted CI at `015db33` then
  passed 3,102 tests but one outdated presentation-text assertion failed;
  its expected message is corrected in this work branch.
- Upstream dependency-collection changes are stacked in draft
  [PyTUFLOW PR #73](https://github.com/Chain-Frost/PyTuflow/pull/73),
  based on the original [PyTUFLOW PR #17](https://github.com/TUFLOW-Support/PyTuflow/pull/17).
  #73 reuses immutable offset rebasing and duplicate BC source syntax
  within an invocation, without caching filesystem existence.
  The current ryan-tools pin/wheel is **unchanged** until qualification.

### Original measured baseline (not rerun)

| Phase | Duration |
| --- | ---: |
| Parsing | 1.08 s |
| Dependency collection | 27.76 s |
| Physical inspection | 8.10 s |
| Other | 3.64 s |
| **Total** | **40.58 s** |

The 17 November 2026 user-model check covered 1,040 runs on Windows/SMB
with Python 3.14.4, ryan-tools 26.11.17.4 and PyTUFLOW 1.3.1.
It reported 3,059 findings affecting 133 runs, including 3,036
missing files and 23 unresolved headers. The known empty 0.2% AEP
45-minute TP08 source must still fail. This was a single warm-state
observation, not evidence of an improvement. The instrumented 129.7 s
profile is not a comparable unprofiled timing.

## Next action

1. Review focused hosted parity and synthetic benchmark results in
   [PyTUFLOW PR #73](https://github.com/Chain-Frost/PyTuflow/pull/73).
   Correct defects without weakening fallback equality checks.
2. Reconcile exact-head CI for ryan-tools #321 after the single
   presentation expectation correction.
3. Compare repeated baseline/optimised 1,040-run measurements on
   Windows/SMB under the same package versions and model conditions.
   Require exact per-run issue/provenance parity, including missing
   and empty sources, before accepting the PyTUFLOW candidate.
4. Only after qualifying the dependency revision, build/verify the
   new pinned PyTUFLOW wheel and integrate that consumer update in
   **the existing ryan-tools PR #321**, not a second ryan-tools PR.
   Do not merge or release without explicit approval.

## Completion criteria

- All original and advisory findings retained and available in CSV.
- Grouped console output preserves distinct original runs, statuses
  and source spellings. Missing and zero-byte sources cannot be hidden.
- Strict preflight exits nonzero for physical failures, unavailable
  validation, or required report export failure. Background checks
  remain nonblocking.
- Required test, Windows/SMB and installed-package acceptance passes.
- Measured performance comparison is repeatable and finding-identical.

## Validation and delivery

Prepared via GitHub connector on 2026-11-17. No local Python 3.14,
Ruff, Pyright, supported PyTUFLOW wheel, mapped SMB model or licensed
TUFLOW environment was available to this execution environment.
**No local test pass, numerical speedup or native acceptance is
claimed.** Review workflow outcomes at the exact PR commit.
The changed library also requires the normal
`python repo-scripts/build_library.py` wheel/version workflow in an
eligible environment before a release. No merge or issue closure.

## Progress

### 2026-11-17 — PyTUFLOW optimisation stack

Opened [PyTUFLOW draft PR #73](https://github.com/Chain-Frost/PyTuflow/pull/73),
targeting the upstream PR #17 contributor branch. It adds run-scoped
immutable command-offset rebasing and cached syntax-only BC source
templates with distinct per-row CSV column/owner attribution. Synthetic
fallback parity tests, failure-cache isolation and a 1,040-run benchmark
harness are included. Targeted Linux Python 3.11/3.14 CI was initiated.
There is no established speedup or approval to change the installed
wheel, ryan-tools dependency pin, or upstream release.

The recorded native UON candidate trial against the *existing* pin
on PR #321 showed parse 1.05 s, context 27.20 s, inventory 8.01 s,
and 133 affected/3,059 finding totals. It is **not** a comparison of
the new PyTUFLOW optimisations; no performance improvement was established.

### 2026-11-17 — additional issue comment

Added a presentation-only newline between successive ensemble scenario
coverage groups, alongside a multi-scenario console regression test. The
standalone wrapper revision is incremented; no filename inference, coverage
counts, missing summaries, export semantics or parser behaviour changes.
Updated the synthetic standalone preflight mock to include the public
inspection counters, correcting the latest hosted regression without making
unknown actual validator output pass as complete. Exact-head validation
remains required; performance benchmarking remains outstanding.

### 2026-11-17

Reviewed current runner/checker and upstream dependency contract.
Prepared compact presentation grouping, opt-in full CSV and timing
counters with synthetic regression tests. Upstream dependency
collection optimization and paired benchmark remain outstanding.

## Corrected PyTuflow material parser wheel (19 November 2026)

Consumer dependency pinned to upstream PR #17 commit 0121101650fd8e931af8d09cadb23bd43f6fefbd; SHA-256 b223a803d5c900b85c50855fc18128855dde016ac60e191de63043b440538fc3, 978,076 bytes. This is separate from the unaccepted performance changes in PyTuflow #73. No model run, native acceptance or release is claimed.
