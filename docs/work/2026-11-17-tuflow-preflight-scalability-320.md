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

1. Validate exact proposed head using focused tests, Ruff format/check,
   strict Pyright and current hosted CI; correct any failures.
2. Run Windows Python 3.14 with the approved installed PyTUFLOW wheel
   and copied-runner preflight; verify full report and report collisions.
3. Compare repeated 1,040-run measurements against the original
   baseline on the same model and storage, with identical per-run
   findings before assessing performance. Record warm/cold state,
   package revision, wall/stage times and I/O counters.
4. Coordinate any measured dependency-assembly optimization with
   [upstream PR #17](https://github.com/TUFLOW-Support/PyTuflow/pull/17),
   retaining substitutions, active conditional BC selection, GIS
   boundaries, source ownership and case semantics. Do not infer a
   speedup from grouping alone.

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

### 2026-11-17

Reviewed current runner/checker and upstream dependency contract.
Prepared compact presentation grouping, opt-in full CSV and timing
counters with synthetic regression tests. Upstream dependency
collection optimization and paired benchmark remain outstanding.
