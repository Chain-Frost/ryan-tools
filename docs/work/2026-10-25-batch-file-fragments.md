# Batch file existence with immutable dependency fragments

Status: Needs review - publication through existing paired PRs. Owner: ChatGPT. Updated: 2026-10-25.
Next review: 2026-11-01. Next action: review exact-head hosted CI on parser PR 39 and consumer PR 161; no further measurement is queued.

## Scope and decisions

Owner requested implementation, explicitly excluding further benchmarks. Target:
at least 50% less batch elapsed time, not yet measured. Group TGC/TBC/other sections
by relevant event/scenario flags, TEF replacements and transitive Set Variable
values. Reuse immutable dependency results instead of reinitialising full trees.
TBC boundary control files and bc_dbase source databases are distinct.

The workflow checks existence of required files and GeoPackage layers, retaining
essential linked-file discovery from GIS fields. It omits raster metadata validation.
Do not optimise rare linked-GIS reads in this work; retain correctness and existing
deduplication. Four bounded I/O workers are configurable down to one.

## Implementation and ownership

- PyTUFLOW owns expression compilation, context semantics and immutable subtree
  fragments. Known leaf constructors run only on misses; includes bind cached child
  fragments directly. Relative ordinals are rebound per occurrence/run.
- The ordinary context-default/variable setup and deferred include selector are
  shared with the existing evaluator, avoiding duplicated interpretation rules.
- Failed runs publish no partial fragment cache entries. Caches are invocation-local
  and bounded to 10,000 entries per pool. Custom/unsupported constructs and AD
  ownership use the ordinary batch evaluator with explicit fallback diagnostics.
- Complex TEF replacement uses the established translator; simple expressions use
  compiled literal/token operations. Conservative keys can miss reuse opportunities.
- ryan-tools consumes the public result, discovers unique paths, reads essential
  linked-file fields, and runs bounded deterministic filesystem/layer checks.
- MCP assessment: no new catalogue entry while the workflow requires an unpublished
  parser capability. API guidance is in TUFLOW_INPUT_PREFLIGHT.md; the existing CLI
  is extended rather than adding a duplicate entry point.

## Delivery

Separate normal local clones, both on `feat/batch-file-fragments`:
`E:/Github/PyTuflow-batch-existence` from parser `ee44350a9312f365316498708c3f958036eebc3a`;
`E:/Github/ryan-tools-batch-existence` from consumer `a428516f`.
Original workspaces, native inputs and retained benchmark evidence are unchanged.
Initial delivery was local only; the publication follow-up below supersedes that delivery state. No PR merge or release authorised.
Only tests/test_data and qgis-resources were initialized in the consumer clone;
unsorted was not inspected or initialized.

## Validation checkpoint

Normal Windows Python 3.14.6, source imports explicitly selecting the parser clone.
15 new parser correctness tests and 92 existing batch/deferred tests pass (107 total).
Tests assert avoided constructor calls and exact ordered results, not timing:
20 invariant-geometry runs construct three input states total.
Consumer focused checks against the final installed wheel pair: 83 passed, zero skips, no warnings.
New parser module/tests pass strict Pyright (private usage allowed for internal APIs).
The three touched legacy parser files have 25 default-Pyright findings; the unchanged
base has 28, with no added diagnostic messages. These remain inherited typing debt.
Consumer modified Python passes strict Pyright. Ruff check/format, Loguru policy, documentation/index/link checks and whitespace checks pass.

No native workload, profile, benchmark, solver, full archive replay or speed claim.
The first consumer build failed because the new clone lacked QGIS style resources;
only the required qgis-resources submodule was then initialized for the retry.


## Final local validation and artifacts

Parser command: `python -m pytest -q tests/test_fragment_dependencies.py tests/test_batch_dependencies.py tests/test_deferred_file_references.py` -> **107 passed**.
New parser module/tests: Ruff check/format and strict Pyright -> zero errors.
Strict configuration in `.validation/strict.json` uses `typeCheckingMode=strict`,
`extraPaths=[".."]`, and permits private usage of parser-internal APIs; the legacy
file comparison is in `.validation/baseline-typing.json` and `modified-typing.json`.

Consumer tests: `test_batch_file_existence.py`, `test_threaded_dependency_inventory.py`,
`test_input_preflight.py`, `test_batch_preflight_adapter.py`, `test_ordered_batch_adapter.py`
under `tests/orchestrators/tuflow/`, plus
`tests/functions/tuflow/test_gis_attribute_dependencies.py` -> **83 passed**.
Both `RYAN_TEST_BATCH_PYTUFLOW=1` and `RYAN_TEST_DEFERRED_PYTUFLOW=1` were enabled.
The final check imports both packages from `.validation/installed`, verifies their
module paths, and uses `pytest --import-mode=importlib`; no global installation changed.

Both wheels built successfully. Consumer build:
`python repo-scripts/build_library.py --skip-pip`, followed by `--no-bump` for the
final formatting verification rebuild. Version is **26.10.25.1**; wheel under `dist/`.
Parser build: `python -m build --wheel --no-isolation --outdir .validation/dist`,
producing **pytuflow-1.2.6** from this exact working source (not the published build).
Wheel/source hashes are recorded in consumer `.validation/implementation-manifest.json`.

Installed pair validation uses pip `--no-deps --no-compile --target .validation/installed`.
A copied standalone wrapper in `.validation/copied-wrapper` passes headlessly against
that installed pair: two synthetic proposed runs, two unique input files, no issues,
one reused section, no fallback. This is a functional smoke check, not a benchmark.

Only the consumer package version changed outside the listed implementation/docs/tests;
no files are staged. No commit/push/merge/release, hosted CI, full archive replay,
Python 3.12 run, private-model access, profiler or benchmark was performed.
Unrelated overdue work-register entries remain unchanged and outside this task.

## Remaining boundaries

At the initial implementation checkpoint the target 50% reduction was unmeasured; see the later bounded comparison below. The first implementation still
computes subtree signatures and binds flat output records per run. A persistent
source IR, state-effect summaries, partial fallback for arbitrary custom nodes and
lazy mutable facades are not implemented. Unsupported/custom source classes or AD
ownership use the established evaluator for that batch and expose fallback counts.
Broader extensions require their own correctness work; no automatic measurement is queued.


## 2026-10-25 owner-authorized bounded benchmark

Owner subsequently requested benchmarking with a maximum of ten minutes. The new
standalone private coordinator enforced one shared 600-second deadline with child
process termination on expiry. All four invocations completed in 127.72 seconds
(2.13 minutes), including fresh interpreter/import/provenance overhead.
No solver, profiler, full campaign, global install, commit or push was run.

Normal Python 3.14.6 loaded the matching installed pair from `.validation/installed`.
Source/wheel hashes matched `implementation-manifest.json` before/after each worker.
The existing private retained workload plan supplied 480 ordered proposed runs and
one SMB TCF. This reuses the earlier extracted run plan rather than extracting the
BAT again; input size/mtime metadata was stable across each invocation. No new
full input-byte hash or full parser command/reference provenance parity gate ran.

Both conditions use `check_simulation_inputs(context_mode="batch",
inspection_mode="files", io_workers=4)`; only `reuse_fragments` changes. Timing
includes model load, dependency collection, file/layer/link checks and CSV writing;
imports and provenance guards are excluded. Each invocation uses a fresh process.
Order was baseline, fragments, fragments, baseline; no cache flush or cache-state
claim. Sources/reports are local, model inputs remain on SMB.

| Condition | Trial seconds | Median seconds |
| --- | --- | ---: |
| Reuse disabled | 41.527174, 42.566088 | 42.046631 |
| Reuse enabled | 19.804411, 18.621055 | 19.212733 |

Observed median elapsed reduction: **54.31%** (2.19x speed ratio).
Both opposing-order pairs exceed 50% reduction. All invocations report 480 runs,
50 checked paths and zero issues. Exact checked-path, ordered-issue and BC
activation/source report projections match across all four invocations. Each
fragment run records 5,256 evaluations, 66,290 reuses and zero fallback runs.
This is report-level parity, not full parser semantic/provenance acceptance.

This small descriptive comparison meets the 50% target for this workload and
file-only scope. Two observations per condition and uncontrolled SMB/cache/load
conditions limit generalization; it is not a comparison with the earlier full
raster-metadata preflight or evidence of physical SMB traffic reduction.

Private reproducible script, protocol, four JSON rows, projections, CSVs and logs:
`.validation/benchmark-20261025-bounded/`; final `summary.json` reports `complete`.
Raw paths and flags remain ignored and private. Implementation stays unchanged;
this handoff/register update is unstaged/uncommitted. Next review: 2026-11-01.

## Local Git milestone

Owner requested regression checkpoints after the benchmark. Parser implementation
is commit `02868e43e69279e74c7bc920df87c183e1f3641d` on `feat/batch-file-fragments`
in `E:/Github/PyTuflow-batch-existence`, parent `ee44350`. The consumer milestone
is the commit containing this section on the same branch name in
`E:/Github/ryan-tools-batch-existence`, parent `a428516f`. Use these paired
revisions to rebuild the measured implementation; the reused nominal parser
version alone does not identify it. Earlier uncommitted statements above describe
their historical checkpoints. Both milestones are local; neither was pushed.

The [tracked numeric summary](evidence/2026-10-25-batch-fragments/summary.json),
[protocol](evidence/2026-10-25-batch-fragments/protocol.json),
[runner source](evidence/2026-10-25-batch-fragments/benchmark.py.txt), and
[source/wheel hashes](evidence/2026-10-25-batch-fragments/implementation-hashes.json)
preserve the benchmark checkpoint. The runner is an audit copy with private
checkout locations replaced by `BATCH_EXISTENCE_REPO` and
`BATCH_EXISTENCE_PRIVATE_WORKLOAD` environment variables. Copy it to a new ignored
output directory as `benchmark.py`; it writes beside itself. The private workload
directory must supply `config.json`, `runs.json` and `input-manifest-before.json`;
these inputs and the installed wheels remain local, not in Git. No benchmark was
rerun for this commit. The numeric observations are unchanged.

Commit-time focused verification: parser 107 passed; installed-pair consumer
83 passed. No implementation changes since the measured hashes. No full suite,
hosted CI, push, merge or release. Documentation and whitespace checks pass.

## Existing PR publication and continuous I/O scheduling

Owner authorized commit/push on 2026-10-25. Existing parser
[PR 39](https://github.com/phantom-branch/PyTuflow/pull/39) now contains
`566d5a3f011f520810def9d6e5405457b692bcc6`, including original milestone
`02868e4` and a history-preserving merge of updated PR head `9fdc13b`.
Consumer delivery uses existing [PR 161](https://github.com/Chain-Frost/ryan-tools/pull/161),
branch `test/tuflow-streaming-io-overlap`; this follow-up retains milestone
`5a781647`. Local clones track the corresponding GitHub PR branches; original
workspaces remain untouched. The commit containing this entry is the consumer
publication checkpoint. Hosted CI must be reviewed separately; no merge/release.

The inventory now replenishes its bounded worker pool whenever any job completes,
instead of waiting for an entire wave. At most the configured worker count is
outstanding; output order, one scan per dense directory and per-worker SQLite
ownership remain unchanged. An event-coordinated regression blocks the first job
until the third starts, proving that a slow request cannot stall other workers.

A heap adds no benefit without a priority policy. Full discovery/I/O overlap remains
a future option: deduplicate pending/running/completed paths, share results with all
run owners, admit linked discoveries and await producer completion plus queue drain.
Do not repeat authoritative checks after speculative checks. The measured inventory
was about 0.09 seconds; reducing repeated signature construction and output binding
is the more promising next optimisation. Compile unique signature slots and memoise
their values within a prepared context before considering broader cross-run caching.
No performance claim is made for this scheduler change and no benchmark was rerun.

Validation: parser 110 passed (107 fragment/batch/deferred plus three material-parser
tests after upstream integration). Rebuilt installed pair: consumer 97 passed, zero
skips, including previous streaming integration. Ruff check/format and strict Pyright
pass for all three changed Python files. Consumer wheel is 26.10.25.2; new pair is
retained separately under ignored followup artifact directories, preserving the
measured wheels. The consumer Windows workflow pins exact parser `566d5a3` and
requires the fragment API before running tests; parser Windows 3.12/3.14 CI now
includes fragment contracts. Local validation used Python 3.14 only.

### Rainfall coverage reconciliation

Owner questioned whether 50 checked files could cover 480 rainfall selections.
A read-only audit of the retained run flags, active BC database rows and TEF
substitutions found 24 distinct absolute Source paths and 480 distinct
(Source, Column 2) pairs. All 480 run-specific Source paths occur in the saved
checked-path inventory, after lexical Windows path normalisation. The e1/e2
flags select source filenames; e3/e4 select 20 Column 2 values across those files.
Thus the recorded 50 paths comprise 24 rainfall source files and 26 other
dependencies. Owner confirmed that the rainfalls are grouped into files.

This audit reads source listings, not rainfall data, and does not validate CSV
column presence or data contents. Empty BC activation/source report maps are
expected for this single-active-database workload: those maps are populated for
multiple simultaneously active BC declarations. Report equality alone is not a
completeness proof. A synthetic regression now verifies that multiple column
selections share one file check while missing-file issues retain every affected
run. Numerical benchmark artifacts remain unchanged.

Next review: 2026-11-01. Unrelated overdue register items remain outside this work.

## Required header verification (2026-10-25)

Owner extended rapid testing to verify rainfall CSV column names, without reading
rainfall values. Parser PR 39 now exposes resolved immutable `csv_columns` at
`64cb0d51141b4b5ea43e86c8b08f882eea98faa4`; the consumer integration workflow pins
that revision and asserts the metadata capability. Both existing PRs remain the
delivery targets. Earlier statements excluding CSV-column checking describe the
previous implementation, not this milestone.

Rapid mode now checks each distinct ordinary BC-source CSV header once using the
existing bounded I/O workers, then routes missing/ambiguous-column errors to every
affected run. TEF/transitive variables and multiple active BC databases retain
their selector ownership; wildcard expansion retains the column requirements.
A synthetic 480-selection / 24-file regression asserts 24 header inspections.
Additional tests cover stop-before-data behaviour, malformed/oversized headers,
BOM/comments/quoted and multiline headers, duplicates, cache lifetime and correct
run ownership. No native-model replay or benchmark was performed.

The reader supports named selectors in ordinary UTF-8 CSVs; blank selectors and
FEWS CSV forms fail closed. Header inspection stops at the first noncomment CSV
record with a 1 MiB bound. Other preambles and non-CSV schemas are not claimed as
verified. Rainfall values are never parsed or validated.

Parser validation: 170 focused tests; new tests pass strict Pyright. Consumer
retained-wheel validation: 108 passed, zero skips, using separately installed
header-capable wheels under `.validation/header-installed`. Wheel 26.10.25.3
built and verified; Ruff check/format, strict Pyright on all six modified consumer
Python files, Loguru policy, documentation links and whitespace checks pass.
The commit containing this entry is published to existing consumer PR 161, paired
with parser `64cb0d5`; hosted CI remains a separate review gate.
MCP assessment unchanged: extend the maintained wrapper/API guidance, not a new
tool for this focused helper. No merge or release authorised. Next review 2026-11-01.

## Checked-out PR rerun

Owner requested a rerun using the checked-out PRs, retaining the ten-minute cap.
Consumer PR 161: `c3bb6923cfe2cf885267364a0e30c97dfa8c7a05`.
Parser PR 39: `566d5a3f011f520810def9d6e5405457b692bcc6`.
Both initially clean. An initial baseline attempt was invalidated by concurrent
tracked edits in both checkouts; its post-run source guard failed after 42.75 s.
No timing from that attempt is accepted. Edits were preserved untouched.

The replacement runner reads immutable package snapshots made with `git archive`
of those exact commits, verifies physical import locations and hashes every
snapshot file before/after each worker. It excludes the concurrent uncommitted
changes. A shared 480-second subprocess timeout bounds the replacement matrix,
leaving room for the discarded attempt under the original ten-minute cap.
No solver, profiler, install or additional full correctness campaign ran.

Four fresh-process invocations completed in 125.26 seconds;
including the discarded attempt, benchmark coordinators took 168.01 seconds.
Normal Python 3.14.6, same retained 480-run SMB plan, four I/O workers, file-only
inspection, API plus report writing, order baseline/fragments/fragments/baseline.
Imports and guards excluded from each timing; caches/background load uncontrolled.
Input metadata unchanged within each invocation. No new full input-content hash.

| Condition | Trial seconds | Median seconds |
| --- | --- | ---: |
| Reuse disabled | 40.957415, 41.182248 | 41.069832 |
| Reuse enabled | 18.767939, 18.634433 | 18.701186 |

Median elapsed reduction: **54.46%**. All four report 480 runs,
50 paths, zero issues and identical report projections. Each fragment run reports
66,290 reuses and zero fallbacks. This reproduces the earlier greater-than-50%
workload-specific observation; two trials do not establish a general speedup or
a statistically meaningful improvement over the earlier revision. No new full
parser command/reference provenance parity gate was performed.

Tracked [summary](evidence/2026-10-25-batch-fragments-pr-heads/summary.json),
[protocol](evidence/2026-10-25-batch-fragments-pr-heads/protocol.json),
[runner audit copy](evidence/2026-10-25-batch-fragments-pr-heads/benchmark.py.txt),
and [snapshot hashes](evidence/2026-10-25-batch-fragments-pr-heads/snapshot-hashes.json).
Runner audit copy replaces the private workload directory with the
`BATCH_EXISTENCE_PRIVATE_WORKLOAD` environment variable. Materialize exact-commit
package snapshots under `sources/consumer` and `sources/parser` beside the runner
and provide `source-heads.json` with original source paths mapped to exact SHAs
(as recorded privately); the runner expects `config.json`, `runs.json` and
`input-manifest-before.json` in the private workload directory.
Private originals: `.validation/benchmark-pr-heads-20261025-frozen/`; discarded
attempt retained in `.validation/benchmark-pr-heads-20261025/`.

This evidence checkpoint is committed locally; not pushed. Concurrent parser and
consumer code edits are excluded and preserved. Documentation/link/whitespace
checks pass; no tests rerun because implementation was not changed by this task.
Next action: review paired exact-head results and CI; no further measurement queued.
Next review: 2026-11-01. Unrelated overdue reviews remain outside scope.

## CSV-header PR benchmark: correctness blocker

Owner requested a rerun of published consumer PR 161 at
`1143a976c5d1cd6e32bc40bd88cbd21783941594`, paired with parser PR 39 at
`64cb0d51141b4b5ea43e86c8b08f882eea98faa4`. GitHub PR heads were verified
against local commits before freezing package snapshots. The shared 600-second
timeout, retained 480-run plan, four workers and opposing-order two-pair protocol
were retained; BC CSV header checks were explicitly enabled in both conditions.

**Stopped on correctness failure after the first baseline.** It took 39.970801 s,
checking 50 paths, 24 unique CSV headers and 480 column selections, but reported
480 unresolved missing-column issues. Source-snapshot hashes and input metadata
guards passed. No fragment timing or comparative speedup is accepted. The total
coordinator runtime is in the linked summary and is below ten minutes. This does
not invalidate earlier timings of the workflow without header checks; those did
not establish header correctness.

A subsequent bounded read-only preamble inspection of all 24 source files found
the same structure: line 1 is a ! comment; line 2 is a noncomment preamble record
with 31 fields; line 3 contains the required time column and first temporal-pattern
column, also with 31 fields. Inspection stopped at that header. No rainfall data
rows were parsed. The current reader stops at the first nonblank/non-! record
and therefore selects line 2, causing the native coverage failure. Presence of
one pattern column does not prove every requested column is present.

Next action: define and implement supported preamble/header selection semantics
with a focused synthetic regression, then rerun the bounded comparison. No parser
or consumer implementation was changed and no automatic timed retry was made.

Tracked [failed-run summary](evidence/2026-10-25-batch-fragments-csv-headers/summary.json),
[protocol](evidence/2026-10-25-batch-fragments-csv-headers/protocol.json),
[runner audit copy](evidence/2026-10-25-batch-fragments-csv-headers/benchmark.py.txt)
and [snapshot hashes](evidence/2026-10-25-batch-fragments-csv-headers/snapshot-hashes.json)
preserve this milestone. Runner reproduction has the same private configuration
and frozen-snapshot requirements as the prior PR rerun; the private workload path
is replaced by `BATCH_EXISTENCE_PRIVATE_WORKLOAD`. Raw identities/CSV issue reports
remain private under `.validation/benchmark-csv-headers-20261025/`.

Evidence checkpoint committed locally, not pushed. No solver, profiler, rainfall
data validation, test rerun, merge or release. Documentation/link/whitespace checks
pass. Unrelated overdue reviews remain outside scope. Next review: 2026-11-01.

## Header-selection correction after failed checkpoint

Preserved failed checkpoint `e3ea664e` and its numeric artifacts unchanged. Owner
requested reading up to five lines. The final reader therefore examines at most
five physical lines, counting blank/comment lines, preambles and multiline CSV
records. The first record containing any requested column name becomes the
header; missing/duplicate selectors are diagnosed against it without looking
further. The consumer supplies the union of each source's requested time/value
names, maintaining one open per file. No fixed Event ID label or metadata width
is required. Without selector hints, the helper retains first-record behaviour;
rapid preflight always supplies available parser selectors.

A bounded read-only check replayed the 480 resolved time/value selections recorded
in the failed issue report against the 24 source headers. Result: 24 opens, 480
selections, zero issues. A read guard rejected any attempt beyond physical line 3;
none occurred. This checks saved selections against current headers, not a full
model rerun or timing comparison. Sanitized result:
[header-selection correction](evidence/2026-10-25-batch-fragments-csv-headers/header-selection-correction.json).
No rainfall values were parsed during this check and no benchmark was rerun.

Synthetic tests cover headers on lines 3/5/6, a guard rejecting a sixth read,
comments/preambles, multiline headers, missing selectors without further reads,
byte limits, cache reuse and run attribution. The 480-selection/24-file fixture
includes the failed native layout. Unrecognisable headers fail within the bounded
prefix; no numeric rainfall conversion or full-series validation is performed.
Parser remains pinned to `64cb0d5`; no parser change was needed.

Next action: review this correction on existing PR 161 and exact-head CI. A future
benchmark requires a separate owner request; no automatic retry is queued.
Historical header-enabled benchmark remains failed with no valid speed comparison.
Unrelated overdue work remains outside scope. Next review: 2026-11-01.

Correction validation: 119 focused consumer tests passed against separately
installed wheel 26.10.25.4 and parser 64cb0d5, with zero skips. Ruff check/format,
strict Pyright on the five modified Python files, documentation links and
whitespace checks pass. The wheel builds and verifies. This commit is the
consumer correction checkpoint for PR 161; publication also preserves/pushes
local failed evidence checkpoint e3ea664e. Hosted CI is a separate review gate.
