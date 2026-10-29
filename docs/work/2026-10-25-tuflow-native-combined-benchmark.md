# Matched native SMB combined-parser benchmark

| Field | Value |
| --- | --- |
| Status | Needs review; collection closed, methodology and performance conclusions provisional |
| Owner | ChatGPT |
| Created / Updated | 2026-10-25 |
| Next review | 2026-10-26 or campaign completion |
| Scope | Read-only native Windows SMB input preflight; no solver, model edits, merge or release |

## Authorization and privacy

The owner explicitly requested the native benchmark against a supplied SMB BAT.
The BAT is read as text, never executed. Only anonymized aggregate evidence belongs
in this repository. Model paths, original commands, hashes mapping the private
workload, full reference identities and raw reports remain in ignored local
validation storage. Existing accepted historical benchmark evidence is immutable.
One benchmark process coordinates sequential fresh-process conditions; no concurrent
benchmark or TUFLOW engine was observed during preparation. Existing MCP processes
are preserved. No virtual environment or new Git worktree is created.

## Frozen source and workload identity

| Component | Exact commit |
| --- | --- |
| Common parser integration base | `fad6dae3038a9381c268b308ef4aae216682aba2` |
| P6 alone | `05d132e45176f117b1c75a176fcc9d5027c429a1` |
| P7 alone | `1d8b59b538654443434399e596938f4c9fe73a0f` |
| Combined #39 | `ee44350a9312f365316498708c3f958036eebc3a` |
| Frozen diagnostic/consumer | `f1f7958b79f3ffe84ba8ac0b04658f27c311460a` |
| Experimental read-ahead consumer | `01e3e46c76e07602afba187284aa539652398fff` |

The same integration base is an ancestor of every parser candidate; do not substitute
an advanced current master or historical unmatched environment. Sources are clean,
pinned normal local clones; original working checkouts and frozen executable files
are unchanged. Normal Windows CPython 3.14.6 and one unchanged installed dependency
inventory serve every condition. The imported parser path is verified for every
worker. BAT/TCF hashes are checked before and after every invocation.

The static planner from independent #160 found **480 ordered commands, 480 distinct
flag sets, one effective TCF and one working directory**, with two SET statements
and a final PAUSE. Exact order/provenance are retained privately. This count does
not establish equivalence with either earlier 480-run dataset. Directory/file
identity counts will be established by the fresh full gates, not imposed from
historical 8,160/158 or 12,480/49 records.

## Declared campaign

1. All four modes (eager, deferred, graph-off, graph-on) pass representative gates
   for each revision, followed by one full four-mode and public-preflight gate.
   Retain every materialised identity and command/reference semantic projection;
   require complete cross-revision equality before timing.
2. Four balanced revision rotations time the ordinary public batch preflight
   through complete authoritative inventory and report writing. The graph policy
   is parser default; the public API has no graph switch.
3. Four balanced revision rotations separately time frozen diagnostic graph-off
   and graph-on, alternating the two modes. These diagnostic collection timings
   are not described as whole-public-preflight timings.
4. Four balanced rotations on combined #39 compare authoritative-only, preliminary
   plain producer, no-I/O callback, and bounded one-worker probe. The latter three
   include the unchanged final authoritative preflight. This deliberately measures
   the current double-collection pilot; speculative results never replace checks.

No cProfile, audit hook or filesystem monkeypatch runs in elapsed-time trials.
RSS is sampled at 20 ms and is not an exact memory peak. Client/server caches and
server/background load are uncontrolled; no cache flush occurs. Plan extraction,
accessibility and correctness gates precede timing. Report medians, all trials and
spread; do not infer causal speedup from counts or profiler timing.

Stop on failed/incomplete parity, input/source drift or worker failure, preserving
failed reports without an automatic timed retry. Setup failures occurred before
measurement: a site-packages `tests` namespace collision was avoided by loading the
unchanged frozen module by file; a setup helper's final status print lacked a json
import after writing the runners. Both runner files compile, sources remain clean,
and all four representative gates pass. These are preparation failures, not hidden
successful benchmark trials.

## Validation, delivery and next action

Representative gates: all four revisions pass all four modes. That was the initial checkpoint; the main matrix subsequently completed. No elapsed-time result or production adoption is
accepted at this checkpoint. Continue the frozen sequential campaign, inspect all
parity/input/source gates and save anonymized results here. The existing #39
performance handoff owns producer adoption; #158/#159/#160/#161 remain separate
PRs. Hosted CI is deferred per owner instruction. This record/index/register change
is initially uncommitted; complete, validate and push the bounded documentation
handoff when the native campaign reaches a result or blocker.


## Full correctness milestone

All four source revisions pass their complete 480-run eager, deferred, graph-off
and graph-on gates, with **12,480 materialised identities / 49 diagnostic files**
in every mode and zero collection/inventory findings. Independent individual and
default-batch public preflight pass and agree. Every cross-revision complete
identity list, ordered-command hash and deferred-reference hash agrees with the
common-base control. An independent legacy extraction also matches #160's exact
480 ordered flag dictionaries for this bounded BAT.

Private provenance now includes 62 input file spellings, including existing GIS
and raster sidecars; content hashes were captured before elapsed trials. File
size/mtime, source cleanliness and installed dependency guards run around timed
invocations. The SMB mapping is confirmed, while sources/reports are on local
NVMe. Hashing and correctness reads precede timing, so no cold-cache claim is made.
The four balanced production rotations are in progress; no comparative speedup
or production-adoption decision is accepted at this milestone.


## Completed full-authoritative production timing phase

Four balanced revision rotations completed with 480 runs and 50 checked path
spellings in every invocation, zero findings, unchanged exact source identity and
frozen experiment hashes. Positive change means slower than the common base.

| Revision | Trials | Median seconds | Minimum-max seconds | Median change |
| --- | ---: | ---: | ---: | ---: |
| Common base | 4 | 45.666532 | 43.022613-47.056367 | +0.000% |
| P6 alone | 4 | 48.947005 | 47.315576-55.684396 | +7.184% |
| P7 alone | 4 | 39.443646 | 36.364412-41.957872 | -13.627% |
| Combined #39 | 4 | 42.627403 | 40.556289-44.511410 | -6.655% |

P7 is faster in each of the four paired trial blocks; P6 is slower in each.
Combined #39 improves on the common base in each block but its median is slower
than P7 alone. This supports investigating P7 independently rather than assuming
all combined optimisations are beneficial. It is not permission to merge or to
multiply component percentage changes. Background/server/cache conditions remain
uncontrolled and results apply to this measured workload.

The common-base median public context/dependency construction is about 44.72 s,
versus 0.14 s for final deduplicated inventory. These named consumer phase timings
are not proof that all context cost is CPU or that SMB traffic is eliminated.
Graph-control repetitions, callback/probe timing and separate I/O observations
remain active; the entire declared campaign is not yet complete.


## Owner-directed closeout: methodology review before further testing

The declared main matrix completed all **68 invocations** (four correctness gates
and 64 timed conditions) in **89.544 minutes**, plus initial preparation. It
performed about 48,000 logical run evaluations and approximately 100 model loads
in the main matrix. No solver simulations were launched. This was an excessive
matrix for an initial comparison, and no adequate runtime estimate was given.

The owner requested closure and an independently reviewable methodology. No
additional I/O observation, public-parser metric or profile passes were launched;
the prepared helper files are not measurement evidence. No final full input-byte
hash recheck was performed. Existing input metadata/source/dependency guards
passed, but collection completion is not final experimental acceptance.

Review the [methodology and complete sanitized data bundle](evidence/2026-10-25-native-smb/METHODOLOGY.md).
It includes all 64 timing rows, exact executed worker/coordinator/aggregation
sources as text, formulas, library versions, timing boundaries, setup failures,
cache limitations and the double-collection callback/pilot caveat. The private
raw logs, full identities, input manifests and configuration remain in ignored
local validation storage. The local pointer and private CLOSEOUT.md identify them.

Results remain **provisional pending independent methodology/data review**.
Do not launch another benchmark or profile automatically. Refine a smaller,
explicitly bounded testing process before any further measurement or adoption
recommendation. The diagnostic and full-authoritative timings are different
boundaries; the duplicated pilot is not an integrated parallel checker benchmark.
This documentation-only closeout is prepared for the existing #161 branch;
#158/#159/#160/#39 remain separate. No merge, release or GitHub comment is made.
Next review: 2026-10-26, or the requested independent review.
