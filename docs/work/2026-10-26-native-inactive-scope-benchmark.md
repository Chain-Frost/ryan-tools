# Native SMB/SSD inactive-scope investigation

Status: Active (expanded evidence complete; publication pending). Owner: ChatGPT.
Updated: 2026-10-26. Next review: 2026-10-27. Next action: commit/push the sanitised bundle on existing consumer PR #161
and link the expanded evidence from parser PR #39. Owner explicitly authorised the evidence push.

[Expanded 32-trial SMB/SSD evidence bundle](evidence/2026-10-26-native-smb-ssd-inactive-scope/README.md).

[Published PR #39 evidence and recommendation](https://github.com/phantom-branch/PyTuflow/pull/39#issuecomment-6414181004).
Recommend reverting only the inactive-scope production fast path pending stronger matched evidence, retaining its
regression tests and diagnostics. No production revert was performed.

## Scope and frozen sources

Owner authorised a different private SMB command list and existing local checkouts. No solver execution, production
changes, revision switching, new branch, worktree or environment. Private artifacts remain outside repositories.

- Parser A: `1ca131f98a307f8054d2cf658c733cfa987d66c5`.
- Parser B: `36ad04bd7cb045c48f3b77e8794c48a956c90c06`, verified remote PR #39 head.
- Consumer: `a710f24ffb1698e949937f56a0212b6ae599e54e`.
- Sources exported with `git archive`; existing checkouts were not switched or edited.
- Windows, normal CPython 3.14.6, pandas 3.0.5, NumPy 2.5.3, Loguru 0.7.3,
  GeoPandas 1.1.3 and Shapely 2.1.2. Source snapshots override installed distributions.

## Correctness gate

480 active commands, no duplicates; exact supplied order and scenario/event switches retained. Launcher-only `-b`
is excluded from parser context strings; original commands and workload checksum are private.
Independent processes compare complete public run dataclasses recursively, preserving tuple order and every field.
UUID generation is deterministic during parity-only model parsing and restored before evaluation. Timing workers
use ordinary UUID generation and no instrumentation.

All 480 runs pass in ordinary uncached, ordinary cached, A fragments and B fragments. Their complete serialisations
have identical SHA-256 `318bcf175395c35066a8bc8dd65e7c757f20681d4d7930e52b666b810ecf17a7`.
This includes input UUIDs, command text/ordinals, ordered inputs, file/layer references, BC activations,
database sources, CSV selectors and errors. All runs retain only the applicable cell-size assignment, value 32;
13 alternative declarations are excluded. Existing synthetic tests cover overlapping/default declarations separately.

## Uninstrumented measurement

Two serial A/B/B/A campaigns, fresh processes, one I/O worker, batch/files mode, fragment reuse and BC header checks.
End-to-end boundary: preflight call through private CSV report completion; import/process startup is excluded and
recorded separately. No cProfile, tracing, diagnostic CSV writing or diagnostic logging during measurement.

| Campaign | A | B | B | A |
| --- | ---: | ---: | ---: | ---: |
| 1, seconds | 21.973760 | 19.743048 | 20.222496 | 20.129618 |
| 2, seconds | 19.847371 | 19.948533 | 19.586333 | 21.037567 |

A median 20.583592 s, range 19.847371–21.973760 s, sample SD 0.962611 s.
B median 19.845790 s, range 19.586333–20.222496 s, sample SD 0.275015 s.
Median reduction 3.5844%; overlapping ranges and only two fixed-order blocks preclude statistical acceptance.
Both block means favour B (5.078% and 3.302%); this is descriptive evidence, with uncontrolled client/server caches
and background load. No cold-cache or causal claim; the historical 16.259 s has a different/unknown boundary.

All eight consumer passes: 480 runs, 50 paths, 24 headers, 480 column selections, zero issues and zero fallbacks.
Source Python files, seven control/database sources and batch bytes match their saved hashes after timing.

## Delivery

Separate diagnostic passes use the existing `tests/profile_fragment_evaluations.py` with
`--private-file-attribution --private-key-phases --cprofile`, plus observation-only fragment counters.
Both independently pass complete ordinary/fragment parity. All detailed command/file CSVs stay private.

| Operation | A | B | Difference |
| --- | ---: | ---: | ---: |
| Input visits, active / inactive | 24,495 / 47,051 | 24,495 / 47,051 | Unchanged |
| Full keys | 71,546 | 24,495 | -47,051 |
| Inactive keys | 47,051 | 0 | -47,051 |
| Scope calls | 157,442 | 173,801 | +16,359 |
| Local scope cache hits | 31,682 | 50,921 | +19,239 |
| Local misses / underlying scope evaluations | 125,760 | 122,880 | -2,880 |
| Membership evaluations | 389,468 | 386,108 | -3,360 |
| Fragment translations | 104,445 | 84,045 | -20,400 |
| Expression-pool translations | 89,335 | 68,935 | -20,400 |
| Context variable/event translations | 35,915 | 34,299 | -1,616 |
| Transitive bindings | 91,735 | 70,375 | -21,360 |
| Key expression entries visited | 143,546 | 72,255 | -71,291 |
| Key scope entries visited | 113,306 | 63,375 | -49,931 |
| Descendant scope entries in include keys | 41,760 | 38,880 | -2,880 |
| Database cell entries in database keys | 30,240 | 8,880 | -21,360 |
| Fragment evaluations | 5,256 | 2,494 | -2,762 |
| Fragment reuses | 66,290 | 22,001 | -44,289 |
| Contexts | 480 | 480 | 0 |
| Input states | 2,494 | 2,494 | 0 |

Signature entries are per-key visits, including already-deduplicated database cell text, not physical reads or unique
database rows. Reduced evaluation/reuse counters remove empty inactive fragments. Active state construction is unchanged.
Inside-key scope calls fall 113,306 to 63,375, but outside-key calls rise 44,136 to 110,426. Most scope work moves
before keys. Active-input overhead is not isolated as a native wall-time contribution; it cannot be asserted free.

| Anonymous group | A keys | B keys | Avoided |
| --- | ---: | ---: | ---: |
| Cell-size variable | 6,720 | 480 | 6,240 |
| End Time | 6,240 | 960 | 5,280 |
| SGS Max Sample Frequency | 2,880 | 0 | 2,880 |
| Other settings | 44,644 | 15,364 | 29,280 |
| GIS reads | 2,896 | 2,886 | 10 |
| GRID reads | 486 | 485 | 1 |
| Database commands | 2,400 | 480 | 1,920 |
| Material database commands | 1,440 | 480 | 960 |
| Included control files | 1,920 | 1,440 | 480 |

End Time retains 960 active visits: do not assume only one applicable declaration per group.

The separate consumer B cProfile confirms the three largest application self-time entries:
scope construction, 5,877,539 calls / 9.349021 s; expansion, 5,511,000 / 4.947289 s;
equality, 6,164,954 / 4.687001 s. These instrumented times are not native performance results.
Inclusive times overlap and must not be added. Investigate immutable expanded scopes, cached expansion respecting
negation/Else If, and canonical scope comparison or complete-context membership caching. Preserve full provenance
and overlapping/default assignment tests. No parent/child split or worker optimisation is proposed.

Native median context-plus-projection time: A 19.161791 s / B 18.492784 s. Median parse: 0.808377 / 0.767632 s;
inventory plus header checks: 0.352678 / 0.342094 s. Complete individual phases and process-wall times are in the PR
comment and private `campaign-progress.json`; medians of phases must not be summed.
Process-wide diagnostic filesystem counts include local imports: 57 opens, 4,152 stats, 26 scandirs, 14 SQLite
connects. These are not physical SMB-request counts. Warm filesystem inventory is not the measured dominant cost.

Focused source-snapshot validation:
`python -m pytest tests/test_fragment_inactive_scope.py tests/test_fragment_filename_attribution.py -q -p no:cacheprovider`
passes 8 tests, zero skips. No new tests or production changes. Final source/workload hash verification passes;
remote parser head remains B. A statistically distinguishable native win and isolated active-input wall overhead
remain unestablished, explicitly limiting performance acceptance.

Private output locations are recorded locally. Frozen archives, manifests, complete records, detailed CSVs and raw
profiles remain outside repositories. Only sanitised documentation, aggregates and runner audit copies are published.
No merge, release or production acceptance. Git publication state is recorded below.

## SSD follow-up, 2026-10-26

Owner supplied the existing SSD command list, then requested plenty of data and a push to the repository. Its bytes
and all seven control/database source files match the SMB copy. The same frozen parser/consumer sources and options
are used. All 50 dependency patterns, including database Source CSVs, physically resolve to the local NVMe SSD.

SSD full-record parity passes in all four independent modes, SHA-256
`68ceba1fb5e865c8d3894165c8e99eb2d73096dfda9609dc2307f5959a8d8d93`.
Cross-storage full records are identical after replacing only mapped-drive/UNC roots with the local root.

Collection totals 32 native trials: eight SMB, eight initial SSD, and 16 supplementary SSD trials with process CPU
and peak working-set measurements. The supplementary blocks use ABBA/BAAB/BAAB/ABBA order. Separate A/B SSD profiles
are excluded from native timings. All native and profile passes retain 480 runs, 50 files, 24 headers,
480 selections, zero issues and zero fallbacks. Guards pass before/after every supplementary trial.

| Native subset | A median s | B median s | Reduction |
| --- | ---: | ---: | ---: |
| SMB, 4 per revision | 20.583592 | 19.845790 | 3.584% |
| Initial SSD, 4 per revision | 19.130097 | 18.564542 | 2.956% |
| Supplementary SSD, 8 per revision | 18.296748 | 18.083090 | 1.168% |
| All SSD, 12 per revision | 18.433583 | 18.278605 | 0.841% |

CPU seconds nearly equal wall seconds: supplementary B median CPU 18.062500 s, median per-run CPU/wall ratio
99.929%, median inventory/header 0.007527 s and peak working set 207.889 MiB (A 211.277 MiB).
The warm workload is CPU-bound. Inventory alone excludes initial control/database parsing; cold SMB remains untested.
Sequential storage sessions and timing drift prevent attributing their whole difference to storage.

All six SSD block means favour B, by 0.197% to 4.091%. Exploratory block-sign-swap p=0.03125 for all six, versus
0.125 for the four supplementary blocks alone. Additional collection followed initial observations, ordering is fixed
and caches/load are uncontrolled; do not present this as a preregistered randomized acceptance test.
The small fast-path benefit does not resolve the remaining 18 seconds of CPU work or establish native SMB acceptance.

The B SSD profile still observes 5,877,564 Scope constructions, 5,511,000 expansions and 6,164,954 equality calls.
Its largest application self times are construction 8.033143 s, expansion 4.205323 s and equality 3.938287 s.
These are instrumented seconds; the profile takes about 54 s overall. Follow-up targets are immutable expanded scope
representations, compiled membership with complete context invalidation, and canonical comparison/normalisation.
No production optimisation is implemented.

Every individual timing, resource range/variation, complete operation-group counters, selected A/B profile aggregates,
environment, limitations and runner audit copies are in the linked bundle. The SSD is a Samsung 990 EVO 2TB NVMe;
resource sampling uses existing psutil 7.2.2 and does not poll the process during native timing.
