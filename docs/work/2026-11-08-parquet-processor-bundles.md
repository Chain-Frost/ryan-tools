# Parquet-directory snapshots for native TUFLOW processor collections

**Decision:** 8 November 2026. **Status:** Scoped Windows native acceptance PASS at `7ac3c0c5`; revised exclusive-create contract and filtered-location regressions pass. Final hosted CI remains a separate gate. **Next review:** 15 November 2026.

**Scope:** [PR #293](https://github.com/Chain-Frost/ryan-tools/pull/293) stacked on draft [#251](https://github.com/Chain-Frost/ryan-tools/pull/251). WP04 #284 has been incorporated into the parent #251; retain its native validity, lifecycle and source-selection contracts.

## Revised architectural decision

The previous single-file `.rtparquet` ZIP proposal is **superseded** before release.
The public `ProcessorCollection.to_parquet(directory)` and
`ProcessorCollection.from_parquet(directory)` APIs now use a **directory** of
ordinary ZSTD-compressed PyArrow Parquet tables with a versioned
`manifest.json`. This does not replace standard DataFrame `.parquet` exports,
adopt PyTuflow readers, add PyTables/HDF5 or introduce Dask.

For a fresh dataset the directory holds:
- `manifest.json`
- `processors/000000.parquet`, `processors/000001.parquet`, etc.
- optional `basic_info_lookup.parquet`.

Each member is independently loadable with pandas/PyArrow. Manifest entries
include SHA-256, byte size, pandas column/index/dtype contract, source
fingerprint, factory type/class/module, canonical run code, filters and input
health. Raw PO/POMM source locations are recorded independently from processed
rows to distinguish absent locations from locations with wholly rejected
observations. The only permitted processor instantiation is the current
installed `BaseProcessor.from_file` factory, checked against the manifest;
there is no arbitrary dynamic module loading based on serialized data.

## Source provenance and semantics

`BaseProcessor` records the bounded source signature before processing
(path, byte size, nanosecond modification time, plus GeoPackage WAL state).
The signature travels with the processed data through imports and subsequent
exports. A default export checks this original identity both before writing
and just before publication. The default import also verifies the current
source. `verify_sources=False` is an explicit historical/offline choice
that does not change the recorded original identity.

For strict snapshot compatibility, a registry digest and a conservative
processing-source digest (relevant native processor/source files) must match.
Data payloads are rehydrated without reopening the source result CSV. This
bounded fingerprint is not equivalent to content-hashing a huge source:
a deliberate or accidental same-size/same-mtime replacement can evade it.

## Safe publication and selection

For a new directory, an owned sibling staging directory is written, validated,
and moved into place by an atomic no-clobber directory rename. Unsupported
POSIX rename semantics fail closed. Existing targets are never overwritten
implicitly. On `overwrite=True` the writer publishes an immutable generation
under `.versions/<uuid>/` and atomically replaces only `manifest.json`,
preserving the prior generation for readers who have already opened it.
Linux pins directory descriptors for the entire publication window and uses
atomic RENAME_NOREPLACE into a pinned versions directory and descriptor-relative
manifest replacement. Windows holds non-reparse-point CreateFileW GENERIC_READ handles with
FILE_SHARE_DELETE denied on the destination, its ancestors and versions folder
through cleanup, preventing path swaps. If these protections are unavailable
the overwrite fails closed. Overwrite consent is rechecked after staging, so a
concurrent initial writer cannot be replaced without explicit consent.
Readers resolve member paths relative to the generation recorded in the
manifest. Old generations are intentionally **not automatically deleted**.
The writer cleans only its owned temporary files/directories on failure,
never existing snapshots or source files. Windows NTFS and SMB mapped/UNC
rename/replace behaviour is a required native acceptance gate, not inferred
from POSIX validation.

The manifest is inspected before loading Parquet frames. Data-type selection
and independent raw-location presence evidence can skip unrelated members
without materialising their DataFrames. Per-frame row filtering and big
multi-run aggregation are deliberately not introduced: restoring an entire
300-run time-series collection may still exhaust RAM. Use independent
partitioned Parquet/reduction for those workloads.

The normal collection retains only nonempty successfully processed processor
objects. `parquet_selection_diagnostics` records `SELECTED`, `FILTERED`, `ABSENT`
or `INVALID` outcomes for location-filtered restores, so a source-present
but wholly rejected location is not silently rebranded as no-match.

## Acceptance and dependencies

- Native real factory Q/PO/POMM and heterogeneous DataFrame round-trip,
  categories/nullable/index and compacted lookup parity.
- Type/location selection before unrelated frame reads; present-invalid vs
  absent location evidence, with the final WP04 #284 behaviour integrated.
- Source modification after processing and immediately before publication,
  explicit historical/offline signatures, missing/moved sources, semantic
  incompatibility, corrupt/hash-mismatched and duplicate/traversal paths.
- Directory no-clobber, interruption/overwrite preservation and owned-temp
  cleanup, including native Windows NTFS, mapped-drive, UNC and installed wheel.
- Ruff format/lint, Python 3.14 strict Pyright on modified files, focused
  pytest, all six hosted CI jobs and an exact-head independent Codex review.
- No merge into draft #251 until WP04 #284 is incorporated and approval
  is explicit. No merge into main, release or issue closure.

The two original Codex P1 findings at the superseded ZIP implementation are
addressed by writing ordinary on-disk Parquet and capturing source provenance
before processing. The prior stream-incompatibility and export-time signature
bugs must not be carried forward.

## Delivery checkpoint

Changes are staged as a single coherent GitHub branch batch rather than
pushing incomplete file-by-file commits. The current execution environment
does not provide Windows Python 3.14, native SMB, a checked-out repository
worktree, or a locally installed PyArrow wheel. Do not infer local, native,
installed-wheel or hosted success without separate evidence. Log exact-head
CI and Codex results on PR #293 once available, without merging.


## 2026-11-09 Windows repair checkpoint

Continued the existing #293 branch from `b67c270f` in an isolated worktree;
the parent checkout and WP04 remain untouched. Fixed all six strict-Pyright
errors reproduced from the hosted Policy job. Windows execution additionally
exposed three failures in the existing Parquet tests: integer column-axis names
became strings, object lookup `pd.NA` values became `None`, and attribute-only
Windows directory handles did not prevent POSIX-style renames.

The manifest now preserves typed column-axis names and the positions of object
`pd.NA` values. Restoration validates those positions and refuses to replace
non-null values. Windows publication handles request GENERIC_READ and deny
FILE_SHARE_DELETE; the existing native directory-swap regression now passes.
No new MCP surface is needed: this repairs the existing Python snapshot API.

Validation on Windows Python 3.14.6 / PyArrow 24.0.0:

- `python -m pytest tests/processors/tuflow/test_processor_collection_parquet_bundle.py -q`:
  21 passed, two Linux-only safety tests skipped.
- Related collection/info/coverage/channel-selection/persistence tests: 57 passed.
- Ruff lint/format and strict Pyright on the PR's four Python implementation
  files and Parquet test module: passed with the declared development typing dependencies.
- `python repo-scripts/build_library.py --skip-pip`: built and verified local
  wheel `26.11.9.1`. Initial build refused missing QML resources; initialised only
  the required `qgis-resources` and `tests/test_data` submodules, without LFS downloads.
- Installed that wheel into an isolated temporary package directory, verified
  the imported module path, and ran a copied Parquet test module against it:
  21 passed, two Linux-only skips. No environment or installed package replacement.
- Documentation checker and diff whitespace checks: passed.

Delivery: code, version and this record are batched for the existing PR branch;
the ignored wheel remains local. Hosted results will be recorded on the PR at
the final pushed SHA. NTFS acceptance above does not establish mapped-drive/UNC
or native SMB acceptance. Independent final-head review and WP04 integration
remain separate gates. No merge, release or issue closure is authorised.
Next review remains 2026-11-15.


### Review follow-up: bind verification to consumed Parquet bytes

A new review thread identified a checksum/read race after the first repair
checkpoint. Processor and lookup reads now copy selected members to an owned
on-disk temporary file in bounded chunks, hash the copied bytes, enforce the
manifest byte length, and give PyArrow that same open temporary handle. No
mutable public pathname is reopened after successful verification. This adds
temporary disk I/O for selected members, without buffering a whole member in RAM.

Two parameterised regressions replace the public processor/lookup file after
verification and before Arrow reads it; restored values retain the original
verified snapshot. Updated selective-read instrumentation still proves unrelated
members are not loaded. Windows source tests: 23 passed / two Linux-only skips;
installed wheel 26.11.9.2: the same 23 passed / two skips. Ruff and strict Pyright
pass; the package build verifies. Earlier 57 related tests remain unchanged.

Review-thread resolutions link these fixes and the existing RangeIndex,
no-clobber, pinned-directory/generation, cleanup and EOF regressions. Linux-only
identity tests and UNC/SMB acceptance are not relabelled as Windows passes.
Final-head hosted validation and an independent fresh review remain separate.

### 9 November 2026 — WP04 reconciliation

The combined tree starts from draft #251 `feae324a`, preserving its CI and WP04 changes while incorporating #293 as the first commit parent without force pushing. Parquet now records `selected_unusable` and canonical native source identifiers, retains empty-invalid selections on restore, and renews original source signatures when a processor reprocesses a changed file. Three regression tests cover these contracts. New combined-head hosted CI, fresh independent Codex review and mapped/UNC/SMB native acceptance remain outstanding; no integration into #251 or main.

### Corrected focused CI finding

Hosted CI at `9fa26311` reported that the original destination validator rejected a processed-empty WP04 `selected_unusable=True`, `input_health=INVALID` frame. This guard now permits only the explicit source-present invalid case; unprocessed or other empty processors still fail closed, with a negative regression. Hosted and source/installed-wheel acceptance on this correction are pending, as are native mapped/UNC/SMB checks and independent final review.

### Independent WP04 post-restoration review (9 November 2026)

The exact-parent Codex review identified two P2 integration issues. The empty-invalid export gate was fixed first; the import frame contract now accepts only the explicit `selected_unusable=True` and `input_health=INVALID` outcome, subject to exact schema/hash checks. The Parquet manifest also retains native WP04 `source_locations` / `usable_source_locations` and Q/H `source_channels` / `usable_source_channels`, so filtering **after complete restoration** has the original source-presence and unusable-sample context. Restore validates the allowlisted inventories, source subsets and usable-subset invariants before assigning them; new round-trip and malformed-inventory regressions cover the change. Fresh exact-head tests and review remain required.


### 2026-11-09 final Windows native acceptance

Native verdict: **BLOCKED** pending the intermittent UNC cleanup observation below, for corrected code revision
`e04c625afb12f273e1882557fed33380a14a088e`, based on current #293
`65db31f36909e2e1854a839dc9bc7b5b07c1d64c`; #251 remains
`92351854625dae81001c033306969f73eff4a7f7`.
The upstream empty-invalid import and native-inventory repairs superseded local
repairs during testing. Current-head acceptance then reproduced an overwrite
regression: the double-escaped generation regex rejected valid `.versions/<uuid>`
paths. Restore the original regex; retain the existing regression assertion.
No WP04 changes or alternate implementation are introduced.

Environment: Windows 10 build 19045, normal Python 3.14.6, pandas 3.0.5,
PyArrow 24.0.0. Isolated worktree; source and independently installed wheel
26.11.9.3 are verified separately. All 252 tracked library Python/JSON files
match the installed wheel byte-for-byte. Wheel SHA-256:
`29bee9c347835552a05605b50fb15de4ef4dcd5d3ed7e228b8738373302d8919`.
The normal installed package was not replaced.

| Context | Local NTFS focused regression | Mapped SMB Parquet regression | UNC SMB Parquet regression |
| --- | --- | --- | --- |
| Source | 95 passed, 2 Linux-only skips | 30 passed, 2 Linux-only skips | Initially 30 passed / 2 skips; final check 29 passed / 1 failed / 2 skips |
| Installed wheel | 95 passed, 2 Linux-only skips | 30 passed, 2 Linux-only skips | 30 passed, 2 Linux-only skips |

Focused command: `python -m pytest tests/processors/tuflow/test_processor_collection_parquet_bundle.py tests/processors/tuflow/test_wp04_processor_contracts.py tests/processors/tuflow/test_collection_channel_selection.py -q`.
The mapped/UNC runs execute the Parquet module with a unique disposable
`--basetemp`; installed tests are copied outside the source checkout.
Ruff lint/format, strict Pyright and verified package build pass.

Real tutorial inputs from test-data revision `f3c49e867cd8a165e031217a148293a8968c2d88`
include Q/H/Cmx/EOF from Module 03 and PO/POMM from Module 01: six processors,
543/543/543/3/6/3 rows. Each source/wheel context runs real round-trip,
no-clobber, explicit overwrite, external writer-lock exclusion, original-member
SHA integrity and compacted-lookup restoration on NTFS, a genuine mapped SMB
drive and its corresponding UNC share. Actual server/share names are omitted.
Dedicated directories use the `pr293-native-20261109-final-*` prefix.
All input hashes remain unchanged. Processor and lookup Parquet members have
identical SHA-256 across source/wheel and all three access paths. Manifests
match after excluding only the expected `created_at_utc` timestamp.

The synthetic regression module covers nullable/categorical dtypes including
unused ordered categories, source fingerprints, historical/offline restoration,
health/rejections, INVALID/ABSENT selection, shortened/ambiguous EOF aliases,
interrupted staging/overwrite, simulated rename failure, corrupt/missing members,
stale semantics, competing publication and owned staging/lock cleanup.
WP04 regression coverage includes truncated Q/PO rejection, real tutorial ccA
no-match SKIP and state recovery, EOF no-match, partial-invalid classifications.
Additional native checks in all six context/filesystem cells verify RangeIndex,
string Index, DatetimeIndex and MultiIndex, plus two genuinely concurrent child
writers: exactly one publishes and one refuses; restoration and cleanup pass.

Limitations: Windows symlink creation is unavailable (WinError 1314 locally,
access denied on SMB). SMB junction creation is unsupported. These are unexecuted
link scenarios, not passing safety cases. Actual local junction destinations
are refused without changing target bytes; Windows directory-share-delete guard
regressions pass on NTFS and SMB. Linux-only directory-descriptor tests remain
skipped. Fault injection is synthetic, not a machine crash or SMB disconnect.
No broad suite, redundant hosted rerun, merge, release or issue closure.

Private reproducible scripts, JUnit, per-input/member SHA inventories and reports
are retained outside the repository at `E:/Github/pr293-native-evidence`.
Native acceptance remains blocked on confirming UNC cleanup semantics; fresh
final-head hosted CI and independent Codex review also require separate
confirmation before integration. Correction and evidence form one batch for
existing #293; preserve #251/main. Next review: 2026-11-15.


#### Final delivery check: do not erase the intermittent UNC failure

At documentation-only revision `44d86c24837b856feb2a5643f834fc5f2bb97865`,
the same six regression runs produced **309 passed, 1 failed, 12 skipped**.
Source UNC `test_directory_no_clobber_overwrite_uses_immutable_generation`
failed because `.publish.lock.exists()` returned True immediately after a
successful overwrite. The completed dataset restored and the lock was absent
on subsequent direct inspection/listing. An unchanged focused rerun passed
(1 passed / 31 deselected). The installed-wheel UNC run passed 30/2.
Both source/wheel mapped runs passed 30/2 and both NTFS focused runs passed 95/2.
Do not replace the failure with the successful rerun or weaken the assertion.
Client SMB directory/file-info cache lifetimes are 10 seconds (negative cache
5 seconds), but caching is only a hypothesis until reproduced and distinguished
from an actual live lock. Evidence remains in `delivery-source-unc.xml`.
Next action: diagnose immediate UNC lock visibility versus exclusive creation,
then repeat the unchanged cleanup/concurrency acceptance on the agreed head.
No data loss or source/member corruption was observed.


UNC follow-up probe: 120 fresh snapshot/overwrite cycles all restored; zero
post-cleanup `exists()` observations and zero failures to acquire a new lock
using `O_CREAT|O_EXCL`. This does not reproduce or explain the preserved failure;
`cache-probe.json` retains every observation. Verdict remains BLOCKED pending
root cause or an agreed, documented SMB acceptance boundary.

### 2026-11-09 synchronous UNC cleanup capture

Implementation and remote #293 remain at `9d8a82b730be9ddedf9b7941f2f48f1624c5cb1a`;
#251 remains `92351854625dae81001c033306969f73eff4a7f7`. No production code, test,
assertion, installed package or remote branch was changed during this investigation.

Private harness `E:/Github/pr293-native-evidence/observe_cleanup.py` intercepts
only the existing cleanup test's `.publish.lock.exists()` call, returns its exact
original boolean, and synchronously records positive observations before returning
to the unchanged assertion. Capture includes nanosecond timestamps, PID, literal
path, repeated exists, `os.stat(..., follow_symlinks=False)` or exact exception,
directory listing, and `O_CREAT|O_EXCL|O_WRONLY`. A successfully created diagnostic
lock is closed and removed only after matching its file identity; failed exclusive
creation never removes a lock. Subsequent samples use requested delays of 10 ms,
100 ms and one second; diagnostic I/O adds elapsed time. A separate child process
then repeats read-only exists/listing. A local positive control preserved an
unowned lock and returned the original True to the caller.

Ten fresh-process full Parquet-module runs (five UNC and five mapped basetemp)
passed **300 tests / 20 Linux-only skips**, with no positive cleanup observations.
Pytest canonicalises mapped basetemp paths to UNC. To retain literal path spelling,
private `direct_cleanup.py` calls the unchanged fixture body and unchanged test
function directly; these are diagnostic calls, not additional pytest counts.
Literal UNC failed on its first call; literal mapped failed on call two, then
again on call eight in a separate process. Total direct calls: eight passed and
three original cleanup assertions failed. No assertion was caught and relabelled
as passing.

At the first positive observation on **both genuine UNC and mapped SMB**:

- Immediate repeated `exists()` also returned True.
- Non-following `stat` returned `FileNotFoundError`, errno 2 / WinError 2.
- `os.listdir` returned `.versions`, `manifest.json`, `processors`, with no lock.
- Exclusive creation succeeded; only the diagnostic-owned lock was removed.
- Later samples again sometimes returned True while stat/listing/exclusive-create
  continued to report no outstanding lock. Diagnostic create/delete operations
  can themselves affect later metadata, so the initial sample is decisive evidence.
- The independent child sometimes also saw True while its listing omitted the lock.

This directly supports stale existence metadata for the captured observations,
rather than a genuinely outstanding writer lock at exclusive creation. It does
not identify the responsible Windows/SMB cache layer or establish an accepted
cleanup contract. The original delivery failure and assertion remain preserved.
Raw captures: `observe-direct-unc.jsonl`, `observe-direct-mapped.jsonl`,
`observe-direct-mapped-confirm.jsonl`; direct-call summaries and ten JUnit reports
are retained in the same private evidence directory. Connection details remain
outside the repository.

Source concurrency/index checks were rerun on genuine UNC and mapped paths:
each produced one publication and one refusal, restored correctly, and cleaned
owned staging/locks; four index contracts passed per path. An initial invocation
of the external script imported the older normal installed package and failed
before publication (`to_parquet` unavailable); those attempts are not acceptance
evidence. Corrected `python -c`/`runpy.run_path` invocations verified the module
origin under this worktree before executing the checks and their child writers.

Hosted run [42514379086](https://github.com/Chain-Frost/ryan-tools/actions/runs/42514379086)
now has all six jobs terminal and successful: Ruff, Policy, Package, POSIX security,
Hosted tests and MCP smoke. No hosted rerun was requested.

Next action: decide and validate the SMB existence/cleanup acceptance contract
from these captures while retaining the assertion until understood. Separately,
the active filtered-location P2
[review thread](https://github.com/Chain-Frost/ryan-tools/pull/293#discussion_r4507456784)
still needs its focused regression and verified fix; this investigation does not
resolve it. Native acceptance remains BLOCKED; PR remains draft. This handoff is
local and uncommitted; no push, merge, release or issue closure. Next review:
2026-11-15.


### 2026-11-09 revised-head native acceptance

Actioned [the maintainer handoff](https://github.com/Chain-Frost/ryan-tools/pull/293#issuecomment-6729859375)
on exact code `7ac3c0c527ad067b913da67c2b35ffd7f853ce18`, in the existing isolated
Windows worktree. Preserved the uncommitted synchronous-capture record above and
all raw failure captures. Earlier BLOCKED verdicts remain historical evidence;
this verdict applies to the revised exclusive-create availability contract.
No production locking, cleanup or no-clobber implementation changed here.

Windows 10 build 19045 / normal Python 3.14.6, pandas 3.0.5, PyArrow 24.0.0,
pytest 9.1.1. Independent no-bump build of 26.11.9.3 verifies with SHA-256
`964c27043fd4a5f44e2d3b89aeb9f192cb4c12ecbbdb3a750624f43cf4d3d78f`.
All 252 library Python/JSON files match the isolated installed wheel byte-for-byte;
source and wheel import origins were recorded independently. The normal installed
package was not replaced. Accepted PyTuflow 1.3.0 archive/direct-URL hash is
`4bfcd24734349f9e0dcf60776ee06a2e17953ac342f40e3611b0e2aae33b30e3`, identical
to the repository artifact at source `887534ea8c15b80d185a38a7ead1d42ab42a7167`.
The verifier's literal archive-location check rejects the installation URL from
another worktree; archive bytes/hash and recorded installed hash independently match.

| Context | NTFS focused WP04/selection/Parquet | Mapped SMB Parquet | Genuine UNC Parquet |
| --- | --- | --- | --- |
| Source | 99 passed / 2 Linux-only skips | 34 passed / 2 skips | 34 passed / 2 skips |
| Independent installed wheel | 99 passed / 2 Linux-only skips | 34 passed / 2 skips | 34 passed / 2 skips |

Total **334 passes, zero failures, 12 Linux-only skips**. Tests are the three
focused modules named above on NTFS, and the Parquet module on SMB, with fresh
unique `--basetemp` and JUnit reports. Installed tests were copied outside the
checkout, retaining only the needed real ccA fixture. Revised tests verify
exclusive-create lock availability and preservation of an existing unowned lock;
PO/Q preselection now distinguishes FILTERED, INVALID and ABSENT.

Additional original native checks pass in all six source/wheel/storage cells:

- Real six-processor Q/H/PO/POMM/Cmx/EOF round trip, compacted lookup, no-clobber,
  external writer-lock exclusion, explicit overwrite and original-member integrity.
- Four index contracts and actual two-process initial publication: exactly one
  publisher and one refusal, successful restore and owned staging cleanup.
- Actual overlapping overwrite processes: hold the first writer after exclusive
  lock acquisition, refuse the competing writer, verify the old snapshot remains
  readable/unchanged, release the first writer and restore its new generation.
- Ten literal-path calls per cell to the actual revised overwrite/cleanup test,
  and ten existing-unowned-lock preservation checks: **60 cycles each** overall.
  This retains mapped spelling independently of pytest basetemp canonicalisation.

All original six input SHA-256 values match the retained prior native capture.
Processor and lookup payload hashes match across all six comparable snapshots;
manifest parity is exact after excluding only `created_at_utc`. Initial source/wheel
harness runs used different working directories, producing expected relative-path
lookup differences; the three wheel real-input runs were repeated with the same
base directory and explicit verified isolated imports before asserting byte parity.
No source/snapshot input was regenerated or edited.

Raw reproduction scripts, logs, six JUnit files, module identities, per-input/member
hashes and parity report remain private under
`E:/Github/pr293-native-evidence/revised-7ac3c0c5/`, alongside the retained
`delivery-source-unc.xml` and synchronous `observe-direct-*.jsonl` captures.
No share/server identity is published. Reproduction uses `run_revised.py`,
`run_overwrite_revised.py`, `run_samebase.py` and `check_revised.py` in the parent
evidence directory. Link privilege, unsupported SMB junction, Linux-only descriptor,
crash/disconnect and scale boundaries above remain; no broader acceptance is inferred.

Delivery: documentation-only acceptance batch on existing #293. PR was found ready
for review and restored to **draft** as the handoff explicitly requires. #251/main
are unchanged by this task; no merge, release or issue closure. Exact-code hosted
run 42543915538 is still running at this checkpoint; prior independent no-major-issue
review is reported in the linked handoff, not rerun here. Next action: verify final
documentation-head CI, then seek explicit integration authorisation. Next review:
2026-11-15.
