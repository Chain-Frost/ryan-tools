# WP05 — TUFLOW processor persistence and ccA WAL safety

| Field | Value |
| --- | --- |
| Status | Bounded WAL retry passes native source/wheel checks; final CI/review pending; HDF remains separate |
| Owner | WP05 delivery agent / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | `main` at `5ad632cc9a17b302ad3f4f2b55315a1e2e8d1d51`; separate GitHub branches per issue, no main edits |

## Outcome and scope

Bounded work pack from [allocation #240](https://github.com/Chain-Frost/ryan-tools/issues/240), with first delivery
[#222](https://github.com/Chain-Frost/ryan-tools/issues/222) followed separately by
[#226](https://github.com/Chain-Frost/ryan-tools/issues/226). Shared tracker [#212](https://github.com/Chain-Frost/ryan-tools/issues/212).

- **#222:** [PR #254](https://github.com/Chain-Frost/ryan-tools/pull/254), stacked directly on [WP01 parent #251](https://github.com/Chain-Frost/ryan-tools/pull/251),
  branch `fix/wp05-222-cca-wal-consistent-read`: read-only WAL-aware ccA GeoPackage processing, bounded SQLite
  busy handling, no writable or immutable fallback, geometry metadata, deterministic layer selection, optional
  metric handling, disposable active-writer and corruption regressions.
- **#226:** [PR #261](https://github.com/Chain-Frost/ryan-tools/pull/261), stacked on #254,
  branch `fix/wp05-226-processor-hdf-cache`: real processor class/filter/frame/lookup snapshot fidelity,
  Arrow IPC in compressed HDF5, version and source validation, atomic no-clobber publication, optional PyTables
  dependency, focused on-disk tests, local API documentation.

## Boundaries and coordination

- WP04 owns [#223](https://github.com/Chain-Frost/ryan-tools/issues/223) processor reset/success semantics.
  WP05 does not change the generic lifecycle or promise to save unprocessed/empty processors.
- WP11 owns [#229](https://github.com/Chain-Frost/ryan-tools/issues/229) materialisation/performance and
  separate PO Parquet cache. The HDF cache is scoped strictly to processor collection snapshots.
- WP05 owns `ccAProcessor.py` and `processor_collection.py` changes in these PRs. The review sequence is
  `main` ← #251 (WP01/WP02) ← #254 (ccA) ← #261 (HDF); #254 and #261 are **not** merged into #251.
  Preserve their histories and review scopes. After all children are integrated and accepted, #251 alone is
  intended for one explicitly approved squash merge to main.
- This execution environment lacked a usable remote Git checkout and Python 3.14, so source edits and commits
  were published through the authenticated GitHub contents interface. Hosted Windows/Python 3.14 CI is the
  executable repository-validation source; isolated local synthetic worktree work must not be mistaken
  for a full repository checkout.

## Validation and delivery

- **#222**: prior unstacked head `c34ccea9` passed all six hosted jobs (2,530 tests passed; 14 skipped;
  8 deselected), including Policy/strict Pyright, package/wheel and Ruff. Exact-head combined-stack CI must be
  assessed separately; previous green checks are not evidence for that integration commit.
- **#226**: on-disk Q/PO/POMM roundtrip, dtype fidelity, compact lookup, overwrite preservation, corruption and
  stale-source regressions committed; hosted checks pending after Ruff adjustments.
- **Native acceptance not run**: dedicated Windows mapped-drive and UNC/SMB ccA GeoPackage with an active writer,
  busy locks, WAL lifecycle and permissions; native installed-cache and copied-wrapper smoke. Do not imply
  these are covered by CI's local disposable fixtures.
- Both PRs are open and pushed (currently not drafts). No main merge, auto-merge, package version bump or release.
- Focused validation for handoff: `python -m pytest -q tests/processors/tuflow/maximums_1d/test_cca_processor.py`
  for #222; `python -m pytest -q tests/processors/tuflow/test_processor_collection_persistence.py`
  for #226. Python 3.14 is required. Run `python -m ruff check`, `python -m ruff format --check`,
  strict Pyright on changed Python files, `python repo-scripts/check_documentation.py` and
  `python repo-scripts/build_library.py --no-bump --skip-artifacts` (or CI package equivalent).

## Next action

Reconcile the stacked current-head CI of #254 and #261; resolve any focused failures with the existing PR owners,
then perform or obtain independent Windows/SMB native acceptance and maintainer review.
Do not merge without separate authorization.

## Progress

### 2026-11-08

Read allocation/handoff, #222, #226, repository architecture and work records. Created independent draft
PRs #254 and #261. Added regressions and explicit source/file mutation contracts. On 2026-11-08,
merged the #251 branch into #254 and #254 into #261 without force-push, retargeted both child PRs,
and revised their names and #251's consolidated title; no merge to main. Automated validation and
native acceptance remain distinct gates.

### P1 review correction: closed WAL mode without sidecars

Codex's [#254 P1 review finding](https://github.com/Chain-Frost/ryan-tools/pull/254#discussion_r4502825951) identified that SQLite `mode=ro` can create new `-wal` and `-shm` files for a **closed, checkpointed WAL-mode** GeoPackage even when both sidecars are initially absent. Unlike a rollback-journal database, that case is not safe to open in a writable result directory.

The processor now reads the fixed SQLite database-header journal markers **before opening SQLite**. If the header denotes WAL mode but the pair of existing `-wal`/`-shm` sidecars is incomplete or absent, it **fails explicitly without opening a connection or creating files**. An active WAL writer with both sidecars remains supported and its committed snapshot is read with `mode=ro`. No unsafe `immutable=1` bypass or writable fallback is added; closed WAL-mode results lacking sidecars must be changed to a rollback journal by their producer or otherwise processed under a separately validated read-only strategy.

A disposable SQLite regression reproduces the closed-WAL case and verifies source bytes and no sidecars after failed read. Source header bytes, actual WAL lifecycle and results are tested without a licensed solver. Exact-head Windows/Python 3.14 CI and native Windows/SMB acceptance remain separate, pending results; **the P1 must not be marked fully accepted from static changes alone**.

### Follow-up review finding — writer shutdown between preflight and SQLite open

[Codex P2 on #254](https://github.com/Chain-Frost/ryan-tools/pull/254#discussion_r4505393356) identifies a **remaining TOCTOU source-mutation risk**. The new header check in `3394578f` prevents WAL/SHM creation if a closed WAL-mode database has no sidecars *at preflight*. However, if an active writer closes/checkpoints **after** the sidecar check but **before** the read-only SQLite connection opens, SQLite can create its own WAL/SHM sidecars. The earlier P1 fix is therefore **partial and unaccepted**, not a full solution to #222's no-source-mutation requirement.

**Required resolution:** demonstrate a portable no-create read contract that preserves committed active WAL data without unsafe `immutable=1` stale reads, or explicitly document and agree to a narrower unsupported/failed-read case. Add deterministic shutdown-between-check-and-open regression, check original bytes and all sidecars, then obtain Windows/SMB validation and exact-head CI. The P2 must be resolved before #254/#261 feature-branch integration. Both children have been returned to **draft** pending this gate. The [blocking comment on #254](https://github.com/Chain-Frost/ryan-tools/pull/254#issuecomment-6718649064) records the handoff. Do not close #222 or #226 or merge into `main`.

### P2 writer-shutdown race correction: owned SQLite read snapshot

Codex P2 found that an active WAL writer can close between the preflight sidecar check and a SQLite mode=ro open, allowing the reader to recreate -wal/-shm beside the source. The original header check does not close this race.

The SQLite reader now copies **both rollback and WAL-mode** source databases (plus any existing WAL) to invocation-owned local scratch, checks source inode/size/mtime metadata before and after copying, and lets SQLite build any required shared-memory index only inside scratch. A closed checkpointed WAL-mode database is read from its staged main file. A stable active WAL copy retains committed data; incomplete or changing sources fail closed. No immutable=1 or writable direct-path fallback is used for WAL-mode files.

A deterministic regression now shuts down the active writer after the owned copy but before the SQLite connection opens. It verifies the previously committed value, unchanged source bytes after legitimate writer checkpoint, and no reader-created sidecars. The closed-WAL case now requires a successful staged read and unchanged source bytes.

Performance and acceptance limits: WAL-mode reads copy the entire main database and WAL to local scratch, adding disk/network I/O and temporary space. Before/after signatures detect ordinary changes but are not a distributed lock nor a cryptographic full-byte consistency proof against same-metadata rewrites. Locked, changing, or inaccessible sources fail safely. Rollback-journal databases retain their previous direct read-only connection. Real Windows/UNC/SMB writer-shutdown and large-result performance acceptance plus current-head CI remain required before integration. SQLite WAL across separate hosts on a network file system is not generally supported.

### Additional Codex P2: journal-mode transition during direct read

A fresh [review finding](https://github.com/Chain-Frost/ryan-tools/pull/254#discussion_r4505510849) identified a separate transition: the source could switch from rollback journal to WAL *after* header inspection, before direct `mode=ro` reading, and thereby create reader-owned source sidecars. The owned snapshot is now used for **all SQLite ccA source modes**, avoiding SQLite connections to the source path entirely. A deterministic test transitions the source to WAL immediately before opening the already staged copy; it reads the earlier stable snapshot without recreating source sidecars or overwriting the legitimately updated source. The prior direct-rollback fast path is intentionally removed.

**Cost boundary:** this is an I/O-heavy safety choice for large result files; its memory footprint is bounded by streaming `shutil.copyfile`, but local scratch disk usage equals the main database plus WAL. Changing source stat signatures or inconsistent copied SQLite files fail closed. Validate real large-file/SMB behaviour and documented costs before feature-branch integration. Do not describe a running writer's read as a globally atomic, latest-timepoint snapshot.

### Hosted two-case acceptance repair

The first fully refreshed #254 Windows hosted run at `2243d6a5` passed Ruff/Policy/Package/MCP/security but found **two real regressions** among 2,791 passing tests. The rollback→WAL test used a `with sqlite3.connect(...)` context that commits but does **not close** the SQLite connection; it now closes explicitly before asserting that the last writer removed both WAL sidecars. More importantly, the owned source copy bypassed SQLite's exclusive rollback-journal lock and could read an uncommitted database snapshot. The source preflight now includes the rollback `-journal` in its before/after file signatures and **fails closed whenever a journal is present**, rather than returning uncommitted values. The existing exclusive-writer test must remain a refusal. The acceptance boundary for nonstandard `journal_mode=OFF` or unsupported network locks is not expanded. Confirm exact-head hosted and Windows/UNC/SMB behaviour before accepting this work pack.

The actual [Codex P2 rollback-to-WAL transition finding](https://github.com/Chain-Frost/ryan-tools/pull/254#discussion_r4505510849) is addressed by staging all source modes instead of opening the source directly; previous stored drafts incorrectly used a provisional comment ID.


### Native follow-up: leave ccA styling to the TUFLOW QGIS plugin

The maintained `apply_qgis_styles_to_results.py` wrapper no longer selects
`Results1D` shapefiles/GeoPackages for ccA styling. Standalone `1d_ccA_L`
files were already absent from the default mapping. The TUFLOW QGIS plugin
now provides the built-in ccA style, so this script leaves both forms alone.
Existing result files and QML sidecars are preserved; raster mappings remain.
The wrapper revision is `2026-11-08.1`.

Validation on Windows 10/Python 3.14.6: seven existing styling tests pass;
Ruff lint/format, strict modified-file Pyright, compilation and documentation
links pass. Disposable source and extracted-wheel probes verify zero SQLite
opens, unchanged hashes for ccA/Results1D inputs and existing QML sidecars,
and successful depth-raster QML creation in root and nested folders. Copied
wrapper `--help` succeeds against source and wheel. This mapping removal
requires no new MCP exposure; the existing public workflow remains the same.

The no-bump build retains package version `26.11.8.2`; wheel SHA256 is
`59ad1ed511ae7633e796db9f43744ba357054e55fa88f642a3e5d15cc5a63f5b`.
The wheel is a local ignored build artifact, not a published release or a
global installation. The existing source checkout and submodules remain untouched.
Delivery is on the existing draft #254; current-head hosted checks remain to
be reviewed. This styling change does not clear WAL/share shutdown acceptance
or the missing-PyTables HDF gate. Native reports remain under #254/#261.
Next review: 2026-11-15. No parent/main merge, release or issue closure.

### 2026-11-08 completion follow-up: bounded source-change retry

Refreshed #254 from #251 at `dc6d74455e092d3611b975f0fc3975a977578ecc`
without force-push, preserving styling commit `71c84c80`, the consolidated CI,
vendored PyTuflow wheel and all accepted parent work. Main remains
`c1a66970c8ed228ab94404b4f747a40c755847cd`; no child is integrated into #251.

A changed copy is discarded before retrying, with at most three complete copies
and 50/100 ms backoff. A disappeared WAL during checkpoint is retried only when
source signatures changed. All signature, journal and incomplete-sidecar checks
remain intact. Failed attempts never reach SQLite and owned scratch is removed.
No immutable/direct-source SQLite fallback is added.

Windows 10/Python 3.14.6: **23 focused pytest PASS, no skips**; **15 native direct
checks PASS** (five each NTFS, mapped, UNC): shutdown during copy, shutdown after
copy, rollback-to-WAL transition, detected-change recovery and three-copy
exhaustion. Successful shutdown reads preserve committed 99 and source bytes,
with no source sidecars recreated. Initial harness assertions assumed exactly two
attempts; SMB legitimately needed a third because mtime updates were delayed.
Those harness results are superseded after allowing the specified three-attempt
bound. An explicit mtime change makes the detected-change test deterministic;
same-metadata rewrites remain outside the stat-signature guarantee.

Ruff/strict modified-file Pyright and Loguru policy pass. Built/verified package
26.11.8.3; SHA256 `215c9e289d5da95fa4466f6582c8062fa91bf0b78c293d1590c1150b7db8cc58`.
Only tests/test_data and qgis-resources were initialized for tests/build; no
unsorted access. Private harness/evidence: `E:/Github/wp0508-completion-20261108`.
The prior native report on `cd6fdb28` remains historical; successful share-shutdown
coverage above supersedes its two unsuccessful shutdown assertions only.

Copy-cost boundary: one attempt's scratch is released before the next, so peak
copied payload remains DB+WAL plus SQLite overhead; I/O may triple. Retain the
prior 64 MiB synthetic copy measurements; representative multi-GB performance,
cross-host WAL concurrency and same-metadata rewrites are NOT RUN/unsupported.
MCP assessment: existing generic processor inspection remains appropriate; the
retry does not warrant a new tool. Next: exact-head hosted CI and fresh Codex
review, then refresh/test #261. No issue closure or integration is claimed yet.
