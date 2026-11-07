# WP05 — TUFLOW processor persistence and ccA WAL safety

| Field | Value |
| --- | --- |
| Status | Active; draft PRs, hosted validation in progress |
| Owner | WP05 delivery agent / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | `main` at `5ad632cc9a17b302ad3f4f2b55315a1e2e8d1d51`; separate GitHub branches per issue, no main edits |

## Outcome and scope

Bounded work pack from [allocation #240](https://github.com/Chain-Frost/ryan-tools/issues/240), with first delivery
[#222](https://github.com/Chain-Frost/ryan-tools/issues/222) followed separately by
[#226](https://github.com/Chain-Frost/ryan-tools/issues/226). Shared tracker [#212](https://github.com/Chain-Frost/ryan-tools/issues/212).

- **#222:** [Draft PR #254](https://github.com/Chain-Frost/ryan-tools/pull/254),
  branch `fix/wp05-222-cca-wal-consistent-read`: read-only WAL-aware ccA GeoPackage processing, bounded SQLite
  busy handling, no writable or immutable fallback, geometry metadata, deterministic layer selection, optional
  metric handling, disposable active-writer and corruption regressions.
- **#226:** [Draft PR #261](https://github.com/Chain-Frost/ryan-tools/pull/261),
  branch `fix/wp05-226-processor-hdf-cache`: real processor class/filter/frame/lookup snapshot fidelity,
  Arrow IPC in compressed HDF5, version and source validation, atomic no-clobber publication, optional PyTables
  dependency, focused on-disk tests, local API documentation.

## Boundaries and coordination

- WP04 owns [#223](https://github.com/Chain-Frost/ryan-tools/issues/223) processor reset/success semantics.
  WP05 does not change the generic lifecycle or promise to save unprocessed/empty processors.
- WP11 owns [#229](https://github.com/Chain-Frost/ryan-tools/issues/229) materialisation/performance and
  separate PO Parquet cache. The HDF cache is scoped strictly to processor collection snapshots.
- Only WP05 edits `ccAProcessor.py` and `processor_collection.py` in these PRs. Both branches are
  independent and each targets main; neither is merged.
- This execution environment lacked a usable remote Git checkout and Python 3.14, so source edits and commits
  were published through the authenticated GitHub contents interface. Hosted Windows/Python 3.14 CI is the
  executable repository-validation source; isolated local synthetic worktree work must not be mistaken
  for a full repository checkout.

## Validation and delivery

- **#222**: current new-head Fast PR Ruff/whitespace previously passed; hosted full test and package checks passed
  on an earlier PR revision, while Policy strict Pyright identified untyped test fixtures. Type annotations have
  been corrected, with final exact-head CI still pending.
- **#226**: on-disk Q/PO/POMM roundtrip, dtype fidelity, compact lookup, overwrite preservation, corruption and
  stale-source regressions committed; hosted checks pending after Ruff adjustments.
- **Native acceptance not run**: dedicated Windows mapped-drive and UNC/SMB ccA GeoPackage with an active writer,
  busy locks, WAL lifecycle and permissions; native installed-cache and copied-wrapper smoke. Do not imply
  these are covered by CI's local disposable fixtures.
- Both PRs are drafts and pushed. No main merge, auto-merge, package version bump or release.
- Focused validation for handoff: `python -m pytest -q tests/processors/tuflow/maximums_1d/test_cca_processor.py`
  for #222; `python -m pytest -q tests/processors/tuflow/test_processor_collection_persistence.py`
  for #226. Python 3.14 is required. Run `python -m ruff check`, `python -m ruff format --check`,
  strict Pyright on changed Python files, `python repo-scripts/check_documentation.py` and
  `python repo-scripts/build_library.py --no-bump --skip-artifacts` (or CI package equivalent).

## Next action

Reconcile final-head draft PR #254/#261 CI, address any failing focused policy/pytest checks without crossing
other pack boundaries, then perform or obtain independent Windows/SMB native acceptance and maintainer review.
Do not merge without separate authorization.

## Progress

### 2026-11-08

Read allocation/handoff, #222, #226, repository architecture and work records. Created independent draft
PRs #254 and #261. Added regressions and explicit source/file mutation contracts. Automated validation
and native acceptance remain distinct gates.
