# WP05 — TUFLOW processor persistence and ccA WAL safety

| Field | Value |
| --- | --- |
| Status | Blocked on unresolved #254 writer-shutdown race; native HDF acceptance not run |
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

### 2026-11-08 local-agent gate audit - HDF acceptance not run

Read native handoff https://github.com/Chain-Frost/ryan-tools/pull/261#issuecomment-6718682286 and verified live #254 at `f70e69519499dc071d03863c6d0b9233f0536653`, with the writer-shutdown race thread https://github.com/Chain-Frost/ryan-tools/pull/254#discussion_r4505393356 still unresolved. Both the original closed-WAL review and shutdown-race review remain unresolved; the parent canonical WP05 record explicitly marks the P1 correction partial/unaccepted.

#261 remains at code `7df8202eef2c50b8af5aecb0378bfd18be75d030`, draft, stacked on #254. **BLOCKED:** the handoff explicitly requires correction and review of #254 before refreshing the HDF branch and running native acceptance. Therefore no HDF source/native/installed-wheel/cache/mapped/UNC tests, build or dependency changes were performed, and no source-immutability claim is made. This documentation-only gate audit does not refresh implementation or incorporate the child anywhere.

Next action: #254 owner corrects the no-create WAL contract and obtains review; then refresh this existing branch without force-push and execute the stated Q/PO/POMM HDF acceptance matrix on its new exact SHA. Next review 2026-11-15. No merge, auto-merge, release, issue closure, production operation or unsorted inspection.
