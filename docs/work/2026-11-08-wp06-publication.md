# WP06 — owned staging in standalone output workflows

**Status:** Active — #232 draft PR #258 stacked on WP01/#251; #197 archive draft delivery stacked on #258; #200 requires GDAL environment.
**Owner:** WP06 delivery agent.
**Updated:** 2026-11-08 (Perth).
**Next review:** 2026-11-15 or when current-head CI/native validation is available.
**Allocation:** [#240](https://github.com/Chain-Frost/ryan-tools/issues/240).
**Issues:** [#232](https://github.com/Chain-Frost/ryan-tools/issues/232), [#197](https://github.com/Chain-Frost/ryan-tools/issues/197), [#200](https://github.com/Chain-Frost/ryan-tools/issues/200).

## First delivery — profile PNG publication (#232)

- Draft [PR #258](https://github.com/Chain-Frost/ryan-tools/pull/258), branch `fix/wp06-profile-publication-232`. Source edit at `34ccbc1c`, regression tests at `53360981`; commits pushed through the connected GitHub API against live main `5ad632cc`.
- Profile PNGs now render through an exclusive unique same-directory staging file; publication is explicit overwrite or operating-system atomic no-replace. Casefold name collisions are rejected, and only the invocation's stage is cleaned.
- Focused tests cover concurrent publishers, a late competing destination, case aliases, render/publish failures and independent per-profile publication. Raster sampling is mocked while GIS fixture and output files are real.
- Existing sampling values, CRS handling and profile selection were not intentionally changed.
- **Validation actually run:** isolated Python 3.13.5 filesystem-publication harness `python -m pytest -q test_profile_publish_harness.py`: **5 passed** on Linux. This is a standalone recreation of the publication helper, **not** the repository test suite or an installed wheel check.
- **Not yet run/confirmed:** repository-focused Python 3.14 pytest, changed-file Ruff/strict Pyright, package build/copied-wrapper smoke, native Windows/SMB lock/case behaviour. Hosted CI was triggered by draft #258; follow its exact head/checks rather than treating an earlier commit as validated.
- **Publication boundary:** output files are committed individually; if a later profile fails, already published PNGs remain. POSIX without hard-link support fails safely rather than falling back to a non-atomic copy.

## Second delivery — archive publication (#197)

- Branch `fix/wp06-archive-publication-197` descends from the draft #258 head. This remains a separate reviewable delivery (no main merge).
- `create_7zip_archive` writes to a unique temporary directory on the target filesystem and uses atomic no-replace part publication. A cooperative exclusive-writing claim prevents two library callers from simultaneously publishing the same destination. Failed subprocesses and interrupted execution clean only invocation-owned staging; any published parts are rolled back only when their file identity still matches this invocation.
- Folder archiving now retains dotted source names: `model.v1` → `model.v1.7z`, distinct from `model.v2.7z`. The wrapper preflights physical destination aliases before dispatch, while preserving user-provided path spelling.
- Existing single-volume, split-volume and individual-file consumers retain their public options. Split-volume publication consists of multiple file publishes, **not a filesystem-atomic set**; process/machine interruption can leave a partial set. An abandoned `.*.writing` claim requires inspection before manual removal.
- Focused disposable regression cases added: case-insensitive/volume collisions, late output creation, active-worker contention, incomplete/failed 7-Zip outputs, staged interruptions, split rollback, dotted siblings and symlinked output-directory aliases.
- **Validation status:** new tests committed; Python 3.14/Ruff/strict Pyright and hosted suite **not yet confirmed** for the archive change. Real Windows 7-Zip / SMB active-writer and split-volume checks remain native acceptance.

## Third delivery — cutline GDAL (#200)

Conditional on a suitable GDAL raster/cutline environment to validate CRS and spatial results. No GDAL change is claimed yet. These fixes share a publication principle, not a required common framework.

## Handoff and constraints

The environment cannot resolve `github.com` over Git transport, so no remote clone/checkout or Git push was possible from the container. Work was staged on a dedicated remote branch using the connected GitHub API. A separate local Git worktree at `/mnt/data/wp06-profile-worktree` was used for an isolated filesystem harness only; it is **not** a checkout of the remote repository. No submodules or `unsorted` were traversed. No solver or production input was used. Nothing was merged or released.

**Next action:** confirm #258/stack exact-head CI and archive #197 focused hosted tests; repair check failures on the owning branches. Obtain Windows 7-Zip/SMB evidence for #197. Keep #200 as a native-GDAL gate until a valid environment is available.
