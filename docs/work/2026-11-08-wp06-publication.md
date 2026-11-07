# WP06 — owned staging in standalone output workflows

**Status:** Partial implementation: #232 hosted checks pass on PR #258; #197 code/tests on PR #264 with strict-typing fix under CI; #200 not implemented. Both child PRs remain draft. No main merge.
**Owner:** WP06 delivery agent.
**Updated:** 2026-11-08 (Perth).
**Next review:** 2026-11-15 or when current-head CI/native validation is available.
**Allocation:** [#240](https://github.com/Chain-Frost/ryan-tools/issues/240).
**Issues:** [#232](https://github.com/Chain-Frost/ryan-tools/issues/232), [#197](https://github.com/Chain-Frost/ryan-tools/issues/197), [#200](https://github.com/Chain-Frost/ryan-tools/issues/200).

## First delivery — profile PNG publication (#232)

- Draft [PR #258](https://github.com/Chain-Frost/ryan-tools/pull/258), branch `fix/wp06-profile-publication-232`, is stacked on integration [#251](https://github.com/Chain-Frost/ryan-tools/pull/251). Source edit `34ccbc1c`, regression tests `53360981`, follow-up CI repair `805c6e11`. No change was merged to main.
- Profile PNGs now render through an exclusive unique same-directory staging file; publication is explicit overwrite or operating-system atomic no-replace. Casefold name collisions are rejected, and only the invocation's stage is cleaned.
- Focused tests cover concurrent publishers, a late competing destination, case aliases, render/publish failures and independent per-profile publication. Raster sampling is mocked while GIS fixture and output files are real.
- Existing sampling values, CRS handling and profile selection were not intentionally changed.
- **Verified hosted acceptance:** Windows Python 3.14.10, PR #258 head `805c6e11`: **6/6 checks passed** (including Ruff, strict Pyright policy, packaging, MCP smoke, POSIX security, and 2,534 hosted pytest passes / 14 skips / 8 deselections). Focused profile publication tests were collected and passed. Previous notes of a separate five-test Python 3.13 harness have no retained run artifact and are not independently counted here.
- **Still unverified:** Native Windows mapped-drive/SMB lock and concurrent publication acceptance, and a separate installed/copied-wrapper profile smoke. Hosted package/MCP checks are not automatically evidence of a copied-profile-wrapper run.
- **Publication boundary:** output files are committed individually; if a later profile fails, already published PNGs remain. POSIX without hard-link support fails safely rather than falling back to a non-atomic copy.

## Second delivery — archive publication (#197)

- Draft [PR #264](https://github.com/Chain-Frost/ryan-tools/pull/264), branch `fix/wp06-archive-publication-197`, is the reviewable #197 child of draft #258; this child incorporates the latest profile parent commits without changing that parent's branch.
- `create_7zip_archive` writes to a unique temporary directory on the target filesystem and uses atomic no-replace part publication. A cooperative exclusive-writing claim prevents two library callers from simultaneously publishing the same destination. Failed subprocesses and interrupted execution clean only invocation-owned staging; any published parts are rolled back only when their file identity still matches this invocation.
- Folder archiving now retains dotted source names: `model.v1` → `model.v1.7z`, distinct from `model.v2.7z`. The wrapper preflights physical destination aliases before dispatch, while preserving user-provided path spelling.
- Existing single-volume, split-volume and individual-file consumers retain their public options. Split-volume publication consists of multiple file publishes, **not a filesystem-atomic set**; process/machine interruption can leave a partial set. An abandoned `.*.writing` claim requires inspection before manual removal.
- Focused disposable regression cases added: case-insensitive/volume collisions, late output creation, active-worker contention, incomplete/failed 7-Zip outputs, staged interruptions, split rollback, dotted siblings and symlinked output-directory aliases.
- **Hosted tests before final typing fix:** PR #264 at `327f8411` had **5/6 green**: 2,545 hosted pytest passes / 14 skips / 8 deselections, Ruff, package, MCP, POSIX security all passed; Policy failed on one test-only strict Pyright `reportPrivateUsage`. Fix committed at `8ba5fc41`, with new exact-head CI running at this record update. No independent real 7-Zip integration was accepted: `tests/scripts/test_archive_compress.py` is excluded from the normal hosted suite. Real Windows 7-Zip split volumes and active SMB writer/lock acceptance remain required.

## Third delivery — cutline GDAL (#200)

Conditional on a suitable GDAL raster/cutline environment to validate CRS and spatial results. No GDAL change is claimed yet. These fixes share a publication principle, not a required common framework.

## Handoff and constraints

The environment cannot resolve `github.com` over Git transport, so no remote clone/checkout or Git push was possible from the container. Work was staged on a dedicated remote branch using the connected GitHub API. A separate local Git worktree at `/mnt/data/wp06-profile-worktree` was used for an isolated filesystem harness only; it is **not** a checkout of the remote repository. No submodules or `unsorted` were traversed. No solver or production input was used. Branch integration is by non-force merge commits only; nothing was merged to main or released.

## Issue-closing and integration plan

GitHub's closing keywords only create the automatic issue-closing relationship when the PR targets the repository default branch. PR #258 targets #251's branch, and PR #264 targets #258's branch; their current issue references are **not** configured to auto-close #232 or #197 on a child merge. Do not claim these issues closed.

Once each approved child is incorporated into #251's branch and its acceptance is satisfied, add explicit `Closes #232` and `Closes #197` lines to the **main-targeting parent #251** before any separately authorised final squash merge. Do not add a closing keyword for #200 until its own scope and validation are delivered. No automatic closing keywords are set on #251 for the unintegrated WP06 issues yet.

## Remaining acceptance

1. Confirm PR #264's new exact-head CI, inspect any failing jobs and repair within its own draft branch.
2. Obtain representative native Windows/SMB profile-output concurrency/lock evidence (#232) and native 7-Zip single/split volume, linked-path and SMB evidence (#197). Record accepted exceptions rather than asserting a native pass.
3. Implement #200 in a separate reviewable delivery when a native GDAL environment permits real raster CRS/cutline spatial, collision and failure tests.
4. Coordinate branch integration with #251's owner and other stacked packs; only after approved code and acceptance are in the main-targeting PR should its issue-closing lines be activated.

**Next action:** check #264 latest CI and address any finding; do not merge any PR to `main`, enable auto-merge or release.
