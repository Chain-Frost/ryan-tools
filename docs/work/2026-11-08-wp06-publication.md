# WP06 — owned staging in standalone output workflows

**Status:** Active — #232 implementation in draft PR #258; #197 next; #200 needs native GDAL validation.
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

## Follow-up deliveries

1. **#197 / archive:** use a separate branch/PR from fresh main. Preserve split volumes and existing-output checks; resolve dotted folder collisions and stage only owned files. Native Windows 7-Zip and SMB tests remain independent.
2. **#200 / cutline GDAL:** start only with a suitable GDAL raster/cutline validation environment; otherwise record the blocker rather than claim spatial CRS correctness from mocks alone.

The archive and GDAL fixes share the publication principle but do not depend on #258 or need a shared-file refactor.

## Handoff and constraints

The environment cannot resolve `github.com` over Git transport, so no remote clone/checkout or Git push was possible from the container. Work was staged on a dedicated remote branch using the connected GitHub API. A separate local Git worktree at `/mnt/data/wp06-profile-worktree` was used for an isolated filesystem harness only; it is **not** a checkout of the remote repository. No submodules or `unsorted` were traversed. No solver or production input was used. Nothing was merged or released.

**Next action:** review #258 exact-head hosted checks and repair any genuine CI findings; then complete #197 in its own draft PR. Keep #200 as a native-GDAL gate until a valid environment is available.
