# WP06 — owned staging in standalone output workflows

**Status:** Active — #232 and #197 implemented, validated by all six hosted checks on their final child heads, and integrated into draft parent PR #251 at `66dece4`; native acceptance remains outstanding. #200 is not implemented. No main merge.
**Owner:** WP06 delivery agent.
**Updated:** 2026-11-08 (Perth).
**Next review:** 2026-11-15 or when current-head CI/native validation is available.
**Allocation:** [#240](https://github.com/Chain-Frost/ryan-tools/issues/240).
**Issues:** [#232](https://github.com/Chain-Frost/ryan-tools/issues/232), [#197](https://github.com/Chain-Frost/ryan-tools/issues/197), [#200](https://github.com/Chain-Frost/ryan-tools/issues/200).

## First delivery — profile PNG publication (#232)

- [PR #258](https://github.com/Chain-Frost/ryan-tools/pull/258), branch `fix/wp06-profile-publication-232`, passed hosted checks at `805c6e11` and was merged **into parent [#251](https://github.com/Chain-Frost/ryan-tools/pull/251)**, not into `main`. Original review history is retained; the WP06 integration merge commit is `66dece4`.
- Profile PNGs now render through an exclusive unique same-directory staging file; publication is explicit overwrite or operating-system atomic no-replace. Casefold name collisions are rejected, and only the invocation's stage is cleaned.
- Focused tests cover concurrent publishers, a late competing destination, case aliases, render/publish failures and independent per-profile publication. Raster sampling is mocked while GIS fixture and output files are real.
- Existing sampling values, CRS handling and profile selection were not intentionally changed.
- **Verified hosted acceptance:** Windows Python 3.14.10, PR #258 head `805c6e11`: **6/6 checks passed** (including Ruff, strict Pyright policy, packaging, MCP smoke, POSIX security, and 2,534 hosted pytest passes / 14 skips / 8 deselections). Focused profile publication tests were collected and passed. Previous notes of a separate five-test Python 3.13 harness have no retained run artifact and are not independently counted here.
- **Still unverified:** Native Windows mapped-drive/SMB lock and concurrent publication acceptance, and a separate installed/copied-wrapper profile smoke. Hosted package/MCP checks are not automatically evidence of a copied-profile-wrapper run.
- **Publication boundary:** output files are committed individually; if a later profile fails, already published PNGs remain. POSIX without hard-link support fails safely rather than falling back to a non-atomic copy.

## Second delivery — archive publication (#197)

- [PR #264](https://github.com/Chain-Frost/ryan-tools/pull/264) was the reviewable #197 child of #258; both child histories were incorporated into draft parent #251 at `66dece4`. GitHub marks both child PRs merged into the parent branch, **not `main`**.
- `create_7zip_archive` writes to a unique temporary directory on the target filesystem and uses atomic no-replace part publication. A cooperative exclusive-writing claim prevents two library callers from simultaneously publishing the same destination. Failed subprocesses and interrupted execution clean only invocation-owned staging; any published parts are rolled back only when their file identity still matches this invocation.
- Folder archiving now retains dotted source names: `model.v1` → `model.v1.7z`, distinct from `model.v2.7z`. The wrapper preflights physical destination aliases before dispatch, while preserving user-provided path spelling.
- Existing single-volume, split-volume and individual-file consumers retain their public options. Split-volume publication consists of multiple file publishes, **not a filesystem-atomic set**; process/machine interruption can leave a partial set. An abandoned `.*.writing` claim requires inspection before manual removal.
- Focused disposable regression cases added: case-insensitive/volume collisions, late output creation, active-worker contention, incomplete/failed 7-Zip outputs, staged interruptions, split rollback, dotted siblings and symlinked output-directory aliases.
- **Verified hosted acceptance:** Final child head `2efe6a2f` passed **6/6 checks** (Windows Python 3.14.10: 2,545 hosted pytest passes, 14 skips and 8 deselections; Ruff, strict Pyright Policy, Package, MCP smoke and POSIX wrapper security). This includes the test-only typing fix. **Not verified:** real Windows 7-Zip execution and split-volume correctness, mapped-drive/UNC/SMB active-writer locks, installed/copied-wrapper smoke; `tests/scripts/test_archive_compress.py` is intentionally excluded from normal hosted pytest.

## Third delivery — cutline GDAL (#200)

Conditional on a suitable GDAL raster/cutline environment to validate CRS and spatial results. No GDAL change is claimed yet. These fixes share a publication principle, not a required common framework.

## Handoff and constraints

The environment cannot resolve `github.com` over Git transport, so no remote clone/checkout or Git push was possible from the container. Work was staged on a dedicated remote branch using the connected GitHub API. A separate local Git worktree at `/mnt/data/wp06-profile-worktree` was used for an isolated filesystem harness only; it is **not** a checkout of the remote repository. No submodules or `unsorted` were traversed. No solver or production input was used. Branch integration is by non-force merge commits only; nothing was merged to main or released.

## Issue-closing and integration plan

The reviewed #232/#197 implementation is now included in [main-targeting parent PR #251](https://github.com/Chain-Frost/ryan-tools/pull/251) via **merge commit `66dece4` on its feature branch**. Child PRs #258 and #264 closed as merged into the parent, with their review history retained. The parent remains draft, and **no commit was merged to `main`**.

The parent PR description carries `Closes #232` and `Closes #197` as **conditional closing links**. These cannot close issues until an expressly authorised final parent merge to the default branch. The parent must remain unmerged pending unresolved native acceptance and the other work pack owners' approvals. Do not close #200; it has no completed implementation.

## Local agent acceptance and completion handoff

### Source and permitted environment

- Start with the **current head of parent PR #251** (`fix/tlf-hyphenated-variables-239`), and record the exact SHA before testing. The WP06 merge commit is `66dece4`; new parent documentation or other work packs may advance this head. Do not test a stale standalone child branch as if it were the final integrated parent.
- Use a **separate worktree or disposable source copy** and the normal Python **3.14** installation. Do not create a virtual environment by default; follow `AGENTS.md` and `docs/DEVELOPMENT_GUIDE.md`. Preserve all other owners' edits/submodules and never traverse or initialise `unsorted`.
- Use only **disposable local/UNC test folders**, never production TUFLOW results or live network archives. Do not launch licensed solvers.

### Profile PNGs — #232

- Verify the tested package/source matches the parent head. Run `py -3.14 -m pytest -q tests/orchestrators/tuflow/test_profile_publication_232.py tests/orchestrators/tuflow/test_water_level_profiles.py`.
- On Windows local and a **permitted disposable SMB/UNC test share**, prove no-clobber when a destination appears after preflight, case-only aliases, two simultaneous publishers, failed PNG rendering, and cleanup that leaves a competing invocation's files intact. Include existing-output `overwrite_existing=False` and explicit `True` behavior.
- Copy/install the current wheel-backed `plot_water_level_profiles.py` wrapper outside the checkout and run a non-interactive smoke against tiny disposable GPKG/TIF profile inputs; compare output bytes/profiles and confirm that original inputs remain unchanged. Record actual copied wrapper/module versions and exit codes.
- Native Windows/SMB lock semantics and copied-wrapper parity were **not** established by hosted GitHub checks, so report them explicitly.

### 7-Zip archive publication — #197

- Use the real permitted Windows **7-Zip** executable and disposable small folder/file inputs. Run `py -3.14 -m pytest -q tests/functions/test_archive_publication_197.py tests/scripts/test_archive_compress_folder_wrapper.py tests/scripts/test_archive_compress.py`. The final script test file is excluded from hosted CI, and native 7-Zip cases may otherwise skip when 7-Zip is missing.
- Validate dotted folders (`model.v1`/`model.v2`), excluded folders/extensions, source preservation, existing archive/volume skips, concurrent creators, failure/interrupt cleanup, and ordinary un-split `.7z` archives by checking with **`7z t` and extraction of disposable files**.
- Exercise `--max-part-size-gib 1` on a permitted disposable data set large enough to create **at least two numbered volumes** if disk/network capacity permits; verify all parts with 7-Zip, readable extraction, and safe cleanup on an injected failure. A single small file with a split flag is **not** evidence of multi-part publication.
- Repeat representative no-overwrite and concurrent-writer cases on a permitted mapped-drive/UNC/SMB share; check stale `.*.writing` lock handling and document any file-sharing policy restrictions. Do not remove a stale lock without confirming no active owner.
- Check an independently copied `archive_compress_folder.py` wrapper against the installed version. Do not present GitHub hosted wheel build/smoke alone as full copied-wrapper/native acceptance.

### GDAL cutline publication — #200 (not implemented)

- A local agent with working `osgeo.gdal` / OSGeo4W or QGIS Python and synthetic raster/vector test data must implement this as a **separate reviewable PR**, stacked onto the latest parent #251 or another agreed integration branch. No change was claimed for `ryan-scripts/gdal-python/gdalwarp_clip_to_polygon.py` in WP06 first/second deliveries.
- Derive source CRS from each raster's metadata unless an explicit source override is provided; keep target/destination CRS independent. Use safe invocation-owned staging, preflight input/output physical and case collisions **across all input roots before creating any output**, and enforce no-overwrite at publication. Confirm preservation of source rasters after warp errors, aliases, partial batches, and publication races.
- Test real different-CRS synthetic rasters/cutlines and inspect actual output projection, geometry, extent, pixel values/NoData and failed warp behavior. Ruff/strict Pyright, wrapper `--help`, independent installed/copied script and native Windows/SMB acceptance must be recorded. Mock-only GDAL tests do not establish spatial correctness.

### Validation and handback

- Run changed-file `py -3.14 -m ruff check` / `ruff format --check` and strict `py -3.14 -m pyright` using the repository configuration for any newly changed code. Use `py -3.14 repo-scripts/build_library.py --no-bump` to verify a library change without silently bumping/releasing its version, then perform installed/copied-wrapper checks from the resulting supported package build. Do not use full recursive submodule checkout.
- Post a **SHA-specific acceptance summary** to PR #251 and update this canonical WP06 record: OS/Python, package and 7-Zip/GDAL versions, drive/share path type, exact commands, pass/fail/skip, artifacts and remaining exceptions. Include #200's PR if/when delivered.
- Do **not** merge into `main`, enable auto-merge, release, close issues manually or silently convert unverified native checks into passes. Any final squash merge requires separate explicit maintainer authorisation.

**Next action:** WP06 local agent completes #232/#197 native evidence and #200's GDAL implementation/acceptance. PR #251 integration owners separately reconcile all other stacked packs, final-head CI and closing links before any main merge.
