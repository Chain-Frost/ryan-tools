# WP06 — owned staging in standalone output workflows

**Status:** WP06 #232/#197/#200 changes are integrated into draft parent PR #251; #271 was squash-merged into the feature branch at `24241d7` after six green child checks and fresh Codex review with no remaining findings. Native Windows/SMB/7-Zip/GDAL evidence is recorded below; one **new GCP-specific native test remains unrun**, and documented environment/crash-recovery limits remain. Parent exact-head checks and other work-pack integration are separate gates. No `main` merge or issue closure.
**Owner:** WP06 local validation / maintainer review.
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

Implemented in [PR #271](https://github.com/Chain-Frost/ryan-tools/pull/271), **squash-merged into parent feature branch #251** as `24241d7` after native validation and Codex review. No merge to `main`.
The maintained script retains its processing locally; no new orchestrator or compatibility layer is introduced.
Source CRS comes from raster metadata unless explicitly corrected with `--source-crs`; `--target-crs`
independently controls the destination. The complete batch is checked for physical/case aliases and output
collisions before output creation. Unique owned staging, closed/reopened TIFF validation, default atomic
no-replace publication and explicit overwrite protect previous outputs and source rasters. Linked paths and
output sidecars fail closed. CLI/defaults, wrapper revision and the existing MCP catalogue are updated.

## Historical connector handoff and constraints

The environment cannot resolve `github.com` over Git transport, so no remote clone/checkout or Git push was possible from the container. Work was staged on a dedicated remote branch using the connected GitHub API. A separate local Git worktree at `/mnt/data/wp06-profile-worktree` was used for an isolated filesystem harness only; it is **not** a checkout of the remote repository. No submodules or `unsorted` were traversed. No solver or production input was used. Branch integration is by non-force merge commits only; nothing was merged to main or released.

## Issue-closing and integration plan

The reviewed #232/#197 implementation is now included in [main-targeting parent PR #251](https://github.com/Chain-Frost/ryan-tools/pull/251) via **merge commit `66dece4` on its feature branch**. Child PRs #258 and #264 closed as merged into the parent, with their review history retained. The parent remains draft, and **no commit was merged to `main`**.

The parent PR description carries `Closes #232` and `Closes #197` as **conditional closing links**. These cannot close issues until an expressly authorised final parent merge to the default branch. The parent must remain unmerged pending unresolved native acceptance and the other work pack owners' approvals. Keep #200 open while the parent is unmerged. Its implementation is now incorporated, so add the conditional `Closes #200` to the **main-targeting parent PR #251**; it must only take effect after a separately authorised final merge.

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

### Historical GDAL cutline implementation handoff — #200 (completed in #271)

- The following requirements were the original local-agent handoff and have since been implemented and tested in #271, apart from the separately logged GCP follow-up. The child has been incorporated into #251's feature branch; the original acceptance requirements are retained for provenance.
- Derive source CRS from each raster's metadata unless an explicit source override is provided; keep target/destination CRS independent. Use safe invocation-owned staging, preflight input/output physical and case collisions **across all input roots before creating any output**, and enforce no-overwrite at publication. Confirm preservation of source rasters after warp errors, aliases, partial batches, and publication races.
- Test real different-CRS synthetic rasters/cutlines and inspect actual output projection, geometry, extent, pixel values/NoData and failed warp behavior. Ruff/strict Pyright, wrapper `--help`, independent installed/copied script and native Windows/SMB acceptance must be recorded. Mock-only GDAL tests do not establish spatial correctness.

### Validation and handback

- Run changed-file `py -3.14 -m ruff check` / `ruff format --check` and strict `py -3.14 -m pyright` using the repository configuration for any newly changed code. Use `py -3.14 repo-scripts/build_library.py --no-bump` to verify a library change without silently bumping/releasing its version, then perform installed/copied-wrapper checks from the resulting supported package build. Do not use full recursive submodule checkout.
- Post a **SHA-specific acceptance summary** to PR #251 and update this canonical WP06 record: OS/Python, package and 7-Zip/GDAL versions, drive/share path type, exact commands, pass/fail/skip, artifacts and remaining exceptions. Include #200's PR if/when delivered.
- Do **not** merge into `main`, enable auto-merge, release, close issues manually or silently convert unverified native checks into passes. Any final squash merge requires separate explicit maintainer authorisation.

**Next action:** Run the newly added GCP-only raster native regression on Windows/GDAL; verify the updated parent-head CI and review other stacked packs and main conflicts. No main merge, auto-merge, release or manual issue closure is authorised.

## 2026-11-08 local Windows acceptance

### Source and delivery boundary

- Started from the live parent head `63a36d487e7d34961fedf95452f8f83611b9a71b`, not a child head.
- Isolated branch `fix/wp06-native-acceptance-20261108`; initial implementation `081c84ab6ac943b5ac0a908f07d4818273beda79`, final code **`ea118c6e`**, pushed as draft [PR #271](https://github.com/Chain-Frost/ryan-tools/pull/271) against #251. Subsequent handoff/CI-only commits retain that tested implementation.
  The shared main checkout and unrelated submodules were preserved. Only `tests/test_data` and `qgis-resources`
  were explicitly initialised to satisfy validation/build requirements; `unsorted` was never inspected.
- Restored #251 to draft because its live state contradicted the review's draft requirement. Parent/main conflicts
  and the other work packs remain the integration owners' responsibility.
- Tests use Windows 10 build 19045, normal Python **3.14.6**, native **7-Zip 22.01 x64**, and Python 3.14 GDAL
  **3.13.3**. Independent read-back uses OSGeo4W `python-qgis.bat`: Python 3.12.15, GDAL 3.13.3, QGIS 4.2.3.
  The application runtime validates outputs; it is not substituted for the required Python 3.14 source tests.
- The maintainer explicitly approved a disposable `T:\temp` folder. Tests use a unique owned child directory
  through both the mapped drive and its SMB/UNC spelling. Server/share identifiers and raw transcripts are kept
  in private local evidence, not published. No production data or licensed solver was used.

### Findings and fixes

The original integrated source passed **39 tests / one privilege skip** locally. Its SMB run returned **one failure,
38 passes / one skip**: when two PNG publishers raced, the server returned Windows `ACCESS_DENIED` for the loser
rather than `FILE_EXISTS`. Output bytes remained safe, but the public collision exception differed. The fix
translates that permission error only when a destination has actually appeared; a real permission/lock failure
without a destination remains `PermissionError`. Two regression cases prove that distinction.

#200 adds real spatial and publication regressions for source metadata, differing cutline CRS, independent target
CRS, explicit source override, rejection of untagged sources without an explicit correction, source hard links, cross-input aliases, batch collisions, failed native warps,
late/concurrent creators, Windows sharing locks, case aliases, hard-linked batch outputs, sidecars, dry runs and directory junctions.

### Validation matrix

| Check | Result and limits |
| --- | --- |
| Focused #232/#197 tests on modified source | **41 passed / one skip**, both mapped SMB and UNC; local final aggregate includes these tests. The skip is the pre-existing directory-symlink privilege case. |
| Real GDAL #200 regressions | **17 passed locally**; **16 passed / one skip on UNC**. The SMB server cannot create a directory junction (WinError 4390); local junction rejection passes. |
| Combined focused Python 3.14 suite | **118 passed / one skip**: profile, archive (including hosted-excluded native script tests), cutline, promoted utilities, TLF hyphens and MCP registry/overview tests. |
| Ruff/strict Pyright | All 13 parent-diff Python files pass; all newly changed Python files also pass. Compilation, CLI help and Loguru policy pass. |
| Parent package verification | Rebuilt parent version **26.11.7.13** with `--no-bump --skip-pip`; wheel verification and isolated installed-wheel smoke pass. |
| Candidate package verification | Built/verified **26.11.8.2** with `--skip-pip`; installed-wheel smoke and all six copied result-wrapper checks pass. |
| Copied profile wrapper | Real tiny GPKG/TIF inputs on local, mapped SMB and UNC; no-overwrite preserves bytes, explicit overwrite rerenders identical PNG bytes, all input hashes unchanged. Wrapper 2026-11-07.2 uses candidate library 26.11.8.2. |
| Copied folder archive wrapper | Real 7-Zip on local, mapped SMB and UNC: dotted sibling names remain distinct, exclusions work, `7z t` and extraction pass, existing archive hashes remain unchanged. |
| Real native archive races | Local, mapped SMB and UNC: exclusive active claim rejects a second creator; first archive passes `7z t`; a late independent output survives real compression; an unowned writing claim is preserved for inspection. |
| Genuine split-volume archive | Real `create_7zip_archive(..., max_part_size_gib=1)` on **1,082,130,432 random bytes** produces **two parts: 1,073,741,824 and 8,455,705 bytes**. `7z t`, extraction SHA-256, source preservation and existing-volume preservation pass. Injected second-part interruption rolls back owned published parts and staging. |
| Native two-part publication on SMB | The genuine locally generated volumes are copied into invocation-owned SMB staging, published through the archive helper, tested with `7z t` and extracted on the share. Both parts retain their sizes; extracted SHA-256 matches the random input. Compression itself was local, not repeated on SMB. |
| Copied cutline wrapper | Local, mapped SMB and UNC: dry-run creates no output; differing raster/cutline CRS yields 6 by 6 EPSG:4326 output with values 7/-9999 and NoData -9999; default no-overwrite and explicit overwrite behave correctly; source hash unchanged. Wrapper 2026-11-08.2 / library 26.11.8.2. |
| OSGeo4W/QGIS read-back | Independently opens local and UNC clipped outputs as valid EPSG:4326, 6 by 6 rasters; extent 115.02,-31.08 to 115.08,-31.02 and decoded values/NoData match. |

The parent wheel's SHA-256 is `94680d87d89948f12dc1bcdd4ccd314eae8089913ee8aec22b23d9ab2b5995d3`.
The final 26.11.8.2 candidate wheel's SHA-256 is `848e19d2adfcdf6645cb798616c4c03fbe09c96abcd891702e759e437f9ad285`.
The copied PNG SHA-256 is `34754ce68eb73568c48d6fc15da703e1a878393badc1a31d638a4147807b0a0a` on all
three path types. The split input and extracted output both hash to
`fccc021febf135c355f6247d786adc830ffd28e30a544d3a151a6da546dfdfc3`.

### Commands and reproducibility

Run from the isolated source root using normal Python 3.14:

```powershell
python -m pytest -q -rs tests/orchestrators/tuflow/test_profile_publication_232.py tests/orchestrators/tuflow/test_water_level_profiles.py tests/functions/test_archive_publication_197.py tests/scripts/test_archive_compress_folder_wrapper.py tests/scripts/test_archive_compress.py
python -m pytest -q -rs tests/scripts/gdal/test_cutline_publication_200.py
python -m pytest -q tests/scripts/test_promoted_utilities.py tests/functions/test_parse_tlf_hyphenated.py tests/mcp/test_registry.py tests/mcp/test_overview_validation.py
# Repeat the first two commands with --basetemp <unique-owned-SMB-test-directory>.
python -m ruff check <changed-Python-files>
python -m ruff format --check <changed-Python-files>
python -m pyright <changed-Python-files>
python -m py_compile ryan-scripts/gdal-python/gdalwarp_clip_to_polygon.py
python ryan-scripts/gdal-python/gdalwarp_clip_to_polygon.py --help
python repo-scripts/check_loguru_formatting.py
python repo-scripts/build_library.py --no-bump --skip-pip  # parent verification
python repo-scripts/build_library.py --skip-pip            # candidate version/build
python repo-scripts/verify_wheel.py
```

The private evidence directory retains `native_split.py`, `native_share_split.py`, `native_wrappers.py`,
`native_archive_races.py`, `native_cutline_wrapper.py`, `qgis_readback.py`, dependency versions, summaries,
per-wrapper logs and archive hashes. The harnesses run outside the checkout with `PYTHONPATH` pointing only to
the separate wheel installation; copied wrapper subprocesses also set `PYTHONNOUSERSITE=1` and clear
`RYAN_TOOLS_REPOSITORY_ROOT`. Dependencies are exact file snapshots of the user's existing installed
distributions, because generic dependency installation cannot build Fiona against this Windows GDAL setup.
No normal/user Python package was replaced. `smoke_test_installed_wheel.py --expected-root <installation>` and
`smoke_result_dashboard_installed_wheel.py --expected-root <installation>` run outside the checkout.

### Remaining exceptions and acceptance

- One broader parent test, `tests/test_compatibility_import_isolation.py`, initially fails locally because it
  deliberately disables the user site containing Loguru and discards external dependency paths. This is an
  environment limitation in the WP02 probe, not a changed WP06 regression. The same source probe passes with
  a private dependency root explicitly provided, and the installed-wheel import-isolation check passes.
  Do not relabel the original test failure as a pass or weaken its assertions.
- Directory-symlink creation requires privileges unavailable here; the existing archive alias test remains
  skipped. Directory junction rejection is verified locally; this SMB server cannot host junctions.
- Several-file split publication remains non-atomic across a process/machine crash. Injected in-process
  interruption/rollback passed; no power-loss or forced-process-kill recovery acceptance is claimed.
- Hosted CI deliberately excludes the new native GDAL file before collection, matching the existing native GDAL test boundary; its real local/SMB results above are separate evidence.
- Hosted checks and fresh independent review for the new child are recorded separately at delivery. After
  approved child integration, rerun the latest **parent** SHA; this child evidence cannot certify a later parent.
- The documented privilege/SMB-junction/crash limits are acknowledged for **feature-branch integration**, not described as tests that passed. Final-head review and any main merge require separate approval. #200 remains open, with a **conditional** closing keyword on the parent PR only.

**Delivery checkpoint:** Child [#271](https://github.com/Chain-Frost/ryan-tools/pull/271) is closed as squash-merged into draft parent #251 at `24241d7`, **not `main`**. Final child head `d0064a80` passed **all six hosted checks (2,548 tests)**. Fresh Codex review at that exact head reported no new findings and the previous GCP P2 discussion was resolved following the code/test correction. The review's new **native GCP-specific test remains to be executed**. Parent head is advanced by this integration and documentation; record exact-head parent checks separately. Parent/main conflicts and other packs remain integration-owner responsibilities.

The final wrapper revision is **2026-11-08.2**. Additional real regressions reject untagged rasters unless a source CRS is explicitly supplied and reject hard-linked existing batch outputs before mutation. The final mapped-to-UNC source identity probe also refuses the physical source alias and preserves its bytes. All final package/copied-wrapper checks are repeated with **26.11.8.2**.

## Independent review follow-up — GCP CRS metadata (2026-11-08)

Codex identified a P2 input-compatibility omission in [PR #271's GDAL CRS guard](https://github.com/Chain-Frost/ryan-tools/pull/271#discussion_r4505150571): rasters georeferenced with ground control points can have an empty `GetProjectionRef()` but a valid `GetGCPProjection()`. The script now accepts either form before allowing GDAL to infer the source CRS, without overriding it by default. A real GCP-only GeoTIFF and cutline regression was added to `tests/scripts/gdal/test_cutline_publication_200.py`.

**Native GCP test remains to be rerun:** the GitHub-connector environment does not provide `osgeo.gdal`. The prior 118-pass native validation covers the earlier affine-CRS version and **must not** be presented as a run of the newly added GCP case. The local Windows/GDAL agent should rerun `python -m pytest -q -rs tests/scripts/gdal/test_cutline_publication_200.py` at the amended child or integrated parent SHA, verify the GCP output projection/nonempty pixels and original raster hash, and record the result before a future `main` merge. Hosted CI separately checks Python 3.14 lint/type/package and the non-native test suite. No production data, release, auto-merge or `main` merge is authorised.

## Integration disposition — 2026-11-08

- [x] Integrated #232 and #197 via earlier reviewed child PRs #258/#264; integrated #200 and #232 SMB follow-up via [#271](https://github.com/Chain-Frost/ryan-tools/pull/271) at parent feature-branch commit [`24241d7`](https://github.com/Chain-Frost/ryan-tools/commit/24241d721bb06c75ab56756c144d44bc6973eb21).
- [x] Final amended child head `d0064a80` passed all **six hosted checks**, 2,548 tests passed/14 skipped/8 deselected. [Fresh Codex review](https://github.com/Chain-Frost/ryan-tools/pull/271#issuecomment-6717786508) found no further issues; its earlier [P2 GCP review thread](https://github.com/Chain-Frost/ryan-tools/pull/271#discussion_r4505150571) is resolved. Native Windows/SMB, real 7-Zip split archives, copied wrappers and GDAL/QGIS results for the prior affine-CRS head remain recorded above (118 focused passes/one skip).
- [x] Accepted the **documented environmental limitations for feature-branch integration** only: unavailable directory-symlink privilege; SMB server cannot host junctions; sudden power-loss/forced-process-crash recovery untested. These are limitations, **not passing tests**. The earlier WP02 source import-probe environment failure remains WP02-owned and documented.
- [ ] **Native GCP regression:** run `python -m pytest -q -rs tests/scripts/gdal/test_cutline_publication_200.py` with GDAL 3.13.3 and Python 3.14 against the *current integrated parent SHA*. Confirm `test_source_crs_from_ground_control_points` passes, output projection/values are valid and source bytes unchanged; post version, path type, evidence and exact SHA. Hosted CI does **not** collect this native test. Do not conflate this new test with the earlier 118 accepted focused cases.
- [ ] Check new parent-head CI after its documentation update and reconcile all other stacked work packs. Resolve current parent versus `main` conflicts when integration owner approves. Conduct final review before a separately **explicitly authorised future squash merge to `main`**.

The parent PR description uses conditional issue closers for #232, #197 and **#200**. No issue has been manually closed. Neither #271's parent integration nor this record is authorisation to merge or release.
