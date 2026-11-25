# Python 3.11 compatibility migration

| Field | Value |
| --- | --- |
| Status | Needs review: local acceptance passes; hosted checks and independent final-head review pending |
| Owner | Compatibility migration |
| Updated | 2026-11-24 |
| Next review | 2026-11-25 |
| Baseline | `1c0dd0ad1b44d69b4d090b39ca56071a4d71ff83` |
| Branch | `compat/python311` |
| Issue | [#335](https://github.com/Chain-Frost/ryan-tools/issues/335) |

## Outcome and scope

Support Python 3.11, 3.12, 3.13 and 3.14. Python 3.14 remains preferred for development.
The owner's specific target is CPython **3.11.11**, matching the ArcGIS Pro version under consideration.
Standalone Python acceptance does not establish installation inside ArcGIS Pro or change its environment.
No scientific algorithm, golden fixture, numerical tolerance, publishing control or release version changes.
At this historical compatibility milestone the standalone `ryan-rorb-readers` package used the canonical RORB sources.
The subsequent ownership transfer to PyRORB and downstream compatibility migration are tracked in [#340](https://github.com/Chain-Frost/ryan-tools/issues/340); do not publish the standalone distribution.
No merge or publication is authorized by this task.

## Implementation

- Replace PEP 695 aliases/generics and Python 3.14 exception syntax with Python 3.11 equivalents.
- Preserve eager runtime aliases where needed; postpone only genuinely recursive/forward aliases.
- Replace newer pathlib walking/junction APIs, typing.override and process CPU-count lookup with compatible operations.
  Windows mount-point reparse tags still detect junctions, including missing targets; overwrite/link protections remain.
- Correct positional-only strftime/str.replace arguments and Enum membership differences.
- Make import-safety inventory respect eager annotations on older interpreters and future annotations when requested.
- Enforce package Python metadata in installers; remove the previous --ignore-requires-python bypass.
- Keep reader wheel selection bounded to the six canonical modules and package initializers/PEP 561 marker.
- Test every supported Python version in the existing Windows workflow. Preserve the required Hosted tests check
  through an aggregate gate, plus original OS lanes, actions, permissions and release concurrency.

## Dependency decisions

The checked HY-8/culvert candidate source revisions are temporarily pinned by immutable public archive URLs;
there are no committed local filesystem dependency references. Their compatibility PRs are
[run-hy8 #22](https://github.com/Chain-Frost/run-hy8/pull/22) and
[ryan-culverts #42](https://github.com/Chain-Frost/ryan-culverts/pull/42), now merged by the owner.
Use released compatible versions in a later coordinated dependency update after installation verification.

Python 3.11 uses compatible pandas stubs; types-rasterio currently requires Python 3.12 and is installed for
newer development interpreters. The CI typing profile retains strict Python 3.11 checking with NumPy 2.3.5
annotations and the accepted Rasterio stub revision. This constrains only the type-check job, preserves its
complete declared dependency groups and does not narrow runtime tests or their package resolution.
Newer NumPy annotations refer to standard-library types absent from Python 3.11.

Windows h5py is bounded below 3.16 after an actual GDAL-first import exposed an HDF5 DLL entry-point crash.
The compatible wheel imports are tested separately. Other platforms retain their normal resolution.
The existing accepted PyTuflow wheel/provenance policy remains; pip installation records its required archive hash.

## Validation and delivery

Windows isolated interpreters: **3.11.11 / 3.12.14 / 3.13.15 / 3.14.6**.
The applicable full suite passes **3,610 tests / 44 skips / 2 deselections on each interpreter**.
The two expensive floodway sweep deselections predate this migration and remain explicit; skipped native,
POSIX/privilege and optional-integration cases are not claimed as passed.

Ruff 0.16.6 lint/format passes without unsafe fixes or changed rule families/complexity limits.
Strict Pyright over all **717 tracked first-party Python/stub files** passes with **0 errors/warnings**
under the compatibility typing profile. The unchanged Python 3.14 baseline was separately reconstructed and passed.
Initially the plain PyPI Windows baseline could not build Fiona; the repository's existing binary GIS index
resolved that environment requirement. No global Python or QGIS installation was modified.

The mandated build script passes with --no-bump --skip-pip; the wheel is verified and the release version is unchanged.
Clean wheel dependency resolution, isolated import-origin/API/CLI smoke and pip check pass on all four interpreters.
The standalone reader wheel is built independently from byte-identical canonical source in a fresh build tree;
its contents are bounded to nine Python files (six readers plus three package initializers), with no pandas/GIS dependency.
Further affected checks and final wheel rebuilding follow the final annotation-only adjustments.

Initial Ruff safe fixes corrected 43 findings automatically; 19 remaining generated typing/import findings were
corrected manually. Subsequent import/format corrections are retained in the local evidence logs.
Evidence: E:/Github/compat311-evidence. Hosted results and independent exact-head review are recorded in the PR
rather than generating a documentation-only CI push.

MCP assessment: compatibility updates existing supported surfaces; the basic junction helper is not a new MCP workflow.
No catalogue expansion is needed. Existing workflow discovery and installed-script contracts remain covered.

## Related delivery and limits

- [FFA-methods #44](https://github.com/Chain-Frost/FFA-methods/pull/44): 472 tests on all versions, independently reviewed, owner-merged.
- [PyRomb #3](https://github.com/phantom-branch/pyromb/pull/3): reviewed compatible source pin; 199 passes / 55 QGIS skips on 3.11.11.
- PyRORB #226 follows this reader source revision; Monte Carlo scientific gates remain separate.
- Native HY-8 on 3.11.11: 42 passes / one skip; no broader hydraulic qualification.
- ArcGIS Pro, native QGIS/GRASS/TUFLOW execution and new native RORB/Flike qualification are not established.

Existing overdue work-register reviews were reported without starting those unrelated work fronts.

## Next action

Publish the checked branch and compatibility PR, obtain fresh independent final-head review and verify hosted CI.
Keep the issue open until the maintainer's integration decision; do not merge or publish packages automatically.

## Independent review correction checkpoint

Review of bf2d9c61 found two consequential migration regressions: matrix jobs shared an upload-artifact name,
and os.walk includes linked directories in dirnames whereas Path.walk does not. Corrective changes give
reports per-version names and prune symlinks/junctions before discovery/descent, retaining file-level safeguards.
Focused link-discovery regressions accompany the correction. Fresh final-head review and hosted checks follow.

The hosted 3.11 GIS stack also exposed an older-PROJ synthetic-test assumption: ESRI WKT serialization
normalizes an unnamed datum. Compare exports with the parsed input PRJ, preserving the original PRJ-byte,
stored EPSG-authority and GeoKey assertions. Production projection code and numerical tolerances are unchanged.

Final local parallel-version replay exposed an existing shared-fixture report collision. The three report-export
tests now copy unchanged public POMM inputs into each test's temporary directory. Coverage is retained;
no test/profile is removed, and submodule fixture directories are no longer used as mutable report destinations.
