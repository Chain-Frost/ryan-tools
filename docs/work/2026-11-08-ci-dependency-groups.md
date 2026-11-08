# CI dependency groups and temporary PyTuflow wheel

| Field | Value |
| --- | --- |
| Status | Implementation handoff: vendored wheel and further grouping not implemented |
| Owner | Unassigned / next implementation agent |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Predecessor | [PR #272](https://github.com/Chain-Frost/ryan-tools/pull/272), head `7f45110b37eb143f9b785001fe5d04af526ab339` |
| Timing baseline | Previous PR #272 head `1ae1235c2369950c287c4fafe24ef23848880fe0` |

## Outcome and scope

Keep Windows/Python 3.14 as the primary acceptance environment while avoiding dependencies that a CI job does not
use. Small POSIX checks retain their specific portability and filesystem-safety purpose. Normal user installation,
published package dependencies, native engineering acceptance and release behaviour are separate contracts.

This record owns the dependency audit and any later split into narrower Windows test groups. The profiles below are
implemented in parent PR #272, together with workflow routing and cancellation. This child handoff changes only
documentation. Continue its stacked branch after reviewing the parent's current diff and hosted CI; refresh and
record the predecessor SHA if it changes. See the [parent work record](2026-11-08-ci-resource-efficiency.md).

## First delivery: vendor the accepted PyTuflow wheel

The user proposes retaining a prebuilt PyTuflow wheel in this repository until an upstream release supplies the
required fixes. Implement this bounded distribution change before attempting a broader test-dependency split.
Normal user dependencies and accepted engineering coverage remain intact. No wheel has been added by this handoff.

### Exact candidate and existing Windows evidence

The current [parser requirement](../../repo-scripts/pytuflow-requirement.txt) selects the same source as
[upstream PR #17](https://github.com/TUFLOW-Support/PyTuflow/pull/17), which is still open. This is the fork contribution
rebased onto upstream 1.3.0. The older #63 source and the separate #65 integration wheel are different revisions.

| Property | Observed value |
| --- | --- |
| Source commit | `887534ea8c15b80d185a38a7ead1d42ab42a7167` |
| Wheel | `pytuflow-1.3.0-py3-none-any.whl` |
| Wheel bytes | 976,447 |
| Wheel SHA-256 | `4bfcd24734349f9e0dcf60776ee06a2e17953ac342f40e3611b0e2aae33b30e3` |
| Source archive reported by pip | 34.7 MB |
| Build/consumer environment | Windows CPython 3.14.10 |

Three passing jobs independently generated that identical wheel hash:
[public API, 107 passing tests](https://github.com/Chain-Frost/ryan-tools/actions/runs/42453824703/job/128648480823),
[Hosted tests, 2,666 passes](https://github.com/Chain-Frost/ryan-tools/actions/runs/42453824693/job/128648567535), and
[161-TCF regression](https://github.com/Chain-Frost/ryan-tools/actions/runs/42453824703/job/128648861058).
Their retained artifacts do not contain the PyTuflow wheel; pip wrote it to temporary caches. Recover a verified
copy or build once from this exact source with dependency installation disabled. The Hatchling build hook reads
the version without importing the GIS runtime. If rebuilt ZIP bytes differ, investigate content and tool-version
differences before accepting a new hash; do not silently substitute another source or relabel old acceptance.

The wheel is about 97.2% smaller than the source download. The actual wheel-build step took about 0.08 seconds;
avoidable source download/extraction, build-environment setup and metadata preparation took approximately 6-10
seconds per observed installation. These are preparation observations, not a controlled total CI saving. Native
dependencies account for much of the remaining installation time.

### Installation and repository changes

1. Store the wheel in ordinary Git under
   `repo-scripts/wheels/pytuflow/887534ea8c15b80d185a38a7ead1d42ab42a7167/`, with a small README and provenance manifest.
   Record source repository/SHA, wheel filename/version/hash, build command/tool versions and validation links.
   Keep the existing license and bundled notices, `py.typed`, CLI metadata and runtime resources. Use an explicit
   binary attribute if needed. Do not use LFS or ignored/build-owned `dist/`, or accidentally package this wheel
   inside the published ryan-tools distribution through the existing vendored-Python source boundary.
2. Update the shared parser requirement and selector in `repo-scripts/install_latest_wheel.py` together. The selector
   currently enforces an exact source URL, so changing the requirements line alone breaks normal installation.
   Resolve an absolute wheel path relative to the installer, validate its expected hash/metadata, and fail clearly
   on a missing or corrupt wheel. Keep documented root-relative requirements commands working, including paths
   with spaces. Do not commit machine-specific file URLs or quietly fall back to source installation.
3. Preserve parser-only `--force-reinstall --no-deps`, followed by joint normal dependency resolution, `pip check`
   and the public capability guard. An official build can share the same version string; version equality alone
   does not identify the accepted fork. Preserve normal GIS bootstrap, QGIS MCP defaults, legacy migration and the
   existing separate recovery mode. Keep all declared runtime/developer dependencies.
4. Adapt installed provenance checks rather than deleting them. The real model-replay test currently extracts an
   archive URL from the requirement and asserts it in `direct_url.json`; a local wheel needs a matching archive
   digest and source-manifest evidence. Keep the incompatible-parser rejection and broken-import/shadowing checks.
   Narrow the PyPI-installer test that forbids every `.whl` argument: it should reject a local ryan-tools wheel while
   allowing this explicit PyTuflow dependency wheel.
5. Add wheel/manifest/verifier changes to the targeted TUFLOW workflow's path filters and relevant pip cache inputs.
   Otherwise a wheel-only update can miss the dedicated model acceptance. Preserve distinct cache profiles and
   all existing required checks. Validate the committed artifact cheaply before expensive installations.
6. Keep plain `pip install ryan-tools` semantics explicit: package metadata does not automatically discover a wheel
   beside scripts in this Git repository. Cover the repository's normal installer, developer and CI routes without
   adding a nested dependency-wheel installer or a direct local path to published package metadata.
7. Retire the temporary wheel and its pin when a released upstream build contains the required fixes and passes
   the consumer capability checks. Merging the upstream PR alone does not provide a published installation artifact.

Verify the artifact hash separately or use a deliberate supported hash mechanism. Adding `--hash` to the shared
requirements line can enable pip's global hash-checking mode and conflict with editable installs and unpinned
dependencies. Do not turn this bounded change into a full dependency lock migration. See
[pip installation](https://pip.pypa.io/en/stable/cli/pip_install/) and
[hash-checking rules](https://pip.pypa.io/en/stable/topics/secure-installs/).

### Acceptance for the vendored wheel

- Verify wheel metadata, license notices, packaged resources, source provenance and SHA-256 before use.
- Exercise missing/corrupt wheel, paths with spaces, execution outside the repository working directory,
  same-version replacement, dependency resolution, dry runs, recovery and migration through focused installer tests.
- Preserve and run the existing Windows capability/import and 161-TCF/174-context consumer acceptance on the actual
  vendored artifact; confirm installed provenance and no new missing-dependency skips.
- Keep the ryan-tools wheel free of an accidentally nested dependency wheel. No dependency declaration is removed.
- Reuse accepted native/private engineering evidence for identical package bytes. Do not repeat the separate
  Windows/QGIS/GDAL/UNC/SMB campaign or 40-run benchmark merely because the distribution path changed.
- Measure preparation and full installation separately, then publish one checked batch. Several local commits may
  share that push; do not rebuild or commit the same dependency wheel for every ryan-tools edit.

## Current state

Two CI installation profiles retain dependency resolution through the authoritative package metadata:

| Job | Requirements | Retained contract | Omitted work |
| --- | --- | --- | --- |
| Hosted tests, including consolidated MCP checks | [ci-test-requirements.txt](../../repo-scripts/ci-test-requirements.txt) | All declared runtime dependencies, MCP extra, pytest and immutable PyTUFLOW source | The `dev` extra and separate interactive QGIS MCP bridge |
| Windows strict typing | [ci-policy-requirements.txt](../../repo-scripts/ci-policy-requirements.txt) | All runtime dependencies, canonical development/MCP extras and immutable PyTUFLOW source | Separate interactive QGIS MCP bridge |
| Package / installed-wheel smoke | [wheel-smoke-requirements.txt](../../repo-scripts/wheel-smoke-requirements.txt) | Existing focused smoke dependencies and isolated installation | No new removals in this audit |

Run profile installation from the repository root, after the existing Windows binary GIS bootstrap. The nested
PyTUFLOW requirements include resolves relative to the profile; the editable `.` refers to the repository working
directory. [requirements.txt](../../requirements.txt) and [pyproject.toml](../../pyproject.toml) remain unchanged.

The hosted profile no longer requests Ruff, Pyright, pandas-stubs, the build frontend or pytest-cov through the `dev`
extra. The full test command does not run those tools. [pytest.ini](../../pytest.ini) has only `--strict-markers` in
`addopts`, without a coverage or other third-party-plugin requirement. Packaging unit tests replace the actual build
operation with controlled test helpers; the separate Package job continues to build and verify a real wheel.
Build isolation still installs the backend requirements declared by package metadata.

The QGIS MCP bridge is an independent source archive in the developer requirements. First-party modules and hosted
tests do not import `qgis_mcp`. Installer and Codex setup tests inspect generated command/configuration strings or
mock subprocess execution. Opt-in QGIS 4 acceptance uses the QGIS application bindings and executable, not this
bridge. Removing its CI installation does not remove its normal installation or configuration contract.

### Why the runtime/GIS dependency set remains broad

The hosted suite directly exercises Fiona and Rasterio with real synthetic vector/raster data, GeoPandas, plotting,
spreadsheet exports, hydraulic engines, and the migrated TUFLOW initialiser. The native GDAL bootstrap cannot be
removed merely because the main pytest expression excludes the `gdal` marker: selected conversion checks also use
`pytest.importorskip("osgeo")`. Missing a dependency can silently turn passing coverage into skips.

An import-name search is insufficient evidence for removing a dependency. For example, pandas selects Excel and
Parquet engines dynamically; installed hydraulic packages have their own dependencies; copied wrappers are loaded
or launched dynamically. Preserve the full declared runtime set until test-node coverage and actual capabilities
have been compared in clean Windows environments.

### MCP consolidation boundary

The previous MCP job repeated four `tests/mcp` files and five utility test files already included by Hosted tests.
Preserve its unique `tests/functions/gdal/test_profiling.py`, compilation and MCP import/export assertion when
consolidating. The profiling file needs an explicit command because the main hosted command ignores the entire
`tests/functions/gdal` directory. Do not change the existing hosted exclusions as part of this consolidation.

The old MCP job installed the package non-editably, but [pytest.ini](../../pytest.ini) adds the checkout to `pythonpath`.
Its pytest runs therefore were not isolated installed-wheel acceptance. Hosted still resolves the full runtime/MCP
dependency metadata. Package retains the real build, verification and imports from an isolated installed wheel
outside the checkout, including registered MCP-tool behaviour. The removed duplication is a second source test
environment and package installation, not the sole installed-package check.

## Next action

Parent #272's recorded head passed Windows acceptance with every baseline selected node retained, exactly 26 new
scope-helper tests, unchanged 14 skip identities/reasons and unchanged eight deselections. Confirm the parent has
not changed, then implement the vendored-wheel delivery above. Continue comparing collection and capabilities after
distribution or dependency changes; a faster green job alone is insufficient evidence of retained coverage.

If further dependency reduction is worthwhile after measuring the consolidated jobs, investigate these stages in
a separate reviewable change:

| Stage | Environment and dependency boundary | Required evidence before narrowing |
| --- | --- | --- |
| Cheap checks | Ubuntu; standard library, pinned lint tool and focused helper-test dependency | Same patch, workflow, documentation and logging failures are caught before Windows setup |
| Strict typing | Windows; current full import environment plus typing tools | Changed-module imports and strict results agree, including initialiser stubs |
| Non-GIS behavioural tests | Proposed Windows group; dependency set derived from selected tests and transitive imports | Every selected node is assigned once; dynamic engines, subprocess imports and optional capability branches remain covered |
| GIS/native-backed tests | Windows with proven Fiona/Rasterio/GDAL binaries, pinned parser and supporting runtime packages | Native imports succeed; real synthetic GIS tests run; no missing-dependency skips |
| Installed package | Windows; existing wheel build and focused isolated smoke environment | Imported files resolve below the wheel target; packaged resources and copied wrappers still work |

The two behavioural groups are a proposal, not an adopted replacement for the current hosted suite. Additional
jobs add setup and billing overhead, so retain a single environment unless measured savings justify the split.

## Completion criteria

- Current-head Windows Policy, Hosted tests, preserved MCP gate and Package checks pass.
- Hosted dependencies retain the immutable PyTUFLOW source and every declared runtime dependency.
- MCP profiling, compilation and import/export checks actually run and propagate failures.
- Collection and skip-reason comparisons reveal no lost tests or optional-capability coverage.
- Installed-wheel isolation remains independently verified.
- Job and dependency-installation timings are recorded against the baseline; no speculative percentage saving.

## Validation and delivery

The baseline [Python CI run 42449907300](https://github.com/Chain-Frost/ryan-tools/actions/runs/42449907300) passed.
Its MCP job used 133 seconds, of which 86 seconds were dependency installation. Hosted tests used 280 seconds,
including 79 seconds of GIS/development installation and 169 seconds of pytest. These are one-run observations,
not an account-wide cost projection or a controlled benchmark.

The initial audit used Linux/Python 3.12.14, followed by focused checks on Linux/Python 3.14.9. Inspection covered
package metadata, pytest configuration, installer/setup tests and the hosted/MCP/package commands. Scope/setup/
installer contracts pass without the QGIS MCP bridge; native Windows collection and runtime acceptance remain
hosted checks. Parent PR #272 records its actual current-head results. This documentation handoff neither changes
dependencies nor claims that the proposed further split has passed Windows acceptance.

Local validation passed: `python repo-scripts/check_documentation.py --links-only
docs/work/2026-11-08-ci-dependency-groups.md`; profile include/extra resolution and pytest configuration assertions
also passed. Both profiles continue to resolve all 29 declared runtime requirements through package metadata.

## Focused follow-up prompt

```text
Repository: Chain-Frost/ryan-tools
Read AGENTS.md, docs/DEVELOPMENT_GUIDE.md, docs/work/README.md and
docs/work/2026-11-08-ci-dependency-groups.md. Read the current PR #272 and linked
CI evidence, then continue this stacked follow-up branch in a separate worktree.
Recorded predecessor: 7f45110b37eb143f9b785001fe5d04af526ab339. Fetch and record any
refresh before implementation; do not assume that an old green check covers it.

Audit whether the remaining Windows test dependencies can be reduced further.
First implement the 'First delivery: vendor the accepted PyTuflow wheel' section.
Use exactly 887534ea8c15b80d185a38a7ead1d42ab42a7167. The observed Windows wheel is
pytuflow-1.3.0-py3-none-any.whl, 976,447 bytes, SHA-256
4bfcd24734349f9e0dcf60776ee06a2e17953ac342f40e3611b0e2aae33b30e3.
Recover or build it once, verify its contents/provenance, and integrate the local
wheel across existing installer/requirements/CI routes. Preserve parser-only
same-version replacement, dependency resolution, capability guards and the
real model-replay provenance check. Include the artifact in workflow path/cache
inputs. Keep plain PyPI installation semantics explicit. No fresh native/private
engineering campaign is required solely for identical already accepted bytes.

Preserve normal dependency declarations, published pyproject.toml requirements,
the immutable PyTUFLOW source and installed-wheel isolation. Do not initialize or
inspect unsorted. Do not merge, enable auto-merge, release or publish packages.

Start by recording the exact existing Windows/Python 3.14 test commands,
collected node IDs, pass/deselect/skip counts and skip reasons. Measure setup
and execution separately. Inventory direct, transitive, dynamic-engine and
subprocess imports for any proposed group. An absent import must not make
importorskip or a capability fallback silently remove accepted coverage.

After the wheel delivery, trial only the smallest evidenced dependency change. Compare the same source
and test-data revisions in fresh environments, and require collection/capability
parity before adopting it. Preserve the explicit profiling, compilation and
MCP export checks and all wheel/copy-wrapper checks. Benchmark total runner
minutes as well as elapsed time; extra groups may cost more than one shared
installation. Record actual savings or reject the split if no benefit appears.

Implement only a justified result, run focused validation before one batch
push, and update this record and the PR with evidence and remaining native
acceptance. Keep any optional future work distinct from the delivered change.
```
