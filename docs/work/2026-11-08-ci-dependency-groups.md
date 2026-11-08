# CI dependency groups and temporary PyTuflow wheel

| Field | Value |
| --- | --- |
| Status | Deferred: further dependency reduction; accepted wheel delivery complete |
| Owner | ChatGPT / maintainer review |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Delivery | [PR #274](https://github.com/Chain-Frost/ryan-tools/pull/274) integrated into [PR #272](https://github.com/Chain-Frost/ryan-tools/pull/272); merged to main as `8c03dc961455c124fe4e0996ee15f50813cf5682` |
| Historical predecessor | Initial PR #272 head `7f45110b37eb143f9b785001fe5d04af526ab339` |
| Timing baseline | Previous PR #272 head `1ae1235c2369950c287c4fafe24ef23848880fe0` |

## Outcome and scope

Keep Windows/Python 3.14 as the primary acceptance environment while avoiding dependencies that a CI job does not
use. Small POSIX checks retain their specific portability and filesystem-safety purpose. Normal user installation,
published package dependencies, native engineering acceptance and release behaviour are separate contracts.

The accepted wheel, provenance checks, installer integration and initial CI profiles are complete and merged.
Child #274 was integrated into #272 as `05939c936282aedfce5f090a2434fbd49e6d7d10` on 2026-11-08 at 08:33 UTC;
#272 was merged into main as `8c03dc961455c124fe4e0996ee15f50813cf5682` at 08:51 UTC. Both PRs are closed.
Do not continue the old child branch or rebuild the accepted wheel as unfinished work.

This record now owns only the deferred investigation of further dependency reduction and eventual retirement of
the temporary parser pin. No additional non-GIS/GIS test split is adopted. See the
[CI resource record](2026-11-08-ci-resource-efficiency.md) for operational validation and active-stack adoption.

## Completed delivery: accepted PyTuflow wheel

A temporary PyTuflow wheel is stored in ordinary Git until a released upstream version supplies
the required fixes. The exact Windows/Python 3.14.10 wheel was reproduced on the child branch and
matched the accepted 976,447-byte SHA-256. Only the dependency distribution route changes; no
runtime, normal developer or published project dependencies are removed.

### Exact candidate and existing Windows evidence

The current [parser requirement](../../repo-scripts/pytuflow-requirement.txt) selects the same source as
[upstream PR #17](https://github.com/TUFLOW-Support/PyTuflow/pull/17). This is the fork contribution
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
Their retained artifacts did not contain the PyTuflow wheel; pip wrote it to temporary caches. The exact
candidate was reproduced in [Windows bootstrap run 42458022422](https://github.com/Chain-Frost/ryan-tools/actions/runs/42458022422)
and committed to #274 (source and hash unchanged). The build disabled runtime dependency installation. The Hatchling build hook reads
the version without importing the GIS runtime. If rebuilt ZIP bytes differ, investigate content and tool-version
differences before accepting a new hash; do not silently substitute another source or relabel old acceptance.

The wheel is about 97.2% smaller than the source download. The actual wheel-build step took about 0.08 seconds;
avoidable source download/extraction, build-environment setup and metadata preparation took approximately 6-10
seconds per observed installation. These are preparation observations, not a controlled total CI saving. Native
dependencies account for much of the remaining installation time.

### Implemented installation contract

1. The wheel is ordinary Git binary data under
   `repo-scripts/wheels/pytuflow/887534ea8c15b80d185a38a7ead1d42ab42a7167/`, with a README and provenance manifest.
   The verifier checks source identity, hash, size, metadata, licenses, CLI entries, `py.typed` and runtime resources.
   It is outside LFS and build-owned `dist/`; package verification rejects nested dependency wheels.
2. The shared requirement selects that local wheel. The installer verifies the requirement and artifact, then uses
   an absolute path independent of the working directory. Documented requirements commands run from the repository
   root. Missing or corrupt bytes are errors, with no silent source-download fallback.
3. Normal installers retain parser-only `--force-reinstall --no-deps`, followed by joint dependency resolution,
   `pip check`, installed provenance and the public capability guard. Same-version replacement, GIS bootstrap,
   QGIS MCP defaults, legacy migration and separate recovery mode remain covered.
4. Installed provenance requires pip's `direct_url.json` to name the accepted local file and digest. Consumer/model
   checks retain incompatible-parser rejection and broken-import/shadowing checks. PyPI-installer tests allow the
   explicit parser wheel while continuing to reject accidental local ryan-tools wheel substitution.
5. Wheel, manifest and verifier changes are inputs to targeted TUFLOW triggers and relevant pip caches. The cheap
   preflight verifies the committed artifact before native installation; required checks and separate cache profiles
   remain intact.
6. Plain `pip install ryan-tools` still resolves published dependency metadata through PyPI. It does not discover a
   wheel beside repository scripts. Published metadata and every normal runtime/developer dependency are unchanged.

Retire the temporary wheel and its pin only when a released upstream build contains the required fixes and passes
consumer capability checks. Merging the upstream PR alone does not provide a published installation artifact.

Verify the artifact hash separately or use a deliberate supported hash mechanism. Adding `--hash` to the shared
requirements line can enable pip's global hash-checking mode and conflict with editable installs and unpinned
dependencies. Do not turn this bounded change into a full dependency lock migration. See
[pip installation](https://pip.pypa.io/en/stable/cli/pip_install/) and
[hash-checking rules](https://pip.pypa.io/en/stable/topics/secure-installs/).

### Acceptance to preserve for any replacement

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

Keep further dependency reduction deferred while the merged CI reaches active stacks and operational validation
is reconciled in the [CI resource record](2026-11-08-ci-resource-efficiency.md). Select a separate, bounded benchmark
only if measured setup or execution costs justify it. Do not reopen the completed wheel delivery or split profiles
without same-source collection, skip-reason, capability and total-runner-use evidence.

One focused candidate is moving the 26 real-Git CI-scope helper tests to Ubuntu preflight while retaining the actual
Windows Policy manifest invocation and all Windows runtime, MCP and installed-package acceptance. At initial parent
acceptance those tests consumed 21.382 summed testcase seconds on Windows; a local Linux/Python 3.14 run took
1.86 seconds for the suite. These are different measures on different machines, not a controlled saving estimate.
No test move or new performance experiment is implemented by this handoff refresh.

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

## Acceptance criteria for a future dependency change

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
hosted checks. The final parent and child results below establish Windows acceptance for the delivered wheel
integration. No narrower Windows groups have been adopted.

Local validation passed: `python repo-scripts/check_documentation.py --links-only
docs/work/2026-11-08-ci-dependency-groups.md`; profile include/extra resolution and pytest configuration assertions
also passed. Both profiles continue to resolve all 29 declared runtime requirements through package metadata.

### Vendored-wheel implementation checkpoint (2026-11-08)

- **Source and identity:** fork source `887534ea8c15b80d185a38a7ead1d42ab42a7167`;
  `pytuflow-1.3.0-py3-none-any.whl`; **976,447 bytes**;
  SHA-256 `4bfcd24734349f9e0dcf60776ee06a2e17953ac342f40e3611b0e2aae33b30e3`.
  The original accepted Windows three-job digest was reproduced exactly in
  [run 42458022422](https://github.com/Chain-Frost/ryan-tools/actions/runs/42458022422).
  Wheel, provenance manifest and README are checked into ordinary Git in `repo-scripts/wheels/pytuflow/`.
- **Build provenance:** Windows CPython 3.14.10, pip 26.3, Hatchling 1.33.0;
  source download, wheel build, no runtime dependency install. Original wheel bytes were reproduced, not
  taken from the separate #65 fork build. The one-time bootstrap workflow was removed after successful
  artifact delivery to avoid repeated wheel builds.
- **Integrity:** `vendor_pytuflow_wheel.py` verifies SHA-256, size, ZIP checksums, metadata, licensing,
  CLI entry points, `py.typed`, required runtime JSON resources and provenance manifest;
  `--installed` also requires matching pip-installed wheel archive URL and SHA-256.
  The two existing installers retain parser-only force reinstall, full dependency resolution,
  `pip check` and public capability verification. Requirements are root-relative for normal
  repository commands; installers use absolute paths independent of current directory.
- **Validation routing:** the cheap preflight checks wheel integrity before native setup.
  Windows Hosted, Policy and TUFLOW caches include binary/provenance inputs.
  The TUFLOW workflow reacts to a wheel-only change and retains incompatible-parser rejection
  and the 161-TCF/174-run replay. The ryan-tools wheel verifier rejects nested dependency wheels.
- **Contracts retained:** declared normal/dev/runtime PyPI dependencies unchanged, including
  `pytuflow>=1.2,<2` metadata; plain `pip install ryan-tools` cannot discover a checked-in
  sidecar wheel. Reuse accepted native/private engineering evidence for identical wheel bytes.
  Windows source/installer/CI acceptance passed at the final child and integrated parent heads below.
- **Further grouping:** no additional GIS/non-GIS test split is adopted. Existing Windows collection
  parity and skipped capabilities must be preserved before trialing any new dependency profile.
  No controlled total-runner-minute or installation saving is yet claimed.

### Completed Windows acceptance and integration (2026-11-08)

- Child head `169c4631801c0c8d237fe87f110b8307e4a4832c` passed
  [Python CI 42459842586](https://github.com/Chain-Frost/ryan-tools/actions/runs/42459842586) and
  [TUFLOW validation 42459842680](https://github.com/Chain-Frost/ryan-tools/actions/runs/42459842680).
- Integrated parent `05939c936282aedfce5f090a2434fbd49e6d7d10` passed
  [Python CI 42463452659](https://github.com/Chain-Frost/ryan-tools/actions/runs/42463452659) and
  [TUFLOW validation 42463452656](https://github.com/Chain-Frost/ryan-tools/actions/runs/42463452656).
  Hosted reported 2,695 collected, eight deselected, 2,673 passed and the same 14 skipped cases/reasons;
  separate profiling passed all 10 tests. Installed provenance, strict typing, isolated wheel/copied-wrapper
  checks and the 161-TCF/174-context replay passed. The seven added passing cases cover the wheel integration.
- Codex reviews of the child and integrated parent reported no findings or unresolved findings. The integrated
  implementation was merged to main as `8c03dc961455c124fe4e0996ee15f50813cf5682`. These premerge results do not
  substitute for checks on subsequent source changes; post-merge operations are tracked in the linked CI record.

## Focused follow-up prompt

```text
Repository: Chain-Frost/ryan-tools
Read AGENTS.md, docs/DEVELOPMENT_GUIDE.md, docs/work/README.md and
docs/work/2026-11-08-ci-dependency-groups.md, then fetch current main and record
its exact SHA. PRs #274 and #272 are closed and merged; implementation baseline
8c03dc961455c124fe4e0996ee15f50813cf5682 already includes the accepted wheel,
provenance checks, CI profiles and MCP consolidation. Do not continue the old
child branch or rebuild/reintegrate the same accepted wheel.

This brief is deferred future work, not an instruction to start a benchmark.
When a maintainer selects it, use a separate worktree and one bounded candidate.
Coordinate with active stack owners before adopting their source. Keep the
accepted parser bytes and normal user dependency declarations unchanged.

Preserve normal dependency declarations, published pyproject.toml requirements,
the immutable PyTUFLOW source and installed-wheel isolation. Do not initialize or
inspect unsorted. Do not merge, enable auto-merge, release or publish packages.

Start by recording the exact existing Windows/Python 3.14 test commands,
collected node IDs, pass/deselect/skip counts and skip reasons. Measure setup
and execution separately. Inventory direct, transitive, dynamic-engine and
subprocess imports for any proposed group. An absent import must not make
importorskip or a capability fallback silently remove accepted coverage.

Consider the CI-scope helper-test move before a broad GIS/non-GIS split, while
keeping real Windows Policy scope invocation and user-runtime acceptance.
Trial only the smallest evidenced change. Compare the same source
and test-data revisions in fresh environments, and require collection/capability
parity before adopting it. Preserve the explicit profiling, compilation and
MCP export checks and all wheel/copy-wrapper checks. Benchmark total runner
minutes as well as elapsed time; extra groups may cost more than one shared
installation. Record actual savings or reject the split if no benefit appears.

Implement only a justified result, run focused validation before one batch
push, and update this record and the PR with evidence and remaining native
acceptance. Keep any optional future work distinct from the delivered change.
```
