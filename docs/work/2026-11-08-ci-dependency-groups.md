# CI dependency groups and temporary PyTuflow wheel

| Field | Value |
| --- | --- |
| Status | Complete: Windows Policy benchmark [#299](https://github.com/Chain-Frost/ryan-tools/issues/299); retain canonical profile. Parser-pin retirement is a separate deferred upstream-release follow-up |
| Owner | ChatGPT / maintainer review |
| Created | 2026-11-08 |
| Updated | 2026-11-09 |
| Next review | — for completed delivery/benchmark; parser-pin retirement resumes only after an upstream release containing the required fixes |
| Fixed benchmark baseline | PR #251 at `92351854625dae81001c033306969f73eff4a7f7`, containing main `585e5d06dd599e68b1c470420481b5b9a3c79333` |
| Helper delivery | [#295](https://github.com/Chain-Frost/ryan-tools/pull/295) merged into #251 as `feae324a`; all nine integrated checks passed at `9235185` |
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

The CI-scope contracts now run in a pytest-only phase at the start of the existing Windows Hosted job, delivered by
#295. The full runtime/MCP and Policy environments remain intact. The bounded Policy dependency benchmark in #299
is complete and found no repeatable saving; the canonical profile is retained. This record owns that dependency
boundary, the assessment of further reductions and the separate future retirement of the temporary parser pin.
No separate non-GIS/GIS jobs are adopted. See the
[CI resource record](2026-11-08-ci-resource-efficiency.md) for the preceding delivery history.

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

### Upstream retirement trigger

This is a separate deferred follow-up, not unfinished work for #299. Retire the temporary wheel and its pin only
when a released upstream build contains the required fixes and passes
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

The full CI installation profiles retain dependency resolution through the authoritative package metadata. A small
shared helper profile allows Windows CI-scope failures to stop Hosted before fixture and native setup:

| Job | Requirements | Retained contract | Omitted work |
| --- | --- | --- | --- |
| Early Windows CI-scope contracts | [ci-helper-requirements.txt](../../repo-scripts/ci-helper-requirements.txt) | Pytest, the standard library and Git; the entire existing scope-test file runs once | Project/runtime installation and fixture checkout are delayed until these tests pass |
| Hosted tests, including consolidated MCP checks | [ci-test-requirements.txt](../../repo-scripts/ci-test-requirements.txt) | All declared runtime dependencies, MCP extra, pytest and immutable PyTUFLOW source | The `dev` extra and separate interactive QGIS MCP bridge |
| Windows strict typing | [ci-policy-requirements.txt](../../repo-scripts/ci-policy-requirements.txt) | All runtime dependencies, canonical development/MCP extras and immutable PyTUFLOW source | Separate interactive QGIS MCP bridge |
| Package / installed-wheel smoke | [wheel-smoke-requirements.txt](../../repo-scripts/wheel-smoke-requirements.txt) | Existing focused smoke dependencies and isolated installation | No new removals in this audit |

Run profile installation from the repository root. Install the helper profile before fixture checkout and native
setup; install the full hosted and Policy profiles after the existing Windows binary GIS bootstrap. Hosted includes
the shared helper profile instead of maintaining a second pytest requirement. The nested
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

Retain the canonical Policy development extra. [Issue #299](https://github.com/Chain-Frost/ryan-tools/issues/299)
is complete; no new production profile, additional hosted experiment or CI setting change is justified for this
candidate. The temporary workflow is retired and compact evidence is archived on the isolated
`ci/policy-dependency-benchmark-299` branch. No PR or main merge is needed for this negative benchmark result.

The only separate dependency follow-up is [parser-pin retirement](#upstream-retirement-trigger) after a released
upstream build supplies the required fixes and passes the existing acceptance contract.

## Completed Policy benchmark (2026-11-09)

[Run 42516847229](https://github.com/Chain-Frost/ryan-tools/actions/runs/42516847229) passed on its first attempt at
experiment commit `33c9c650a6df719b5cb7369b869c28ce26f5cd74`, using the fixed source above. Windows image
`win25-vs2026 20261103.298.1`, Python 3.14.10, pip 26.3 and Pyright 1.1.414 were fixed across eight fresh source
snapshots and environments. Canonical and candidate profiles shared identical retained package versions and
remote artifact identities. The second repeat reversed the profile order.

| Cache | Repeat | Canonical Policy stages (s) | Candidate Policy stages (s) | Candidate minus canonical (s) |
| --- | --- | ---: | ---: | ---: |
| Empty | 1 | 67.195 | 67.516 | +0.321 |
| Populated | 1 | 62.527 | 63.467 | +0.941 |
| Empty | 2 | 67.257 | 72.026 | +4.769 |
| Populated | 2 | 64.770 | 64.069 | -0.701 |

These sums include GIS bootstrap, requirements generation, profile resolution/installation, accepted-parser
verification and the same 20-file strict typing replay. They are not full production Policy job durations.
The candidate was slower in three of four pairs; the single 0.701-second improvement did not repeat. This pilot
does not prove an intrinsic slowdown, but it establishes no saving that justifies another maintained profile.

All eight environments passed `pip check`, installed-parser provenance, 11 capability probes and the identical
20-file Pyright selection with zero errors, warnings or information messages. The installed distribution count
fell from 143 to 138: only `build`, `ruff`, `pytest-cov` and their unnecessary transitive distributions `coverage`
and `pyproject-hooks` disappeared. Shared versions/artifacts remained identical; the only removed pytest plugin
was `pytest_cov`. Runtime/MCP requirements, the other ten development entries and accepted parser bytes remained.

The complete single Windows job took 789 seconds (13 minutes 9 seconds), including 100.077 seconds of preparation;
the harness took 764.224 seconds. One serial runner and two repeats limit extrapolation. Cold/warm refers only to
pip caches; each populated cache exactly inherited its paired empty-cache result but still needed some Fiona/GDAL
downloads. OS/network/CDN caches were uncontrolled. Exact artifact constraints also differ from unconstrained
routine resolution. No ordinary CI, full Hosted suite or native solver campaign was triggered by the experiment.

The [benchmark guide](../../repo-scripts/benchmarks/README.md#windows-policy-dependencies) links the archived plan,
artifact lock, raw commands/timings and independently reconciled comparison. The original 5,719,318-byte artifact
has SHA-256 `05b75a298a4d9794cb2bef7ae827666e3fc18ed6504c36b31de75898a21a581b` and expires on 2026-11-23
Australia/Perth time. The retirement/evidence batch only removes the temporary workflow and updates this isolated
experiment archive; production profiles, normal workflows, main and #251 are unchanged by it. Results and final
delivery links are recorded in #299 rather than a further status-only source commit.

### Completed helper-stage evidence

#295's reviewed source `9fa11c9` passed all six Python CI jobs with an exact 3,241-node union. After its integration
and the independent WP04 additions, `9235185` passed all nine checks: 57 early passes, 3,174 broad passes and the same
14 skips, plus ten profiling and 64 pandas compatibility passes. All original nodes remain, with exactly four new
WP04 cases. See the [final acceptance and timing receipt](https://github.com/Chain-Frost/ryan-tools/pull/295).
Successful-run savings were not demonstrated; the established benefit is earlier scope-failure detection.

The feature baseline contains **57** real-Git CI-scope cases, including the three dependency/version cases added
after main's 54-case checkpoint. They stay on Windows. The early phase uses `--confcutdir=tests/repo_scripts` and
disables plugin autoload only for that step; the later broad suite ignores only this exact file. No test assertion
or runtime capability is removed. On the first PR attempt, a helper failure stops Hosted's subsequent fixture/GIS
installation. Main/manual runs and PR reruns retain the remaining broad diagnostics, then explicitly fail Hosted
using the helper step's original outcome. The required MCP gate rejects the failed Hosted job. Independent Policy
and Package jobs retain their existing scheduling.

The dependency audit at #251/package `26.11.9.2` retains 29 declared runtime requirements and 13 development entries.
Hosted already omits the development extra. Policy still needs the runtime import environment and typing support;
`distributed`, `pillow_heif` and `docx` have actual first-party imports. The only evidenced optional Policy removals
are `build`, `ruff` and `pytest-cov`. The completed #299 benchmark found no repeatable timing benefit, so the
canonical development extra remains. The focused prompt below preserves the completed experiment's scope.

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

**Historical scope, completed by #299 on 2026-11-09.** Retain the canonical profile using the evidence above;
do not treat this prompt as a request for another experiment.

```text
Repository: Chain-Frost/ryan-tools
Optional bounded follow-up: benchmark removing unused execution tools from
Windows Policy only. Read AGENTS.md, docs/DEVELOPMENT_GUIDE.md, docs/work/README.md
and this record; inspect current main, #251 and open CI PRs before choosing a
fresh isolated child. Do not repeat the accepted wheel or early-helper work.

Compare the canonical .[dev,mcp] profile with a scratch candidate containing
the same runtime/MCP/parser requirements and every current dev entry except
build, ruff and pytest-cov. Preserve exact version constraints and markers.
At baseline 63c7da77 the ten retained development entries are:
  pandas-stubs>=3.1.0.261029, pyarrow-stubs, types-psutil,
  types-rasterio>=1.5.2.20261029, scipy-stubs, distributed,
  pillow-heif>=1.8.0, python-docx, pyright>=1.1.414, pytest.
Recompute that set from current metadata; retain all newly added entries unless
their omission is independently justified. Never drop them through a stale list.

Use the identical Windows/Python 3.14 source and fixture revisions. Resolve each
candidate once and reuse exact package artifacts/versions for timing. Compare
clean installation with an empty download cache and clean installation from a
populated cache; an already-installed environment is not a warm-cache test.
Time GIS bootstrap, profile resolution/install, strict checking and total runner
use separately, with a repeat in reversed order to expose transient variance.

Require matching pip-check results, runtime/MCP capability probes and strict
Pyright diagnostics for the same maintained files, including initialiser stubs
and files importing the development-provided runtime modules. Keep Hosted's
full runtime/MCP profile, selected nodes, deselections, skip reasons, profiling,
pandas compatibility, parser provenance and installed-wheel checks unchanged.

Adopt a production profile only if repeatable Windows setup savings justify its
maintenance and coordination cost. A smaller download alone is insufficient.
If no benefit is established, record the result and retain the canonical extra.
Do not introduce a production installer merely to perform this experiment.

Preserve normal user dependency declarations and accepted PyTuflow bytes; never
initialize or inspect unsorted. Publish one checked batch with evidence and
fresh remote-head guards. No main merge, release or package publication is
authorized by this optional handoff.
```
