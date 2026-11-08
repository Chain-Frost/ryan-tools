# CI resource efficiency and Windows acceptance

| Field | Value |
| --- | --- |
| Status | Active: stack adoption; main CI recovery verified |
| Owner | ChatGPT / maintainer |
| Created | 2026-11-08 |
| Updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Delivery | [PR #272](https://github.com/Chain-Frost/ryan-tools/pull/272), including [#274](https://github.com/Chain-Frost/ryan-tools/pull/274), merged to main as `8c03dc961455c124fe4e0996ee15f50813cf5682` |
| Recovery | [PR #277](https://github.com/Chain-Frost/ryan-tools/pull/277) merged as `c1a66970c8ed228ab94404b4f747a40c755847cd`; all six post-merge jobs passed |
| Scope correction | [PR #278](https://github.com/Chain-Frost/ryan-tools/pull/278) owns current-head acceptance and integration evidence |
| Historical timing baseline | `1ae1235c2369950c287c4fafe24ef23848880fe0` |

## Scope and decisions

Windows/Python 3.14 remains the target acceptance environment. Ubuntu supplies inexpensive portable preflight and
the existing narrow POSIX filesystem checks. Normal package and developer dependencies remain unchanged. This work
reduces repeated environments and unnecessary starts; it does not remove engineering or installed-wheel acceptance.

The merged implementation makes cheap validation a prerequisite for expensive jobs, consolidates duplicated MCP
tests, adds CI-specific dependency profiles and the verified PyTuflow wheel, and documents batched pushes. PR #274
was integrated into #272 at 08:33 UTC on 2026-11-08; #272 was merged to main at 08:51 UTC. Both PRs are closed.
This record now tracks operational main validation and adoption by active stacks. Further dependency reduction
is deferred in the [dependency record](2026-11-08-ci-dependency-groups.md).

## Implemented behaviour

- `Ruff and whitespace` now runs inside Python CI before runtime, packaging and native type-check installation.
  It also checks workflow syntax, documentation and logging policy. The independent Fast PR workflow is removed.
- The existing required names remain: `Ruff and whitespace`, `Policy`, `Hosted tests`, `Package` and `MCP smoke`.
  Policy explicitly fails after a failed preflight rather than inheriting a skipped dependency status.
- `repo-scripts/ci_scope.py` classifies complete PR diffs with rename detection disabled. Missing or invalid
  comparisons select full validation and all first-party static inputs. Helper execution errors fail preflight.
  Nonempty, entirely Markdown PR changes can skip runtime jobs; main and manual runs request full validation.
- Scope manifests avoid Windows environment-variable limits. Policy recomputes the same complete-PR inputs locally
  and bounds Pyright command length. Lint/type configuration changes expand the appropriate static-check selection.
  A numeric `project.version` string replacement alone retains changed-file checks when both committed TOML
  documents otherwise match in values and types. Runtime/package checks still run. Other metadata or dependency
  changes, nested project configuration and unreadable/invalid comparisons keep full static expansion.
- Hosted tests use the new test profile: all 29 runtime requirements, the MCP extra, pytest and pinned PyTUFLOW.
  Unused development tools and the independent QGIS MCP bridge are omitted only from this CI installation.
- The old MCP environment is removed. Its overlapping source tests remain in Hosted tests; explicit profiling,
  compilation and MCP import/export acceptance are retained on Windows. The cheap required MCP gate verifies both
  successful Hosted tests and explicit MCP evidence. Real isolated wheel checks remain in Package.
- The Windows typing profile preserves the full runtime/development/MCP import environment but omits the unused
  QGIS MCP bridge. Package retains its existing focused smoke profile and all real build checks.
- First-attempt PR pytest stops at the first failure. Reruns on the same revision and main runs retain complete
  diagnostics. Existing PR cancellation and per-revision main release validation remain.
- TUFLOW input validation cancels superseded PR runs and starts the exhaustive 161-TCF regression only after its
  API/rejection prerequisites pass. Three distinct pip-cache profiles prevent lightweight jobs from freezing the
  native-wheel cache. Every model report is still collected; retention is seven days.
- AGENTS and canonical guides now require Windows-first acceptance, selective source/fixture checkout, focused
  local checks and one batched push per review/hosted-validation milestone. Normal user installation is unchanged.

See [environment and CI guidance](../ENVIRONMENTS.md#github-actions-ci) for current commands and profiles.

## Evidence and validation

### Initial implementation evidence

The previous head's [Python CI run 42449907300](https://github.com/Chain-Frost/ryan-tools/actions/runs/42449907300)
passed. Its Hosted job used 280 seconds, including 79 seconds installing GIS/development dependencies and 169 seconds
running pytest. MCP used 133 seconds, including 86 seconds installing dependencies. These are individual observations,
not a controlled saving estimate. Removing the second source-test environment targets that repeated setup.

The initial audit used Linux/Python 3.12.14. Final scope/static checks also ran on Linux/Python 3.14.9; neither
establishes Windows/PowerShell or native GIS acceptance:

- 26 focused real-Git scope tests pass, including cumulative changes, code-to-Markdown renames, deleted files,
  unknown comparisons, configuration changes, excluded submodules, manifest output and manifest-write failure.
- Scope plus existing QGIS setup and installer checks pass: 70 tests without the external QGIS MCP bridge.
- Modified Python files pass Ruff lint/format and strict Pyright with no diagnostics.
- Default documentation/index coverage, all changed-document links and the Python 3.14 logging policy check pass.
- Actionlint passes the final workflow structure. Independent review checked required-status propagation, explicit
  MCP evidence, bounded Pyright arguments, preserved wheel validation and distinct cache hashes.
- Package/developer metadata and the existing hosted selection/exclusions remain unchanged. This initial checkpoint
  added the scope-helper test file; the later wheel integration added seven further passing cases.

The initial parent head `7f45110b37eb143f9b785001fe5d04af526ab339` passed
[Python CI 42453824693](https://github.com/Chain-Frost/ryan-tools/actions/runs/42453824693) and
[TUFLOW validation 42453824703](https://github.com/Chain-Frost/ryan-tools/actions/runs/42453824703).
JUnit comparison retained every one of the 2,654 baseline selected nodes, added exactly 26 scope cases and changed
no skip identities or reasons. The broad suite passed 2,666 with 14 skips and eight deselections; the separate
profiling suite passed all 10 cases. The original Markdown-only child also exercised the cheap routing path in
[run 42454127128](https://github.com/Chain-Frost/ryan-tools/actions/runs/42454127128): preflight and Policy passed,
heavy jobs skipped, and its separate PR concurrency group left the parent run intact.

Observed summed job wall time rose from 624 to 696 seconds across those different-input runs. The initial baseline
had no Python changes and skipped real typing; the new source exercised the full Windows type environment and
added tests. These observations do not establish a controlled total-runner saving or billed-minute reduction.

### Final reviewed integration

Integrated parent head `05939c936282aedfce5f090a2434fbd49e6d7d10`, containing child #274's accepted wheel, passed
[all six Python CI jobs](https://github.com/Chain-Frost/ryan-tools/actions/runs/42463452659) and
[all three TUFLOW jobs](https://github.com/Chain-Frost/ryan-tools/actions/runs/42463452656).
Hosted collected 2,695 cases, deselected eight, passed 2,673 and retained the same 14 skips/reasons; the test phase
took 189.57 seconds. Explicit profiling passed 10 cases. Windows typing, MCP compilation/exports, installed parser
provenance, isolated wheel/copied-wrapper checks and the 161-TCF/174-context replay all passed. Codex's
[integrated review](https://github.com/Chain-Frost/ryan-tools/pull/272#issuecomment-6719351353) reported no findings.
This accepted implementation is now in main at `8c03dc961455c124fe4e0996ee15f50813cf5682`.

### Historical post-merge download failure and recovery

Main [Python CI 42464717133](https://github.com/Chain-Frost/ryan-tools/actions/runs/42464717133) failed only in Policy:
the GIS bootstrap received HTTP 504 downloading the Fiona wheel from the geospatial wheel release. Typing had
not started. The other five jobs passed, including Hosted's parser-origin verification and Package/MCP acceptance.
The same-head failed-job rerun (attempt 2) encountered the same HTTP 504. A separate read-only request reproduced
the failure even though the release API still listed the asset.

The official public GitHub asset API for
[Fiona asset 521560770](https://api.github.com/repos/cgohlke/geospatial-wheels/releases/assets/521560770)
returned the same 834,605-byte artifact with its expected SHA-256 while the canonical release-download URL still
returned HTTP 504. Recovery must retain that exact release-file identity; an alternative route is not permission
to substitute another package or version.

The focused CI recovery uses the normal GIS installer command first. Only a matching transient 502/503/504 failure
for an official Fiona/Rasterio/GDAL release wheel can use a bounded public-API download. The release/tag/filename,
asset URL, size and SHA-256 must agree before installing the temporary wheel. The original command must then run
again with full dependency resolution; a verified download or `--no-deps` pre-install alone cannot pass the check.
Normal developer installers and declared dependencies remain unchanged. Final-head validation and merge evidence
belong in the recovery PR description, avoiding another source push solely to record its CI outcome.

Recovery [PR #277](https://github.com/Chain-Frost/ryan-tools/pull/277) passed all nine candidate checks and merged
as `c1a66970c8ed228ab94404b4f747a40c755847cd`, with the reviewed tree unchanged. All six
[post-merge jobs](https://github.com/Chain-Frost/ryan-tools/actions/runs/42468485543) passed. Hosted reported
2,721 passes, 14 skips and eight deselections; explicit profiling passed all ten tests and strict Pyright had no
diagnostics. The original GIS dependency resolver succeeded after verified Fiona/GDAL API recovery. Existing
release opt-outs were respected by the release, publisher and asset jobs; no package was published.

### Stack adoption and version-only metadata

The #251 owner integrated WP08 [#265](https://github.com/Chain-Frost/ryan-tools/pull/265) into the feature branch
at `e8dba9797f0a4df01a32eb62d17f1e5dd4df4352`, then merged repaired main into it at
`0e7b8ca50a14e560866b20697c31fc494907199d`. Both engineering and CI parents are retained. This supersedes the
earlier plan to update `d6192567`; do not publish an adoption candidate prepared against that stale head.

That revision's [preflight](https://github.com/Chain-Frost/ryan-tools/actions/runs/42468936313) failed in 14 seconds.
Policy explicitly failed and Hosted, Package, MCP and POSIX skipped, so no heavy Windows setup began. The only
project-metadata difference from main was version `26.11.8.1` to `26.11.8.2`, but the classifier selected every
first-party Python file and exposed 23 existing findings in 14 unchanged files. An unchanged rerun cannot fix this.

The narrow classifier correction compares the committed before/after TOML rather than ignoring metadata paths.
Only a numeric release-version string replacement is exempt from broad static selection. It preserves
changed-Python checks, complete-PR scope, full runtime/package validation and conservative handling of every other
metadata/configuration change, including nested `pyproject.toml` files. Real-Git regression cases cover the
exception and its failure boundaries. Final exact-head Windows outcomes belong in the correction PR and #251
descriptions, without a notes-only source push.

The corrected candidate `7480da5` passed all six [Windows/portable CI jobs](https://github.com/Chain-Frost/ryan-tools/actions/runs/42471008323),
including 2,744 broad-suite passes, unchanged 14 skips/eight deselections and all 49 scope cases. Windows acceptance
caught a subprocess reader-thread decoding difference; explicit byte capture and caller-thread UTF-8 decoding now
ensure malformed metadata reaches the conservative fallback. The fresh configured review found no issues.

While that validation ran, [#275](https://github.com/Chain-Frost/ryan-tools/pull/275) merged into main as `8de77a6`,
including projection authority corrections and an initial version-only classifier. The consolidated correction
preserves its runtime source, projection tests, metadata and native acceptance record, and retains all five of its
scope regressions alongside the 49 accepted cases. The resulting 54-case helper and updated base need their own
current-head CI, recorded in #278. Reuse the accepted projection-native evidence for the unchanged engineering code.

## Acceptance and next action

1. Review exact-head CI on stack root [#251](https://github.com/Chain-Frost/ryan-tools/pull/251) after its owner-led
   main adoption and the narrow version-scope correction. Main recovery is complete; the two failures on
   `8c03dc9` and deterministic version-scope failure on `0e7b8ca` remain historical evidence. Do not rerun those
   unchanged revisions or rebuild the accepted parser wheel merely to refresh status notes.
2. Re-read main and the feature ref before any further integration; owners are changing the same stack. Merge
   the latest accepted CI correction in one locally checked batch, retaining concurrent engineering work and
   the updated WP08 acceptance record. Never force-update a candidate prepared against an older feature head.
3. Retain #251's native cutline-test exclusion and pandas 2.3.3 POMM/culvert compatibility step. Run that
   compatibility override after the default-environment MCP profiling/export
   checks; a failure still fails Hosted and its downstream required MCP gate.
4. Dependent owners should fetch their updated immediate parent at their next coherent update: #254, #255, #259
   follow #251; #261 follows #254 and #263 follows #259 after those parents adopt #251. WP08 #265 is already
   integrated and must not be treated as an outstanding child.
   Re-read each child's current head before integration,
   preserve concurrent engineering changes, and push one checked batch per owner milestone. Do not automatically
   update every child or merge these engineering PRs into main as part of this CI adoption.
5. Keep further dependency experiments separate and deferred. The follow-up brief starts from the delivered wheel
   and current CI, requires same-source collection/skip/capability parity and compares total runner use, including
   extra setup. No additional profile rewrite or performance experiment is part of this refresh.

Several unrelated work-register rows already have overdue review dates. Their owners and histories are preserved;
this CI task does not reopen or implement those work fronts.
