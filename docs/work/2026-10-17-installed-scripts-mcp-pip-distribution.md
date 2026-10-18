# Installed scripts, MCP workflow distribution and package identity — requirements traceability

| Field | Value |
| --- | --- |
| Status | **Needs review — requirements restored; corrective PR #137 pending validation** |
| Owner | ChatGPT (corrective PR author); maintainer retains acceptance |
| Created | 2026-10-17 |
| Updated | 2026-10-19 |
| Next review | On corrective PR checks/review; no later than 2026-10-26 |
| Original requirements | [Complete 17 October baseline, preserved verbatim](../audits/2026-10-17-installed-scripts-original-requirements.md) |
| Previous implementation | [Prematurely merged PR #130](https://github.com/Chain-Frost/ryan-tools/pull/130), squash commit `1ac42c6` |
| Corrective branch | `fix/pr130-review-and-requirements-20261019` / [PR #137](https://github.com/Chain-Frost/ryan-tools/pull/137); **no automatic merge** |

## Controlling rule: no silent scope reductions

The linked **original requirements document** is part of the active requirements set. **Every original bullet remains in
scope** until it is verified against the implementation, or a later, explicit maintainer decision supersedes it.
Historical "no implementation/leave draft" sequencing instructions describe the 17 October planning phase,
not permanent restrictions on future authorized work.

**Merged PR #130 did not prove that the original scope was complete.** It implemented a manifest and a copy helper,
and several other requirements were already implemented in PR #120, but it prematurely converted incomplete
validation into optional follow-ups and declared 83 files "reviewed" without per-file assessments.
The manifest currently installs 83 files and excludes 63 source-tree files; inclusion/exclusion is **provisional**.
Neither "not included in the wheel" nor "included in the wheel" proves a file's lifecycle or operational safety.

The requirement to finish/merge PRs #118 and #119 before PR #130 **was specifically removed by the maintainer**.
Those PRs remain independent work; their own technical correctness and data validation remain required.
The separately decided Python minimum is **>=3.13**, including `ryan-culverts`; Python 3.14 remains the
preferred development environment. Any future move to 3.14-only needs its own decision.

## Status definitions

- **Implemented, verification incomplete**: code or packaging exists, but at least one original acceptance
  gate lacks evidence. Do not mark "complete" simply because hosted CI passed.
- **Partially implemented**: some behavior exists, but the original requirement has an unimplemented portion.
- **Not assessed**: evidence was not gathered; do not interpret this as working or defective.
- **Verified (bounded)**: named tests establish the specific limited behavior, not general engineering acceptance.
- **Superseded with decision**: a documented later maintainer decision explicitly changed the requirement.

## Original non-negotiable decisions (D1–D9)

| ID | Original requirement | Current assessment and supporting evidence |
| --- | --- | --- |
| D1 | Keep GitHub repository `Chain-Frost/ryan-tools` | Verified (bounded): repository identity unchanged |
| D2 | Comprehensive default engineering/scientific/GIS pip install | Partially verified: `pyproject.toml` retains broad default dependencies; full supported-environment installation matrix not independently accepted |
| D3 | Plan to stop Python 3.13 support in November | **Superseded with decision:** maintainer subsequently required Python 3.13 support to remain, including `ryan-culverts`; `requires-python >=3.13` remains the contract |
| D4 | Install maintained scripts, preserve editable defaults and original CLI contracts | Partially implemented: sibling wheel layout exists; per-script behavioral and lifecycle audit incomplete |
| D5 | Curated MCP exposure and capability/mutation approval metadata | Implemented, verification incomplete: JSON catalogue and profile tests exist, check individual command flags and real mutation boundaries |
| D6 | Migrate hydraulic submodules only after tested pip parity | Partially verified: `run-hy8` and `ryan-culverts` pip dependencies exist and source gitlinks were removed; full numerical/executable parity and reproducible constraints remain unverified |
| D7 | Treat PyPI rename as separate deliberate transition | Later implementation in #120: distribution now `ryan-tools`; maintain upgrade/release verification, trusted publication evidence and retired-name instructions |
| D8 | Windows/QGIS/TUFLOW/HY-8, UNC/mapped paths, `Path.absolute()` | Partially implemented; external and network/manual tests outstanding |
| D9 | Original PR was planning-only, not authorized to change runtime then | Historical phase restriction; later implementation was requested but original functional/validation scope **was not cancelled** |

## Workstream A — inventory, lifecycle and packaging policy

| ID | Requirement (full wording in preserved baseline) | Status and remaining acceptance |
| --- | --- | --- |
| A1 | Inventory every source script by domain, owner, lifecycle, dependencies, invocation, headless behavior, mutation and external application | **Partially implemented.** The [file-by-file audit ledger](../audits/2026-10-19-installed-scripts-inventory.md) enumerates current paths and the new `repo-scripts/audit_installed_scripts.py` safely generates syntax/import/headless/path signals without executing wrappers. Manual owner/lifecycle/invocation and acceptance remain outstanding |
| A2 | Independently classify packaged maintained, packaged not MCP, or repository-only | **Not completed.** Current manifest is provisional; individual disposition/evidence still required |
| A3 | Prioritize TUFLOW/GDAL/hydraulic/RORB/archive and other representative maintained workflows | Partially implemented; 33 catalogue script paths currently represented, not a guarantee that all useful maintained scripts were assessed |
| A4 | Exclude unverified/obsolete/project-specific and repo maintenance material | Partially implemented; verifier excludes repo maintenance files and an allowlist exists, but individual lifecycle decisions were not justified |
| A5 | Audit import-time side effects, hard-coded paths and checkout-relative resource behavior | **Not assessed for every included script**; cannot claim installed-runnable beyond bounded tested examples |
| A6 | Preserve copyable original file, editable defaults and wrapper + library version display | Copy helper added in #130; version display and default/CLI parity are not verified for every maintained wrapper |

## Workstream B — wheel layout and installed invocation

| ID | Requirement | Status and remaining acceptance |
| --- | --- | --- |
| B1 | Stable original-script wheel strategy and resource handling | Implemented, verification incomplete: sibling `ryan-scripts` staging exists; validate layout, package resource metadata and supported install platforms |
| B2 | **Only individually allowlisted maintained wrappers**, with companion resources | Partially implemented: 83-file allowlist and exact member checks pass; individual lifecycle/resource review incomplete |
| B3 | Location resolver independent of cwd, checkout and PYTHONPATH | Implemented, bounded no-checkout wheel smoke passes; cross-install/source selection cases need additional evidence |
| B4 | Checkout development precedence and explicit diagnostics without stale wheel execution | Partially implemented in MCP registry; test selected checkout, wheel version/source differences and diagnostics |
| B5 | CLI help/no-pause/defaults/error codes/no implicit destructive behavior | Not verified for every packaged script |
| B6 | Stable installed wrapper list/copy without uncontrolled PATH executables | Implemented in #130 but **P2 symlink race found in review**; corrective code/tests in new PR, pending CI and review |
| B7 | Update wheel build/verifier/smoke/tests/install/release docs in implementation | Partially implemented; exact member tests and smoke exist, but release/migration and external acceptance remain |

## Workstream C — MCP reconciliation

| ID | Requirement | Status and remaining acceptance |
| --- | --- | --- |
| C1 | Reuse existing MCP server, registry, models and JSONs | Implemented, bounded source inspection |
| C2 | Installed wheel selection for users, explicit checkout precedence for development | Implemented, verification incomplete on mixed environments |
| C3 | Explicit module/script/console execution targets | Implemented, catalogue validation still needed per target |
| C4 | Command/help/scenarios, wrapper and installed version, external prerequisites and approval diagnostics | **Partially implemented**: paths/help/scenarios/version/approval metadata exist; PR #137 now reports script provenance, installed distribution version and explicit `not_verified`/`not_declared` external prerequisite state. Runtime executable/version checks and comprehensive per-workflow assessments are outstanding |
| C5 | Small curated discovery/read-only surface; no indiscriminate command publication | Implemented, bounded hosted tests |
| C6 | Safe capability categorization for overwrites/deletes/external programs and flag escalation | Partially implemented; catalog metadata present, per-workflow destructive-flag review incomplete |
| C7 | Safe defaults, preview/dry run, confirmation and useful agent-facing failures | Partially implemented; test each exposed workflow and sensitive flag |
| C8 | Reconcile all actual workflow catalogue entries; classify dead/unavailable truthfully | Catalogue script target file presence is tested, **not** equivalent to runtime capability checks or full CLI validation |
| C9 | Distinguish installed/repository sources, absent external applications and distribution versions in `check_repo_health` | **Partially implemented**; PR #137 adds `workflow_availability_scope=entry_point_only`, `external_prerequisite_validation=not_performed`, and `active_script_source`. Verify actual external binaries and installed/checkout contexts separately |
| C10 | Direct `run_workflow` execution requires separate reviewed design | Retained as **separate future decision**, not quietly removed; no automatic execution authorized |

## Workstream D — hydraulic pip migration

| ID | Requirement | Status and remaining acceptance |
| --- | --- | --- |
| H1 | Check independent PyPI releases, ownership, trust and tested versions | Partially verified; publication/ownership provenance and cross-platform compatibility evidence incomplete |
| H2 | Replace embedded solver contents with package dependencies | Verified (bounded): dependency metadata and wheel exclusion tests |
| H3 | Remove obsolete solver gitlinks, package paths and utilities after parity | Removal implemented; original parity-before-removal gate not fully substantiated |
| H4 | Reproducible tested dependency versions/bounds/constraints | Not fully verified; existing lower bounds alone do not guarantee future numerical reproducibility |
| H5 | Verify external solver dependency declarations and imports | Verified (bounded) by package and smoke jobs |
| H6 | Preserve engineering parity, numerical validation, Windows HY-8 acceptance | **Outstanding**; do not equate import smoke with hydraulic validation |

## Workstream E — package identity and Python support

| ID | Requirement | Status and remaining acceptance |
| --- | --- | --- |
| E1 | Preserve repo identity and `ryan-mcp` command | Implemented |
| E2 | Confirm new PyPI distribution name and ownership | Renamed in #120; check actual release credentials and published migration evidence |
| E3 | Migration, Trusted Publishing, wheel metadata, docs and old-name handling | Partially implemented; assess upgrade edge cases and release audit separately |
| E4 | Keep Python distribution/import namespace distinctions and compatibility | `ryan_library` remains stable; legacy `ryan_functions` removal was separately authorized; verify external upgrade instructions |
| E5 | Planned >=3.14 floor in November | **Superseded with maintainer decision:** >=3.13 continues; do not silently apply 3.14-only |
| E6 | Retain broad default dependency environment | Implemented metadata; end-user installation validation remains separate |

## Workstream F — validation and rollout

| ID | Requirement | Status and remaining acceptance |
| --- | --- | --- |
| F1 | Exact included/excluded maintained-script inventory tests | Wheel member equality tests pass, but maintained/legacy designation **has not been proven individually** |
| F2 | Verified wheel in neutral no-checkout Windows Python, MCP and representative help | Bounded prior installed smoke recorded in #120/#130; repeat where needed for final scoped changes |
| F3 | Synthetic read-only/creating workflow and failure behavior tests | **Partial**; systematically cover declared catalog targets and missing inputs/tools |
| F4 | Application-specific GDAL/QGIS/TUFLOW/HY-8 acceptance | **Outstanding** on supported external environments |
| F5 | Copy wrappers on UNC/mapped/long/whitespace paths; version/log/progress/exit parity | **Outstanding** manual or supported platform tests; safety regression covered by this corrective PR |
| F6 | Profile approval filtering; no implicit MCP execution | Existing regression tests, but destructive-flag audit remains |
| F7 | Ruff/Pyright/pytest/docs/wheel/hosted validation on exact final head | #130 hosted CI passed; corrective PR must re-run and report exact head, not claim prior CI covers new code |
| F8 | Migration order, constraints, publication cutover and old-name timelines | **Partially documented**, release evidence/acceptance still required |

## PR #130 review finding and correction

The automated review submitted after #130 merged identified
[P2 destination symlink-swap vulnerability](https://github.com/Chain-Frost/ryan-tools/pull/130#discussion_r4307875276)
in `ryan_library/mcp/installed_scripts.py`. It remained unresolved at merge.

The corrective PR changes copying to use exclusive creation when not overwriting, and to stage a temporary file in
the destination directory followed by `os.replace` when overwriting. The latter replaces the destination **entry**
without following a symlink swapped in after validation. Regression tests simulate symlink and hard-link swaps,
as well as a failed staging copy. Review on PR #137 additionally identified POSIX permissions loss from temporary-file mode `0600`; this is addressed by copying the source mode to the staged file before atomic replacement and testing both copy paths. Comment/thread closure must follow a verified code fix; do not dismiss feedback
or mark a thread resolved merely because a PR merged.

## Required review process

1. For every review comment, record the exact link, code impact, response, fixing commit, tests and resolution state.
2. Resolve threads **only after** evidence passes; a green CI run from before review is insufficient.
3. Review any newly posted comments at the final head, including comments arriving after checks/ready-for-review.
4. Complete and agree the original workstreams above or preserve each unresolved requirement as a tracked
   blocker/open item. **Do not merge an "all scope complete" PR while A/F engineering gates remain unresolved.**
5. Keep #118/#119 independent; do not use their open status to conceal other unfinished requirements.

## Delivery state

- **2026-10-17:** Original requirements drafted in PR #130.
- **2026-10-18:** Existing implementation in #100/#120 reviewed; partial script packaging added in #130.
- **2026-10-18:** #130 merged at `1ac42c6`, before automated review produced the P2 finding.
- **2026-10-19:** New corrective branch restores the complete original baseline and status matrix, introduces a
  file-by-file inventory ledger, and fixes the P2 target-swap issue; **CI/maintainer review and follow-up work remain**.

No release publication or merge is authorized merely by updating this record.
