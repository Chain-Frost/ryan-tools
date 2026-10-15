# CI validation efficiency and publication smoke checks

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | ChatGPT |
| Created | 2026-10-15 |
| Updated | 2026-10-16 |
| Next review | 2026-10-22 |
| Baseline | main at 00a87f0, PR #123 merged; branch ci/streamline-release-validation |

## Outcome and scope

Enforce Actionlint in Policy, reduce unnecessary dependency installation and
duplicate wheel verification, and smoke-test the exact publication wheel.
Keep the four required CI jobs and the existing temporary floodway exclusions.

The user subsequently requested consistent release arrangements across
ryan-tools, ryan-culverts and run-hy8. PR #125 also adds the shared preparation
helper and its calendar/rollback tests here. It uses Perth time, preserves the
existing version scheme, and delegates the wheel to the existing verified builder.
Finalization now parses TOML instead of matching its layout with a regex.
The open-release-PR check precedes any build. New repository setups have
separate branches and PRs; their hydraulics and independent Git state remain intact.

## Current state

Policy checks all workflows using pinned Actionlint 1.7.12 with SHA-256 archive
verification. Full Python/GIS dependencies are installed only when modified
Python files require static checks. Package uses build, pandas and Loguru;
hosted tests and Python 3.13 checks keep their existing dependency coverage.
The builder remains responsible for verifying each new wheel; retained-wheel
verification is preserved. Publication smoke-tests its wheel before artifact upload.

## Next action

Review [PR #125](https://github.com/Chain-Frost/ryan-tools/pull/125) and its final-head checks.
Merge after validation; exercise publication smoke in a subsequent release.

## Completion criteria

- Local workflow/documentation checks and reduced dependency smoke pass.
- All four required checks pass on the final PR head.
- Publishing smoke check is exercised after this follow-up reaches main.

## Validation and delivery

Actionlint passed for all current workflows. Default documentation checks passed
for seven files and central-index coverage; explicit link checks passed for all
five touched/new documents. `git diff --check` passed.

Installed pandas and Loguru (with their dependencies) and the retained 26.10.15.4
wheel into a fresh temporary target. Ran the installed-wheel smoke script with
`python -S`, that target as PYTHONPATH and the working directory outside the
repository: passed. This excludes the normal installation's site-packages and
demonstrates the focused dependency boundary. No Python/package metadata changed,
so a local package rebuild or unrelated full test suite was not required.

Changes are committed and pushed on `ci/streamline-release-validation` as
[PR #125](https://github.com/Chain-Frost/ryan-tools/pull/125). The PR is the source
for the exact delivery head and hosted checks. Publication
smoke behaviour still requires a release after this follow-up is merged.

## Related release test

PR #123 merged after all four required jobs passed. Preparation run
[38764697991](https://github.com/Chain-Frost/ryan-tools/actions/runs/38764697991)
succeeded and opened [PR #124](https://github.com/Chain-Frost/ryan-tools/pull/124)
for 26.10.15.5. Approved its required bot-created PR-event CI run
38764857814. Publication test progress belongs to the
[release record](2026-10-15-release-automation.md).

### 2026-10-16

PR #124 passed all four checks and merged as e37cc25. Merged current main into
the follow-up branch to satisfy strict branch freshness. Initial PR #125
Policy passed, including the hosted Actionlint download/checksum/run. Final-head
hosted CI must be checked after the branch update.

PR #125 head 8319020 passed all four jobs in run 38765607825. Policy took
73 seconds and Package 76 seconds, including a successful reduced-dependency
installed-wheel smoke check. Repeated the minimal dependency smoke locally
on retained wheel 26.10.15.5: passed. The first publication separately succeeded
using main's original workflow; the new publication smoke step still needs
this follow-up merged and a subsequent release to execute in GitHub.

The final handoff/settings documentation update requires a fresh hosted run;
the PR remains unmerged. Local workflow and documentation checks are rerun
before pushing that update. The temporary floodway exclusions remain under
this record's next review date.

## Cross-repository consistency follow-up, 2026-10-16

At the user's request, PR #125 now also aligns release preparation with the
new ryan-culverts and run-hy8 setup PRs. The common helper uses Perth calendar
dates, preserves each existing year width and restores metadata on builder
failure. Existing builders retain wheel verification and promotion. Added
16 focused calendar/rollback tests here; all passed. Changed-file Ruff,
strict Pyright and documentation checks passed. No library code or package
metadata changed in this repository.

Finalization uses Python TOML parsing. Publication rejects direct URL dependencies,
including optional extras. Workflow JavaScript passed 13 mocked release/finalization
scenarios locally; PowerShell parsing and Actionlint passed. The metadata gate
accepted the retained wheel and a normal optional PyPI requirement and rejected
a synthetic extra containing a Git URL. Existing PR #125 hosted passes predate
these additions; require fresh final-head CI after pushing.

Both new repositories have matching main/tag protections, automatic branch
deletion, Actions PR creation and automatic tag-only pypi environments. The
user's screenshot confirms both pending PyPI publishers. Those repositories'
first publications remain separate from this repository's successful release.
