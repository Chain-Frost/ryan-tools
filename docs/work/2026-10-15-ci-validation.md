# CI validation efficiency and publication smoke checks

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | ChatGPT |
| Created | 2026-10-15 |
| Updated | 2026-10-15 |
| Next review | 2026-10-22 |
| Baseline | main at 00a87f0, PR #123 merged; branch ci/streamline-release-validation |

## Outcome and scope

Enforce Actionlint in Policy, reduce unnecessary dependency installation and
duplicate wheel verification, and smoke-test the exact publication wheel.
Keep the four required CI jobs and the existing temporary floodway exclusions.

## Current state

Policy checks all workflows using pinned Actionlint 1.7.12 with SHA-256 archive
verification. Full Python/GIS dependencies are installed only when modified
Python files require static checks. Package uses build, pandas and Loguru;
hosted tests and Python 3.13 checks keep their existing dependency coverage.
The builder remains responsible for verifying each new wheel; retained-wheel
verification is preserved. Publication smoke-tests its wheel before artifact upload.

## Next action

Push a separate PR and inspect all four hosted checks. Review and merge after validation.

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

Changes are being committed and pushed on `ci/streamline-release-validation`;
the PR is the source for the exact delivery head and hosted checks. Publication
smoke behaviour still requires a release after this follow-up is merged.

## Related release test

PR #123 merged after all four required jobs passed. Preparation run
[38764697991](https://github.com/Chain-Frost/ryan-tools/actions/runs/38764697991)
succeeded and opened [PR #124](https://github.com/Chain-Frost/ryan-tools/pull/124)
for 26.10.15.5. Approved its required bot-created PR-event CI run
38764857814. Publication test progress belongs to the
[release record](2026-10-15-release-automation.md).
