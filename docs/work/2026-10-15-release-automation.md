# PyPI release automation

| Field | Value |
| --- | --- |
| Status | Complete |
| Owner | ChatGPT |
| Created | 2026-10-15 |
| Updated | 2026-10-16 |
| Next review | None |
| Baseline | feature/pypi-trusted-publishing at 2d1c978; PR #122 merged as 915b0eb; clean parent worktree before edits |

## Outcome and scope

One manual workflow start prepares a version/wheel PR. Human squash merge
and successful main CI create the release and dispatch the existing OIDC
publisher. Preserve branch checks and the PyPI publisher identity.

## Current state

Preparation, PR-event CI, post-merge CI, tag/release creation and OIDC publication
completed successfully for 26.10.15.5. The published PyPI wheel was downloaded,
its SHA-256 checked against PyPI metadata, and its isolated installation smoke-tested.
See [publishing guidance](../PYPI_PUBLISHING.md) for future releases.

Temporarily deselect only the two slow floodway discharge-sweep tests from
hosted CI, as requested. They remain runnable locally. Remove the two
`--deselect` arguments in `.github/workflows/ci.yml` to restore them.
Other floodway tests remain selected. Revisit this temporary exclusion under
the [CI follow-up](2026-10-15-ci-validation.md), next review 2026-10-22.
The existing floodway work record is already overdue for
review; no unrelated implementation was started.

## Next action

No release-automation work remains. Review PR #125 for the separate CI and
publication-smoke improvements; they were not part of this first publication.

## Completion criteria

- Workflow syntax validation passes and existing protections remain intact.
- Preparation PR CI and post-merge finalization succeed in GitHub.
- First publication is visible on PyPI.

## Validation and delivery

Delivery branch: `feature/automated-pypi-release`, committed and pushed as
[PR #123](https://github.com/Chain-Frost/ryan-tools/pull/123) against current main,
following explicit user authorization. Local syntax/docs checks passed after rebasing.
PR #123 merged as 00a87f0; release PR #124 merged as e37cc25. All hosted release
stages passed, and 26.10.15.5 is published. Detailed run evidence follows below.

On 2026-10-15:

- Actionlint passed for all four changed/new workflows.
- Default documentation validation passed for seven files and central-index coverage.
- Explicit link validation passed for all four touched/new Markdown documents.
- `git diff --check` passed.
- Focused sweep collection confirmed exactly two deselected tests and one remaining test.
- Focused sweep execution passed: `1 passed, 2 deselected in 0.12s`.
- Enabled repository Actions PR creation; default workflow permission remains read-only.
- No Python implementation or package metadata changed, so no wheel rebuild is required.
- JavaScript execution was not checked locally because Node is unavailable; Actionlint
  checks workflow syntax, while API and OIDC behaviour still require hosted validation.

## Review 2026-10-15

- Required-check blocker: `workflow_dispatch` job checks do not satisfy PR branch-ruleset
  requirements. Use an eligible PR event (approving the bot-created run where required)
  or an appropriate GitHub App token; correct the publishing guide's dispatched-check claim.
- Finalization gap: main CI cancels earlier main runs, but finalization requires a successful
  push run at the exact release merge SHA. A subsequent merge can cancel that run and leave
  the release unpublished. Preserve each required main run or provide recovery for that SHA.
- Re-ran default documentation checks (seven files and index), explicit link checks (four
  documents), and `git diff --check`; all passed. No hosted workflows or publication ran.
- Review covers current unstaged/untracked automation at `2d1c978`; no implementation fixes,
  staging, commits, or publication were performed. Floodway review remains overdue (2026-10-13).

## Review fixes 2026-10-15

- P1: Removed preparation's CI dispatch and unnecessary Actions write permission.
  Required checks come from the bot-created PR event, with maintainer workflow
  approval where required. Corrected the guide and generated PR instructions.
- P2: Main push CI now uses a unique run-ID concurrency group. This preserves
  both running and pending release merge validation even after multiple later
  merges. PR concurrency still cancels superseded runs. Finalization groups
  are specific to the validated SHA, and recovery reruns that exact push run.
- Hosted execution and publication remain untested. Changes remain uncommitted.
- After both fixes, Actionlint passed for all four workflows, default documentation
  checks passed for seven files and index coverage, explicit link checks passed
  for four documents, and `git diff --check` passed.

## PR #123 stable-release gate review

The P1 review finding was valid: manual publication dispatch bypassed the
release-event prerelease condition. Added a GitHub API check before checkout
and building for both trigger types. An exact-tag release must exist, be
published and have neither draft nor prerelease status. API errors fail closed.
Updated publishing guidance to describe the gate. Prior-head hosted CI passed
all four jobs; the fix requires fresh CI and has not published anything.
Local validation of this fix passed: Actionlint, default documentation checks
(seven files and index), explicit links for both changed documents, and
`git diff --check`. The fix is committed and pushed on PR #123; fresh hosted
validation remains pending. Live release API gating and OIDC upload are untested.

## First hosted release test

PR #123 final head 55cb603 passed all four CI jobs and was squash merged as
00a87f0 with user authorization. Started preparation run 38764697991, which
succeeded and opened [PR #124](https://github.com/Chain-Frost/ryan-tools/pull/124)
for 26.10.15.5. Approved the bot-created PR-event CI run 38764857814.
Release PR CI and publication remain pending. A separate
[CI validation follow-up](2026-10-15-ci-validation.md) preserves the release
test baseline and adds workflow linting and exact publication-wheel smoke checks.

### 2026-10-16

PR #124 head 2bdbf1b passed all four required checks and was squash merged
as e37cc25. Await exact merge-commit CI before automatic tag/release creation.
Removed only the `creation` restriction from tag ruleset 25169344 so the
workflow token can create release tags. Required CI checks and restrictions
against updating/deleting tags remain enabled; no bypass actor was added.
Follow-up improvements are in [PR #125](https://github.com/Chain-Frost/ryan-tools/pull/125).

### First publication complete, 2026-10-16

- Main CI [38765505134](https://github.com/Chain-Frost/ryan-tools/actions/runs/38765505134)
  passed all four jobs at e37cc25.
- Finalization [38766063930](https://github.com/Chain-Frost/ryan-tools/actions/runs/38766063930)
  created the stable release v26.10.15.5 and dispatched publication.
- Publication [38766089354](https://github.com/Chain-Frost/ryan-tools/actions/runs/38766089354)
  passed both build/verification and OIDC upload jobs.
- [PyPI 26.10.15.5](https://pypi.org/project/ryan-functions/26.10.15.5/) downloaded
  successfully through the public PyPI index. Wheel SHA-256:
  `f1c007ceeb47a0cb34e49134e3b3cec36a37b53078dfecf89fbb9432aea4b27e`.
  Verified against PyPI JSON metadata.
- Installed that downloaded wheel into a fresh target with pandas and Loguru,
  then ran the installed-wheel smoke script using `python -S` outside the
  checkout: passed. This does not assert full GIS/runtime dependency compatibility.
- Applied the requested repository settings: squash only, resolve review threads,
  automatically delete merged branches and main bypass limited to PRs. Retained
  strict CI and zero required approvals. PyPI has no manual reviewer gate.
