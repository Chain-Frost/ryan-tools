# PyPI release automation

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | ChatGPT |
| Created | 2026-10-15 |
| Updated | 2026-10-15 |
| Next review | 2026-10-22 |
| Baseline | feature/pypi-trusted-publishing at 2d1c978; PR #122 merged as 915b0eb; clean parent worktree before edits |

## Outcome and scope

One manual workflow start prepares a version/wheel PR. Human squash merge
and successful main CI create the release and dispatch the existing OIDC
publisher. Preserve branch checks and the PyPI publisher identity.

## Current state

Added preparation and finalization workflows, tag-scoped manual publisher
dispatch, and recovery instructions in [publishing guidance](../PYPI_PUBLISHING.md).
No release has been started by these changes.

Temporarily deselect only the two slow floodway discharge-sweep tests from
hosted CI, as requested. They remain runnable locally. Remove the two
`--deselect` arguments in `.github/workflows/ci.yml` to restore them.
Other floodway tests remain selected. Revisit this temporary exclusion at
the next review. The existing floodway work record is already overdue for
review; no unrelated implementation was started.

## Next action

Review and merge the automation PR, then validate the first release in GitHub.
Actions PR creation is enabled; the release PR still needs human merge.

## Completion criteria

- Workflow syntax validation passes and existing protections remain intact.
- Preparation PR CI and post-merge finalization succeed in GitHub.
- First publication is visible on PyPI.

## Validation and delivery

Delivery branch: `feature/automated-pypi-release`, committed and pushed as
[PR #123](https://github.com/Chain-Frost/ryan-tools/pull/123) against current main,
following explicit user authorization. Local syntax/docs checks passed after rebasing.
Hosted execution and publication have not run; these workflows must reach main
before they can be started. The PR supplies the exact delivery commit and CI state.

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
