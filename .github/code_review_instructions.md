# Codex Code Review Instructions for ryan-tools

## Purpose

These guidelines help automated reviewers such as ChatGPT Codex evaluate changes in this repository
consistently. They supplement the pull request template by explaining what to verify and how to
frame feedback for contributors.

Read [`../docs/DEVELOPMENT_GUIDE.md`](../docs/DEVELOPMENT_GUIDE.md) before making architectural recommendations. It
defines the repository's code categories, dependency direction, lifecycle labels and validation expectations. Do not
assume every script is intended to become maintained library code.

## Review Preparation

- Read the pull request description and linked issues to understand the intent of the change.
- Inspect the files touched by the pull request within the context of the repository structure:
  - Reusable logic should live in `ryan_library/functions/` or an appropriate processor.
  - Complete workflow coordination should live in `ryan_library/orchestrators/`.
  - Maintained wrappers in `ryan-scripts/` should preserve user-editable configuration and process-boundary behaviour
    while delegating reusable work to the library.
  - Standalone and project-specific scripts may reasonably remain self-contained when no reuse case exists.
  - `ryan_library/scripts/` and `ryan_functions/` are deprecated compatibility namespaces and must not gain new
    behaviour.
- Note whether the change updates vendored code under `vendor/`; flag it if dependencies appear to
  be modified without justification.
- Check whether a touched path is a submodule and keep parent-repository and submodule changes distinct.

## Review Checklist

- **Correctness**: Does the change implement the requested behaviour? Watch for edge cases related
  to geospatial workflows (TUFLOW, RORB, 12D, GDAL) and confirm that data paths and file handling
  remain robust.
- **Structure**: Ensure new logic is added to reusable functions rather than duplicating code across
  scripts. Suggest refactors when library code would be a better fit.
- **Testing**: Apply the development guide's change-type validation matrix. Verify that relevant manual or automated
  checks are listed, and encourage focused tests or reproduction steps when behaviour changes.
- **Documentation**: Confirm that user-facing behaviour changes include README or script docstring
  updates so that automation and humans can follow the workflow.
- **Standards**: Watch for style issues (Black formatting with 120 character lines, type hints on
  public functions, absolute imports) and mention them when missing.
- **Scope**: Preserve project-specific settings in wrappers, avoid expanding deprecated APIs, and distinguish a focused
  repair from an unsolicited architectural migration.

## Feedback Style

- Separate **blocking** issues (correctness, security, major regressions) from **non-blocking**
  suggestions (style nits, optional refactors).
- Provide actionable reasoning for each comment. Reference specific files and line numbers to make
  follow-up straightforward for automated agents.
- When approving with nits, clearly label them as optional so that automation can choose whether to
  address them immediately or defer.

## Follow-Up Guidance

- Encourage authors to update the pull request template when new review considerations emerge.
- Suggest additions to these instructions if the review uncovered recurring gaps that automation
  should catch earlier.
