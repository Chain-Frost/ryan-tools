# Fast PR checks and hosted timing, PR #129

- Status: In validation
- Owner: ChatGPT
- Updated: 2026-10-17
- Next review: 2026-10-22
- Scope: Independent Windows/Python 3.14 patch and Ruff feedback, retaining the Python CI release gates and test boundary.
- Delivery: Existing implementation committed and pushed on `ci/fast-pr-checks-20261017` at `eaf4b0e`;
  [PR #129](https://github.com/Chain-Frost/ryan-tools/pull/129) remains open and unmerged.
  Documentation corrections and this status record are included in the authorized follow-up commit.

## Implementation and validation

The checkout already contained the requested implementation and both review repairs: Ruff remains in Policy, and
each main-push fast run has a distinct concurrency group. Hosted dependencies are split into pip, GIS and development
steps; pytest prints 15 slow durations. No additional tests are excluded by this change.

Live GitHub inspection confirmed head `eaf4b0ecf4970c720f3f70748a9da3339b29c4d6` and successful runs:

- [Fast PR checks 39001435683](https://github.com/Chain-Frost/ryan-tools/actions/runs/39001435683): patch validation passed.
- [Python CI 39001435924](https://github.com/Chain-Frost/ryan-tools/actions/runs/39001435924): Policy, Hosted tests,
  Package, and Python 3.13 compatibility and MCP all passed. Package includes retained-wheel verification, no-bump
  rebuild and isolated installed-wheel smoke. Policy includes Actionlint.
- Hosted tests: 1057 passed, 7 deselected, 63 warnings in 56.83 seconds; no skips reported.
- Fast job logs finish at 04:08:38 UTC, Policy at 04:09:03 UTC and hosted tests at 04:10:56 UTC.
- Approximate hosted step boundaries: pip 3 seconds, GIS 13 seconds, development installation 39 seconds.
- Conditional Ruff and Pyright steps did not execute because this PR changes only YAML/Markdown. A Python-changing
  PR must exercise those branches before claiming hosted early Ruff timing evidence.

Local default documentation/index checks, explicit environment-guide checks, Loguru policy and patch whitespace passed.
No library code or metadata changed; a local package rebuild or unrelated test suite is unnecessary.
The environment guide now matches the actual hosted command, including existing exclusions and JUnit output, and records
the observed timings. The guide owns the measurements; this record owns delivery and outstanding actions.

## Next action and completion criteria

The user authorized commit, push and squash merge on 2026-10-17 once ready. Obtain fresh hosted checks on the follow-up
commit and merge only after all five required checks pass and reviews are resolved. The runs above cover the previous
head; final delivery evidence will be recorded after merge.
Exercise the fast Ruff branch on a Python-changing PR and collect several timings before deciding whether dependency
installation or selected test parallelism should be optimized. Do not infer overall CI wall-clock savings.

At the user's explicit request, added `Ruff and whitespace` (GitHub Actions integration 15368) to active branch ruleset
`main only` (25169132) on 2026-10-17. Read-back verification confirmed all four existing checks and every other editable
setting were preserved. Tag ruleset 25169344 was read before and after and remained identical. Before/update/after JSON
proof is retained at `C:/Users/Ryan/AppData/Local/Temp/ryan-tools-ruleset-129-m2frwod3`.
This administration change is live and does not require a commit or merge. Policy continues to retain Ruff.
No MCP catalogue change is appropriate: this work concerns repository CI administration, not a reusable data workflow.
