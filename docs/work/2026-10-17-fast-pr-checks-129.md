# Fast PR checks and hosted timing, PR #129

- Status: Complete
- Owner: ChatGPT
- Updated: 2026-10-17
- Next review: None; future timing analysis requires a separately selected follow-up
- Scope: Independent Windows/Python 3.14 patch and Ruff feedback, retaining the Python CI release gates and test boundary.
- Delivery: Existing implementation committed and pushed on `ci/fast-pr-checks-20261017` at `eaf4b0e`;
  Documentation follow-up `a2c26dc` was pushed and validated, then
  [PR #129](https://github.com/Chain-Frost/ryan-tools/pull/129) was squash-merged as `037cff3`
  on 2026-10-17 at 18:46 Perth time. The final completion update is carried with the existing draft planning PR #130.

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

The user authorized commit, push and squash merge on 2026-10-17 once ready. All five required checks passed on
`a2c26dc490711ae8db283192a5d7f06dd4cefb39`, and GitHub reported CLEAN before the head-guarded squash merge.
[Python CI 39025862987](https://github.com/Chain-Frost/ryan-tools/actions/runs/39025862987) passed Policy (44 s),
Package (75 s), Python 3.13/MCP (139 s) and Hosted tests (220 s).
[Fast checks 39025862995](https://github.com/Chain-Frost/ryan-tools/actions/runs/39025862995) passed in 16 s.
Hosted pytest reported 1057 passed, 7 deselected and 63 warnings in 98.95 s. No skips were reported.
Local documentation/index, explicit five-document links and patch whitespace checks passed before commit.
No implementation work remains for this PR; post-merge main CI is distinct from this verified PR evidence.
Exercise the fast Ruff branch on a Python-changing PR and collect several timings before deciding whether dependency
installation or selected test parallelism should be optimized. Do not infer overall CI wall-clock savings.

At the user's explicit request, added `Ruff and whitespace` (GitHub Actions integration 15368) to active branch ruleset
`main only` (25169132) on 2026-10-17. Read-back verification confirmed all four existing checks and every other editable
setting were preserved. Tag ruleset 25169344 was read before and after and remained identical. Before/update/after JSON
proof is retained at `C:/Users/Ryan/AppData/Local/Temp/ryan-tools-ruleset-129-m2frwod3`.
This administration change is live and does not require a commit or merge. Policy continues to retain Ruff.
No MCP catalogue change is appropriate: this work concerns repository CI administration, not a reusable data workflow.
