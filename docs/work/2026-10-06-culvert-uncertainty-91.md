# Culvert uncertainty and sensitivity workflow

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | ChatGPT |
| Created | 2026-10-06 |
| Updated | 2026-10-06 |
| Next review | 2026-10-20 |
| Baseline | `feature/culvert-uncertainty-91` from `main` at `1d8aa177be7b074003e0e355b5db56f4cd764e99`; `vendor/ryan_culverts` initially at `00f8274b4702bfe721d111436a15ae435f672460` |

## Outcome and scope

Implement GitHub issue #91 as the application/orchestration layer for culvert uncertainty and sensitivity analysis.
The workflow consumes public uncertainty contracts and deterministic hydraulic solver APIs from `ryan-culverts`
rather than defining competing hydraulic-domain classes or equations.

Upstream dependency: `ryan-culverts` issue #19 was implemented and merged through PR #25 at
`0213eac2bf10b5751feb13ae7d88d00026eeb9f1`.

## Current state

A dedicated branch has been created from current `ryan-tools` `main`. The upstream uncertainty API is merged and the
parent repository's `vendor/ryan_culverts` pointer has been advanced to the merged dependency commit.

The existing culvert application boundary remains the integration point:
`CulvertProject` / crossing / scenario models -> solver adapter -> public `culvert_solver` APIs.

No uncertainty-domain distribution, bounds or sampled-parameter classes will be reimplemented in `ryan-tools`.

## Next action

Add the first typed application-level study models and orchestration for deterministic bounded sweeps and seeded
stochastic studies, preserving failed/unresolved evaluations and solver status/warning provenance.

Then add focused tests for seeded reproducibility, deterministic bounds, failure propagation and a representative
project/scenario/alternative matrix before extending the maintained wrapper and machine-readable outputs.

## Completion criteria

- Public `ryan-culverts` uncertainty contracts are consumed directly.
- Seeded stochastic studies are reproducible.
- Deterministic bounded sweeps do not require random sampling.
- Failed, unresolved, approximate and advisory solver outcomes remain visible.
- Base crossings and alternatives can be evaluated across selected scenarios.
- CSV/JSON outputs and a concise review summary preserve sample provenance and solver warning/status information.
- Focused Ruff, strict Pyright, pytest, wrapper/documentation checks and package verification pass.

## Validation and delivery

2026-10-06: repository and upstream APIs inspected through the GitHub connector. Local shell checkout/validation is not
available in this environment because outbound GitHub DNS/network access is unavailable. No validation result is claimed
for implementation code until it is run in a normal checkout.

Branch: `feature/culvert-uncertainty-91`.

No pull request has been opened.

## Progress

### 2026-10-06

Created the feature branch from current `main`, confirmed upstream PR #25 is merged, mapped the existing culvert
application/solver boundary, and advanced the vendored solver pointer to the merged uncertainty-contract commit.
