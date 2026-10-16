# Review temporary floodway CI exclusions

| Field | Value |
| --- | --- |
| Status | Deferred |
| Owner | Unassigned |
| Created | 2026-10-16 |
| Updated | 2026-10-16 |
| Next review | 2026-10-22 |
| Baseline | main 42a7511; CI changes merged through PR #125 |

## Scope and current state

The user requested temporarily disabling the two slowest floodway sweep tests in hosted CI.
Both remain available in local pytest runs. All other hosted floodway tests remain selected.
The exclusions are the two explicit `--deselect` arguments in `.github/workflows/ci.yml`:

- `tests/floodway/test_sweep.py::test_discharge_sweep_detects_overtopping_and_retains_non_aep_interior_states`
- `tests/floodway/test_sweep.py::test_discharge_sweep_honours_explicit_minimum_bound`

Their earlier hosted durations were approximately 42 and 37 seconds in run 38757984295.
No hydraulic implementation or expectations were weakened to improve CI time.

## Next action and completion

At the next session on/after 2026-10-22, or earlier if the user requests it, assess safe
test/runtime improvements or restore these two tests by removing the explicit exclusions.
Run the affected sweep tests and verify restored hosted coverage before closing this front.
The review date is a session-driven reminder; it does not schedule an automatic action.

## Delivery

The exclusions are merged and published as part of the completed
[CI/release work](2026-10-15-ci-validation.md). This bounded follow-up record and its
index/register links are local uncommitted documentation; no implementation work is selected.
