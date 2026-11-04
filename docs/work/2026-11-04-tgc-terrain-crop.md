# TGC terrain crop workflow

| Field | Value |
| --- | --- |
| Status | Ready for review; PR #204 open and exact-head CI green |
| Owner | ChatGPT |
| Created | 2026-11-04 |
| Updated | 2026-11-04 |
| Next review | 2026-11-11 |
| Baseline | `main` at `ec873425`; branch `feat/tgc-terrain-crop`; unrelated local GDAL-script changes preserved |

## Outcome and scope

Add a maintained, reusable script that asks PyTUFLOW to select the ordered
`Read Grid Zpts` stack for one `-s1` scenario, clips it to a user-supplied box,
removes deep lower-priority overlap, and retains a configurable seam allowance.
The supported contract deliberately has one scenario flag and no multi-context
or scenario-slot switching.

## Current state

Implementation, focused tests, user documentation and real plan-only model
validation are complete. The script defaults to plan-only, writes only
new destinations with explicit `--write`, preserves native grids and source
values, and produces a patched TGC plus manifest. No production rasters or TGCs
were changed during validation. PR #204 is open, mergeable and ready for
maintainer review.

## Next action

Obtain maintainer approval before merge. No merge or release is authorised by
this work.

## Completion criteria

- PyTUFLOW owns conditional selection for one scenario value.
- The analysis box is the hard limit and the default seam allowance is 50 m.
- Focused selection, precedence/buffer, write and overwrite-safety tests pass.
- Strict Pyright, documentation checks, a package-free CLI smoke and an actual
  plan-only model check pass.
- The branch is pushed and a pull request is open without unrelated changes.

## Validation and delivery

- `python -B -m pytest -q -p no:cacheprovider tests/test_crop_tgc_terrains.py`:
  3 passed with the current Q: PyTUFLOW checkout; one existing pytest
  configuration warning.
- Strict Pyright on the maintained script and focused tests: 0 errors, 0
  warnings.
- TUFLOW Python README link check: passed.
- Actual private model plan-only checks at 50 m and the shipped 10 m planning
  resolution selected 12 ordered terrain layers and completed without writes.
- Local Ruff execution is blocked by corporate executable policy; GitHub's
  pinned Ruff lint and format check passed.
- Exact-head GitHub validation on `c8359713` passed Ruff and whitespace, Policy,
  Package, MCP smoke, POSIX wrapper security and Hosted tests.
- Delivery: branch `feat/tgc-terrain-crop` pushed; PR #204 open and mergeable.

## Progress

### 2026-11-04

Implemented single-scenario PyTUFLOW scope evaluation, overview-aware bounded
coverage reads, reverse-precedence allocation, a 50 m buffered seam, native-grid
lossless outputs, a non-overwriting patched TGC and JSON audit manifest. Replaced
an initial virtual-warp mask scan after real-data evidence showed excessive time
and memory; the final overview-aware window path bounded memory and completed.
