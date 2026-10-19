# Vendored simil and PyHMA integration

| Field | Value |
| --- | --- |
| Status | Needs review |
| Owner | ChatGPT |
| Created | 2026-10-19 |
| Updated | 2026-10-19 |
| Next review | 2026-10-21 |
| Baseline | `main` at `1ac42c63`; `fix/vendor-simil-pyhma-integration-20261019` |
| Pull request | [#139](https://github.com/Chain-Frost/ryan-tools/pull/139) |
| Delivery | Commits pushed to PR branch; not merged or published |

## Outcome and scope

Improve integration for the retained `vendor/simil/simil.py` and `vendor/PyHMA/__init__.py` using the repository's maintained architecture and vendored-code boundaries. Keep the numerical algorithms vendor-owned; provide a narrow supported `simil` API, package both modules with licence notices, retain historical `vendor.HMA` importing lazily, and add focused validation. No new PyHMA first-party algorithm API or MCP tool is justified without a supported workflow.

## Current state

- Vendored `simil` now has a proper package initializer and a public `ryan_library.functions.geometry.find_similarity_transform` entry point.
- PyHMA continues to be vendored; scientific runtime dependencies are explicit in `pyproject.toml`.
- `vendor.__init__` no longer imports PyHMA eagerly when only `simil` is used.
- Both MIT licenses and source/usage documentation are shipped as package data. The original upstream source revisions are unknown; documentation records that limitation.
- Wheel verifier checks vendored Python, README and licence files byte-for-byte.
- Focused tests cover the upstream transform worked example, fixed-scale and weighted inputs, invalid points, lazy imports and PyHMA identity-score smoke.
- Codex P2 finding on incorrect source-point uniqueness is addressed by a narrow vendor precondition correction and public/vendor regression tests for diagonal and duplicated controls; re-review and exact-head CI are required.
- Latest PR checks must be inspected after all commits; do not treat an earlier green run as exact-head evidence.

## Next action

Confirm new control-point regressions, request a fresh Codex review and resolve the original P2 review thread after verification. Review all checks against the latest PR commit. If a failure occurs, inspect its logs and fix on this branch. If checks pass, perform maintainer review of API fit and Python 3.14 compatibility before marking this integration complete.

## Completion criteria

- Fast PR checks, strict Pyright, Python 3.13/3.14 CI and package jobs pass at the same final commit.
- `python repo-scripts/build_library.py --no-bump --skip-pip` builds a wheel verified by `python repo-scripts/verify_wheel.py`.
- Numerical regression and import/dependency smoke tests pass.
- Third-party attribution remains accurate and vendored algorithms are not inadvertently rewritten.
- The PR is ready for maintainer review, without merging or publishing unless separately authorised.

## Validation and delivery

2026-10-19:
- Fast PR checks at `f08b40f`: passed.
- Python CI at `f08b40f`: package job passed, remaining jobs in progress at record creation.
- Local Python 3.14 runtime, strict Pyright, full installed-wheel execution: not available through this GitHub-only development environment; hosted CI is the required acceptance evidence.
- Git commits: `7b64bf3` (vendor packaging), `f08b40f` (API and verification), and follow-up tracking/checks commit.
- Delivery: changes committed to branch, draft PR #139 open. The base repository `main` has not been modified.

## Progress

### 2026-10-19 — Codex P2 follow-up

Corrected the vendored source-point distinctness predicate to compare complete XYZ point columns instead of coordinate rows. Added two- and three-point diagonal controls and duplicate-source regression cases for both public and vendor imports. Awaiting exact-head CI and independent Codex re-review. No merge authorised.

### 2026-10-19

Implemented a focused vendor-packaging repair and reuse boundary without introducing a PyHMA public facade or duplicating either solver. Held merge pending exact-head CI and maintainer review.
