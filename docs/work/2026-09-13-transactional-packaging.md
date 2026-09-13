# Transactional package build and verification

| Field | Value |
| --- | --- |
| Status | Complete |
| Owner | Unassigned |
| Created | 2026-09-13 |
| Updated | 2026-09-13 |
| Next review | — |
| Baseline | `main` at `735f8cb`; `vendor/ryan_culverts` gitlink staged separately; MCP typing fix unstaged |

## Outcome and scope

Adapt the proven `ryan-culverts` transactional wheel workflow to `ryan-tools`. Preserve the existing calendar-version
release path while adding a no-bump build, staged-wheel verification, failure-safe promotion, focused packaging tests
and an installed-wheel smoke test for the bundled packages and resources.

After implementation, review the independent `run-hy8` and `ryan-culverts` packaging workflows and create GitHub issues
only for non-duplicate improvements, referring to the `ryan-tools` implementation where useful.

## Current state

The transactional workflow is complete locally. The retained `ryan_functions 26.9.12.2` wheel was rebuilt without a
version increment, verified before promotion, and smoke-tested from an isolated target outside the source checkout.

## Completion criteria

- Default, explicit-version and no-bump version selection are validated.
- Build or verification failure restores metadata and retains the previous wheel.
- Successful promotion atomically leaves one current `ryan_functions` wheel while preserving unrelated artifacts.
- Wheel verification covers package metadata, licence, bundled packages, typed markers and required resources.
- An isolated install proves imports resolve from the built wheel rather than the checkout.
- Relevant non-duplicate upstream repository improvements are recorded as GitHub issues.

## Validation and delivery

- `python repo-scripts/build_library.py --no-bump --skip-pip`: passed; `pyproject.toml` remained at normalized version
  `26.9.12.2` and the verified wheel was promoted.
- `python repo-scripts/verify_wheel.py`: passed for the retained 577546-byte wheel, SHA-256
  `d9f76dacc80026fedf5de59d9fbc6f9fb4638fc4063d3ecaadb7602ef25daeb1`.
- Isolated `pip --target --no-deps` install and `repo-scripts/smoke_test_installed_wheel.py --expected-root`: passed for
  `ryan_library`, `run_hy8`, `culvert_solver` and representative bundled resources.
- `python -m pytest tests/test_packaging.py tests/mcp`: 37 passed.
- Strict Pyright on all modified Python files: 0 errors, warnings or information messages.
- Repository Ruff check and Loguru formatting policy: passed. Ruff format required one final mechanical adjustment before
  the closing validation rerun.
- Documentation link validation for the six touched documents: passed.

The completed source, documentation, `vendor/ryan_culverts` gitlink, MCP typing correction and rebuilt wheel are
delivered together in the packaging commit on `main`.

Upstream follow-ups were created after searches found no matching issues:

- [`run-hy8` issue 1](https://github.com/Chain-Frost/run-hy8/issues/1): make package builds transactional and verify
  wheels before promotion.
- [`ryan-culverts` issue 11](https://github.com/Chain-Frost/ryan-culverts/issues/11): make package version reporting safe
  when `culvert_solver` is vendored.

## Progress

### 2026-09-13

Compared the current packaging implementations. `ryan-culverts` provides the strongest transactional baseline;
`run-hy8` provides optional local source staging but deletes its retained wheel before successful replacement.

Implemented version validation, explicit `--no-bump` support, temporary candidate builds, repository-specific wheel
verification and atomic promotion. Added focused failure-path tests and an installed-wheel smoke script. The smoke test
also demonstrated that vendored `culvert_solver` can report version metadata from a separate standalone distribution,
which is tracked upstream.
