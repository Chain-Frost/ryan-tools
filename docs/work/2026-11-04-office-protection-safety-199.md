# Office protection copy safety - issue #199

| Field | Value |
| --- | --- |
| Status | Needs review - published as stacked PR #201 |
| Owner | ChatGPT |
| Created | 2026-11-04 |
| Updated | 2026-11-04 |
| Next review | 2026-11-05 |
| Baseline | PR #183 audit commit 50cf9c6e |
| Branch / base | fix/office-protection-safety-199 -> feat/batch-dashboard-foundation-180 |
| Issue | [#199](https://github.com/Chain-Frost/ryan-tools/issues/199), AB-027 |

## Outcome

Keep this maintained utility self-contained. Eliminate archive extraction, remove
only intended namespace-aware protection elements, retain untouched member bytes
and metadata, preflight batch collisions and publish owned staged archives without
replacing existing outputs. Add preview and consistent dashboard outcomes.
The [local guide](../../ryan-scripts/misc-python/README.md) owns the user contract.

## Validation and delivery

Windows Python 3.14.6: 22 focused tests pass (7 unrelated promoted-utility tests
deselected), covering malicious paths/links, malformed XML/DTDs, macro/member
preservation, collisions, concurrent creation, failed hard-link publication,
preview, mixed success/failure, interruption, real Windows junction rejection and
copied CLI behavior. Ruff and strict changed-file Pyright pass. No real Office
workbook or production share was accessed. Native Office fidelity, live UNC and
filesystems without hard links are not accepted by these tests.

Parent #183 was published at 50cf9c6e, but conflicts with current main prevent
its PR checks from starting. Resolve those conflicts and merge the updated
parent into this branch before opening the child PR. No main merge or release
is authorized. This record will retain exact commit/CI delivery evidence.

## Next action

Finish parent reconciliation, validate the integrated child, publish the stacked
PR and wait for terminal hosted checks. Then continue #197 archive safety and
#194/#195 result outcomes; #198 dashboard work depends on truthful outcomes.

## MCP assessment

Keep existing packaging/catalogue membership. This is a bounded safety repair
of an existing Office entry point, not a new MCP workflow or library boundary.

## Parent integration

Merged updated #183 head 786cd44d (normal merge of main ec873425) into the
child. Both parent dashboard contracts remain intact; the work-register conflict
retains the independent Office row and all updated parent records. Parent CI has
started. Current inventory is 157 tracked entries, 83 selected files and 33 MCP
targets; the additional child entry is the repository-only Office README.


## GitHub delivery

Published [PR #201](https://github.com/Chain-Frost/ryan-tools/pull/201), based on
#183 at 786cd44d. Implementation c9f0a958 and parent integration 88b366ba are
pushed. The branch is not merged; no auto-merge/release is enabled.

The integrated child rerun passes 22 tests (7 deselected). Fresh verified
26.11.3.9 wheel SHA-256:
`ba7c6cc421ad39cdb9dd1bc19ebdd2dd7028224997d2d1ed25868c7887b11767`.
It is installed in normal Python; the copied CLI test passes (1 pass, 19
deselected). Ruff, strict Pyright, docs and Loguru policy pass. Both PRs now
have running hosted checks; terminal exact-head results are maintained in the
[PR checks](https://github.com/Chain-Frost/ryan-tools/pull/201/checks) and PR body.
This record's publication milestone precedes those terminal results.

Next action: review the exact published child checks and safety diff. Broader
native Office fidelity and live UNC acceptance remain explicitly separate.
Reassess #197 against current main before implementation: its pre-existing
archive guard has improved since the original audit, while publication races
still require checking. #194/#195 remain the next shared TUFLOW result work.
