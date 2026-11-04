# Office protection copy safety - issue #199

| Field | Value |
| --- | --- |
| Status | Active - implemented locally, parent integration and hosted CI pending |
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
