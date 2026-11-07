# Office protection copy safety - issue #199

| Field | Value |
| --- | --- |
| Status | Needs review — serial rendering parent refreshed; fresh hosted checks and review pending |
| Owner | ChatGPT |
| Created | 2026-11-04 |
| Updated | 2026-11-07 |
| Next review | 2026-11-14 |
| Baseline | Reviewed child 811b936bb8481f8c9ad0745732c42c7d78f49007; final parent f003a762b354c6f06126aed6bbae330b79f66fa4, package 26.11.7.1 |
| Branch / base | fix/office-protection-safety-199 -> feat/batch-dashboard-foundation-180 |
| Issue | [#199](https://github.com/Chain-Frost/ryan-tools/issues/199), AB-027 |

## Outcome

Keep this maintained utility self-contained. Eliminate archive extraction, remove
only intended namespace-aware protection elements, retain untouched member bytes
and metadata, preflight batch collisions and publish owned staged archives without
replacing existing outputs. Add preview and consistent dashboard outcomes.
The [local guide](../../ryan-scripts/misc-python/README.md) owns the user contract.

## Current state

The refreshed Office tree adopts published #183 parent `f003a762`, including
current-main integration `0b5a07cb`, the #202 export-safety squash and closure
repair `8878d42b`, plus serial TUFLOW rendering correction `26d6e223`, with
package version 26.11.7.1. This final parent merge is conflict-free. All current
parent handoffs, links and register rows remain alongside the independent
Office links and updated Office row. The Office script and its tests are
unchanged from published child `2c06d8d4` and reviewed child `811b936b`.

The normal merge retains reviewed child `811b936b` and final parent `f003a762`
as its parents. Fresh hosted checks and review are pending on the refreshed
child head. Earlier checks and review apply to the exact earlier head recorded
below. Existing published history is preserved. No main merge or release is
part of this continuation.

## Next action

Inspect the refreshed PR #201's new hosted checks and safety review. Native
Office fidelity and live UNC acceptance remain separate;
no new Windows, native Office or network-share acceptance is claimed here.

## Serial rendering parent refresh — 2026-11-07

The parent follow-up changes only direct serial TUFLOW dashboard rendering,
its regression tests and the parent handoff/register. The Office implementation,
tests, dependency pins and nine-file child scope are unchanged. The parent's
same-version verification wheel is 26.11.7.1, SHA-256
`f114a3ba7e27349815407e9113cd9615e782b21f72c79c8fea6688e10dc2de72`, installed at
`/workspace/scratch/174af26f7506/wheel-install-final`. Its verification evidence
is recorded in the [foundation handoff](2026-11-01-batch-dashboard-foundation-180.md).

This narrow refresh rechecks documentation and the inventory index/ledger;
the complete Office suite is not repeated locally. Fresh hosted CI and Codex
review must cover the resulting child head. The earlier copied Office CLI run
below used the preceding parent wheel and is retained as historical evidence.

Validation in the same Linux/Python 3.14.10 environment:

| Check | Result |
| --- | --- |
| `python -m pytest -q tests/test_script_inventory.py -k 'documentation_indexes_track_live_inventory_counts or individual_lifecycle_ledger_tracks_manifest_and_mcp_targets'` | 2 passed, 20 deselected; inventory remains 157 tracked, 83 selected, 74 excluded and 33 MCP paths. |
| `python repo-scripts/check_documentation.py` | All seven default READMEs and central-index coverage passed. |
| Explicit `check_documentation.py --links-only` for this record, the work register and foundation handoff | All three documents passed. |

Git comparisons confirm the Office script/tests are unchanged from `811b936b`,
the inherited package and dependency state match `f003a762`, and only the same
nine Office files differ from that parent. Whitespace checks pass.

## Earlier hosted validation — child 811b936b

All six checks passed at `811b936bb8481f8c9ad0745732c42c7d78f49007` against
parent `ca788bf4`: [Python CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/42350876276)
and [Fast PR checks](https://github.com/Chain-Frost/ryan-tools/actions/runs/42350876229).
Hosted tests reported 2,390 passed, 14 skipped and 8 deselected; MCP checks
reported 103 plus 73 passes; POSIX wrapper security reported 25 passes and
2 skips. Package, Policy and Ruff/whitespace checks passed. The
[Codex receipt](https://github.com/Chain-Frost/ryan-tools/pull/201#issuecomment-6703875022)
reported no major issues on that exact head. These results do not establish
acceptance of the subsequent refresh to `f003a762`.

## Earlier local validation and delivery — 2026-11-07

Validation used Linux/Python 3.14.10 at
`/workspace/scratch/143a79d89a6a/gdal-runtime/bin/python3.14`, with
`PYTHONUSERBASE=/workspace/scratch/174af26f7506/py-userbase` and
`WP08_VALIDATION_SPAWN=1`. Commands ran from the isolated Office worktree at
`/workspace/scratch/174af26f7506/office-worktree`.

| Check | Result |
| --- | --- |
| `python -m pytest -q tests/scripts/test_excel_protection_security.py tests/scripts/test_promoted_utilities.py -k 'excel_protection and not copied_cli'` | 22 passed, 1 Windows junction test skipped, 8 deselected (copied CLI plus unrelated utilities). |
| Full focused Office suite, including the copied CLI, using `-k excel_protection` | 23 passed, 1 Windows junction test skipped, 7 unrelated utilities deselected. Copied CLI preview, successful write and existing-output refusal passed against installed 26.11.7.1. |
| `python -m pytest -q tests/test_script_inventory.py` | 22 passed. |
| `python repo-scripts/audit_installed_scripts.py --check --output /workspace/scratch/174af26f7506/office-inventory-2026-11-07.json` | 157 tracked entries, 83 selected, 74 excluded, 33 MCP paths; no missing or unsafe selected entries. |
| Ruff lint and format checks on the Office script and its two test files | Passed; all three files already formatted. |
| Strict Pyright on the same three files, using the validation interpreter | 0 errors, 0 warnings. |
| `python repo-scripts/check_documentation.py` | Passed all seven default READMEs and central-index coverage. |
| Explicit `check_documentation.py --links-only` on the six Office index, work, audit and guide documents | Passed. |

The installed-package run invoked the same interpreter from `/tmp`, passing
absolute paths to the two test files, with
`PYTHONPATH=/workspace/scratch/174af26f7506/wheel-install`,
`PYTHONNOUSERSITE=1` and `WP08_VALIDATION_SPAWN=1`. An independent import from
`/tmp` confirmed `ryan_library` comes from that installed target. The final
parent merge changes closure/export code and metadata; it does not change the
Office implementation, tests or shared dashboard/logging helpers validated by
the Office run. Inventory and documentation checks were repeated after that
merge. Only `tests/test_data` and `qgis-resources` were initialized, at the
parent's pinned commits. No native Office application or live network share was
exercised. The Windows-specific result below is historical, not a claim about
this run.

## Historical validation and delivery — 2026-11-04

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

### Historical next action

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

## Review correction

The #201 review identified loss of the previous persistent DEBUG file sink.
Restored editable LOG_FILE/FILE_LOG_LEVEL after batch preflight, preserving
mutation-free preview and rejecting workbook/log path aliases. Added explicit
DEBUG retention, preview and alias tests: 24 focused checks pass, 7 deselected.
This supersedes the earlier 22-test code-validation count.

## Final integration validation

Parent #183 at 94a63b89 passes all six GitHub checks (2,098 hosted tests,
14 skipped, 8 deselected). Child 71261b47 passes all code-focused tests but
the hosted inventory checks found the new README missing from the ledger
and stale index totals. Reconciled all 157 entries and 83/74 selections;
focused inventory validation passes. The current child check result is linked
in the PR body and checks tab; no merge is authorised. Final code wheel
SHA-256: `aee480140e2dc21247f154e8baacce6f89f3450ed274b8de006ccd7e2f414662`.
