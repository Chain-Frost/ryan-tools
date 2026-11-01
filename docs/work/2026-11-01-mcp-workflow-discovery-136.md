# MCP workflow keywords and domain overview

| Field | Value |
| --- | --- |
| Status | Needs review — strict MCP boolean correction delivered in draft PR #186 |
| Owner | ChatGPT (MCP correction); maintainer final review |
| Created | 2026-11-01 |
| Updated | 2026-11-01 |
| Next review | 2026-11-08 |
| Issue | [#136](https://github.com/Chain-Frost/ryan-tools/issues/136) |
| Baseline | [main at 99cd3cc](https://github.com/Chain-Frost/ryan-tools/commit/99cd3ccaaabd83b682392f7e285d90b570d353cb), merge commit ddcf6e7 |
| Delivery branch | `fix/mcp-overview-boolean-136-stack184`, targeting PR #184's `feat/tuflow-result-dashboards-180` |
| Delivery state | [Draft PR #186](https://github.com/Chain-Frost/ryan-tools/pull/186) targets #184. Code `984dbab84fb83233345fcc8ae18b6fc39f269f6c` is pushed and has actual installed-wheel acceptance. A normal merge incorporates parent `d8f9d782` and this receipt; the PR description records the resulting head and its final CI. PR #182 previously merged as `82c1a1b`; no new PR merge or release is authorized. |

## Outcome and scope

Make existing MCP workflows discoverable through familiar terms such as crop and pyramids, and expose a compact
domain overview before an agent narrows its search. PR #182 delivered the original feature. The current follow-up
rejects non-boolean overview requests at the registered MCP boundary. This file remains the authoritative contract
and status record for issue #136 and its correction.

The initial setup commit did not implement the feature. Subsequent commits implement the agreed functionality on the same PR branch.
The implementation and review prompts below authorize their agents to commit and push task-related changes to the
existing PR branch. Those original delivery prompts are historical after #182 merged. The user's subsequent
instruction authorizes a new PR stacked onto #184; no PR merge, auto-merge, release or force-push is authorized.

Read [AGENTS.md](../../AGENTS.md), the [development guide](../DEVELOPMENT_GUIDE.md),
[work tracking](../WORK_TRACKING.md), [MCP setup](../MCP_SETUP.md) and the
[MCP architecture handoff](../mcp-handoff/README.md) before editing.

## Current state and dependencies

- PR #120 introduced literal search and pagination; its
  [follow-up decision](https://github.com/Chain-Frost/ryan-tools/pull/120#issuecomment-6250031000)
  explicitly reserved curated terms and domain overview for #136.
- [PR #137](https://github.com/Chain-Frost/ryan-tools/pull/137) is merged into the baseline.
  Its older description and work-record wording do not override its live merged state.
- [PR #159](https://github.com/Chain-Frost/ryan-tools/pull/159) has since merged to main at `99cd3cc`.
  Its generic catalogue and installed-wheel smoke changes are retained by merging main into this feature branch
  before implementing #136; the PR remains based on main, not stacked on #159.
- Before this change, the registry returned no matches for crop or pyramids. Raster clip finds
  `clip_rasters_to_polygon`; overviews finds the existing overview and raster-translation workflows.
  These observations used `include_unavailable=True` and the create profile, so installation did not affect matching.
- The baseline contains 36 workflow definitions and 27 create-profile-visible entries before availability filtering.
  These are review observations, not hard-coded acceptance totals.
- Existing registry/server tests cover filtering, pagination and health totals. The actual installed-wheel smoke
  already checks discovery without a checkout.
- No feature implementation is blocked. Several older work-register rows have past review dates; this task does not
  resume those work fronts or treat stale status text as a dependency.

## Original delivery and agent sequence

Use one feature PR. The model, registry, server, catalogue and tests form one small connected change.

1. Agent 1 implements and validates the whole feature, updates this record and the PR description, pushes, and reports
   its tested commit SHA. Only one agent writes to the delivery branch at a time.
2. Agent 2 starts after Agent 1's handoff, independently reviews the current remote head, fixes substantiated defects,
   and records acceptance evidence at the final head.
3. Leave the PR draft and unmerged for the user. Report whether it is ready for final review and any concrete blockers.

Use a separate worktree/checkout if the existing working directory contains unrelated changes. Prefer GitHub CLI or
the GitHub connector for repository operations. Preserve concurrent work, re-fetch before pushing, and use ordinary
forward commits. Do not create replacement PRs or a separate reviewer PR.

## Agreed implementation contract

### Curated keywords

- Add one optional `keywords` field to `WorkflowSpec`, typed as `tuple[str, ...]` with an empty default.
  JSON catalogue entries use an array of strings. There is no separate aliases field.
- A missing field and an empty array are valid. Reject supplied null, scalar strings, non-string elements and
  whitespace-only terms with an actionable catalogue error.
- Trim each term; remove case-insensitive duplicates in input order, retaining the first trimmed spelling.
  Permit meaningful multiword terms. Do not change workflow IDs.
- Apply one shared validation/normalization contract through both generic and GDAL loading paths.
  GDAL currently constructs `WorkflowSpec` directly; validation only in `from_mapping()` is insufficient.
- Treat keywords as a known model field, not a duplicate arbitrary metadata entry.
  Expose normalized keywords with selected workflow details for inspection; keep paginated summaries compact.
- Keep metadata in the existing authoritative JSON entries. Do not duplicate GDAL records in the generic catalogue.

Minimum useful curation:

| Existing workflow | Required discovery term(s) |
| --- | --- |
| `clip_rasters_to_polygon` | crop, cropping |
| `build_external_overviews` | pyramids |
| `tuflow_log_summary` | logs |

Additional terms are optional only where their meaning clearly matches existing behavior. Do not add workflows,
promote legacy scripts or relabel safety profiles to satisfy a search example.

### Search and compatibility

- Preserve case-insensitive literal substring matching with all whitespace-separated query words required somewhere
  in the combined searchable fields. Add keywords alongside the existing ID, title, domain and purpose.
- Preserve exact case-insensitive domain filtering, configured-profile ceilings and availability filtering.
- Preserve existing workflow ordering by `(domain, title, workflow_id)` and return each workflow at most once.
- Keywords are search metadata. `get_workflow` continues to require the canonical workflow ID.
- No global synonym replacement, fuzzy ranking, embeddings, external search service or new dependency.
- Preserve complete health totals and the explicitly requested full catalogue resource.
  Keep full catalogue data and scripts out of startup instructions.

### Domain overview interface

Add `overview: bool = False` to the existing MCP `list_workflows` tool. Use the existing tool rather than adding
one endpoint per workflow or a second catalogue.

Normal mode must retain the current response fields, defaults and behavior. In particular, keep default limit 10,
maximum limit 25, nonnegative offset, the workflow-summary payload and complete filtered `workflow_count`.
Do not repurpose the existing `domains` field: it remains a sorted list of domain-name strings.

Overview mode has this contract:

| Field or behavior | Required meaning |
| --- | --- |
| `overview` | Explicitly true in the overview response |
| `workflow_count` | Number of matching workflows after all filters, before pagination |
| `domain_count` | Number of matching domains before pagination |
| `domains` | Sorted matching domain names, retaining the existing string-list meaning |
| `domain_overview` | The current page of compact domain rows |
| Domain row | `domain`, full filtered `workflow_count` and `examples` |
| Examples | At most three entries, containing only `id`, `title` and `available` |
| Domain ordering | Deterministic sorted domain names |
| Example ordering | Existing workflow ordering within each domain |
| `limit` / `offset` | Apply to domain rows in this mode |
| `returned_count` / `has_more` / `next_offset` | Describe the domain page, using the existing pagination conventions |
| Workflow payload | Omit `workflows` from overview mode; do not return command arrays, execution targets or full summaries |

Apply domain, query, effective-profile and `include_unavailable` filters before aggregating or selecting examples.
A hidden or unavailable-and-excluded workflow must contribute neither counts nor examples. Omit empty domains.
Unknown terms produce zero counts, empty overview rows and no next offset. An offset past the last domain returns
an empty page while retaining complete filtered totals.

Use the same limit/offset validation and profile-escalation errors as normal listing. Validate the new overview
flag as a boolean for direct Python callers as well as MCP requests.

Registry code owns filtering and aggregation. The server remains a thin adapter for input validation, pagination
and tool registration. Share existing filtering rather than create a second policy implementation.

Entry-point availability must keep its existing meaning: it does not verify external GDAL/TUFLOW/HY-8 binaries or
licensed execution. Preserve all mutation, approval, help-safety and linked-path protections.

### Agent-facing guidance

Update the tool description, concise startup instructions and MCP setup guide with:

1. `list_workflows(overview=True)` when the relevant domain is unclear.
2. `list_workflows(domain="gdal", query="crop")` for narrowed discovery, with pagination as needed.
3. `get_workflow("clip_rasters_to_polygon")` for details before normal shell execution.

Agents may search directly when they already know the domain or task. Do not require an extra overview call.
Document which entity the pagination fields count in each mode, the three-example bound and literal-search semantics.

## Expected change areas

| Area | Files |
| --- | --- |
| Model and registry | [models.py](../../ryan_library/mcp/models.py), [registry.py](../../ryan_library/mcp/registry.py) |
| MCP adapter and instructions | [server.py](../../ryan_library/mcp/server.py) |
| Authoritative catalogues | [workflows.json](../../ryan_library/resources/mcp/workflows.json), [gdal_cli_tools.json](../../ryan_library/resources/mcp/gdal_cli_tools.json) |
| Focused coverage | [test_registry.py](../../tests/mcp/test_registry.py), [test_server.py](../../tests/mcp/test_server.py), [catalogue contracts](../../tests/mcp/test_catalogue_contracts.py) where relevant |
| Installed distribution | [installed-wheel smoke](../../repo-scripts/smoke_test_installed_wheel.py) |
| Guidance and tracking | [MCP setup](../MCP_SETUP.md), this record, [work register](README.md), [documentation index](../README.md), [local handoff pointer](../mcp-handoff/README.md) |

This is an expected surface, not a requirement to edit every listed file. Avoid unrelated wrappers, solver code,
installation-policy changes, CI redesign or submodule updates. Do not inspect or traverse the unsorted submodule.
Target the repository's Windows/Python 3.14 environment and current annotation syntax; no Linux-support work is included.

## Completion criteria

- [x] Omitted keywords remain compatible; invalid keywords fail clearly through both catalogue loaders.
- [x] Normalization trims and de-duplicates deterministically.
- [x] Crop/cropping finds the existing raster clip, pyramids finds external overviews, and logs finds the generic log summary.
- [x] Keyword plus literal query terms, mixed case, exact domain filtering and unknown terms behave as specified.
- [x] Keywords never duplicate records, change canonical IDs or bypass profile/availability restrictions.
- [x] Default listing retains its contract, stable pagination and complete match totals.
- [x] Overview counts cover all filtered workflows, including results beyond the first workflow page.
- [x] Overview has deterministic domain pagination, bounded examples, correct empty/past-end results and no hidden entries.
- [x] Core/analysis/create/privileged and narrower requested profiles retain their existing visibility contracts.
- [x] Full catalogue and health totals remain complete; no catalogue preload or execution is introduced.
- [x] Actual installed-wheel discovery proves packaged keywords and overview work outside the checkout.
- [x] Focused tests, relevant formatting/typing, documentation checks and package verification pass at tested source `dcdf3d0`.
- [x] Independent review findings are resolved or explicitly reported; the PR and work record match delivered behavior.

## Validation and delivery

### Setup evidence — 2026-11-01

- Reviewed current issue, source, relevant tests, PR #137 merged state and PR #159 overlap.
- The seed changes documentation only. Feature tests, strict typing, package build and installed-wheel acceptance have
  not run for this feature because implementation has not started.
- Passed: Git-tree validation of 123 relative links across the four changed documents and central-index coverage
  for all 70 Markdown documents excluding the index itself, including this new record. No unresolved paths or fences.
  A normal local clone was unavailable because the execution host could not reach its network proxy, so the repository
  documentation-checker CLI was not run locally. Hosted checks and the implementation agent provide the full checkout gate.
- Delivery is a documentation seed on the named branch, with a draft PR targeting main.
  Record the actual tested/accepted feature SHA in the next handoff; do not reuse setup evidence as feature acceptance.

### Implementation verification

Implementation commits:

- `ddcf6e7` merges main `99cd3cc` without touching submodules and retains #159's catalogue/wheel changes.
- `780c3a3` adds keyword validation/search, bounded registry domain aggregation, the existing MCP tool's
  `overview` flag, the three curated JSON entries and selected-detail keyword inspection.
- `94b562c` extends focused model/registry/server regression coverage and the actual installed-wheel smoke.
- Documentation records and the final exact-head validation receipt follow in subsequent commits.

The implementation, focused tests and actual installed-wheel acceptance criteria passed on Python 3.14 at tested
source `dcdf3d04169b9b1028d28d6f23fc2312229ba443`. Independent review subsequently found and fixed a
positional-constructor compatibility regression at `5f9aa7ed93ca01c87de03e5bb541001708c46985`.
The PR description records final-head CI evidence after the documentation acceptance receipt is pushed.

Run focused MCP tests sufficient to cover this contract and existing safety boundaries. Use current repository
formatting/lint guidance and strict Pyright on modified Python files. Record exact commands, environment and results.

Build with the existing package workflow; `python repo-scripts/build_library.py --no-bump` is appropriate for a
verification build. Exercise the actual installed wheel from a neutral directory with repository discovery disabled,
using and extending the existing smoke path. Synthetic installed-layout tests do not replace actual wheel evidence.

Run `python repo-scripts/check_documentation.py` and explicit links-only checks on touched status/policy files.
Review required GitHub checks at the latest pushed SHA. Do not add a CI workflow if existing checks can carry the change.

No new licensed TUFLOW/GDAL run or broad engineering acceptance programme is required for these metadata changes.
Do not run an unrelated full suite locally by default. If a required check cannot run, state the exact blocker and
leave the corresponding criterion pending.

### Hosted acceptance receipt — 2026-11-01

**Tested source SHA:** `dcdf3d04169b9b1028d28d6f23fc2312229ba443`, Windows/Python 3.14.
[Fast PR checks run #41279007038](https://github.com/Chain-Frost/ryan-tools/actions/runs/41279007038): **success**.
[Python CI run #41279007020](https://github.com/Chain-Frost/ryan-tools/actions/runs/41279007020): **success**;
all five jobs green (Policy, MCP smoke, Hosted tests, Package, POSIX wrapper security).

| Verification | Exact command / execution | Observed result at source SHA |
| --- | --- | --- |
| Changed-file whitespace | `git diff --check "$base...$env:GITHUB_SHA"` in Fast PR checks and `git diff --check` in Policy | Both pass |
| Pinned Ruff lint and format | `python -m ruff check $pythonFiles`; `python -m ruff format --check $pythonFiles` | Pass; six changed Python files already formatted; Ruff 0.16.6 |
| Strict Python 3.14 Pyright | `python -m pyright $pythonFiles` | **0 errors, 0 warnings** on six changed Python files |
| Focused MCP discovery and safety | `python -m pytest -q tests/mcp/test_registry.py tests/mcp/test_server.py tests/mcp/test_catalogue_contracts.py` | **94 passed, 1 warning** |
| Focused utility regressions | Existing MCP smoke step `python -m pytest tests/functions/RORB/test_read_rorb_files.py tests/functions/gdal/test_profiling.py tests/functions/test_process_12D_culverts.py tests/functions/tuflow/test_logsummary_excel_append.py tests/functions/tuflow/test_notebook_helpers.py tests/functions/tuflow/test_po_timeseries_checks.py -m "not gui and not external"` | **73 passed, 22 warnings** |
| Hosted test boundary | `python -m pytest tests --ignore=tests/functions/gdal --ignore=tests/orchestrators/gdal --ignore=tests/scripts/gdal/test_gdal_flood_extent.py --ignore=tests/scripts/test_promoted_utilities.py --ignore=tests/scripts/test_archive_compress.py -m "not gdal and not gui and not external" --deselect=tests/floodway/test_sweep.py::test_discharge_sweep_detects_overtopping_and_retains_non_aep_interior_states --deselect=tests/floodway/test_sweep.py::test_discharge_sweep_honours_explicit_minimum_bound` | **1,949 passed, 14 skipped, 8 deselected, 127 warnings**; hosted command additionally uses `-rs --durations=15 --junitxml=test-results/hosted-tests.xml`. |
| Documentation and links | `python repo-scripts/check_documentation.py`; `python repo-scripts/check_documentation.py --links-only docs/MCP_SETUP.md docs/mcp-handoff/README.md docs/work/README.md docs/work/2026-11-01-mcp-workflow-discovery-136.md` | Both pass: seven default documents plus index, four touched documents |
| Logging policy | `python repo-scripts/check_loguru_formatting.py` | Pass |
| Verified package | `python repo-scripts/build_library.py --no-bump --skip-pip`; `python repo-scripts/verify_wheel.py` | Pass; verified wheel `ryan_tools-26.11.1.1-py3-none-any.whl`, no version bump |
| Real neutral-directory wheel smoke | `python -m pip install --no-deps --target $target $wheel[0].FullName`; install separate `run-hy8` / `ryan-culverts`; set `PYTHONPATH=$target`; run outside checkout: `python "$env:GITHUB_WORKSPACE/repo-scripts/smoke_test_installed_wheel.py" --expected-root $target` | Pass; logs explicitly confirm **Wheel-only MCP keyword search and domain overview passed** and installed-wheel smoke passed |
| POSIX wrapper security (unchanged existing CI) | `python -m pytest -q tests/mcp/test_installed_scripts.py --confcutdir=tests/mcp` | **25 passed, 2 skipped** |

The focused warning is the pre-existing pandas deprecation in the TUFLOW `HProcessor`; it is not a discovery test
failure. Skips in the Hosted suite include deliberate opt-in real GDAL/QGIS, UNC/mapped-drive and long-path acceptance.
No licensed TUFLOW or HY-8 execution, external binary readiness, real GIS acceptance, release or publication occurred.
Script execution/approval policies were unchanged. The final documentation-only receipt commit needs its own exact-head
CI confirmation; this record captures the implementation SHA and reproducible commands, not a fabricated receipt.

### Independent review and source verification — 2026-11-01

- **Reviewed original PR head:** [`4215cc2`](https://github.com/Chain-Frost/ryan-tools/commit/4215cc29ca6bea5e94a853c74d33479c9455ef82)
  against `main` at `99cd3cc` (zero commits behind). Independently inspected the diff, review threads (none),
  model, both catalogue loaders, registry and server, tests, full health/catalogue, documentation and wheel smoke.
- **Substantiated defect:** Inserting `WorkflowSpec.keywords` before `metadata` changed the public dataclass
  positional signature; valid callers with an eleventh positional `metadata` argument could fail validation.
  [`5f9aa7e`](https://github.com/Chain-Frost/ryan-tools/commit/5f9aa7ed93ca01c87de03e5bb541001708c46985)
  restores the original argument position and adds a targeted positive regression test. It changes no catalogue
  entry, workflow summary, approval rule, capability profile or external execution path.
- **Hosted Python 3.14 source verification:** [Fast PR checks #41281038047](https://github.com/Chain-Frost/ryan-tools/actions/runs/41281038047)
  passed Ruff/whitespace; [Python CI #41281037984](https://github.com/Chain-Frost/ryan-tools/actions/runs/41281037984)
  runs Policy, MCP smoke, Hosted tests, Package and POSIX wrapper security. Strict Pyright reported **0 errors,
  0 warnings**; focused MCP discovery and safety coverage reported **95 passed, 1 warning** and focused utility
  regressions **73 passed, 22 warnings**. Package and security jobs passed; GitHub Actions is authoritative for
  each remaining job outcome and the PR description records the final exact-head results.
- **Actual distribution:** Package CI built and verified `ryan_tools-26.11.1.1-py3-none-any.whl` and logged
  **Wheel-only MCP keyword search and domain overview passed** and **Installed-wheel smoke test passed**, from
  a neutral installed location with no checkout discovery. This is not a synthetic layout substitute.
- **Review conclusions:** Shared keyword validation/normalization and literal all-word matching remain deterministic;
  canonical IDs, default listing shape, profile ceilings and availability filtering are preserved. Overview
  counts and examples use the entire filtered set before deterministic domain pagination, exclude hidden records,
  and include at most three entries containing only `id`, `title`, `available`. Health and catalogue totals remain
  complete, the server is a thin adapter, and no review threads were open.
- **Limitations:** No real licensed TUFLOW/HY-8/GDAL/QGIS execution, UNC/mapped-drive or long-path engineering
  acceptance was performed; those are outside #136. This review environment could not clone the repository because
  outbound GitHub DNS was unavailable, so the reported Windows/Python 3.14 acceptance and installed-wheel execution
  rely on inspected GitHub Actions logs rather than a second local test run. No release, merge or auto-merge occurred.

The documentation receipt is a separate forward commit; confirm that commit's exact-head required checks using
the PR description. Source test results alone are not proof for a later head.

## Agent 1 prompt — implementation

```text
Implement issue #136 in the existing draft PR on Chain-Frost/ryan-tools branch
feat/mcp-workflow-discovery-136, targeting main. Do not create another PR.

Read docs/work/2026-11-01-mcp-workflow-discovery-136.md in full, plus AGENTS.md and
the linked canonical/MCP guidance. Treat the implementation contract and
completion criteria as agreed scope.

Fetch the latest remote branch and main. Use an isolated worktree/checkout if
needed; preserve unrelated work and do not touch submodules. This handoff
authorizes committing and pushing task-related changes to the existing PR branch.

Implement the single optional keywords field, shared validation for generic and
GDAL catalogues, deterministic literal keyword search, and overview=True on the
existing list_workflows tool. Preserve normal listing behavior. Follow the exact
overview filtering, counts, pagination and bounded-example contract in the record.

Curate the three minimum examples, update agent-facing guidance, add meaningful
focused coverage, and extend actual installed-wheel discovery. Keep execution,
safety profiles, approval rules and script behavior unchanged.

Validate with Python 3.14, focused tests, formatting/lint, strict Pyright on changed
Python files, documentation checks, package build and actual installed-wheel
smoke outside the checkout. Use existing CI. Resolve failures caused by the change;
report unavailable gates accurately instead of claiming synthetic substitutes.

Reconcile overlapping catalogue/smoke changes from latest main without losing
concurrent work. Update the PR description, completion criteria, work record and
register with exact evidence. Change the PR's issue reference to Closes #136 once
the implementation actually satisfies the issue.

Push normal forward commits. Leave the PR draft and unmerged; no release,
auto-merge or force-push. Return the PR URL, pushed/tested SHA, changed behavior,
test results, remaining limitations and a clear handoff for independent review.
```

## Agent 2 prompt — independent review and acceptance

```text
Independently review and finish issue #136 in the existing Chain-Frost/ryan-tools
draft PR on branch feat/mcp-workflow-discovery-136, targeting main.
Start after the implementation agent has pushed and handed off the branch.

Read docs/work/2026-11-01-mcp-workflow-discovery-136.md in full, AGENTS.md, the PR
diff and current GitHub review/CI state. Fetch the current remote PR head and main;
record the SHA you review. Do not rely solely on the implementation summary or
test counts from an earlier head.

Review both catalogue-loading paths, normalization, literal search semantics,
canonical IDs, normal-response compatibility, profile ceilings and availability.
Check that overview counts aggregate the full filtered set before pagination,
examples are bounded and visible, and domain pagination is deterministic.
Check actual installed-wheel discovery, complete health/catalogue totals,
thin server integration and preservation of all existing safety boundaries.

Fix substantiated defects and add targeted regression coverage on this same
branch. This handoff authorizes task-related commits/pushes, PR-description updates
and replies/resolution of addressed review threads with evidence. Do not create a
replacement or reviewer PR, broaden scope or weaken assertions to obtain green CI.

Run proportionate verification after fixes. Verify the final pushed SHA's required
checks and actual wheel evidence, re-fetch before each push, and preserve concurrent
changes. Update the record/register and PR description so completed and pending
criteria match the evidence.

Leave the PR draft and unmerged; no release, auto-merge or force-push. Return the
final SHA, findings and fixes, validation results and remaining limitations, and
state whether it is ready for the user's final review.
```

## Next action

Independently review [draft PR #186](https://github.com/Chain-Frost/ryan-tools/pull/186), its final-head CI and the
installed-wheel receipt below. Handle parent #184 before this child when the maintainer authorizes stack delivery.
Keep both draft and unmerged; any later parent updates must preserve the correction and be revalidated.

## Progress

### 2026-11-01

Reviewed #136 against current main and related PRs. Selected one draft PR with sequential implementation and
independent review, defined the previously ambiguous keyword and overview contracts, and prepared this indexed
handoff. Feature implementation and acceptance remain pending.

### 2026-11-01 — implementation delivery

The keyword model, both catalogue loaders, registry search, domain aggregation and existing MCP tool are updated.
Original user-facing pagination and profile filtering remain available. Generic and GDAL curation use authoritative
JSON; neither creates a new script nor changes CLI behavior. Added focused tests including failure paths and actual
installed-wheel smoke assertions; validation outcomes are recorded separately once CI finishes.

### 2026-11-01 — delivered verification

The implementation and all five Python 3.14 CI jobs passed at `dcdf3d0` with the exact commands/results above.
Actual installed-wheel discovery of all curated terms and the complete paginated domain overview passed outside
checkout. PR #182 stays draft and unmerged, pending independent review.

### 2026-11-01 — independent review and handoff

Independently audited the delivery against the agreed contract at `4215cc2`, corrected the existing public
`WorkflowSpec.metadata` positional-argument regression with a targeted test at `5f9aa7e`, and examined hosted
source CI and isolated installed-wheel discovery. The next step is maintainer final review of draft #182 after
exact-head CI is verified and recorded in its description.

## Installer launcher names - 2026-11-01

At the maintainer's request, this PR also carries five batch-file renames that distinguish PyPI installation,
local-wheel installation and local build operations. Calls, setup documentation and the existing launcher test
use the new names. The generic install-ryan-tools launcher remains a PyPI alias. Installer behavior is preserved.

Local Python 3.14 validation: installer/setup tests 35 passed, 1 symlink-permission skip; scoped Ruff lint/format
and strict Pyright passed. README/environment documentation checks and MCP links passed. All batch call targets
exist. No installation or package rebuild was needed. This follow-up is committed/pushed on the existing PR;
CI receipts above apply to their named earlier commits until the new head finishes. No merge or release.

## Strict MCP boolean follow-up — 2026-11-01

PR #182 merged externally at 09:29 UTC on 2026-11-01. Its implementation was complete, but a later check exposed
one remaining boundary defect: the MCP SDK coerced raw `0`, `1`, `"true"` and `"false"` into booleans before
`list_workflows` could reject them. Direct Python calls already returned the required validation error.

The correction adds strict boolean metadata to the existing argument while keeping its advertised JSON boolean
schema and `false` default. The metadata uses the optional SDK's Pydantic dependency; direct helpers still work
without either MCP or Pydantic. Eight registered-tool cases cover invalid values, omitted/false normal listing,
true overview and the schema/default. The existing Package smoke now installs the declared SDK range and checks
those same boundaries against the actual installed wheel outside the checkout. The existing MCP test step includes
the new regression file. The release workflow's existing smoke caller receives the same SDK test dependency;
its release triggers and approval conditions are unchanged. No new workflow or production dependency is introduced.

### Stack and preservation

- Parent #184 initially stood at `d16dce0fbc5495cbf54ca7cf2c1c91af4e63a17c`, before #182. Before publication it
  advanced to `89124cc239a3b730007657ad454df9f570bf4089` and incorporated the foundation fixes and `main` at
  `82c1a1b6e30573971ac5e1ac5fb64b41b20e9495` itself. The child was recreated from that newer parent with only
  the MCP correction and verification changes. The earlier local main merge was never published.
- Both dashboard register rows, existing wheel-smoke steps, MCP documentation checks and main's installer changes
  are retained. Neither parent branch is modified or retargeted.
- After publication, #184 advanced through `d8f9d782b4bb20ed0635cd7c653f074294b773f7`. The child incorporates it
  with an ordinary merge, retaining the parent's TUFLOW CSV processing code and Phase B status. The only textual
  conflict was adjacent work-register rows; both work fronts' current states are retained.
- The prepared local correction originated at `10a41ad7bf76f996b36df169d2153a09f4124c75`. The new branch also adds
  omitted-argument and installed registered-tool acceptance. Earlier broad local implementation work is excluded.
- No submodule is initialized or modified. Unrelated overdue register items are preserved and not resumed.

### Local correction verification

Python 3.14.8, MCP 2.3.0, Pydantic 2.14.2, Ruff 0.16.6 and Pyright 1.1.414. In these commands, `python` is
`/workspace/scratch/6184835f02e8/validation-env/bin/python`; the working directory is the isolated child checkout.

```bash
python -m pytest -q tests/mcp/test_registry.py tests/mcp/test_catalogue_contracts.py \
  tests/mcp/test_server.py tests/mcp/test_overview_validation.py --confcutdir=tests/mcp \
  --deselect=tests/mcp/test_server.py::test_inspect_tuflow_result_uses_processor_factory_and_bounds_sample \
  --deselect=tests/mcp/test_server.py::test_inspect_tuflow_result_filters_location_and_processed_time \
  --deselect=tests/mcp/test_server.py::test_inspect_tuflow_result_supports_tlf_processor \
  --deselect=tests/mcp/test_server.py::test_inspect_tuflow_collection_summarizes_processors_and_combined_sample \
  --deselect=tests/mcp/test_server.py::test_inspect_tuflow_collection_reports_per_file_errors \
  --deselect=tests/mcp/test_server.py::test_inspect_tuflow_collection_filters_data_types_case_insensitively
python -m ruff check ryan_library/mcp/server.py tests/mcp/test_overview_validation.py \
  repo-scripts/smoke_test_installed_wheel.py
python -m ruff format --check ryan_library/mcp/server.py tests/mcp/test_overview_validation.py \
  repo-scripts/smoke_test_installed_wheel.py
python -m pyright --pythonpath /workspace/scratch/6184835f02e8/validation-env/bin/python \
  ryan_library/mcp/server.py tests/mcp/test_overview_validation.py repo-scripts/smoke_test_installed_wheel.py
python repo-scripts/check_documentation.py
python repo-scripts/check_documentation.py --links-only \
  docs/work/2026-11-01-mcp-workflow-discovery-136.md docs/work/README.md
git diff --check
python repo-scripts/build_library.py --no-bump --skip-pip
```

Local results: **97 passed, 6 deselected**; Ruff lint/format passed; strict Pyright **0 errors, 0 warnings**;
documentation defaults, explicit links and whitespace passed. These commands ran at local checkpoint `0a1c721`;
the same three changed Python blobs are validated at published `984dbab` in the hosted receipt below.
Six existing Module 11 tests require the unavailable `tests/test_data` submodule and are explicitly deselected locally. A fresh interpreter
with both `mcp` and `pydantic` imports blocked successfully imports the server, returns an overview and rejects a
direct integer flag. SDK async tests use an execution environment with functioning thread wakeups.

The local package build cannot complete without the uninitialized QGIS style resources. Existing Windows Package
CI supplies its own resources and remains the actual-wheel verification route; a synthetic installed layout is not
substituted. Parent #184 had unrelated Hosted tests and Package wrapper failures at its earlier `d16dce0` head;
the updated parent includes fixes. Those old failures are historical, and the child's current results determine
its remaining gates.

### Correction completion criteria

- [x] Reproduce coercion against merged main and implement strict registered argument validation.
- [x] Preserve direct helpers, boolean schema/default and both listing modes.
- [x] Add focused registered-tool regressions and actual installed-wheel assertions to existing CI.
- [x] Obtain authorization for a new draft child targeting #184 after #182 merged.
- [x] Publish the child, record its tested SHA and obtain actual installed-wheel results.
- [x] Complete independent read-only integration review and fix its shared-smoke dependency finding.
- [ ] Maintainer independent review and stack acceptance; preserve final-head CI and parent-work boundaries.

### Published correction receipt

Published code: `984dbab84fb83233345fcc8ae18b6fc39f269f6c`. The
[Python CI run](https://github.com/Chain-Frost/ryan-tools/actions/runs/41301798227) checked GitHub merge commit
`53ff8b924b7049af4d9c16f1008a57ea43fa32fd`, whose tree `70595a3dea99ef8c0c91be5bb4c950ea8f902bc8` exactly matches
the published code tree. These are genuine Windows/Python 3.14 results, distinct from the earlier local environment.

| Gate / exact command | Result at `984dbab` |
| --- | --- |
| `python -m pytest -q tests/mcp/test_registry.py tests/mcp/test_server.py tests/mcp/test_catalogue_contracts.py tests/mcp/test_overview_validation.py` | **103 passed**, including all eight registered-tool cases and the six locally unavailable Module 11 cases; [MCP job](https://github.com/Chain-Frost/ryan-tools/actions/runs/41301798227/job/124834110749) |
| Existing separate focused utility command in the same MCP job | **73 passed** |
| Changed-file Ruff and `python -m pyright $pythonFiles`; documentation and workflow policy | Passed; strict Pyright **0 errors, 0 warnings**; [Policy job](https://github.com/Chain-Frost/ryan-tools/actions/runs/41301798227/job/124834110714) |
| `python repo-scripts/build_library.py --no-bump --skip-pip`; `python repo-scripts/verify_wheel.py` | Passed with declared version unchanged; [Package job](https://github.com/Chain-Frost/ryan-tools/actions/runs/41301798227/job/124834110740) |
| `python -m pip install --no-deps --target $target $wheel[0].FullName`; install the separate hydraulic packages; set `PYTHONPATH=$target`; change to `RUNNER_TEMP`; `python "$env:GITHUB_WORKSPACE/repo-scripts/smoke_test_installed_wheel.py" --expected-root $target` | Passed: real wheel keyword/overview discovery, registered strict-boolean validation and copied-wrapper behavior |
| `python "$env:GITHUB_WORKSPACE/repo-scripts/smoke_dashboard_installed_wheel.py" --expected-root $target` from `RUNNER_TEMP` | Passed: parent's copied log-summary on/off, workbook, DEBUG, warning and failure checks |
| [Fast PR checks](https://github.com/Chain-Frost/ryan-tools/actions/runs/41301798198) and POSIX wrapper security | Passed |

The built wheel is `ryan_tools-26.11.1.1-py3-none-any.whl`, 890,895 bytes,
SHA256 `551ac866f3bd8fa322ceb1e6ccbca4b734974925d137b8fe87e2d095b87a866a`.
The verified-wheel artifact is `13081567832` on that run. Logs explicitly confirm both
`Wheel-only MCP keyword search and domain overview passed.` and
`Wheel-only registered MCP overview boolean validation passed.` before the full installed-wheel success receipt.

Independent integration review found one shared-caller omission: the release verification job also invokes the smoke
script and needed its SDK test dependency. That omission was fixed before publication; no correctness or safety
findings remained in the bounded seven-file correction. Release execution itself is not authorized or attempted.

The full Hosted tests job and any later parent-merge/head receipts are recorded in the PR description when complete.
The prior red results at parent `d16dce0` are historical; acceptance depends on the actual head being reviewed.
No proprietary application, licensed solver or external GDAL/QGIS execution is claimed by these metadata checks.
