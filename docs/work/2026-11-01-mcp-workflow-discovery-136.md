# MCP workflow keywords and domain overview

| Field | Value |
| --- | --- |
| Status | Independently reviewed; awaiting maintainer final review of draft PR #182 |
| Owner | ChatGPT (implementation and independent review); maintainer final decision |
| Created | 2026-11-01 |
| Updated | 2026-11-01 |
| Next review | 2026-11-08 |
| Issue | [#136](https://github.com/Chain-Frost/ryan-tools/issues/136) |
| Baseline | [main at 99cd3cc](https://github.com/Chain-Frost/ryan-tools/commit/99cd3ccaaabd83b682392f7e285d90b570d353cb), merge commit ddcf6e7 |
| Delivery branch | `feat/mcp-workflow-discovery-136`, targeting `main` |
| Delivery state | PR #182 remains draft/unmerged. Reviewer fix and regression test are committed at `5f9aa7e`; source and final-head CI receipts are linked from the PR. No release. |

## Outcome and scope

Make existing MCP workflows discoverable through familiar terms such as crop and pyramids, and expose a compact
domain overview before an agent narrows its search. Continue the existing draft PR on the delivery branch above.
This file is the authoritative plan, acceptance contract and agent handoff for issue #136.

The initial setup commit did not implement the feature. Subsequent commits implement the agreed functionality on the same PR branch.
The implementation and review prompts below authorize their agents to commit and push task-related changes to the
existing PR branch. No merge, auto-merge, release or force-push is part of either handoff.

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

## Delivery and agent sequence

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

The maintainer should review the existing [draft PR #182](https://github.com/Chain-Frost/ryan-tools/pull/182),
including the independent compatibility fix and exact-head CI receipts in the PR description. The branch remains
unmerged; any merge and later release require separate authorisation.

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
