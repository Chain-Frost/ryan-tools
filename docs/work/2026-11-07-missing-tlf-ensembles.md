# Missing TLF ensemble inference

| Field | Value |
| --- | --- |
| Status | Complete; local delivery |
| Owner | ChatGPT |
| Created / Updated | 2026-11-07 |
| Next review | None |
| Git delivery | Implementation and this record committed together on `feat/tlf-ensemble-inference`; GitHub push authorized |

## Outcome

The maintained folder wrapper now infers the Cartesian product of observed filename fields within compatible layouts.
It supports absent TP/duration fields, expects TP01–TP10 for TP layouts, and deduplicates TLF/HPC log pairs.
All gaps are labelled inferred; log presence is independent of successful completion. Values absent everywhere remain
unknown. The original table-based interface remains available. See the [usage guide](../../ryan-scripts/tuflow/MISSING_RUNS.md).

## Validation

- Normal Python 3.14: 19 focused inventory and legacy table tests pass.
- Ruff format/lint, strict Pyright on all three modified Python files, compilation, CLI help and Loguru policy pass.
- Built/verified and installed wheel 26.11.7.9. Copied wrapper checks pass for no-TP inference, CSV export,
  overwrite refusal and nonexistent roots.
- Read-only mapped-drive campaign scan: 1,468 logs, 734 unique runs, 770 inferred expected, 36 missing at scan time.
  Models were still running. No identifying filenames or paths are retained here.
- Documentation links checked. No hosted CI, commit, push or merge performed.

## Remaining scope

None for filename inference. Successful-completion checks and pre-run simulation-plan generation are separate features.
MCP assessment: document the existing maintained headless CLI; no duplicate endpoint added for contextual inference.

## Next action

Use the folder CLI near campaign completion and review inferred gaps against intentional campaign exclusions.

## Wrapper compliance follow-up, 2026-11-07

Wrapper revision 2026-11-07.3 adds closing identity banners, descriptive CLI help and a Windows example,
configuration guidance, and validation within the handled workflow boundary. Parsing and pausing live at the process
boundary; failures never pause. Legacy table summaries use SUCCESS visibility and exclusively create their CSV.
The table collision regression verifies input and existing output bytes remain unchanged.

All 19 focused tests pass after these changes. Ruff, strict Pyright on the three changed Python files, compilation,
CLI help, Loguru policy, documentation links and whitespace checks pass. Built, verified and installed package
26.11.7.10. Disposable copied-wrapper checks cover folder/table success, opening/closing versions, visible table
details, both overwrite refusals, invalid edited booleans/blank paths, invalid levels/limits, missing working directory
and conflicting CLI arguments. Delivery is on `feat/tlf-ensemble-inference`; GitHub push is authorized. Hosted CI is unverified; no merge authorized.

## GitHub delivery, 2026-11-07

The user authorized commit/push of this work and an AGENTS.md policy change permitting routine pushes of task-related
commits without a separate request. Explicit user restrictions still apply; force-pushes, unrelated commits and merges
are outside that routine permission. Branch starts at main `312c7f51`; no unrelated worktree edits were included.
