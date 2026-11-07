# WP01 — TLF parser metadata reliability and diagnostics

| Field | Value |
| --- | --- |
| Status | Active; #239 and #224 draft PRs require validation; #231 remains separate |
| Owner | ChatGPT (WP01) |
| Created / updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | main 5ad632cc9a17b302ad3f4f2b55315a1e2e8d1d51 |
| Allocation | [#240 WP01](https://github.com/Chain-Frost/ryan-tools/issues/240) |
| Original requirements | [#239](https://github.com/Chain-Frost/ryan-tools/issues/239), [#224](https://github.com/Chain-Frost/ryan-tools/issues/224), [#231](https://github.com/Chain-Frost/ryan-tools/issues/231) |
| Delivery | [PR #251](https://github.com/Chain-Frost/ryan-tools/pull/251); #224 separate stacked PR; neither merged |

## Scope and decisions

Keep shared TLF metadata parsing in ryan_library/functions/parse_tlf.py.
TLFProcessor, activity reporting and log-summary consume the same parser.
Do not duplicate TPC parsing, alter unique-run semantics or change failure-state
contracts owned by #195/#223.

### Stage 1 — #239

- Support hyphen-separated names, retaining ordinary/underscore names and
  reserved event/scenario placeholder exclusions.
- Keep malformed-assignment warnings.
- Assert values in direct parser, TLFProcessor and public log-summary paths.
- Branch: fix/tlf-hyphenated-variables-239; PR #251 targets main as a draft.

### Stage 2 — #224

- Validate whole signed decimal/exponent tokens and reject invalid/non-finite
  values without consuming a numeric prefix. Completion status is distinct from
  the presence of a valid final mass-error percentage.
- Preserve control-file names with dots, spaces, Windows/UNC, forward slash and
  bare paths. ECF/TEF remain stems without extensions; TGC/TBC/BC_dbase remain
  basenames with extensions, consistent with column_definitions.json.
- Strip only delimited explanatory/redirection suffixes.
- Support explicit start timestamps with month abbreviations or numeric months,
  with minutes or seconds: YYYY-Mon-DD HH:MM[:SS] and YYYY-MM-DD HH:MM[:SS].
- Decode UTF-8 and CP1252 per complete line. Small log reading remains bounded
  by the 10 MiB threshold; large-log headers are streamed and the tail seeks
  the last 30 KB, capped at 100 lines. Discard the first partial tail line
  where an arbitrary seek could split a multibyte character.
- Branch: fix/tlf-metadata-224, stacked on PR #251. No main merge.

### Stage 3 — #231 (separate delivery)

- Define inexpensive main-TLF diagnostics with typed units first; consider
  opt-in HPC companion scans later with sampled/observed labels, provenance
  and bounded reads without exclusive locks.
- Reuse existing PyTuflow capabilities; no duplicate TPC parser.
- Native HPC/share acceptance cannot be claimed from synthetic fixtures.

## Validation and delivery

- #239 implementation 7234dd57, regression test 0b1b6f1c, lint repair
  6f067f1f. Fast PR checks passed on that head. Full Python CI and
  installed/copied-wrapper smoke still require verification.
- #224 parser implementation 1e228e09 and regression test 827fb759.
  Focused tests, Ruff/strict Pyright, GitHub CI, installed/copied-wrapper smoke
  and packaging not yet verified at this record revision.
- GitHub connector access does not itself provide the normal local Python 3.14
  environment. No licensed solver or production-data mutation was performed.
- Neither draft is authorized to merge, auto-merge or publish a package.

## Next action

Open the stacked #224 draft PR and inspect exact-head GitHub CI, repair any
regressions, validate #239's public wrapper headlessly and update this
record with real evidence. Continue #231 in a separate reviewable PR.
