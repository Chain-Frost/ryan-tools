# RORB reader audit and MCP implementation scope

| Field | Value |
| --- | --- |
| Status | Implemented: PR open for review; final-head CI underway |
| Owner | ChatGPT / maintainer |
| Created | 2026-11-05 |
| Updated | 2026-11-07 |
| Next review | 2026-11-12 |
| Baseline | Local main at `10b71898220d11feaff8256618ffb8eeec35f4ef` |
| Delivery | [PR #233](https://github.com/Chain-Frost/ryan-tools/pull/233), branch `feat/rorb-readers-mcp-20261105`; committed and pushed, unmerged |

## Outcome and scope

Inventory existing RORB functions and scripts, assess the supplied batch summary, standard output, CSV and parameter
file, and define reliable read-only APIs before exposing them through MCP. This is an initial source-and-example audit,
not certification of all RORB versions or run modes. Proprietary inputs remain outside Git. No RORB engine was launched.
The excluded submodule was not inspected. Unrelated untracked `QGIS-TUFLOW-Plugin/` content was left untouched.

## Current implementation

2026-11-07 continuation: Standard-output hydrograph increments are validated against the reported step using exact
source decimals and the printed precision of both the step and the time records. Regression tests cover contradictory,
rounded and Fortran-exponent time values, including increment metadata following the summary. Both Codex review threads
are resolved (`0cad1d9`, `d144148`). Current `main` has been merged without losing MCP registration or current package
metadata; all six hosted checks passed at `680ecff`. Closure processing now raises on nonuniform or insufficient CSV
time samples rather than silently omitting batch runs (`4074ee0`, `920ae7f`). Review current-head CI before merging;
no engine runs, merge or release were performed.

The user authorized implementation after the audit and clarified that CMD parameter layouts and executable feature
support can differ. Implemented source-preserving readers, structured diagnostics, strict DataFrame adapters, batch/CSV
association, output/CSV comparison and bounded MCP inspection. The [reader guide](../../ryan_library/functions/RORB/README.md)
defines the exact supported subset and migration behavior. No RORB engine was launched and no project inputs were edited.

- The supplied batch retains all 420 records and all 420 CSV associations are now corroborated using filename,
  signature, report reference, named sites and rounded peaks. Ambiguity and mismatches remain explicit.
- Standard output preserves 201 hydrograph rows and two 24-row rainfall tables. The CSV time offset remains reported
  as +1 hour, without correcting either source. Mapped and UNC spellings preserve their supplied forms and read identical hashes.
- The CMD reader implements the installed V2 single-burst layout, including lumped/distributed routing and IL/CL versus
  IL/ROC. Other options, additional records and multi-burst variants remain explicit partial results. Parsing never
  certifies executable compatibility. Saved RORBWin ARR2016 settings expose corroborated candidate records only;
  activity and conditional interpretation remain unknown. Unknown settings layouts are not guessed.
- `inspect_rorb_file` and `ryan-tools://guidance/rorb-readers` are registered in the actual MCP server. A live in-process
  tool call against the supplied CSV returned bounded structured data. Default inspection follows no references.
- Existing DataFrame callers now reject malformed input; unresolved CSV associations block closure processing before
  hydrograph jobs. Missing parameters are null. Two closure metric definitions remain unchanged and documented.

Validation: **103 focused reader/workflow/MCP tests passed**, with one Windows symlink-permission skip.
Strict Pyright passed on all 15 modified Python files; documentation links and patch whitespace passed.
The current-version wheel built successfully without a version bump. An isolated Q: wheel installation successfully
read all four supplied file types, exercised MCP inspection and verified packaged guidance without importing the checkout.
Hosted package/isolated-wheel, MCP smoke and POSIX security jobs passed on `062d0d24`. Hosted Ruff lint passed;
Ruff lint and formatting both passed on `237ed132`, as did its clean-checkout package build and isolated-wheel smoke.
Local Ruff remains blocked by Windows application control. A final local rebuild produced a wheel but failed source
verification because concurrently edited `ryan_library/functions/tuflow/pomm_utils.py` differed from the packaged file.
Do not distribute that shared-workspace wheel; use the verified hosted PR artifact. Earlier isolated RORB smoke evidence
remains valid. Final documentation-head CI is pending; see the PR for current check results.

The stages are delivered together on one coherent draft branch because the shared result contract, adapter migration
and MCP surface need to remain consistent. Further formats can be stacked on this branch or added after merge; no merge
or release is authorized. Storm/catchment readers remain the explicitly deferred follow-up scope from the audit.

## Initial audit inventory (baseline before implementation)

Paths below are relative to the repository root. These capabilities exist on the audited main commit.

| Source | Functions / behavior | Limits and MCP suitability |
| --- | --- | --- |
| `ryan_library/functions/RORB/read_rorb_files.py` | `find_batch_files`, `parse_batch_output`, `read_hydrograph_csv`, `analyze_hydrograph` | Recursive batch discovery; run-table reader; CSV reader; sample-count threshold exceedance. Partial format support, diagnostics and association gaps described below. |
| `ryan_library/functions/RORB/ensemble_analysis.py` | `read_rorb_parquet`, `calculate_peak_flows`, `calculate_closure_times` | Reads already transformed Parquet, groups maxima, calculates first-to-last exceedance span. Not a native RORB file parser. Source retains a production-validation caveat. |
| `ryan_library/functions/RORB/ensemble_plotting.py` | `plot_peak_flow_distribution`, `plot_exceedance_duration` | PNG plotting helpers, not read-only inspection. |
| `ryan_library/orchestrators/rorb/closure_durations.py` | `run_closure_durations` | Discovers batch files, associates CSVs, processes hydrographs and exports summaries. Depends on the incorrect filename assumption found here. |
| `ryan_library/orchestrators/rorb/ensemble_plotting.py` | `orchestrate_ensemble_plotting` | Reads long-form ensemble data and writes plots. |
| `ryan-scripts/RORB-python/RORB-find-closure-durations.py` | Maintained library-backed closure wrapper | MCP catalogue entry `rorb_closure_durations`, create profile. |
| `ryan-scripts/RORB-python/plot_ensemble_results.py` | Maintained library-backed ensemble plotting wrapper | MCP catalogue entry `rorb_ensemble_plots`, create profile. |
| `ryan-scripts/RORB-python/hydrograph_extract_rorb_csv.py` | Standalone `extract_metadata_from_filename`, `import_data`, `create_plot` | Excel/PNG exports; assumes creek/run-prefixed filenames and selects 12-hour files. Not a generic reader for the supplied unprefixed 24-hour CSV. |
| `ryan-scripts/RORB-python/extract_rainfall-to-TUFLOW.py` | Standalone `parse_time_increment`, `collect_header_lines`, `build_column_names`, `fix_and_extract`, `process_out_files` | Reads a caller-selected row interval in standard output; transforms rainfall, including first-increment adjustment and a trailing zero row. Useful extraction logic, not a complete or lossless report reader. |
| `ryan-scripts/RORB-python/rorb-peaks_v2.py` | Legacy repository-only batch-table parsing and higher-median peak/TP selection | Scans `.out` filenames but expects a batch run-table marker. Does not constitute a standard single-run output parser. |
| `ryan-scripts/python-not-polished/hydrograph_extract_rorb_csv-opt2_132.py` | Experimental hydrograph combining/plotting variant | Not a stable MCP API. |

The installed-script manifest selects the four non-versioned scripts in `RORB-python`; the legacy peaks script is not
selected. Current MCP has two RORB workflow catalogue entries and **no focused RORB file-inspection tool**. Catalogue
entries describe commands; they do not execute scripts or expose Python reading functions.

No general `.par`, `.stm` or `.catg` parser was found in tracked first-party library/scripts on this baseline. There is
also no complete standard `.out` reader on main. This negative finding excludes independent vendored/submodule content.

[PR #145](https://github.com/Chain-Frost/ryan-tools/pull/145) is separate, open and unmerged, verified through GitHub on
2026-11-05 at head `f404642bddf3b7bf8f5d71988cfa0d247d8a0e19`. It contains standalone calibration, command-line parameter
generation and numeric output-table inspection/scoring. That is a reuse candidate, not an available main API or a
general RORBWin `.par` parser. Its real-engine acceptance remains outstanding. The live PR is not marked draft despite
older comments requesting draft status; this audit did not alter it or rerun its acceptance checks.

## Supplied-file evidence

Literal pasted paths first failed at the `1` directory. Parent enumeration established `1_RORB`, then
`LongDurations_Ensembles_CC`; the actual filenames have underscores rather than the extra separators preceding them in
the pasted text. The leading space in the single-run filenames is real and was preserved.

Verified common directory:

```text
Q:\26\RP26188 HD4 TO HD1 PIPELINE - UON\1_RORB\CoondinerCreek\LongDurations_Ensembles_CC\Rest AEP
```

| Actual filename | Observation |
| --- | --- |
| `Updated_CoondinerCreek_batch.out` | 42,355 bytes, 532 lines; RORBWin 6.45; 420 rows, 42 run numbers, 10 temporal patterns, 7 durations, 6 AEP/EY labels. |
| `LongDurations_IL53.9_CL4.3_CC.par` | 30,300 bytes, 754 lines; positional numeric/flag records, paths, ARR2016 markers and adjacent numeric/NaN fields. No parser currently available. |
| ` aep20_du24hourtp2.out` | 15,666 bytes, 480 lines; RORBWin 6.45 standard output, input/data-check sections, routing information, parameters, named hydrographs and a 201-row numeric hydrograph summary. |
| ` aep20_du24hourtp2.csv` | 8,524 bytes, 204 lines; two preamble lines, then 201 increments and two named hydrographs. Existing CSV function returns 201 rows and three columns after dropping `Inc`. |

### Confirmed findings

1. **Batch-to-CSV association fails for every row.** All 420 numeric/text run-table records agree field by field with an
   independent split of the source table. However, zero of the 420 constructed CSV filenames exist. The reader prepends
   the batch filename stem, while actual CSVs start with a space and `aep`. A successful DataFrame is therefore not
   evidence that the downstream workflow can read its hydrographs.
2. **Batch report information is discarded.** Peak identifiers map to named locations in the report, but the API retains
   only `Peak0001`/`Peak0002`. It stops at the representative-hydrograph section, omitting those associations, elapsed
   time, model/version information and rainfall configuration. `Run` repeats across patterns: it is not a unique row ID.
3. **Missing parameters are represented as plausible values.** Defaults are IL/CL/kc/m = 0 and ROC = 1. The supplied file
   reports IL/CL, yet the returned table also contains ROC = 1 without distinguishing not-applicable from reported.
4. **Malformed input can look successful.** In-memory probes demonstrated: a table without RORB signature/parameters is
   accepted; a malformed middle row is dropped while valid rows are returned; an unknown duration unit is divided by 60;
   a numeric field `12garbage` becomes 12. Logs do not provide a structured completeness contract to an MCP caller.
5. **CSV recognition is weak.** The reader skips two lines and accepts arbitrary column names. A controlled pandas-input
   probe returned an unrelated `foo,bar` table. Time/finite-value/series-role validation belongs in the reader contract,
   not only downstream analysis. The preamble's explicit `.out` reference and original increment are currently lost.
6. **Standard-output and CSV time axes disagree in this example.** All 201 increments and both flow arrays match exactly,
   but CSV times are 1..201 hours and `.out` times are 0..200 hours: an offset of +1 hour throughout. Preserve both source
   axes and flag the conflict; the cause and correct interpretation are not established by this audit. Peak magnitudes
   also agree with the corresponding batch row, but that does not validate timing.
7. **Encoding needs an explicit policy.** The report contains non-UTF-8 text; the existing batch reader uses UTF-8 with
   replacement. Audit inspection used CP1252 for readable text. Replacement must be reported rather than silently treated
   as lossless, especially when parsing paths and location names.
8. **Two closure definitions coexist.** `analyze_hydrograph` counts samples above threshold times the timestep;
   `calculate_closure_times` measures first-to-last exceedance span, including gaps. They must not be exposed under one
   ambiguous metric name. Definition reconciliation is an analysis follow-up, not prerequisite parser implementation.

## Required reader contract

These are proposed acceptance requirements, not claims about implemented behavior.

- Identify format using signature, section structure and consistent fields; extension/one marker alone is insufficient.
  Distinguish unsupported, malformed, incomplete and recognized input. Report supported sections independently from
  whole-file completeness; unknown sections must not imply complete interpretation.
- Preserve source path spelling, encoding, format/version evidence, raw labels, units and line/section provenance.
  Keep missing, not-applicable and explicit zero distinct. Retain unknown records for bounded inspection.
- Return structured diagnostics with line, severity and reason. Never silently discard a malformed numeric record or
  fabricate a parameter. Any explicitly requested partial read must identify omissions and cannot claim full success.
- Validate numeric tokens, overflow markers, non-finite values, dimensions, row counts, time ordering, repeated headers
  and record boundaries. Support documented annotations explicitly, rather than accepting arbitrary numeric suffixes.
- Preserve AEP percent, EY and other frequency conventions as different semantic kinds, with original text retained.
  Validate duration units explicitly. Identify records by report/section/run/pattern context, not run number alone.
- Preserve hydrograph IDs, names and reported roles. Never infer observed versus calculated from column order or `HydNNN`.
- Resolve associated files from explicit references where possible. Filename candidates require verification and an
  association reason; report missing/ambiguous/mismatched candidates rather than choosing the first match. Bound all
  optional discovery and preserve leading spaces. File existence alone does not prove event identity.
- Keep inspection read-only, bounded and independent of RORB execution. Existing DataFrame callers need an explicit
  compatibility decision when adding richer results or making errors strict.

## Needed capabilities and proposed PR stages

Use this audit as the parent scope. Each stage can be reviewed separately; no requirement to wait for all format readers
before exposing a fully validated subset. Stack only on code dependencies, and retarget after parent merges.

| Stage | Needed work | Acceptance gate / dependency |
| --- | --- | --- |
| 0: audit and format corpus | This inventory; collect a version/run-mode matrix; identify authoritative format references and reusable portions of #145. | Current examples cover one 6.45 ensemble case plus existing 6.52 regression data, not every mode. |
| 1: shared parsing contract and batch/CSV | Structured results and diagnostics; strict recognition; retain batch metadata/peak names/representative records; validated CSV metadata, increments and time; replace assumed file links with explicit association results. | Synthetic malformed/truncated/ambiguous fixtures, independent row/value checks, 6.45/6.52 cases, no source writes; retain or deliberately migrate existing callers. |
| 2a: standard `.out` | Section-aware metadata, routing/loss settings, data-check messages, site/role mapping, numeric hydrographs and rainfall blocks where supported. Review #145 reuse without importing a wrapper into the library. | Multiple output verbosity/run variants; multiple or absent hydrograph blocks; malformed table rejection; `.out`/CSV cross-checks that flag the observed time offset. Depends on stage 1 contract. |
| 2b: `.par` | Explicit format variants for RORBWin saved settings versus RORB_CMD inputs; conditional flags/blocks, routing/loss/rainfall configuration and referenced files. | Positional fields require documented layouts or corroborated fixtures; preserve inactive/unknown records and NaN tokens without interpreting them as active values. Depends on stage 1 contract. |
| 3: focused MCP inspection | Proposed `inspect_rorb_file` with bounded section samples, filters and diagnostics; optionally a bounded collection/association tool once justified. Add API guidance, catalogue terms, server tests and setup documentation. | Expose only validated supported formats/sections; return total versus sampled counts, warnings and completeness; installed-wheel smoke and read-only checks. Depends on whichever readers it exposes. |
| Later: storm and catchment inputs | `.stm` rainfall/hydrograph and spatial-pattern inspection; `.catg` network/subarea/output-location inspection; reference consistency checks. | Separate scoped formats and fixtures. Needed for full input auditing, but not all required for the first result-reader MCP release. |

Reusable reading belongs under `ryan_library/functions/RORB` unless a demonstrated stateful lifecycle warrants a
processor. Keep export workflows and CLI behavior separate. Do not automatically promote legacy plotting or narrow
rainfall scripts. No package CLI duplication is needed merely to expose existing Python reading functions.

The corpus matrix must cover observed versions and, where real examples are available, single-event, design/ensemble,
calibration/command-line and other run modes; IL/CL versus IL/ROC and variable losses; differing output detail and
hydrograph roles; minute/hour durations and percent/EY labels; encodings; missing/empty/truncated reports; overflow and
non-finite fields; repeated/multiple blocks; filename variants and missing/ambiguous references. These are coverage
targets to verify against documentation/examples, not an assertion that every combination exists in every version.
Use synthetic distributable fixtures; keep proprietary models out of Git.

## Initial audit validation and delivery (historical)

- Python 3.14 source import confirmed from this checkout. Read-only execution of existing readers against the supplied
  files, independent table/value and `.out`/CSV comparisons, and in-memory adverse-input probes produced the evidence above.
- Focused tests: **24 passed**, one configuration warning (`cache_dir` with cache plugin disabled):

  ```powershell
  $env:PYTHONDONTWRITEBYTECODE='1'
  python -B -m pytest tests/functions/RORB tests/scripts/RORB tests/scripts/promoted_scripts/test_rorb_ensemble_reader.py -q -p no:cacheprovider --rootdir=tests --confcutdir=tests --basetemp=C:\Temp\rorb-audit-20261105-tests
  ```

- Initial pytest run using a Q: temporary directory failed with network-share access errors. The rerun above kept source
  execution on Q: and used a local disposable test directory. Passing existing tests does not close the uncovered findings.
- Documentation validation passed: `python -B repo-scripts/check_documentation.py` (seven default files and tracked
  index coverage), and explicit `--links-only` checks of this new record, `docs/work/README.md` and `docs/README.md`
  (three files). `git diff --check` passed. The new record is explicitly checked without staging it.
- No Python source changed, no new tests added, no package build needed. Documentation remains local: two existing
  files edited and this record added; no staging, commit, push or PR. This provides the audit and proposed parent/stack
  scope before implementation. No model runs, source model edits, merge or release are part of this audit.

## Next action and completion criteria

Review draft PR #233 and its final hosted checks before any merge or release. Preserve explicit partial/unsupported outcomes. Future semantic expansion of
saved settings, other CMD variants, multi-block output and storm/catchment readers requires documented fixtures; it is
not justified by a single pattern match or by successful parsing of the installed CMD example.

## Progress

### 2026-11-05

Completed initial audit, supplied-file checks and focused regression run. Confirmed batch association failure, permissive
parsing, missing input readers and the standard-output/CSV time discrepancy. Registered the work and retained unrelated
work. Several older work-register entries have overdue reviews (including vendor integration, installed scripts, release
automation and shared-filesystem validation); they were not resumed or advanced as part of this RORB audit.

### Implementation delivery, 2026-11-05

Implemented the supported reader contract, adapter migration and bounded MCP surface together in draft PR #233.
Validated 420/420 live batch associations and retained the reported one-hour output/CSV discrepancy.
The final local focused command adds `tests/mcp/test_rorb.py tests/mcp/test_server.py` to the audit command above
and uses `--basetemp=C:\Temp\rorb-readers-tests8-20261105`. It passed 103 tests, with one permission skip.
No project files changed, no engine ran, and no merge or release occurred. Next review: 2026-11-12.
