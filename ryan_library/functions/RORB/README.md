# RORB reading and inspection

Use `read_rorb_file(Path(path))` from `ryan_library.functions.RORB.readers` for read-only, source-preserving inspection.
It does not run RORB, follow file references or write outputs. Input is limited to 32 MiB by default; Python callers can
explicitly select a larger `max_bytes`. Decoding tries strict UTF-8 then strict CP1252 with a diagnostic; pass `encoding`
when the source encoding is known. Original decoded lines, source path, encoding and SHA-256 are retained.

```python
from pathlib import Path
from ryan_library.functions.RORB.readers import read_rorb_file

document = read_rorb_file(Path(r"Q:\model\catchment_batch.out"))
print(document.kind, document.status, document.diagnostics)
runs = document.section("runs")
if document.status != "invalid" and runs.valid:
    print(runs.rows[:5])
```

## Interpretation and completeness

| Status | Meaning |
| --- | --- |
| `unsupported` | No supported format structure; filename extension is insufficient evidence. Raw lines remain available. |
| `invalid` | A supported structure contains errors. Retained rows are diagnostic evidence, not valid engineering results. |
| `partial` | Recognized sections can be inspected, but whole-file interpretation or completeness is not established. |
| `ok` | All records in the supported layout were interpreted; this does not validate the model or executable capabilities. |

Each section has its own `valid` and `completeness` values. Rows retain 1-based source line numbers. Diagnostics have
codes, severity, messages and line numbers where applicable. An invalid record never silently becomes a successful
shorter series. Missing and inapplicable parameters are not replaced with zero or a default runoff coefficient.
The complete decoded text is available through `document.lines`, including unsupported records.

## Supported scope

| Kind | Sections and limits |
| --- | --- |
| `batch_output` | Observed RORBWin 6.45/6.52 run-table structure, labelled metadata, parameters, peak/site descriptions and representative-hydrograph references. Requires a recognized table terminator. Percent and EY frequency kinds remain distinct; durations normalize to hours with original value/unit retained. Repeated run numbers across different patterns are allowed; duplicate composite records are errors. Other layouts may be unsupported or invalid. |
| `hydrograph_csv` | Signature and reference preamble, unique `Inc`, `Time (hrs)` and labelled hydrograph columns. Finite values, complete row widths, contiguous increments and increasing times are checked. Source times and increments are preserved. CSV EOF alone cannot prove that a simulation is complete, so status remains partial. |
| `standard_output` | Labelled metadata/parameters, site mappings, numeric hydrograph summaries and simple subarea rainfall/rainfall-excess tables. Reported calculation increments check hydrograph completeness when present. When a time increment is reported, successive hydrograph times must agree within the printed precision of both the time records and the reported increment. Comparisons use exact source decimals to avoid floating-point tolerance errors; contradictions invalidate the section without changing the source times. Multiple summaries are retained separately but marked invalid for unambiguous analysis until scoped mapping is supported. Routing instructions, other input data and unsupported rainfall layouts remain raw. |
| `cmd_parameters` | Column-11 layout described by the installed `RORB_CMD-documentation_V2.pdf`: one BEGIN/END block, labelled paths, lumped/distributed routing, documented output/loss flags and one rainfall burst. Other loss flags, multiple bursts or additional records are partial; malformed records are invalid. |
| `saved_settings` | Observed ARR2016 ensemble saved-settings structure corroborated by 6.45 and 6.52 examples. Returns reference and parameter-pair candidates with `active=None`. Conditional flags and active loss-model interpretation are not validated. Do not use the candidate pairs as active configuration. Other saved-settings layouts remain unsupported. |

The installed CMD V2 document and example were inspected locally; they are not redistributed. The document describes a
specific console implementation based on RORBWin 6.31. CMD files can differ, and executable features can differ from the
desktop application. The [Storm Injector integration documentation](https://csse.com.au/storminjector_online_help/prepare-model-runs.html)
also describes CMD-specific behavior. **A parsed parameter file is not an engine compatibility check.** No executable
is invoked, no executable version is inferred from a parameter file, and unsupported options are not guessed.

## File associations and comparisons

`associate_batch_csvs(document)` from `ryan_library.functions.RORB.associations` checks at most 500 batch rows by default;
Python callers can explicitly raise `max_runs`. The established closure workflow checks its full run table. MCP retains
the 500-row ceiling. It checks
only two observed same-directory filename conventions: batch-stem-prefixed and unprefixed event filenames. It preserves
leading filename spaces. A unique candidate must have a valid signature/table, corresponding output filename in its
preamble, matching site labels and peaks within printed-number rounding tolerance. Results distinguish `corroborated`,
`missing`, `ambiguous` and `mismatch`; no recursive search or arbitrary first-match selection occurs.

Corroboration is not proof of model validity, physical file identity or time origin. A declared output reference may be
stale after a folder move; the check compares its filename and does not follow it. Use `compare_output_csv(output, csv)`
with explicitly selected documents to compare increments, site-labelled flows and times. The supplied 6.45 example has
identical flows but CSV times one hour later than the standard output. The function reports that mismatch without
correcting either source.

## Existing DataFrame callers

`parse_batch_output` and `read_hydrograph_csv` remain in `read_rorb_files.py`, backed by the strict readers. This is an
intentional validation tightening: malformed/unsupported input raises `ValueError`, and file access failures propagate;
callers must handle those failures instead of relying on an empty DataFrame. Batch CSV paths are null when not
corroborated, with a `csv_status` column. Missing or inapplicable IL/CL/ROC/kc/m values are null. The closure orchestrator
rejects missing CSV associations before generating hydrograph jobs, so it cannot silently analyze only the remaining
files. It also raises when a CSV has fewer than two time records or cannot supply uniform, positive time steps; it does
not silently omit that run from closure statistics. CSV DataFrame output still omits `Inc`; use the document API to
retain it. Private filename-construction and row
parsing helpers were replaced by the validated public API.

The two existing closure metrics are unchanged: `analyze_hydrograph` counts above-threshold samples times timestep;
`calculate_closure_times` measures the first-to-last exceedance span, including intervening gaps. Neither is exposed as
an interchangeable MCP metric.

## MCP and follow-up work

`inspect_rorb_file` returns bounded metadata, diagnostics and section counts. Request a section and row page for data;
`section="raw"` inspects numbered source text. Explicit `resolve_csv=True` checks associations, and `comparison_path`
selects a counterpart for output/CSV comparison. See [MCP setup](../../../docs/MCP_SETUP.md).

Further saved-settings variants, CMD variants, scoped multi-block reports, additional rainfall layouts and storm/catchment
readers need documented examples and regression fixtures before semantic support is expanded. The
[work record](../../../docs/work/2026-11-05-rorb-readers-mcp.md) owns audit evidence, acceptance and delivery status.
