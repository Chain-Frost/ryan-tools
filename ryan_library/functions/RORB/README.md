# RORB reading and inspection

Use `read_rorb_file(Path(path))` from `ryan_library.functions.RORB.readers` for read-only, source-preserving inspection.
It does not run RORB, follow file references or write outputs. Input is limited to 32 MiB by default; Python callers can
explicitly select a larger `max_bytes`. Decoding tries strict UTF-8 then strict CP1252 with a diagnostic; pass `encoding`
when the source encoding is known. Original decoded lines, source path, encoding and SHA-256 are retained. The
canonical `read_rorb_bytes(data, path=...)` entry point uses the identical parser for an in-memory
snapshot, and `document.original_bytes` / `document.size_bytes` retain byte-exact CRLF, BOM and
encoding. The `Section.source_tokens` rows preserve raw printed numeric tokens (including
Fortran-D exponent forms and trailing zeros); `section.source_decimal(row, column)` provides
their exact finite Decimal values. The existing floating-point `section.rows` values are only
convenience views.

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
| `standard_output` | Labelled metadata/parameters, site mappings, numeric hydrograph summaries and simple subarea rainfall/rainfall-excess tables. Reported calculation increments check hydrograph completeness when present. Both characterised 0..N and CMD 6.44 1..N+1 origins are accepted and retained explicitly as `section.increment_origin`; clocks are never renumbered. A RORBWin banner alone cannot establish engine identity. When a time increment is reported, successive hydrograph times must agree within the printed precision of both the time records and the reported increment. Comparisons use exact source decimals to avoid floating-point tolerance errors; contradictions invalidate the section without changing the source times. Multiple summaries are retained separately but marked invalid for unambiguous analysis until scoped mapping is supported. Routing instructions, other input data and unsupported rainfall layouts remain raw. |
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


## Minimal reader-only distribution

For Python 3.13/3.14 consumers that must not install the full Python-3.14-only
ryan-tools distribution and its data/GIS dependencies, build the focused
`ryan-rorb-readers` wheel from `packaging/rorb-readers` **in a complete
ryan-tools checkout**. The wheel includes only the six canonical RORB reading
modules at their original `ryan_library.functions.RORB` import paths and
the SUL-1.0 licence; it does not copy or maintain a second parser. Do not install
the focused and full distributions together as both provide some same paths.
The wheel is installable without a native RORB executable, pandas or GIS packages.
An isolated consumer pins an exact reviewed Git revision and the subdirectory.


The standard-output reader additionally exposes `reported_scalars` with original
family/unit labels, token, character offset and ordinal. These are **generic
reported fields**, not inferred site or physical-output bindings. Reports that
have an Inc/Time table and corroborating metadata but lack the exact RORBWin
banner can be inspected with a `missing_report_banner` diagnostic. Missing
program version and incomplete site descriptions remain unverified; they do not
rewrite the numeric table or establish an engine identity. Source/print
association and scalar comparison policy are consumer responsibilities.


## Explicit bare hydrograph-table admission

`read_rorb_table_bytes(data, ...)` is an **opt-in** source/evidence
inspection entry point for incomplete native capture fragments containing an
`Inc Time Hyd...` table but no version, banner, timestep or model metadata.
It delegates to `read_rorb_bytes` and the same `output_reader.read_output`
grammar; it never has a separate numeric parser. The returned
`kind="isolated_hydrograph_table"` and `unverified_table_source` diagnostic
mean the caller still owns source fingerprinting, expected site labels and
horizon. The usual `read_rorb_file`/bytes entry points remain fail-closed
for such bare sources. No bare report is assigned an executable identity.


### Explicit companion overrides

`batch_event_key(row)` returns the source-preserving five-part key
`(Run, duration_raw, duration_unit, aep_raw, TPat)`; **Run alone is never
unique**. For a caller-owned nonstandard or ambiguous companion, pass
`associate_batch_csvs(document, overrides={key: Path("explicit.csv")})`.
A relative path is rooted in the batch report's directory; no recursive search
or fallback happens for an explicitly overridden event. Every supplied key
must be present in the batch, every candidate still needs its signed report
reference and peak/site corroboration, and absent or inconsistent overrides
return explicit missing/mismatch rows. Default candidate selection, the
max-runs guard and upstream pandas wrappers retain their behaviour.


Standard `compare_output_csv` requires unambiguous **full site-column coverage** on both the ordinary output and its CSV. An absent/partial site-description legend or additional unmapped CSV hydrograph returns an explicit mismatch with `flows_match=False`; it is never reported as a successful empty comparison. This does not invalidate independently readable numeric tables or infer physical print sites.
