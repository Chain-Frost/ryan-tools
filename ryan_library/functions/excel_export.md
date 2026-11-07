# Report export safety

`ExcelExporter` is the shared Excel, Parquet and CSV exporter used by TUFLOW
result workflows and POMM report helpers. Existing Python call signatures,
timestamp names, sheet names, compression and data-dictionary options remain
supported.

## Output policy

An export never replaces an existing report. This includes explicit filenames,
generated names from an earlier run in the same minute, dangling symlinks and
case-insensitive filename matches. Choose another output directory or filename,
or move the previous reports before rerunning. There is no overwrite option.

The exporter validates the complete batch before creating output directories:
matching frame/sheet counts, unique sheet names and columns, valid Excel sheet
names, and distinct physical destinations. Parquet/CSV names are sanitized;
names such as `A/B` and `A B` collide and are rejected before writing.

All selected reports are written into owned temporary directories beside their
destinations before publication begins. A writer failure or interruption leaves
no published reports from that invocation. Temporary directories are removed;
newly created output directories may remain empty.

Publication uses an atomic hard link for each completed file. A file created
concurrently at the destination is preserved. Filesystems without hard-link
support fail with an error; check support on the intended mapped drive or UNC
share before relying on this workflow. Local filesystem tests do not establish
live share compatibility. Parent directories must remain under the caller's
control during execution; this is not a sandbox against hostile directory
replacement.

A multi-file publication is not atomic as a group. If a later publication fails,
earlier completed reports remain. The raised exception and error log identify
those already-published paths. The exporter does not delete published reports
while handling an error. It emits final success messages only after the complete
batch is published.

## Failure reporting

Parquet dependency, conversion and I/O failures propagate to callers, as do
Excel/CSV write and publication failures. This applies to `parquet`, `both` and
the automatic Parquet/CSV fallback for frames exceeding Excel limits. A failed
Parquet export cannot silently produce a successful workflow result.

TUFLOW wrappers translate these failures into exit code 1; workflows with the
shared dashboard show `FAILED`. Keyboard interruption propagates to the wrapper
and retains its interruption handling. Logging configuration remains owned by
the caller.

See the [TUFLOW wrapper guide](../../ryan-scripts/TUFLOW-python/README.md) and
[issue #194 work record](../../docs/work/2026-11-04-result-export-safety-194.md).
