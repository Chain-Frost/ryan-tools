# Batch dashboard standard

Substantial interactive file, record, model or other item batches should normally use the shared dashboard.
A small standalone script need not use one. Routine per-item progress belongs in the display; warnings,
errors, interruptions and the final result must remain readable. Disabled dashboards retain useful normal
logging. This guide is the canonical contract for the shared implementation; wrapper settings and lifecycle
principles remain in [WRAPPER_STANDARD.md](../ryan-scripts/WRAPPER_STANDARD.md).

## Boundaries and imports

```python
from ryan_library.functions.live_dashboard import (
    LiveWorkflowDashboard, WorkflowColumn, WorkflowMetric, validate_dashboard_settings,
)
from ryan_library.functions.dashboard_workflow import (
    WorkflowItemOutcome, WorkflowRunReport, run_dashboard_workflow, run_dashboard_workflow_report,
)
from ryan_library.functions.loguru_helpers import setup_logger
```

`LiveWorkflowDashboard` owns display state, phases, rows, optional aggregate measures and the Rich
lifecycle. The workflow owns discovery, processing policy, resource handling, aggregation, export, and
what constitutes success. `run_dashboard_workflow_report` can run a generic serial or process-pool
batch; do not create a second renderer, global event bus or compulsory orchestrator. All rendering
and `on_outcome` callbacks run in the parent; worker requests and small start events are picklable.

## Configuration, phases and metrics

The constructor takes `title: str`, optional `subtitle: str = ""`, `enabled: bool = True`,
`refresh_per_second: float = 2.0`, `max_rows: int = 25`, optional
`columns: Sequence[WorkflowColumn] | None`, `transient: bool = True` and `screen: bool = False`.
`validate_dashboard_settings(refresh_per_second=..., max_rows=...)` rejects zero, negative,
non-finite refresh values and non-positive/non-integer row counts. Validate effective editable
settings and CLI overrides before side effects; explicit `False` and `0` must not be replaced
using `or`. The renderer bounds visible task rows to both `max_rows` and terminal height,
retains at most `max_rows` completed entries and throttles routine redraws.

Use `set_tasks(labels, metadata=None)`, `mark_running(index=...)`,
`mark_finished(index=..., status=...)`, `set_phase("Processing")`,
`set_metrics([...])` and `finish(outcome=..., detail=...)`. Other meaningful phases include
Discovering, Aggregating and Exporting. A finished input batch is **not** a finished report.
Do not announce SUCCESS before output writing completes. `finish` prints one durable summary
in dashboard mode; with redirected/unsupported-terminal output it is plain text with no
animated frames, ANSI or required input. `enabled=False` does not draw Live or print the
dashboard's final summary; the wrapper/workflow should log the result normally.

`WorkflowMetric(label: str, value: int | float | str | None = None, unit: str = "",
total: int | float | None = None, metadata: str = "")` is an optional aggregate. Pass
`set_metrics([])` to omit all optional measures; these are *not* mandatory file-count,
byte-count, percentage, rate or ETA fields. A workflow may choose workers, completed
runs, tiles, rows, groups, or its own units. `value=None` means unknown, not zero.
`total=None` is an unknown denominator even when the current value is zero. Totals
may be set later; no percentage is inferred. `metadata` on a metric is a short
qualification. Per-item `set_tasks(..., metadata=[...])` and metadata columns are
different from aggregates. The existing `set_extra_metrics(mapping)` and
`WorkflowColumn` interfaces remain available to established consumers.

File size is **not bytes read**. When size metadata is already available at no extra
storage cost, name the measure by its semantics: scheduled input size, known completed
input size, or successful input size. Track unknown sizes explicitly, avoid double
counting retried inputs, and never label a stat snapshot as throughput or instrumented
bytes read. Use numerical byte values and explicit decimal MB (1,000,000 bytes) or
binary MiB (1,048,576 bytes) consistently. No extra file-content reads, stat passes
or network scans are justified solely by optional presentation.

## Item outcomes, failures and cleanup

`run_dashboard_workflow_report(items=..., process_item=..., dashboard=..., pool_size=...,
status_for_result=..., detail_for_result=..., log_queue=None, worker_log_level=None,
max_start_events=None, poll_interval=0.1, on_outcome=None)` returns
`WorkflowRunReport[TResult]`, with one `WorkflowItemOutcome` for each completed input
in original one-based order. Each outcome contains `index`, `status` (OK, SKIP or FAIL),
`result`, `has_result`, and `error`. `has_result=True` distinguishes a legitimate
`None` result from a failed call with no return value. `report.results` preserves
all actual returned values in input order, including domain-specific SKIP/FAIL
results; `report.failures` selects failed outcomes. A worker exception is a FAIL
outcome with an error; a returned domain FAIL is still a result.

`on_outcome` executes in the parent once per completed item and is suitable for
updating inexpensive workflow-selected metrics. The callback must not assume
completion order matches input order. Aggregation and export remain the caller's
responsibility. Unexpected pool, transport, callback and interruption exceptions
propagate after worker/queue/dashboard cleanup, rather than being logged and
turned into a successful partial list. A report with FAIL outcomes must not
be described as overall SUCCESS. An unfinished batch must not fabricate SKIP
or OK outcomes for pending items.

The existing `run_dashboard_workflow(...)` retains its **list return** for callers
that do not need a report. It now re-raises the first unexpected per-item
exception in input order rather than silently dropping that result, so existing
`OSError`/multiprocessing fallback policies remain reachable. Domain FAIL
results remain in the returned list; callers inspect their own statuses.
Use the report API when preserving detailed partial outcomes matters. Neither
API chooses retries or changes domain processing policy. Both accept `pool_size=1`
for serial execution. Workers must be importable under Windows spawn; keep
process-start work under guarded entry points.

## Logging and diagnostics

Use `setup_logger(console_log_level=..., log_file=..., file_log_level=...)`
as the existing single process-owner logging context. The listener independently
filters console and file sinks. Set a concise console level, normally
`SUCCESS` in dashboard mode, instead of forcing `worker_log_level="ERROR"`.
The default `worker_log_level=None` captures the lowest sink level (including
DEBUG when a DEBUG file sink is configured). An explicit worker override which
would drop a configured sink's records is rejected. Workers keep severity,
location and exception tracebacks; warning/error records remain visible
through the listener, during or after a live display. Do not replay all
completed-file INFO records after the dashboard closes.

The parent is responsible for closing the dashboard and logging context, and
for displaying one truthful final status. Run ordinary reporting through all
phases even if item processing finishes; an export failure is FAILED and a
normal no-data run is SKIPPED. Do not treat a suppressed console INFO line
as a missing DEBUG file record. See [LOGGING.md](LOGGING.md) for sink formatting,
worker transport and full diagnostics.

## Reference and acceptance

See [create_log_summary_report.py](../ryan-scripts/TUFLOW-python/log_processing/create_log_summary_report.py)
and [tuflow_logsummary.py](../ryan_library/orchestrators/tuflow/tuflow_logsummary.py).
This reference uses optional Workers metrics, does not stat extra files for
display-only size columns, keeps its dashboard open through export, and returns
a failed outcome on partial processing or export failure. Its copied wrapper
exposes an optional DEBUG file log.

Run focused renderer/adapter tests, the real spawn harness and reference tests
with Python 3.14. Verify serial/on-off parity, warning/error console visibility,
DEBUG file details, interruption, bounded non-terminal output and
export-failure exit status. Native Windows and copied-installed-wheel acceptance
must be recorded separately from synthetic unit or hosted CI checks.
