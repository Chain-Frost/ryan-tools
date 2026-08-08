# Logging guide

`ryan-tools` uses Loguru for maintained library-backed workflows. Top-level wrappers and orchestrators configure
logging; functions and processors emit records without adding or removing sinks.

## Choosing console detail

Use the lowest-volume level that still suits the person or system reading the output:

| Console level | Intended use | Visible records |
| --- | --- | --- |
| `DEBUG` | Developer diagnosis | Debug detail and all higher levels |
| `INFO` | Normal interactive use | Progress, completions, warnings and errors |
| `SUCCESS` | Concise AI/MCP or automation context | Completions, warnings and errors |
| `WARNING` | Exception-oriented monitoring | Warnings and errors only |

`SUCCESS` is the recommended low-context setting for an AI agent or MCP client. Loguru orders `SUCCESS` between
`INFO` and `WARNING`, so it naturally removes routine progress without hiding completion or failure signals.

Keep complete diagnostics outside the AI context by using independent console and file levels:

```python
from loguru import logger

from ryan_library.functions.loguru_helpers import setup_logger

with setup_logger(
    console_log_level="SUCCESS",
    log_file="workflow-debug.log",
    file_log_level="DEBUG",
) as log_queue:
    # Pass log_queue to multiprocessing helpers.
    logger.info("Routine progress is retained only in the file.")
    logger.success("The workflow completed.")
```

Maintained wrappers that use the common CLI accept `--console-log-level SUCCESS`. This is preferable to truncating
captured output after execution because warnings and final status remain visible.

## Jupyter notebooks

Notebook cells can be rerun many times. Use the notebook helper, which replaces existing sinks on every call instead
of accumulating duplicate outputs:

```python
from ryan_library.functions.tuflow.notebook_helpers import init_notebook_logging

init_notebook_logging(
    "SUCCESS",
    log_file="notebook-debug.log",
    file_log_level="DEBUG",
)
```

Call it again whenever the desired level changes. The notebook path is process-local and does not start a listener.
TUFLOW notebook workflows default to serial processing because Windows Jupyter kernels do not provide a reliable
`multiprocessing` entry-point guard. A caller may force parallel execution, but that remains environment-dependent.

Notebook workflow helpers return DataFrames or processor collections directly. An AI integration should inspect
those structured results rather than asking the logger to print complete tables.

## Message policy

- Prefer Loguru parameterized formatting for dynamic values at every level. It produces a fully rendered user-facing
  message while allowing Loguru to avoid message formatting when no configured sink accepts the record:

  ```python
  logger.info("Processing {} files from {}", file_count, source_dir)
  logger.success("Exported {} rows to {}", row_count, output_path)
  logger.exception("Processing failed for {}", input_path)
  ```

- Use a plain string for a static message. Eager f-strings are permitted for existing user-facing calls but are not
  required and provide no visibility benefit. Do not use eager f-strings, percent formatting or `str.format()` for
  `DEBUG` and `TRACE` calls.
- Parameterized formatting does not defer evaluation of Python expressions supplied as arguments. For an expensive
  diagnostic value, pass a callable through Loguru's explicit lazy mode:

  ```python
  logger.opt(lazy=True).debug("Frame profile: {}", lambda: calculate_profile(frame))
  ```

- Whether `SUCCESS` is displayed is controlled by the console or file sink level. Eagerly formatting a success message
  does not force it to be emitted.
- Put counts, paths, decisions, and final outcomes at user-facing levels.
- Put row samples, DataFrame previews, query text, and other high-volume diagnostics at `DEBUG` or write them to an
  output artifact.
- Do not add or remove sinks in reusable functions or processors.
- Use `logger.opt(exception=True)` when a non-`exception()` method must include the active traceback; `exc_info=True`
  is a standard-library logging convention, not the Loguru API.

Run the formatting-policy check after changing logging calls:

```powershell
python repo-scripts/check_loguru_formatting.py
```

The check covers active repository-owned Loguru code, excludes the deprecated `ryan_library/scripts` compatibility
namespace, and ignores modules using the standard-library `logging.Logger` API.

## Multiprocessing behaviour

`setup_logger()` starts one listener for the workflow and returns a queue carrying the lowest producer level required
by any sink. This allows an `INFO` or `SUCCESS` console to coexist with a `DEBUG` file without losing worker debug
records before they reach the file sink.

Use `worker_initializer` as the pool initializer and normally omit its level override:

```python
with setup_logger(console_log_level="INFO", log_file="detail.log") as log_queue:
    with context.Pool(
        processes=workers,
        initializer=worker_initializer,
        initargs=(log_queue,),
    ) as pool:
        pool.map(process_item, items)
```

An explicit worker level is reserved for workflows such as a live dashboard that intentionally suppress worker
messages. Nested multiprocessing logging contexts are rejected because Loguru configuration is process-global.
Shutdown is idempotent and drains the normal queue path before closing the listener.

## Legacy standard-library logging

`ryan_library/functions/logging_helpers.py` remains a compatibility implementation for older standalone RORB code,
reached through the deprecated `misc_functions.setup_logging()` wrapper. Do not use it for new library-backed
workflows and do not combine its handlers with Loguru sinks in the same workflow.
