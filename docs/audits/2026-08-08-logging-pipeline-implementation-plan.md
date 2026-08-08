# Logging pipeline implementation plan

Date: 8 August 2026

Status: implemented and validated

Scope: active `ryan_library` logging infrastructure, maintained wrappers, and focused regression coverage

## Purpose

This dated plan replaces `logging_review_tasks.txt` as the actionable description of the remaining Loguru work. The
older checklist is useful historical context, but several of its items now have implementations or tests. This plan
separates confirmed current behaviour from work that still needs implementation and runtime verification.

The baseline below came from a static review of the 8 August 2026 checkout. Tests were not rerun while that baseline
was prepared; the later implementation and fresh validation results are recorded separately below.

## Implementation outcome

The plan was implemented on 8 August 2026:

- Console and file levels are independent. A concise `SUCCESS` console can coexist with a detailed `DEBUG` file,
  including records emitted by worker processes.
- Queue payloads contain a minimal stable record with the original module, function, line, level, message and rendered
  exception traceback. Listener/helper identifiers are not added to forwarded output.
- Multiprocessing contexts reject nesting, drain their normal queue path, unregister their exit callback and shut down
  idempotently. Forced termination emits an explicit record-loss warning.
- Notebook logging is process-local and reconfigurable on every cell run. It supports the same concise-console and
  detailed-file split without starting a listener process.
- Normal diagnostic sinks now use `diagnose=False` so exception output does not expose local variable values by
  default.
- The active-code formatting sweep resolved all 87 violations found at implementation time. The AST policy check now
  distinguishes Loguru from standard-library loggers and excludes the deprecated `ryan_library/scripts` namespace.
- `docs/LOGGING.md`, the shared CLI help and `examples/logging_usage.py` document normal, notebook and low-context
  AI/MCP usage.

### Verified results

- `python repo-scripts/check_loguru_formatting.py`: passed.
- Strict Pyright on every modified production `ryan_library` file and the new logging checker/harness: zero errors.
- Focused logging, notebook, TUFLOW common, Module 11 and CCA tests: 64 passed.
- Module 11 manual harness:
  - `INFO`: 19 output lines, zero debug markers, zero listener/helper identifiers;
  - `DEBUG`: expected detailed output and four NMX debug markers, zero listener/helper identifiers.
- `python repo-scripts/build_library.py --skip-pip`: built
  `dist/ryan_functions-26.8.8.3-py3-none-any.whl`. The first sandboxed isolated-build attempt could not access PyPI;
  the approved network retry succeeded without manual version edits.
- `cmd.exe /C repo-scripts\run_tests.bat`: 677 passed in 43.61 seconds with 85.29% branch coverage, above the 55%
  threshold.

Three older standalone/repository utilities touched only by the mechanical log-call sweep retain unrelated,
pre-existing strict-Pyright diagnostics (`generate_regression_snapshot.py`, `po_timeseries_analysis_runner.py`, and
`rename-msg-files-argparse-folder.py`). Their logging expressions pass the new policy check, and the complete runtime
test suite is clean; broader typing repairs remain separate from this logging implementation.

## Current baseline

| Area | Current state | Remaining concern |
| --- | --- | --- |
| Serial configuration | `configure_serial_logging()` removes existing sinks and installs central console/file formats. | Repeated and nested configuration behaviour is not documented as supported or rejected. |
| Multiprocessing configuration | `LoguruMultiprocessingLogger` starts a listener and routes main/worker records through a queue. | Lifecycle, duplicate-sink, repeated-entry, and shutdown behaviour need stronger regression coverage. |
| Console filtering | Focused tests exercise `INFO`, `DEBUG`, and `WARNING`, including a multiprocessing harness. | The tests need a fresh Windows run and more precise assertions separating serial, main-process, and worker output. |
| File filtering | The listener file sink is configured at `DEBUG`. | Producers currently use the console threshold, so `DEBUG` records can be discarded before reaching a `DEBUG` file sink when the console is `INFO`. |
| Source context | The listener reconstructs `module:function:line` from queued records. | A mocked test covers reconstruction, but a real Loguru record should prove that helper/listener identifiers do not leak or duplicate. |
| Message style | `AGENTS.md` and `examples/logging_usage.py` prefer Loguru parameterized formatting at every level and explicit lazy callables for expensive diagnostics. | The active-code sweep and automated regression check remain incomplete. |
| GeoPackage reads | `ccAProcessor` uses read-only SQLite strategies and has a WAL/SHM regression test. | The focused test should be rerun on local and UNC-compatible paths where available. |
| Workflow harness | The Module 11 harness supports serial, multiprocessing, and threaded NMX processing. | Expected samples have not been recorded in a stable, reviewable form. |

## Goals

1. Make console and file thresholds independent and predictable.
2. Preserve the true originating module/function/line for main-process and worker-process records.
3. Ensure configuration and shutdown cannot silently duplicate sinks, leak processes, or discard queued records.
4. Apply the repository's parameterized message-formatting policy without introducing eager debug or trace logs.
5. Protect these behaviours with focused, deterministic tests and representative Windows workflow checks.
6. Document enough of the contract that wrappers and new orchestrators configure logging consistently.

## Non-goals

- Do not migrate the repository to Python's standard `logging` module or another logging framework as part of this
  work.
- Do not refactor deprecated compatibility namespaces merely to improve their log style.
- Do not change user-visible timestamp, colour, stdout/stderr, rotation, retention, or compression defaults without
  first recording the current output and making the change explicit.
- Do not add tests for unrelated processors or rework raster behaviour.
- Do not combine this work with the broader script-triage roadmap.

## Phase 1: define and test the logging contract

Add focused tests before changing the pipeline. Prefer behavioural assertions over tests that only check
`logger.add()` call counts.

### Required cases

1. **Serial thresholds**
   - `INFO` emits `INFO` and suppresses `DEBUG`.
   - `DEBUG` emits both.
   - `WARNING` suppresses `INFO`.

2. **Multiprocessing thresholds**
   - Assert worker `DEBUG` is absent from an `INFO` console.
   - Assert worker `DEBUG` is present in a `DEBUG` console.
   - Distinguish worker messages from serial setup messages so the test cannot pass solely because the serial portion
     behaved correctly.

3. **Independent console and file thresholds**
   - With console `INFO` and file `DEBUG`, assert worker `DEBUG` is absent from captured console output but present in
     the file.
   - Decide whether the file level should remain fixed at `DEBUG` or become an explicit argument. An explicit
     `file_log_level="DEBUG"` is clearer and easier to test.

4. **Origin context**
   - Use a real queued Loguru record, not only a hand-built dictionary.
   - Assert one and only one origin prefix is rendered.
   - Assert the worker's module/function is shown and `loguru_helpers:listener_process` is not shown.
   - Assert exception type, message, and traceback survive queue transport.

5. **Lifecycle**
   - Enter and exit logging contexts sequentially and confirm messages are not duplicated.
   - Make shutdown idempotent, including the context-manager plus `atexit` path.
   - Confirm the listener exits, the queue feeder is closed, and late or repeated shutdown does not raise.
   - Define nested-context behaviour: either support it deliberately or raise a clear error before replacing an active
     process-wide logger.

### Test structure

- Consolidate overlapping unit coverage in `tests/functions/test_loguru_helpers.py` and
  `tests/functions/test_loguru_helpers_coverage.py` where doing so improves clarity.
- Retain subprocess-based tests for Windows `spawn`; mocks alone cannot validate cross-process formatting or cleanup.
- Give each subprocess a timeout and include stdout/stderr in assertion failures to avoid hanging test runs.
- Use temporary log files and avoid fixed `test.log` paths.

## Phase 2: correct the configuration model

Refactor `ryan_library/functions/loguru_helpers.py` only after the Phase 1 tests express the intended contract.

1. Separate the **producer capture level** from sink thresholds.
   - Producers must enqueue the lowest level required by any active downstream sink.
   - For console `INFO` plus file `DEBUG`, producers must enqueue `DEBUG`; the listener sinks then independently
     filter console and file output.
   - Pass the selected capture level explicitly to every pool initializer instead of relying on the current
     `worker_initializer(..., level="DEBUG")` default.

2. Validate and normalise configured levels at setup time.
   - Fail early with a clear message for an invalid level.
   - Use one internal representation for comparisons and retain normal Loguru level names in public wrapper settings.

3. Make lifecycle ownership explicit.
   - Track whether the context is active and whether shutdown has completed.
   - Unregister or safely neutralise the `atexit` callback after normal context exit.
   - Drain the queue before stopping the listener; if forced termination remains necessary, make possible record loss
     visible rather than silent.
   - Reset `LoggerManager` state consistently if it remains a supported compatibility API.

4. Preserve source context once.
   - Keep the original record's module/function/line through serialization.
   - Avoid depending on a comparison between the Loguru `file` record object and the string
     `"loguru_helpers.py"`; test the actual record shape and compare the appropriate name/path field.
   - Do not suppress legitimate application records merely because they pass through a helper.

5. Centralise sink construction.
   - Reuse shared functions for console and file sink options so serial and multiprocessing modes do not drift.
   - Document why console output uses stdout and confirm this remains compatible with wrapper exit/error handling.

## Phase 3: complete the active-code message sweep

Apply the existing policy consistently:

- Prefer Loguru parameterized formatting for dynamic values at every level, including `SUCCESS` and user-facing
  `EXCEPTION` messages.
- `DEBUG` and `TRACE` messages must not use eager f-strings, percent formatting or `str.format()`.
- Use `logger.opt(lazy=True)` with callables when producing a diagnostic value is itself expensive; parameterized
  formatting alone does not defer Python argument evaluation.

### Scope order

1. `ryan_library/functions/loguru_helpers.py` and active shared logging call sites.
2. `ryan_library/orchestrators/`.
3. `ryan_library/processors/` and active `ryan_library/functions/` modules.
4. Maintained wrappers under `ryan-scripts/`.
5. Standalone scripts only when they are active; exclude vendored code, submodules, deprecated compatibility
   namespaces, generated files, and historical holding areas.

Review each complete modified file, not just matching lines. Confirm that formatting changes do not alter format
specifiers, exception handling, quoting, or path rendering.

### Regression prevention

Add a small AST-based repository check rather than relying on a fragile regular expression. It should report eager
`DEBUG` and `TRACE` calls while allowing:

- constant messages with no interpolation;
- parameterized calls at every level;
- explicit `logger.opt(lazy=True)` calls;
- deliberately pre-rendered strings;
- narrowly documented exceptions.

Start the check against the defined active-code paths. Keep it separate from Black and Pyright so a policy failure
has a clear explanation. Document its invocation in the appropriate repository-maintenance instructions.

## Phase 4: representative workflow verification

After unit coverage passes, run the real paths that motivated the audit.

1. Run the Module 11 logging harness at `INFO` and `DEBUG` in serial and multiprocessing modes.
2. Run the maintained culvert-maximums wrapper against the small synthetic/tutorial dataset where practical.
3. Run the CCA processor's focused SQLite/GeoPackage tests and confirm no `-wal` or `-shm` sidecars remain.
4. If an accessible UNC test location is available, repeat the read-only CCA check there without treating a specific
   network share as a permanent test dependency.
5. Exercise an exception in a worker and confirm the traceback is readable, attributed to the worker call site, and
   emitted once.
6. Exercise two sequential workflows in one interpreter and confirm the second setup does not inherit or duplicate
   sinks from the first.

Capture short, sanitised expected-output samples in this audit or a focused logging document. Include examples for:

- serial `INFO`;
- multiprocessing worker `INFO`;
- suppressed worker `DEBUG` at console `INFO`;
- worker `DEBUG` retained in a debug file;
- a worker exception.

Avoid golden snapshots containing timestamps, process IDs, absolute user paths, ANSI colour codes, or unstable line
numbers. Assertions should target semantic fields and duplication, not the entire rendered line.

## Phase 5: documentation and cleanup

1. Update `examples/logging_usage.py` so its worker initializer receives the intended capture level and its examples
   match the final API.
2. Replace the logging TODO in `AGENTS.md` with the completed, enforceable convention and check command.
3. Add concise public docstrings for new level arguments and lifecycle rules.
4. Retain the short superseded notice in `logging_review_tasks.txt` for historical discoverability, or remove that
   file once references to it have been checked; do not leave two apparently active plans.
5. Decide whether `ryan_library/functions/logging_helpers.py` is a distinct supported standard-library logging path,
   a compatibility path, or dead code. Document that decision before attempting consolidation.

## Additional recommendations

### Safer diagnostic defaults

Review `backtrace=True` and especially `diagnose=True` for normal user-facing sinks. Loguru diagnostics can expose
local variable values in exception output. Prefer a conservative production default, with enhanced diagnostics
enabled explicitly for local debugging, if representative workflows do not depend on the current behaviour.

### Stable structured record transport

Keep queue payloads internal and version the minimal serialized record shape if it is simplified. Pickle is acceptable
only while both producer and listener are trusted processes in the same application; never accept these payloads from
an external or untrusted source.

### Clear ownership boundary

Wrappers and top-level orchestrators should own logger setup. Reusable functions and processors should emit records
but should not remove or replace global sinks. Add a focused check for unexpected `logger.remove()`/`logger.add()` in
active lower-level modules, with explicit allowances for the central helper and genuinely standalone tools.

### Observable forced shutdown

The current timeout/terminate fallback prevents indefinite hangs but may lose records. Record a concise stderr warning
when forced termination occurs, and cover the normal drain path so the warning is exceptional rather than routine.

## Validation commands for implementation

Use a pre-created repository-local pytest temporary directory on Windows. Adjust the exact focused list if tests are
consolidated:

```powershell
New-Item -ItemType Directory -Force .pytest_cache\basetemp_logging_audit | Out-Null
python -m pytest `
  tests/functions/test_loguru_helpers.py `
  tests/functions/test_loguru_helpers_coverage.py `
  tests/test_logging_regression.py `
  tests/processors/tuflow/maximums_1d/test_cca_processor.py `
  --basetemp=.pytest_cache/basetemp_logging_audit `
  -o "cache_dir=.pytest_cache"
```

Run the representative harness separately so its output can be reviewed:

```powershell
python tests/processors/tuflow/module11_logging_harness.py --level INFO
python tests/processors/tuflow/module11_logging_harness.py --level DEBUG
```

For every modified Python file:

1. Run Black using the repository configuration.
2. Run strict Pyright only on the modified files.
3. Run `git diff --check`.
4. Run the focused tests above.
5. Because the implementation changes `ryan_library`, run `python repo-scripts/build_library.py --skip-pip` after
   source validation and inspect Git status for the version bump, rebuilt wheel, and any index changes.
6. Run `cmd.exe /C repo-scripts\run_tests.bat` as the final repository regression check when the complete logging
   change is ready for integration.

Do not stage, commit, or discard unrelated working-tree or submodule changes during validation.

## Completion criteria

The audit is complete when all of the following are demonstrated:

- Console and file thresholds behave independently in serial and multiprocessing modes.
- `INFO` never displays `DEBUG`, while a configured debug file can still retain debug records.
- Main and worker records show the true origin exactly once, with no listener/helper identifier leakage.
- Exceptions retain useful traceback information across the queue.
- Repeated setup/shutdown does not duplicate sinks, leave a listener alive, raise during `atexit`, or silently lose
  records on the normal path.
- Active code prefers parameterized formatting, and debug/trace logs do not eagerly render messages.
- The automated policy check passes on its documented active-code scope.
- Culvert and CCA representative checks pass, with no GeoPackage WAL/SHM sidecars.
- Documentation and examples match the final API and `logging_review_tasks.txt` is not presented as active.

## Suggested implementation sequence

1. Add the missing behavioural tests, including independent file/console levels and lifecycle cases.
2. Correct capture-level routing and idempotent lifecycle handling in `loguru_helpers.py`.
3. Validate real origin/exception rendering and refine listener reconstruction.
4. Complete the active-code message sweep by workflow family.
5. Add and run the AST policy check.
6. Run representative workflows and record sanitised samples.
7. Update the example and agent guidance, then retire the old task list.
8. Build the library and run the final repository regression workflow.
