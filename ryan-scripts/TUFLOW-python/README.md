# TUFLOW Python wrappers

This folder contains maintained, library-backed wrappers, maintained
standalone utilities, and older historical scripts. All maintained entry
points follow the [shared script principles](../README.md#shared-principles-for-maintained-scripts-and-wrappers).
Library-backed wrappers additionally follow
[`WRAPPER_STANDARD.md`](../WRAPPER_STANDARD.md). A useful self-contained
script need not be migrated into a wrapper or an orchestrator merely to remain
maintained; historical/versioned snapshots retain their intentional behaviour.
The TUFLOW runner and its command TXT exports target Windows.

## Input checker reports

The simulation runner reports input findings with the run number, TCF and
event/scenario flags, command-list source, owning control file and physical
line number, full parsed model command, input reference and lookup details.
Paths within each run's working directory are shown relative to that directory.
Launch commands, export destinations, CSV-history locations and configured-file
warnings also use paths relative to the runner working directory where possible.
External paths remain absolute. This presentation does not change subprocess
arguments, working directories or absolute CSV execution-history provenance.
During normal execution, input checking is advisory and runs in a daemon
thread alongside model launches; its findings do not block launches.
`--preflight-only` waits for checking, returns failure for input issues and
never launches TUFLOW. If advisory checking is unfinished at shutdown, the
runner reports it as incomplete rather than waiting or claiming success.
The live dashboard retains an `Input checks` status (Running, Passed, Issues
or Incomplete). At console level INFO or SUCCESS, a completed clean check
prints `Input check passed for N/N runs.` above the live table. Findings are
WARNING messages. Runner log output uses time, level and the message, without
internal function names; single-line source locations show one line number.

The library's default serial logger also supports the live dashboard in older copied
runner wrappers. Launch/completion messages must pass through Rich's redirected stdout
to avoid leaving partial panels in terminal scrollback. See the
[logging guide](../../docs/LOGGING.md#live-dashboards-and-copied-wrappers).
Library upgrades apply to the next Python process, not an active batch. For future
unattended runs, `--no-live-dashboard` selects ordinary console logging.
The dashboard shows event/scenario values in separate `e1`...`e9` and `s1`...`s9`
columns alongside the original run number. The TCF appears once as the heading
of its own boxed run list, with no repeated `Model` column. Batches using multiple
TCFs have separate boxes; repeated basenames include their relative/full paths
to distinguish them. This is presentation grouping only: launch order is unchanged.
`LIVE_MAX_ROWS` remains a global task-row limit across all boxes, and short terminals
may show fewer rows to accommodate headings. Active runs take precedence over
recent completed rows; queued runs remain counted in the batch summary.
Event/scenario values and details stay on one line and use ellipses in narrow
terminals so wrapped cells cannot hide a lower model's active rows.
The `GPU` column shows the group actually assigned at launch, such as `-pu0` or
`-pu0 -pu1`, and retains it after completion. `Auto` means TUFLOW chooses placement;
queued rows say `Pending`, and launch failures say `Not launched`. The dashboard
footer keeps the effective batch flags visible, for example `Batch: -b -t`.
GPU switches embedded in `BATCH_COMMANDS` are excluded because runtime GPU groups
control launches; the startup settings still show the configured value and warning.

GPU names are discovered once for each executing batch using the read-only CUDA driver API.
Startup prints the local launch-environment mapping, for example `-pu0 (NVIDIA GeForce RTX 3090)`;
configured-slot alerts retain the full labels, while task rows show only the assigned
`-puN` identifiers. Identical model names include a PCI bus
identifier where available. Missing drivers or failed discovery leave `name unknown` and do not change
scheduling. Export and preflight do not query hardware. `-puN` uses TUFLOW/CUDA order, which can differ
from NVIDIA `nvidia-smi` and Windows display indexes. The inherited `CUDA_VISIBLE_DEVICES` and
`CUDA_DEVICE_ORDER` affect this mapping; external processes may have different environments.
Older TUFLOW builds may enumerate differently: verify their `.hpc.tlf` device list.
See [TUFLOW's device-selection guidance](https://wiki.tuflow.com/Configure_CUDA_device_selection).

## Live simulation progress

The runner adds `End (h)`, `Model (h)` and `%` to the active run table. These are
simulation hours; `Elapsed` is the real elapsed process time. Percent is
`100 * (model - start) / (end - start)`, so warm-up periods with negative start
times and runs starting after zero are handled correctly. Unknown values are `--`.
The process exit code remains the source of `OK`/`FAIL` and CSV continuation;
reaching the end time in a log never marks a process successful.

The existing `Detail` field also shows the observed run phase: `Initialising`,
`Solving` or `Writing results`. Phase follows records from the current invocation;
the final-writing phase is retained when an older HPC timestep is observed.
It does not add another dashboard column or change the process status.

`SHOW_PROGRESS_DIAGNOSTICS = True` adds compact diagnostic values in the same
field: the latest complete HPC table's `dt` in seconds, and the signed final
cumulative mass error from an explicit `Final Cumulative ME` summary in the main
TLF. These are sampled observations, not estimates. Missing or invalid values
are omitted. A live `CE` value or HPC `Eff` value is not substituted for either
diagnostic. Final mass error is retained with the process exit detail when it
has been observed, including when the process fails.
Set `SHOW_PROGRESS_DIAGNOSTICS = False` or pass `--no-progress-diagnostics` to
hide these two values while retaining phase; `--progress-diagnostics` enables
them explicitly. No ETA or estimated time remaining is calculated.

`MONITOR_PROGRESS = True` enables an advisory background worker when the live
dashboard is enabled. `PROGRESS_POLL_INTERVAL_SECONDS = 10.0` controls the log
read cadence independently of `LIVE_REFRESH_PER_SECOND`. Both can be overridden
with `--monitor-progress` / `--no-monitor-progress` and `--progress-poll-interval`.
Increase the polling interval for a slow network share. Disabling the dashboard
also disables progress observation. Export-only, preflight-only and a fully
completed Resume do not start the monitor.

For each launched run, PyTUFLOW resolves its selected TCF context and supplies
the log folder and output name. Appending `.tlf` preserves decimal points in
names such as `01.0p` without depending on suffix handling in the parser's
`tlf_path()` convenience method. This uses the immutable contributor-fork
PR #63 source selected by the repository installers; it has not been accepted
as an upstream release. TCFs are cached within the monitoring worker and
deferred file resolution avoids repeating GIS input inventory scans. Unresolved
or unsupported predictions produce a warning and leave progress unknown.

The monitor reads bounded header/tail windows of the predicted `.tlf` and its
`.hpc.tlf` companion, skipping unchanged files. It reads model `SIM` / output
times and HPC step-table times, never `Clock` or `CPU` elapsed times. The furthest
verified model time from either source is shown; the companion's final recorded
step can be earlier than the main log's final output. Complete numeric model
start/end times from the TLF take precedence over numeric TCF predictions.
Absolute-date control times are not presented as numeric hours without an
observed hour-based TLF value.

On Windows, log handles request read access with
`FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE`. They open existing files
only and allow compatible writers to continue appending, including writers that
close and reopen the log. The monitor takes no exclusive or byte-range lock, does not use
memory mapping and closes each handle immediately after its bounded read. A
sharing/access error skips the sample; retry occurs at the next normal poll,
without a wait-for-unlock loop. See Microsoft's
[CreateFileW sharing contract](https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-createfilew).
Delete sharing allows a compatible rename of the open source file. It does not
make every replacement operation compatible: legacy Windows `MoveFileExW`
(used by Python `os.replace`) rejects an existing open destination, even with
permissive sharing. Replacement works after the sample's read handle closes.
See the [Windows rename rules](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/87f86c9b-6c2a-4803-84b7-131a74a434fa).
No file reader can guarantee zero storage/network latency or compatibility with
an exclusive writer or every replacement API. Validate the actual solver and SMB
environment using the linked handoff; the scheduler never waits on this I/O.

Log observations must belong to the current invocation. Old, unchanged logs
remain unverified while the new process initializes. Missing logs, sharing
violations, partial writes, replacement and truncation are tolerated. When
concurrent runs predict the same output log, progress is suppressed for the
affected invocations because the writer cannot be attributed safely. A stale
sample shows its log age in `Detail`; this is an observation age, not a stall
diagnosis. Completed rows retain their last observation, which may precede the
final timestep if the process finishes between polls.

All prediction and file reads take place in one daemon worker. The scheduler
does not wait for it before launches, completion callbacks or shutdown. Optional
monitoring cannot change GPU assignments, execution order, exit codes or Resume.
The [local validation handoff](../../docs/TUFLOW_RUNNER_PROGRESS_HANDOFF.md)
records the required Windows/SMB and live-solver checks and supported fixture evidence.

At `INFO` level, startup prints the effective runner settings and the complete
deduplicated simulation plan, including every run's flags, TCF and source location.
This preview is before Resume filtering and does not assign GPU slots; the subsequent
Resume summary identifies how many runs are pending or already successful. The
current wrapper also prints its effective operation, continuation, export, input-check,
logging, pause and follow-up controls after CLI overrides. Use console level `SUCCESS`
to suppress that detailed preview. A complete startup inspection that finds no other
local TUFLOW models logs `No other running local TUFLOW models detected.` at `SUCCESS`;
unknown assignments and incomplete inspection remain warnings.

[`model_management/check_tuflow_inputs.py`](model_management/check_tuflow_inputs.py)
checks every supported model input (including selected GeoPackage layers and
boundary CSV headers), without running TUFLOW. Its terminal report separates the
run plan, control files, input-check totals, numbered issues and final result.
Console paths inside `WORKING_DIR` are shortened relative to that directory;
external paths remain absolute and JSON manifest paths remain absolute.
The script wall time includes library startup, argument parsing, run planning,
input checks and report writing; phase timings describe input-check work only.
Literal `!` characters are ignored everywhere in imported command text and
reported by source line because planned runs and outputs may be wrong after
that lossy interpretation. Configure the straightforward editable settings
near the top: `TCF`, `RUN_FILES`, `WORKING_DIR`, `CONFIGURATION_DIR`,
`REPORT_CSV`, `PLAN_ONLY`, `PLAN_JSON`, `INPUT_SYNTAX`,
`INPUT_ENCODING`, `IO_WORKERS`, `MAX_DIRECTORY_ENTRIES` and
`PAUSE_ON_FINISH`. There is no mandatory `DEFAULT_` prefix or separate
GeoPackage-layer switch.

The initial `RUN_FILES` points to `run_tuflow_simulations.txt` beside the
checker. For a versioned runner, change it to its generated TXT, for example
`(Path("model_v2.txt"),)`, or use `--runs model_v2.txt`.
To create a **new** issues CSV, set `REPORT_CSV = Path("issues.csv")` or pass
`--report issues.csv`; `None` means terminal output only. Existing reports
are never overwritten. Use `--plan-only` for plan inspection without model
file reads, and `--plan-report plan.json` for optional new manifest output.
Command sources selected by `RUN_FILES` or `--runs` are relative to
`WORKING_DIR` (terminal cwd when `None`), even if the invoking terminal
is in another directory. Absolute command-file paths are used unchanged.
A relative configured TCF uses `CONFIGURATION_DIR` (wrapper folder when both
directory settings are `None`).
See the [input-preflight guide](../../docs/TUFLOW_INPUT_PREFLIGHT.md) for supported
BAT/text syntax, multiple TCFs, installation and validation boundaries.

## Simulation runner inputs and launch settings

[`model_management/run_tuflow_simulations.py`](model_management/run_tuflow_simulations.py)
accepts generated parameter combinations, BAT/text command lists, or both.
Edit `MODE`, `RUN_VARIABLES_RAW` and `INPUT_FILES` near the top of a copied
Python wrapper. For `MODE = params.SRC.TEXTFILES`, the entire `TCF` line and
`RUN_VARIABLES_RAW` block may be deleted. Imported commands supply their own
TCFs (explicit command arguments or `SET TCF=...`) and event/scenario flags.
A Python `TCF`, when present, is also a constraint: imported TCFs must match.
Generated modes (`PARAMETER_PRODUCT` and `BOTH`) still require a Python
`TCF` and the `RUN_VARIABLES_RAW` setting. Missing effective TCFs fail planning.
Its operational constants can use concise names; the wrapper standard
does not require a `DEFAULT_` prefix. CLI arguments override editable values
when supplied.

The startup settings print `RUN_VARIABLES` as a Python-style dictionary with
one event/scenario entry per line, preserving input order and values. The
runner colours log levels and message text, including green SUCCESS messages.
Copied wrappers need version `2026-11-03.3` or newer for this console format.
Settings matching current library defaults are grey; changed values and required
paths are cyan. The baseline includes `-b`, LOW priority, minimized windows,
automatic GPU placement and a two-second launch delay; it does not change when
you edit the wrapper. Run controls compare against run/resume, export and input
checks enabled, INFO logging, pause enabled and no follow-up script.

On interruption, the wrapper prints `Batch interrupted.` when `NEXT_RUN_FILE`
is `None`. It mentions that a follow-up was not started only when one was
configured. An interrupted batch never starts the follow-up.

`LIVE_MAX_ROWS = 10` limits the runner dashboard to at most ten task rows
at once, without reducing the number of runs planned or executed. It is
editable or can be overridden using `--live-max-rows N` (positive integer).

`PRIORITY_ORDER` affects only generated combinations: flags listed first vary
more slowly in the Cartesian product. It never reorders imported BAT/TXT
selections. Imported commands retain the order they appear in each source, and
multiple `INPUT_FILES` are consumed in the listed order. Duplicate effective
selections retain only their first occurrence. In `BOTH` mode, imported
selections precede generated combinations.

The dashboard computes separate `e1` through `e9` and `s1` through `s9` columns
from the flags present anywhere in the proposed plan, before Resume filtering.
Only present flags get columns, in event-then-scenario order. Missing values are
blank and the column layout stays fixed throughout the batch.

The shipped `OPERATION = params.OP.RUN` runs pending simulations using
the licensed solver and may overwrite existing model outputs. The default
`CONTINUATION_MODE = params.CONT.RESUME` skips recorded successful runs.
To export editable command TXT without launching, set
`OPERATION = params.OP.EXPORT_ONLY` (or raw `"export_only"`) or pass
`--export-only`. To check inputs without launching, use
`OPERATION = params.OP.PREFLIGHT_ONLY` or `--preflight-only`.
`--run-simulations` explicitly selects execution regardless of the editable
operation setting. CLI arguments override editable values.

The wrapper imports its `StrEnum` choices through `settings`:
`params.OP.RUN`, `params.SRC.TEXTFILES`, `params.CONT.RESUME` and
`params.PRIO.LOW` and `params.LOG.INFO`. Equivalent raw strings are accepted and validated.

Generated list entries intentionally have leading and trailing whitespace
stripped before warnings and deduplication. For example, `[" Wet ", "Wet"]`
selects `"Wet"` once and does not produce a space warning. Embedded spaces
remain part of the value. A whitespace-delimited raw string such as
`"01.0p 02.0p"` represents **two** values and does not warn merely because
of its separator. To supply one value containing an embedded space, use a
list such as `["Wet Season"]`.

After this normalization, individual `-eN` and `-sN` values containing a
space (`" "`) or underscore (`"_"`) trigger a **non-blocking warning**
identifying the flag and value. Imported quoted BAT/CMD/TXT values are
checked as parsed. The warnings do not further change values or block
export or execution.

The Python runner alone launches the configured `TUFLOW_EXE` via Python
subprocess argument lists. Imported BAT/CMD/TXT files are **never executed**,
nor are any START or other source commands replayed. Their supported syntax
is parsed only to obtain model TCF references and event/scenario selections.
Python exclusively controls the executable, child working directory,
`BATCH_COMMANDS`, GPU groups, concurrency and process priority. Imported
executables, shell directory changes, engine/GPU switches and START scheduling
cannot direct launches. First occurrences are kept; duplicate model selections
are discarded. For example, `BATCH_COMMANDS = "-b -t -nlc"` applies to
both generated and imported runs; an imported `-x` does not override it.

`GPU_DEVICES` sets the **maximum configured concurrency**; the GPU availability guard may temporarily reduce active slots: `"-pu1"` assigns one GPU
to one slot; `["-pu0", "-pu1"]` makes two simultaneous single-GPU slots;
`[["-pu0", "-pu1"], "-pu2"]` makes two slots, one using GPUs 0+1 and one
using GPU 2. Legacy nested tuples such as `(("-pu0",), ("-pu1",))`
still work. Each top-level entry is a concurrent slot; each nested list
or tuple is a group of GPUs assigned to that one slot. An empty `[]`, `()` or `None` uses one automatic-placement slot. There is no
`MAXIMUM_CONCURRENT`. GPU tokens must match `-puN` (N is a
nonnegative integer); malformed tokens such as `-pu0,1`, empty
groups and repeated GPU IDs are rejected before export or launch.
By default, `WAIT_FOR_AVAILABLE_GPU = True` checks other **local** TUFLOW
processes before assigning a configured GPU slot. If another run is using
one of that slot's GPUs, the slot is excluded until it becomes available;
other available configured slots continue launching. When all slots are
blocked, no queued model starts and the runner rechecks every
`GPU_CHECK_INTERVAL_SECONDS = 60.0` seconds. A completed owned child
also triggers a fresh check before another model is assigned. A group
such as `[["-pu0", "-pu1"]]` requires **both** GPUs to be free.

Each check runs the local CodeMeter WebAdmin summary request
(`http://127.0.0.1:22352`, half-second request timeout) **in parallel** with a
local TUFLOW process scan. The scan starts speculatively in a daemon thread; if
CodeMeter reports zero use, its result is discarded immediately without waiting.
Unavailable, incomplete or ambiguous CodeMeter responses likewise discard the
scan and continue with a warning. Only confirmed active licence usage permits
the scan result to block an occupied GPU. No allocation-detail or event-history
requests are made. This assumes licensed runs; licence-free/tutorial runs are
not protected when CodeMeter reports zero usage.

The complete optional check has a **two-second budget**, including worker startup
and CodeMeter. Both tasks run inside an owned hidden Python subprocess. The
parent enforces a 1.9-second worker timeout and reserves 0.1 seconds for
termination and cleanup. Unfinished inspection is killed and reaped; partial
results are discarded and launches continue with a warning. The daemon thread
cannot outlive its owning subprocess. Busy machines may therefore continue
without verified GPU occupancy.

If the CodeMeter check fails, open `http://127.0.0.1:22352` to check WebAdmin.
On Windows, open Services (`services.msc`) and start **CmWebAdmin** if stopped;
its exact service name is **CmWebAdmin.exe**. Set its Startup type to **Automatic**
if WebAdmin should start after reboot. This is separate from the CodeMeter licence
runtime service. The warning includes these steps; launches continue without trusting the
speculative GPU scan while usage is unverified.

Process inspection uses running TUFLOW process command lines (`-puN`) and excludes
children already tracked by this runner. Before comparing ordinals, it requires exact matching
`CUDA_VISIBLE_DEVICES` and `CUDA_DEVICE_ORDER` settings in the external and runner environments.
Different settings or denied environment access make the assignment unknown and produce a warning;
computation continues without a verified availability check. the guard never assumes an unreadable environment uses defaults. It does not translate
ordinals across different CUDA environments. Unrelated environment values are never logged. CodeMeter licence allocations
do not reliably identify the local GPU number; this is **GPU occupancy**
protection, not a guarantee that a separate CodeMeter licence is available.
Unknown GPU assignments, incomplete inspection or inspection failures warn and continue.
Only confirmed overlapping assignments block a slot. A failed helper must not stall computation;
continuing under uncertainty can share a GPU or cause a solver failure. The dashboard shows a yellow
`GPU INSPECTION WARNING - CONTINUING` alert and keeps unknown availability explicit. Ctrl+C interrupts the wait. To revert to warning-only startup
inspection use `WAIT_FOR_AVAILABLE_GPU = False` or
`--no-wait-for-available-gpu`. Override the polling interval with
`--gpu-check-interval 60` (positive seconds). Automatic placement
(`GPU_DEVICES = []`) cannot be mapped to a specific GPU: the runner warns
and retains the original one-slot behaviour. Export-only and preflight-only
do not perform this waiting or polling.

When a configured group becomes unavailable, the runner emits one prominent
`ERROR` for that state transition. It reports confirmed external PIDs where
known, distinguishes unknown/incomplete/query-failure states, shows whether
other groups can continue and gives the next check interval. Identical polling
results produce only lower-severity status updates. The live dashboard keeps a
red alert panel above its normal summary and task rows until the restriction
clears; recovery emits a `SUCCESS` message. GPU restrictions do not mark a
simulation failed. See the licensed two-GPU procedure, benchmark and measured
CodeMeter limitations in
[`docs/TUFLOW_GPU_CODEMETER_VALIDATION.md`](../../docs/TUFLOW_GPU_CODEMETER_VALIDATION.md).

The wrapper rejects malformed settings with explicit names and examples:
unsupported enum choices, invalid path types/extensions, blank or
malformed event/scenario values, duplicate GPU IDs, invalid booleans,
non-finite/out-of-range numeric settings and invalid process priorities.
It also checks every `~eN~` or `~sN~` TCF filename placeholder against
selected event/scenario flags, including **imported BAT/TXT runs**.
Valid extra flags not present in the TCF name are allowed.

A TCF must be provided through `TCF` or the imported BAT/TXT selection.
Missing physical TCF files do **not** stop planning or export, but produce
warnings. The configured `TUFLOW_EXE` and optional `NEXT_RUN_FILE` also
produce warnings if absent. The solver is a hard requirement only for
execution that needs it; preflight-only and export-only do not require
the solver. Missing input files selected for BAT/TXT parsing remain hard
errors. Strict preflight checks referenced model inputs and fails on
unresolved files; normal-run background checking remains advisory.

Path settings `WORKING_DIR`, `TCF`, `TUFLOW_EXE` and
`NEXT_RUN_FILE` accept ordinary raw strings (for example,
`r"Q:\Project\TUFLOW"`) or `Path` values, and
`INPUT_FILES` accepts a single path or a list/tuple of paths
(for example, `["runs1.bat", "runs2.txt"]`). They are converted to
`Path` objects at the wrapper boundary. Input TCF, executable,
BAT/TXT and follow-up paths retain their existing relative-to-
`WORKING_DIR` semantics; nothing resolves mapped drives or UNC
aliases.

The sole runner input-check switch is `CHECK_INPUTS` (`True`/`False`),
overridden by `--preflight` or `--no-preflight`. `True` performs all
supported advisory checks, including selected GeoPackage layers and boundary
CSV headers. Failures are reported while launching; they do not block the
solver. Strict `--preflight-only` always checks those inputs and never launches.
There is no separate GeoPackage-layer toggle in the runner.

Both the checker and runner capture inherited environment variables before
reading BAT/text files. Each source file then has independent SET and directory
state. Set Python `TCF = None` when imported commands intentionally select
multiple TCFs. If Python supplies a TCF, every active imported TCF must match it;
conflicts stop planning before output or execution.

The Python runner and its generated TXT support UNC working directories.
Generated and imported paths compare consistently when they contain `..`;
comparison is lexical and does not resolve drive mappings or filesystem links.

## Fixed TXT command export

Command export remains enabled by default. A copied `<runner-name>.py` refreshes
**`<runner-name>.txt` beside that Python file**. The file contains editable Windows
batch syntax, including shared `TUFLOW_EXE`, `BCMD` and TCF variables, but its
`.txt` extension prevents accidental batch execution by double-clicking it.
There is **no alternate export path**: `<runner-name>.txt` beside the copied
Python wrapper is the sole destination. Existing TXT is refreshed after
reparsing and protected-file checks; input files are never replaced.

The export contains the complete deduplicated plan, including completed runs, and
is refreshed even when Resume has no remaining work. There are no timestamped
exports, automatic backup files or per-invocation directories. If manually
editing an export, copy it to a separately named input before another invocation
refreshes the generated file.

The complete export is rendered and reparsed before the destination is replaced.
A failed render leaves the existing export intact. Imported command sources,
model control files, the Python wrapper and execution history are protected,
including filesystem aliases. If an imported source is also the export target,
disable export with `EXPORT_COMMANDS = False` (or `--no-export`)
when running, or copy the input to a different filename. Export-only always
writes the fixed adjacent TXT and refuses a conflict.

`--export-only` prepares the text without launching TUFLOW, inspecting GPUs,
creating execution history or requiring the unused PyTUFLOW validation stack.
An explicit Python `BATCH_COMMANDS` GPU switch is retained in the exported
`BCMD`; actual runner launches use `GPU_DEVICES`. Export does not predict or
assign runtime GPU slots.

## Runner CSV continuation

The execution-history CSV uses the supplying Python wrapper's filename
stem followed by `_runs.csv`, **beside that wrapper**. The intended workflow
is one runner file per TUFLOW project/TCF version, with a unique runner filename
for each version; therefore each wrapper has its **own** CSV, rather than sharing
a project-wide or network-wide history. The CSV is an append-only START / FINISH
/ RESET log of observed process outcomes. No saved run manifest or additional
session directory is required. Each `recorded_at` value uses the local computer's
timezone and includes its UTC offset; existing timezone-aware UTC rows remain valid.
New CSVs place operator-facing fields first: timestamp, event, outcome, return code,
TCF, PID and error. Command context follows, while run hashes and schema metadata are
last. Existing CSVs retain their original column order and remain readable and appendable.

**Resume is the default.** The runner regenerates the full current plan and
selects its launch queue using the CSV:

| Latest recorded state for the current configuration | Resume action |
| --- | --- |
| Observed successful finish | Skip this occurrence for this invocation. |
| Failed finish | Queue another attempt. |
| Start without an observed finish | Queue another attempt. |
| No previous attempt | Queue the run. |

The unique selected plan and previous CSV rows remain intact. For a 400-run batch with the
first 100 successful, the next 3 failed and the rest unstarted, Resume queues
runs 101 through 400 in their original order. If later runs also succeeded,
those are skipped too; original run numbers can therefore have gaps. Input
preflight and dashboard presentation preserve those original identities.

Set `CONTINUATION_MODE = params.CONT.ALL` (or raw `"all"`) or pass `--run-mode all` to append a RESET row and
run every occurrence again. Previous history is retained. Changing model-file contents
is not detected; use Run all when prior successes should be disregarded.
The identity uses the **TCF filename** (case-insensitive on Windows) and effective
event/scenario, engine and model-option switches, excluding runtime GPU placement.
The TCF directory, Python working directory, executable location and source
command file are diagnostic CSV fields, **not** part of the completion key.
No mapped-drive/UNC resolution or cross-project path matching is performed for
Resume. TCF filenames are expected to be unique within the versioned runner's
project. A test-only `-t` run remains distinct from a full simulation.

The Resume key uses the path-independent `tuflow-logical-v2` identity.
Old pre-release `v1` records, if the **same wrapper filename** is reused, remain
in its CSV but do not match v2: previous successes may be queued again. New
versioned wrapper filenames naturally have separate CSVs. Changing ignored
source engine/GPU switches does not change the key; changing effective Python
`BATCH_COMMANDS` switches still can.

Stop the previous batch before resuming. A missing FINISH row does not establish
whether an old process is still alive or failed; Resume does not scan to recover
it. Only an incomplete trailing CSV fragment is repaired. Incompatible formats
or earlier corruption produce an error rather than an implicit migration.

Actual execution first checks that the Python-configured solver exists. A
missing executable fails before changing the CSV completion baseline. Export-only
and preflight-only do not require a solver installation. The solver path and
working directory come exclusively from Python for execution, but they are not
part of the new v2 Resume identity.

Reexecuted models use their current configured output locations and may overwrite
existing solver outputs. This feature resumes the set of simulations; it does
not use TUFLOW's solver checkpoint restart. An already-complete Resume performs
no GPU scan or launch and retains any explicitly configured follow-up policy.

The [integration work record](../../docs/work/2026-10-23-tuflow-runner-model-checks.md)
records delivery and current validation.

## Maintained wrappers

The maintained wrappers delegate processing to `ryan_library` orchestrators and
keep editable project paths, globs, filters, output modes and thresholds near
the top of each file. This includes the log-summary, POMM/PO combination,
culvert reporting, time-series checking, result styling and ASC_to_ASC search
wrappers.

The experimental [grouped raster statistics](../unsorted-python/run_grouped_asc_to_asc_statistics.py) and
[Python raster calculations](../unsorted-python/run_python_raster_calculations.py) entry points have returned to
`unsorted-python`; they are not maintained TUFLOW
wrappers, packaged scripts or MCP workflows. The maintained native implementation's supported command mapping, NoData
policies, closest-source mean, upper median and optional executable-comparison boundary are documented in
[`../../docs/ASC_TO_ASC_NATIVE_OPERATIONS.md`](../../docs/ASC_TO_ASC_NATIVE_OPERATIONS.md).

`raster_processing/run_asc_to_asc_mean_then_maximum.py` and
`raster_processing/run_asc_to_asc_median_then_maximum.py` share one generic
statistic-then-maximum orchestrator. They aggregate temporal patterns for each
duration, then take the maximum across durations. With `--source`, the final
source legend resolves directly to the original temporal-pattern rasters.
The mean wrapper defaults to ASC_to_ASC selection: its value raster contains
the arithmetic mean and its source identifies the lowest contributing TP value
at or above the mean. `MEAN_VALUE_METHOD` or `--mean-value-method` can instead
select the closest contributing value or retain the arithmetic mean with
closest-source provenance. Median uses the upper middle contributing value
when the TP count is even.

[`plot_water_level_profiles.py`](po_and_timeseries/plot_water_level_profiles.py) creates terrain
and TUFLOW water-level profile PNGs along GeoPackage lines.  CLI arguments can override paths and profile settings. The workflow validates CRS metadata and
requires exactly one result raster for every requested AEP. Missing CRS metadata
is inferred from any tagged profile or raster input; when every input is
untagged, their source coordinates are assumed to already align.
The default masked-bilinear sampler returns NoData only when the profile
station's containing raster cell is NoData; adjacent NoData cells are excluded
and the remaining interpolation weights are renormalised.

Only the single-layer GeoPackage renamer,
[`gis_processing/rename_geopackage_layer_to_filename.py`](gis_processing/rename_geopackage_layer_to_filename.py),
prints GeoPackage paths relative to its scan root (the copied script's folder,
or an explicitly configured target directory). Nested files retain their
relative subfolder names; full paths are still used for file operations.
Green denotes a successful layer rename, yellow a skipped file (already
correct or multiple layers), and red a failed inspection or rename.

## Wrapper folders

| Folder | Purpose |
| --- | --- |
| `culvert_results` | Combine and summarise TUFLOW culvert maximum and time-series results. |
| `gis_processing` | Process GeoPackage layers, network layers and QGIS result styles. |
| `log_processing` | Parse TUFLOW logs and maintain log-summary workbooks. |
| `model_management` | Launch simulations and collect or clean model result files. |
| `po_and_timeseries` | Process PO and POMM outputs, check time series and plot profiles. |
| `raster_processing` | Run ASC_to_ASC and native-Python raster calculations. |

Only imports needed to construct or annotate those editable values, such as
`Path` and `Literal`, appear before the settings. Operational third-party and
`ryan_library` imports follow the editable block so users see configuration
before implementation wiring.

Each maintained wrapper embeds a `WRAPPER_VERSION` that remains available when
the file is copied into a job folder. At both the start and end of processing it
prints:

```python
print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
```

The closing call sets `leading_blank_line=True` so the completion banner is
visually separated from processing output without repeating three statements
in every wrapper.

The wrapper version identifies the copied entry-point file; the separately
installed `ryan-tools` version identifies the shared implementation.
Interactive completion uses `pause_console()`, which collects unreachable
Python objects immediately before waiting. Redirected/headless runs do not
pause, and `--no-pause` explicitly disables the wait for automation.

## Shared command-line arguments

Common `argparse` declarations live in
`ryan_library.functions.wrapper_utils` so wrappers do not repeat boilerplate:

| Helper | Arguments added |
| --- | --- |
| `add_execution_cli_arguments()` | `--working-directory`, `--console-log-level`, `--no-pause` |
| `add_filter_cli_arguments()` | `--data-types`, `--locations` |
| `add_live_dashboard_cli_arguments()` | Dashboard enable/disable, refresh rate and maximum rows |
| `add_common_cli_arguments()` | All three groups above |
| `add_export_mode_cli_argument()` | `--export-mode {excel,parquet,both}` |

`parse_common_cli_arguments()` converts the resulting `argparse.Namespace` to
the typed `CommonWrapperOptions` object. A wrapper should select only the groups
its workflow supports, then declare specialised options locally. For example,
ASC_to_ASC worker, dry-run and strictness options remain in those wrappers
because their meaning and help text are workflow-specific.

Run any maintained wrapper with `--help` for its current arguments. For a
headless invocation, include `--no-pause`:

```powershell
py -3.14 .\log_processing\create_log_summary_report.py --working-directory "D:\Model\results" --no-pause
py -3.14 .\po_and_timeseries\combine_pomm_results.py --export-mode parquet --no-pause
```

## Check recent TUFLOW user activity

[`log_processing/check_tuflow_user_activity.py`](log_processing/check_tuflow_user_activity.py) is a read-only,
library-backed utility that checks each user folder below `%ProgramData%\TUFLOW\log`. It identifies distinct
simulation starts, including overlapping runs, instead of relying on the latest event for each user. The default
lookback is 24 hours, and the maximum searchable history is seven days (`--recent-hours 168`).

```powershell
py -3.14 .\ryan-scripts\TUFLOW-python\log_processing\check_tuflow_user_activity.py
py -3.14 .\ryan-scripts\TUFLOW-python\log_processing\check_tuflow_user_activity.py --recent-hours 48
```

For each start, the utility reads the quoted `.tlf` with the maintained TUFLOW parser. A valid completion footer is
labelled `COMPLETED`; an accessible TLF without that footer is `POSSIBLY RUNNING`; missing or inaccessible TLFs remain
`UNKNOWN`. Global-log `Interrupted` and `UNSTABLE` records are recorded as **distinct terminal states**, not
successful completions or potentially active runs, even if the TLF remains incomplete. A separate terminations table
shows their status and any recorded elapsed runtime. Each TLF is assessed independently, so one model finishing does
not imply that another model owned by the same user has stopped. The tables include the computer, licence seat and parsed GPU device IDs. The simulation-starts
table displays both AGE (elapsed time since the run started) and RUNTIME (the measured duration of a completed run).
RUNTIME comes from its matching completion event where available, otherwise the completed TLF; interrupted or
unstable runs show their reported terminal elapsed time if one was logged; still-running runs show `-`.
Matching completions retain their own runtime column, and a final details section prints the full inferred TUFLOW
project folder and TLF path. Use `--log-root` to inspect a non-default copy of the licence-log tree.

### Model progress for running simulations

For a run matched to an active CodeMeter allocation, or identified as possibly running by its TLF when CodeMeter
is unavailable, the report reads the companion **`.tsf`** summary beside its `.tlf`. Both the
**ACTIVE CODEMETER TUFLOW PROCESSES** and **SELECTED SIMULATION STARTS** tables display:

- **SIM TIME** — last reported simulation time, in model hours (e.g. `21.500h`).
- **SIM END** — configured simulation end time, in model hours (e.g. `48.000h`).
- **PROGRESS** — percentage complete reported by TUFLOW (e.g. `44.8%`).

These are *simulated* hours, not actual hours elapsed or a prediction of the wall-clock finish time. The values are
a read-only snapshot of the last TSF update, not a continuously advancing clock. If the TSF is missing, incomplete,
contains invalid time values, belongs to a different model or predates the start, the cells show `-`.
Completions, interruptions and unstable runs never show active model progress, even if an old TSF still says
`Started`. No TCF parsing or guessed simulation end time is used.

The default console report shows active simulations, every start since the oldest active simulation, and two earlier
starts as context. It shows the matching completed/interrupted/unstable terminal records and paths only. If no active simulation can be matched,
it shows the three latest starts. A run may predate the usual 24-hour window; the utility checks available logs
up to seven days back to find its start and predecessors. `POSSIBLY RUNNING` from a TLF is not proof of an active
CodeMeter allocation. Use `--all-history` to restore the full output for the configured `--recent-hours` period.

The `MODEL` column displays the full filename rather than truncating it at 48 characters. The project folder and
TLF paths in the final section can be opened from terminal hyperlinks. Windows Terminal receives OSC 8 links
automatically where supported; VS Code's terminal can usually Ctrl+click the plain Windows paths through its native
file-link detection. Use `--hyperlinks` to force OSC 8 links, or `--no-hyperlinks` to print plain paths.
GPU cells explicitly show TUFLOW switches, for example `-pu1 (NVIDIA GeForce RTX 3080)`.
Names come only from that simulation's companion `.hpc.tlf` device header, never from the current
computer's hardware or CodeMeter allocation counts. Missing, unreadable, conflicting or stale
companions retain `-puN (name unknown)`. The companion must have been updated since the recorded simulation start. If the start timestamp
is missing, companion and main TLF modification times must be within two minutes instead. The report retains each record's machine alongside the GPU label.

The report prints inferred log and TLF activity first, followed by active CodeMeter processes, and finishes with the
authoritative current CodeMeter licence-utilisation table.
Redirected output and other non-OSC terminals get plain readable paths by default.

By default, the wrapper also reads local CodeMeter WebAdmin at `http://localhost:22352`. It reports current TUFLOW
product utilisation, each active user's process ID, first and last access, licence allocation handles, recent
no-more-licences events, and the recent simulation whose start time most closely matches each active process. Known
TUFLOW product codes are 20 (Local), 21 (Local Threads), 22 (Local Multiple Domains), and 26 (Local GPU). Any product
code not recognised by the Python mapping is shown as a warning instead of being silently guessed. Use
`--no-codemeter` when WebAdmin is unavailable or `--codemeter-url` to select another local endpoint.

CodeMeter quantities, allocations, and denial events are filtered to TUFLOW Firm Code `101139`. The report
uses a live WebAdmin snapshot and the existing event log; **CodeMeter License Tracking does not need to be
activated**, and this utility creates no tracking database or background service. Events older than WebAdmin's
retained history may not be available.

The reusable Python function `fetch_codemeter_snapshot()` also accepts `firm_code`, `product_codes`, and
`product_names` for other vendors. Product codes are scoped by firm; when different firms share a product code,
unattributable quantity rows are suppressed with a warning rather than reported against the wrong vendor.

## POMM peak wrapper migration

The maintained mean and median POMM peak wrappers use
`ryan_library.orchestrators.tuflow.pomm_max_items` directly. The deprecated
`PommPeakWrapperDefaults`, `PeakReportExporter` and `run_pomm_peak_report_wrapper`
helpers were removed from `ryan_library.functions.wrapper_utils` on 18 October
2026. Older copied wrappers using these helpers must be replaced by maintained
versions under `ryan-scripts/TUFLOW-python` before upgrading.

## Verify converted FLT/ASC output rasters

The tool reports the embedded wrapper revision and the installed `ryan-tools`
package version at startup and completion. It also displays total source data
size, overall file-count progress, processed source volume, active source and
GeoTIFF sizes, and the current comparison. Live counters track verified matches,
CRS warnings, newly created TIFFs, and excluded files. Non-interactive output
reports each file's relative path, sizes, result, and whether its GeoTIFF was
created in the current run. The completion summary distinguishes verified
existing GeoTIFFs from newly generated and verified GeoTIFFs. Batch sizes are
on-disk source file sizes, **not bytes actually read or written**. Use
`--no-pause` for scheduled or unattended execution.


Use [`model_management/verify_converted_tuflow_rasters.py`](model_management/verify_converted_tuflow_rasters.py)
to optionally **convert -> verify -> delete** legacy TUFLOW `.flt` / `.asc` outputs in a single run.
Existing same-stem `.tif` / `.tiff` files are checked rather than overwritten. With
`--convert-missing`, any absent GeoTIFF is generated via the existing
`ryan_library.functions.gdal.raster_processing.translate_to_geotiff()` using the
lossless `tuflow`/DEFLATE profile, without creating unnecessary overviews.
The independent Rasterio comparison runs before any source deletion.
This standalone utility uses Rasterio and NumPy (both included in the project dependencies).
It reuses `ryan_library.functions.file_verification` for file snapshots and
symlink/junction checks. Its semantic raster comparison remains local: only
valid-cell values and NoData-mask locations must agree, not NoData sentinel
values or entire decoded byte sequences.

- Recursively searches below `ROOT_DIR` (default: current working directory).
  By default the TIF must be in the same directory. Set `TIF_ROOT_DIR` or `--tif-root`
  when converted GeoTIFFs are in a parallel folder tree with the same relative directory layout.
  With `--convert-missing`, missing child directories in an existing TIFF root are created as needed.
- **Optional creation:** `--convert-missing` (or `CONVERT_MISSING = True`) converts
  only when neither `.tif` nor `.tiff` is found; existing/stale/mismatching TIFFs
  are never overwritten, and ambiguous counterparts block conversion. GDAL writes
  into a temporary sibling file before publishing a completed TIFF, with
  exclusive-create fallback for network filesystems that cannot create hard links.
  Conversion failures and exact-comparison failures leave the source intact.
- Compares width, height, band count, **exact grid origin, cell dimensions and full
  geotransform**, NoData validity masks and every valid cell without any tolerance
  or resampling. Raster band scale/offset metadata must also match, because it
  changes the interpretation of stored pixel values. Different NoData sentinels
  are allowed only when their invalid cells coincide. These grid and pixel
  properties are critical, non-regenerable content.
- **If the source declares a CRS, the GeoTIFF must declare the same CRS.** For example,
  source EPSG:28350 and GeoTIFF EPSG:28351 is a blocking mismatch. Strict CRS
  equality is intentional: OGC:CRS84 in a source PRJ versus EPSG:4326 in a GeoTIFF
  also fails (axis-order differences are not ignored). A GeoTIFF without
  CRS when its source declares one is also a blocking mismatch. If the *source*
  lacks CRS metadata (common in legacy FLT/ASC outputs without PRJ files), matching
  pixels and geometry remain eligible under `MATCH_CRS_UNVERIFIED`, with a warning
  that the spatial reference cannot be independently verified.
- Computed statistics tags and regenerable auxiliary-statistics sidecars are
  **not compared** and cannot invalidate matching pixel and grid content. The tool
  deliberately does not require byte-identical FLT/ASC and TIFF files.
- The GeoTIFF must have a **strictly newer modification timestamp** than the source;
  for FLT files also newer than the paired `.hdr` header when scheduled for deletion.
- Paths are converted to non-resolving absolute paths, not `resolve()`, and are rejected if
  any component is a symbolic link or Windows directory junction. Nested linked folders are
  not scanned and linked paths are rechecked before deletion.
- Uses chunked raster reads and a Rich progress indicator, not binary file hashes.
- Creates non-overwriting `tuflow_raster_conversion_delete_candidates.csv`,
  `tuflow_raster_conversion_errors.csv`, and `tuflow_raster_conversion_warnings.csv`
  in the source root. Missing-CRS cases appear in the warning CSV and are clearly marked
  in the deletion candidates (status and warning columns), **not** in the errors CSV.
  True mismatches are excluded and listed in the errors CSV. No per-raster placeholders.
- **Default is validation only.** Use `--convert-missing --delete` to convert,
  verify and clean up in one invocation. Add `--delete` or set `DELETE_FILES = True`,
  then type exactly `DELETE` to remove confirmed matches. Before each removal it checks
  source, GeoTIFF and associated header size and modification timestamps again.
  This protects against ordinary changes, but does not lock files against concurrent writes.
- For verified FLT files, the associated `.hdr` is included by default. The `.prj`,
  `.aux.xml` and other sidecars are retained, as they can contain reusable metadata.
  Set `DELETE_FLT_HEADERS = False` to retain FLT headers.

Example from PowerShell (run verification before considering deletion):

```powershell
py -3.14 .\ryan-scripts\TUFLOW-python\model_management\verify_converted_tuflow_rasters.py --root "D:\Models\results"
py -3.14 .\ryan-scripts\TUFLOW-python\model_management\verify_converted_tuflow_rasters.py --root "D:\Models\results" --convert-missing --delete
```

Check the candidate list, back up the source rasters and make sure model/QGIS references have
been updated to use the GeoTIFFs. The tool does not modify references, nor does it delete
unmatched files. This is an exact-value/georeferencing check rather than a guarantee that the
converted files preserve every possible GDAL metadata tag.

## Duplicate check-file cleanup

`model_management/delete_duplicate_check_files.py` removes byte-identical files within TUFLOW
folders named `check` or `checks` (case-insensitive), retaining one copy per unique content group. Run from the intended result/model root
or change `ROOT_DIR` in the script.

- **Three run modes (set the constants at the top of the Python script):**
  1. **Fresh scan, default:** `RESTORE_FROM_MANIFEST = False`, `APPLY_PROPOSAL_MANIFEST = False`.
     Discover `check`/`checks` files, hash only duplicate-sized candidates, and create a proposal CSV
     and TXT report. `DRY_RUN = True` creates evidence but deletes nothing. With the default
     `DRY_RUN = False`, deletion still requires typing `DELETE`.
  2. **Apply a saved proposal:** set `APPLY_PROPOSAL_MANIFEST = True` and change
     `PROPOSAL_MANIFEST_FILE` to the **exact path of the existing proposal**, including any numbered
     suffix (e.g. `duplicate_files_proposed_2026-10-28_001.csv`). This skips all folder scanning and
     SHA-256 hashing, checks listed paths, filenames, sizes and *original scan modification times*,
     then creates a new report and requests `DELETE` (unless `DRY_RUN = True`). The original content
     hashes are **trusted, not reverified**; timestamp/size checks are not equivalent to rehashing.
     Never apply a proposal that is untrusted or was prepared while files were being modified.
     Multiple proposal rows referring to the same retained copy must agree on its content hash,
     size, suffix and modification time; contradictory rows are rejected before any deletion.
     The enhanced proposal format is required; old six-column proposal CSVs must be regenerated.
  3. **Restore deleted files:** set `RESTORE_FROM_MANIFEST = True` and `MANIFEST_FILE` to the
     corresponding dated recovery CSV. With `DRY_RUN = False`, type `RESTORE`; existing files
     are never overwritten.

  **Do not enable both apply and restore mode at once.** The script refuses to run when both
  switches are `True`, before reading manifests or changing files. Reset both switches to
  `False` before a fresh scan.
- **Default `COMPARE_ACROSS_CHECK_FOLDERS = True`:** keep one copy of each content group across all scanned
  folders. With `False`, keep one representative in each check folder instead.
- **Default `INCLUDE_RASTER_FILES = True`:** GeoTIFFs and auxiliary files are included. Set `False` to
  exclude them. Identical bytes do **not** mean the original file paths are safe to remove: linked QGIS
  projects or scripts may stop finding their inputs.
- Report, proposal and recovery-manifest filenames default to the **local run date** in `YYYY-MM-DD`
  format, for example `duplicate_files_to_delete_2026-10-28.txt`,
  `duplicate_files_proposed_2026-10-28.csv` and `duplicate_files_manifest_2026-10-28.csv`.
  Existing proposal/report names are never overwritten: same-day reruns use numbered filenames
  (for example `duplicate_files_proposed_2026-10-28_001.csv`). Review the keep/delete choices
  before confirming. The three output path constants can still be edited to use custom filenames.
- The console and TXT report show the **total size of all scanned check-folder files**,
  **proposed deletion size in decimal GB and percent of that total**, and **estimated GB and percent
  remaining**. The saved proposal also stores the original scan total, so apply mode reports the
  same baseline without scanning everything again. After deletion, the console **and the
  run's TXT report** record the actual deleted file count/bytes/GB, skipped or failed counts,
  and corresponding remaining estimate. Failed/skipped deletions are not counted as removed.
  Here, 1 GB means 1,000,000,000 bytes.
- A confirmed run writes its dated recovery manifest **before deletion**, recording original
  filenames, retained paths, hashes and outcomes. It refuses deletion if that manifest already
  exists; archive it or choose another recovery filename before another same-day deletion run.
  Restoring from an earlier day requires pointing `MANIFEST_FILE` at that run's saved CSV.
  If the script is interrupted during deletion, the recovery manifest retains `pending`
  records. Restore mode recovers missing originals from eligible `pending` or `deleted`
  rows and never replaces an existing file. The completed manifest records `failed`
  for skipped deletions; these are not restore candidates.
- Each affected check folder gets one `.DUPLICATE_REMOVED.txt` placeholder per unique deleted
  content group, including groups whose kept copy is **in the same folder**. It lists every original
  deleted filename and the location of the retained file.
- During SHA-256 candidate scanning, `SHOW_HASH_PROGRESS = True` displays a Rich live progress bar with
  **bytes hashed / total candidate bytes**, percentage, throughput, ETA and files completed. The current
  filename is shown while it is being read, including for individual multi-gigabyte files. The
  update frequency is set by `HASH_PROGRESS_REFRESH_PER_SECOND` (default 2). The progress view uses
  the same Rich terminal approach as the repo's other dashboards, not a separate GUI window.
  The total counts only files requiring content hashes (equal extension and size), not every
  discovered file. Redirected/headless execution prints the completion summary without live frames.
  `rich` is included in the repo requirements; copied standalone scripts also need it installed.
- Set `SHOW_HASH_PROGRESS = False` to disable the live display.
- The script reuses `ryan_library.functions.file_verification.hash_file()` for streaming
  SHA-256, with the existing 1 MiB chunks, per-chunk Rich callback and hexadecimal
  manifests unchanged. Final metadata checks also reject symlink/junction paths.
- SHA-256 reads candidate contents **once** during grouping. Deletion checks existence, size and
  modification time of both paths, rechecking the retained path before each unlink. It does **not**
  hash file content again. Run only on static/inactive results; metadata checks are not file locks.
- Restoration copies verified retained content back to missing original paths and requires
  the recovery manifest (not the proposal). The restore workflow hashes the retained file to
  verify content. The saved-proposal apply workflow skips hashing by design.

Verify first on a disposable copy of model results, particularly on network drives.
