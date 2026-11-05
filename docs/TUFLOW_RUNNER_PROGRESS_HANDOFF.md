# Local agent handoff: TUFLOW runner grouping and model progress

Use the following prompt in a local Windows agent session. The authoritative
[work record](work/2026-11-04-tuflow-runner-progress.md) records source and CI
evidence; update it with exact-head local results rather than creating a second
status record.

## Copy-ready prompt

Continue TUFLOW runner progress PR #205 in `Chain-Frost/ryan-tools`.

Repository: `https://github.com/Chain-Frost/ryan-tools`
PR: `https://github.com/Chain-Frost/ryan-tools/pull/205`
Branch: `feat/tuflow-runner-progress`
Base: `main`

Fetch the latest branch and `main`. Use a separate checkout/worktree and preserve
unrelated changes. Continue this PR; do not create a replacement PR. Read
`AGENTS.md`, `docs/DEVELOPMENT_GUIDE.md`, `docs/WORK_TRACKING.md`,
`docs/work/README.md`, `docs/work/2026-11-04-tuflow-runner-progress.md` and
`ryan-scripts/TUFLOW-python/README.md`. Do not inspect the excluded `unsorted`
submodule or change any submodule pins.

The requested behaviour is implemented for review: no Model column, one TCF
heading/box per model, compact GPU IDs with hardware names logged above the
table, and periodic TLF/HPC observations for end time, model time and percent.
The follow-up adds observed run phase, the latest HPC timestep and signed final
cumulative mass error in `Detail`; ETA is deliberately outside this scope.
Your task is to test this on Windows and fix demonstrated defects on this same
branch, including any remaining implementation needed for these acceptance cases.

Use Python 3.14 with the immutable PyTUFLOW PR #63 source specified by
`repo-scripts/pytuflow-requirement.txt`. Do not change the global user
installation solely to perform a test; use an isolated validation installation. Build a wheel
with the repository's build helper and test a copied wrapper against that wheel
in a disposable installation target. Record the interpreter, package/wrapper
versions, installed module location, TUFLOW version, exact Git SHA and CUDA
mapping. Do not modify a running batch or production model.

### 1. Source and package checks

Run Ruff format/lint and strict Pyright on the PR's modified Python files, the
focused dashboard/runner/monitor/wrapper tests, documentation/link checks and
Loguru formatting policy. Build/verify the current wheel and exercise copied
wrapper `--help`, explicit `--export-only --no-pause`, invalid settings and
`--no-monitor-progress`. Record blocked tools honestly; Windows application
control has blocked Ruff on some earlier machines. Check hosted CI on the exact
commit as separate evidence.

The progress controls are:

```python
MONITOR_PROGRESS = True
PROGRESS_POLL_INTERVAL_SECONDS = 10.0
SHOW_PROGRESS_DIAGNOSTICS = True
USE_LIVE_DASHBOARD = True
LIVE_MAX_ROWS = 10
```

CLI overrides are `--monitor-progress`, `--no-monitor-progress` and
`--progress-poll-interval SECONDS`. `--progress-diagnostics` /
`--no-progress-diagnostics` control the timestep and final mass-error values;
phase remains visible. Preserve pause, imported order, GPU scheduling
and CSV Resume semantics. Optional observation failure must never gate execution.

### 2. Read-only fixture replay

Use the existing `ryan-tools-test-data` pin listed in the work record. It contains
paired main/HPC logs under `tuflow/tutorials/Module_01` through `Module_11` and
examples under `tuflow/TUFLOW_Example_Model_Dataset/log`.

Replay actual records into disposable files, including complete line appends and
partial trailing lines. Check the main `SIM:` / `Writing HPC Output at:` forms
and the HPC `iStep time ...` table; never interpret wall-clock `Clock`/`CPU` as
model time. Check negative start -72 h and negative-zero times (EG05_018), the
1-to-3 h restart (EG06_004), the 48 h run (EG17_002), and main/HPC disagreement
where the main log has reached the end but HPC's last row is earlier.

Confirm numeric progress uses `(model - start) / (end - start)` and cannot mark a
process successful. No solver run is necessary for these reader cases.

Replay initialization, timestepping and final `Writing maximums...`/summary
records to check the three phase labels. Repeated earlier headers and a lagging
HPC companion must not undo the final-writing phase. Compare the displayed `dt`
with its actual HPC header column and preserve signed final mass error, including
Module 11's negative value. Check tiny values, malformed/nonfinite values,
partial trailing records and new-generation resets. Live `CE` values and HPC
`Eff` are not substitutes for final cumulative mass error or timestep.
At process exit the monitor can hand back an already-sampled pending update,
but it must not force a final read or wait for storage. Verify that this cached
final ME survives completion and that an unobserved summary remains unavailable.
Retiring an invocation must ignore its later in-flight result and must not
remove a replacement invocation with the same run index but a different PID.

### 3. Real Windows execution and display

Use a copied writable tutorial model in a disposable output area. Start with
tutorial mode enabled if the installed executable supports it; this task does
not require turning tutorial mode off or consuming paid licences. If the actual
test arrangement needs paid execution, document the concrete commands and
licence needs before proceeding under the user's existing/local authorization.

- Observe a genuine HPC run through initialization, timestepping and completion.
  Compare the predicted path, selected events/scenarios, `End (h)` and `Model (h)`
  against the actual files. Capture before/during/after terminal screenshots.
  Check phase, timestep and final mass error in `Detail`, including retained
  diagnostics beside the actual process exit. Test the diagnostics toggle and
  confirm it leaves phase visible without adding columns or wrapping rows.
- Run the same command again with completed old `.tlf` and `.hpc.tlf` still
  present. Until current-invocation evidence is available, it must not show the
  previous end time as current progress or show 100% from the old run. Repeat
  within the same minute to exercise coarse header timestamps.
- Exercise a long dotted TCF/run name and decimal event value such as `01.0p`,
  with placeholder order `s2,s1,e1,e2,e4,e3,s4`; compare prediction to the file
  actually written by TUFLOW. Also check a configured Log Folder and an event
  alias if used in the model. Unsupported prediction must warn and remain unknown.
- If `-etN` is used, verify the effective log end time takes precedence over a
  different configured TCF End Time. Do not allow a fallback to imply the wrong
  simulation range.
- Test two distinct TCFs, including equal basenames in different directories.
  Confirm separate readable headings, correct per-run event/scenario columns,
  unchanged launch order and a global ten-row cap. Use a replay/fake-process test
  if two licensed GPU executions are unavailable.
- Check one GPU, grouped GPU IDs and automatic placement. Names should appear
  in the normal startup log, with only `-pu0`, `-pu0 -pu1`, or `Auto` in rows.
  Existing GPU occupancy/uncertainty alerts must still work.
- Resize a real terminal (including VS Code if used). Confirm logs remain above
  Rich, active rows stay visible, and multiple headings do not overflow the
  intended vertical budget. Check an ordinary redirected/headless invocation.
- Interrupt a test batch and confirm only owned children are stopped, callbacks
  and CSV outcomes remain correct, and no progress worker delays shutdown.

The supplied log corpus identifies HPC/Quadtree runs; it does not prove actual
Classic-engine compatibility. If a Classic executable/model is available, test
its log format independently. Otherwise record that case as not run.

### 4. Network and failure behaviour

**Continuous writer acceptance is required.** Verify the Windows reader opens
existing logs with `GENERIC_READ` and all of `FILE_SHARE_READ`, `FILE_SHARE_WRITE`
and `FILE_SHARE_DELETE`. It must not request exclusive access, byte-range locks,
write access or a persistent memory map. Handles must close after each bounded
read, including exception paths. Inspect handle counts across repeated samples.
With a compatible writer already open and while the monitor holds its brief
read handle, confirm continuous appends can proceed. Exercise writer close/reopen
as well as a long-lived writer. Check source-file rename while the reader is open
and direct destination replacement after the sample closes. Legacy Windows
`MoveFileExW` (Python `os.replace`) rejects replacing an existing open destination
even with delete sharing; do not assert that operation must succeed during a read.
The supplied native regression covers source rotation into a vacant name and
replacement after closure. Record whether the actual solver ever replaces logs
during execution and whether that operation is affected. If the writer denies
read sharing, the monitor must skip the sample and retry only at its normal
interval, with no immediate retry/wait loop. Never change the solver's sharing
mode to make a test pass. Record actual TUFLOW and SMB behaviour separately from
the synthetic Win32 sharing tests; permissive reader flags cannot guarantee that
an independently exclusive writer or every replacement API will coexist with
another read handle.

Repeat observation against a mapped drive or UNC path with spaces. Verify paths
keep their user-supplied spelling. Exercise missing logs, temporary sharing or
permission failure, replacement, truncation and slow/unavailable storage using
disposable data. Confirm one transition warning, continued solver scheduling,
no repeated warning flood and honest unknown/stale detail.

Measure log observation cost on a representative large TLF pair (including a
large middle section). Record file sizes, polling interval, actual read volume,
worker duration and scheduler/display responsiveness. Reads must stay bounded
and unchanged files should avoid content reads. Do not scan directories or
reread full logs each display refresh. Do not infer a stalled solver solely
because the log has not changed.

Create two active synthetic runs that predict the same logfile. Progress must
remain unverified for both affected invocations even after one exits; scheduling
itself must remain unchanged. Do not test colliding production output writers.

### 5. Delivery

Fix demonstrated issues with focused regression tests, then rerun the affected
checks. Commit and push task-related changes to this branch; preserve unrelated
work. Update the authoritative work record and its register row with exact
commands, results, screenshots/evidence locations, measured costs, remaining
limitations and next action. Update the existing PR description as appropriate.

Return a concise pass/fail/not-run acceptance table and the commit SHA. Do not
merge, enable auto-merge, publish a release, change production models, repin PyTUFLOW again or claim a native case passed based solely on synthetic tests.
