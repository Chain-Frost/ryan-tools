# Future agent prompt: licensed two-GPU TUFLOW validation

Copy the prompt below into a new agent session on the Windows workstation that has two compatible TUFLOW-capable GPUs
and two usable TUFLOW GPU licences.

```text
# Agent task: execute the licensed two-GPU validation for PR #193

Repository: Chain-Frost/ryan-tools
Existing PR: https://github.com/Chain-Frost/ryan-tools/pull/193
Branch: fix/tuflow-textfile-optional-settings-rows
Base: main

Continue PR #193. Do not create another PR, merge, publish the package, or alter production models. Fetch the latest
remote branch and main, read AGENTS.md, docs/DEVELOPMENT_GUIDE.md, ryan-scripts/WRAPPER_STANDARD.md,
docs/TUFLOW_GPU_CODEMETER_VALIDATION.md, the PR diff and relevant tests. Use an isolated worktree and preserve unrelated
changes and submodules. Never inspect the unsorted submodule.

This machine is expected to have two compatible physical GPUs and two usable TUFLOW GPU licences. Verify that premise
before testing; do not assume display-adapter order, GPU indexes or licence capacity. Record `nvidia-smi -L`, GPU UUIDs,
driver version, TUFLOW build, Python version, ryan-tools commit and CodeMeter products/quantities. If there are not two
compatible GPUs and two usable licences, stop licensed execution, record the actual inventory and report the blocker.

Use a separate copy of a small TUFLOW HPC tutorial model, preferably Module 01. Do not modify the original. In every
copied TCF set:

Tutorial Model == OFF

Confirm no tutorial/demo licence-free settings remain. Do not use `-nlc` or any licence-free test/copy mode. Use
distinct TCF/output names or directories for every simultaneous run. Prove from CodeMeter activity and TUFLOW logs that
each principal simulation checked out the expected paid licence.

First establish physical numbering. Run the copied model once with `-pu0` and once with `-pu1`; correlate command-line
PID, `nvidia-smi` GPU UUID/index and TLF `GPU_Device_IDs`. Do not proceed until these agree or the discrepancy is
documented.

Then execute and independently observe all scenarios:

1. Both GPUs free: both configured single-GPU slots can launch.
2. GPU 0 occupied by a directly launched licensed external TUFLOW process: GPU 0 is excluded, GPU 1 continues, one
   transition ERROR appears, and the persistent red dashboard alert identifies GPU 0 and the external PID.
3. GPU 1 occupied externally: GPU 1 is excluded and GPU 0 continues.
4. Both GPUs occupied externally: no queued run launches and the red alert states the entire batch is waiting.
5. One GPU becomes free: the slot returns at the next poll and the alert clears with a recovery message.
6. A single configured group requires GPUs 0+1: it waits until both devices are available.
7. An external process terminates unexpectedly: occupancy refresh restores the slot.
8. CodeMeter becomes unavailable: no false declaration of licence availability occurs; record local scheduling behavior.
9. A TUFLOW GPU assignment is unknown or process inspection is incomplete: queued runs continue with a
   `GPU INSPECTION WARNING - CONTINUING` warning. Confirmed conflicts still block only their overlapping slots.
10. Interrupt while all slots are waiting: shutdown is graceful and no queued process launches.

For the principal occupancy tests, launch the external licensed TUFLOW process directly on the selected GPU before
starting the runner with `GPU_DEVICES = ["-pu0", "-pu1"]` and `WAIT_FOR_AVAILABLE_GPU = True`. Use the real
`GPU_CHECK_INTERVAL_SECONDS = 60.0` for at least one complete wait/recovery cycle. Independently capture process state,
`nvidia-smi`, CodeMeter and TLF evidence before, during and after each scenario.

Run the non-destructive benchmark in each controlled state, for example:

python repo-scripts\benchmarks\benchmark_tuflow_gpu_polling.py --scenario "GPU 0 occupied externally" --samples 30 --methods local codemeter combined --output .\gpu0-occupied.json

Measure individual and median/P95/maximum wall time, CPU time and RSS delta; process-exit-to-schedulable delay; detected
GPU IDs/PIDs; false positives/negatives; multiple simultaneous processes; licence allocations without GPU identity;
CodeMeter outage; and allocation release timing. Determine whether combined polling adds a reliable scheduling fact.
Do not integrate global logfile scans or full activity-report generation into the one-minute scheduler loop.

Record actual results in docs/TUFLOW_GPU_CODEMETER_VALIDATION.md, replacing only the outstanding two-GPU section and
preserving the prior one-GPU observations as historical evidence. Do not commit tutorial models, TLFs, proprietary
paths, usernames, licence secrets or raw customer data. Summarise sanitized evidence and retain raw benchmark JSON in
an approved external test location if it contains operational identifiers.

Run Ruff, strict Pyright on modified Python, focused tests, documentation checks, package verification and required CI.
Update the PR description with the hardware matrix, measured CodeMeter conclusion, remaining race/limitations and exact
validation. Commit and push to the existing PR branch only. Report the final commit SHA and exact-head CI status. Do not
merge without explicit authorization.
```
