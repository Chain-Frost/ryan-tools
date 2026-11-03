# TUFLOW GPU scheduling and CodeMeter validation

This guide separates physical GPU occupancy from licence availability. The simulation runner schedules explicit
`-puN` groups from local TUFLOW process command lines. CodeMeter observations are independent evidence that a licence
was checked out; a GPU licence allocation does not identify a physical GPU.

For a future agent session on a suitable workstation, use the copy-ready
[licensed two-GPU validation prompt](TUFLOW_GPU_HARDWARE_TEST_PROMPT.md).

## Two-GPU licensed hardware procedure

Use a Windows workstation with two compatible TUFLOW-capable GPUs and a licence that permits the planned concurrent
runs. Do not perform this test with production model outputs.

1. Record the workstation, TUFLOW build, driver, Python and `ryan-tools` versions. Capture `nvidia-smi -L` and
   `nvidia-smi --query-gpu=index,name,uuid,driver_version,memory.total --format=csv` so GPU indexes are tied to physical
   UUIDs rather than assumed card order.
2. Copy a small TUFLOW HPC tutorial model, preferably Module 01, into a dedicated test root. Preserve the original.
   Make separate `external-pu0`, `external-pu1` and `runner` copies or give every run a distinct TCF and output folder.
3. In every copied TCF set `Tutorial Model == OFF`. Search the copied controls for other tutorial/demo or licence-free
   settings and remove them. Do not use `-nlc`. Run a normal licensed simulation and retain its TLF plus a CodeMeter
   snapshot proving the expected licence checkout.
4. Run each copy once with explicit `-pu0` and `-pu1`. Confirm the TLF `GPU_Device_IDs`, the command line and the
   corresponding `nvidia-smi` process/UUID observation agree. This establishes TUFLOW's physical numbering on that
   workstation.
5. Configure the runner with `GPU_DEVICES = ["-pu0", "-pu1"]`, `WAIT_FOR_AVAILABLE_GPU = True`, and
   `GPU_CHECK_INTERVAL_SECONDS = 60.0`. Keep the normal distinct output names/directories and retain the runner log.
6. Before, during and after each scenario, capture local process state, `nvidia-smi`, CodeMeter and relevant TLFs.
   Use the benchmark command below for repeatable local/CodeMeter timing. Do not add global logfile scans to the
   scheduler loop.

Run and record every scenario:

| Scenario | Required observation |
| --- | --- |
| Both GPUs free | Both configured slots launch without sharing a device. |
| GPU 0 occupied externally | `-pu0` is excluded; `-pu1` continues; the red alert names GPU 0 and the external PID. |
| GPU 1 occupied externally | `-pu1` is excluded; `-pu0` continues. |
| Both GPUs occupied | No queued run launches; the persistent alert says the entire batch is waiting. |
| One GPU becomes free | The slot returns on the next poll and a recovery message is emitted. Run at least one real 60-second cycle. |
| One slot requires GPUs 0+1 | The multi-GPU slot waits until both devices are clear. |
| External process terminates unexpectedly | Occupancy refresh restores the slot without marking a simulation failed. |
| CodeMeter unavailable | Licence availability is not declared; local occupancy behavior remains explicit and separate. |
| GPU assignment unknown | All explicit groups remain conservatively blocked with a `GPU STATUS UNKNOWN` alert. |
| User interrupts while waiting | The runner exits gracefully and launches no queued simulation. |

For each occupancy scenario, launch the external licensed TUFLOW executable directly on the selected GPU before
starting the runner. Observe the external process and licence state independently of the runner. After termination,
record the process-exit time, each poll time, CodeMeter allocation-release time and the runner launch time. This exposes
both the configured polling delay and any reporting lag.

## Benchmark harness

The harness performs read-only local process inspection and CodeMeter WebAdmin queries. It does not launch TUFLOW,
scan project logs or mutate licences. An output path uses exclusive creation and will not overwrite an existing file.

```powershell
python repo-scripts\benchmarks\benchmark_tuflow_gpu_polling.py `
  --scenario "GPU 0 occupied externally" `
  --samples 30 `
  --methods local codemeter combined `
  --output .\gpu0-occupied.json
```

Repeat the command in each independently verified state. The JSON retains every elapsed-time, CPU-time and RSS-delta
sample plus median, nearest-rank 95th percentile and maximum summaries. Compare reported GPU IDs/PIDs with the direct
command line, TLF and `nvidia-smi` evidence to count false positives and false negatives. Treat RSS deltas as noisy
process observations, not isolated allocations.

Use a separate `--scenario` result for CodeMeter shutdown/unavailability and for allocation release after process
termination. A `combined` query is justified only if CodeMeter contributes a scheduling fact that local inspection
cannot provide reliably; otherwise it measures unnecessary scheduler overhead.

## Observations on 2026-11-03

These are actual read-only observations from the development workstation, separate from synthetic tests.

- Hardware inventory found one compatible NVIDIA GPU: GeForce RTX 5090, driver 610.88, 32,607 MiB, index 0 and UUID
  recorded locally. An Intel display adapter and remote-display adapter were not a second TUFLOW-capable GPU.
- Another user's licensed TUFLOW model was already running and the workstation's single observed GPU licence was in
  use. The process was observed read-only and was not interrupted. CodeMeter reported products 20 (`TUFLOW Local`), 21
  (`TUFLOW Local Threads`) and 26 (`TUFLOW Local GPU`) allocated to the same PID. CodeMeter exposed no physical GPU ID
  and no device-specific licence allocation.
- The existing activity report correlated that CodeMeter PID to a current TLF whose `GPU_Device_IDs` value was `0`.
  This is useful independent validation, but it depends on global-log/TLF discovery and can be stale or inaccessible;
  it is not suitable for the one-minute scheduler loop.
- Local process inspection found the TUFLOW PID but could not read its command line because Windows denied access. The
  scheduler therefore classified the assignment as unknown and would conservatively pause explicit GPU scheduling.
- Only one compatible GPU was present. Multi-GPU concurrency, simultaneous CodeMeter allocations, allocation release,
  false-positive/negative rates and real 60-second recovery remain outstanding.

Five measurements were captured for each available method while the external process remained active:

| Method | Individual elapsed ms | Median ms | P95 ms | Maximum ms | Median CPU ms | Observed result |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| Local process inspection | 43.179, 47.840, 67.029, 54.903, 59.128 | 54.903 | 67.029 | 67.029 | 62.500 | PID found; GPU assignment unknown |
| CodeMeter snapshot | 10532.269, 10439.552, 10463.361, 10466.340, 10485.680 | 10466.340 | 10532.269 | 10532.269 | 31.250 | Products and PID found; no GPU ID |

Local RSS deltas were 1,290,240, 98,304, 188,416, 151,552 and 151,552 bytes. CodeMeter deltas were 5,201,920,
-602,112, -61,440, -69,632 and -32,768 bytes. Negative values reflect allocator/measurement noise. A separate
unavailable-service check against a closed local endpoint returned a timeout in 235.560 ms and produced no false
licence-availability result.

The CodeMeter snapshot duration reflects several sequential WebAdmin page requests; its nominal per-request timeout is
not an end-to-end snapshot timeout. On this machine it was about 190 times slower than local inspection and still did
not provide physical GPU identity.

## Conclusion and remaining risk

Retain local TUFLOW process inspection as the scheduling mechanism. CodeMeter is valuable for proving licence use and
reporting licence availability, but current observations do not support device-specific scheduling. PID-to-TLF
correlation is supporting evidence only, not a timely occupancy authority. Do not combine CodeMeter with every
scheduler poll unless two-GPU hardware evidence changes that conclusion.

A race remains between observing a free GPU and launching TUFLOW: another process can claim the device after the check.
The runner reduces conflict risk but cannot reserve a physical GPU atomically. A future robust reservation would need
solver-supported device locking or an agreed host-wide lock respected by every launcher.
