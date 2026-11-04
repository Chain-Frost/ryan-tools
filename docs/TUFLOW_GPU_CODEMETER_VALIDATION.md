# TUFLOW GPU scheduling and CodeMeter validation

Current policy (2026-11-04 user clarification): uncertain/failed GPU inspection warns and continues;
only confirmed conflicts wait. Historical unknown-assignment pause results below describe the earlier
implementation and are not the current acceptance criterion. The new behavior has synthetic launch
regressions; the licensed matrix has not been rerun for this policy change.


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
| GPU assignment unknown | Warn and continue with unverified availability; do not stall computation. |
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

## Licensed two-GPU observations on 2026-11-03/04

The hardware exercise ran on a separate Windows workstation using Python 3.14.8, NVIDIA driver 610.88 and
TUFLOW 2026.3.0-iSP-w64 (HPC solver 3.51.9). The tested scheduler was commit `b29f8c2b`; the subsequent `efeff949`
report/history changes were integrated afterwards and do not change the scheduler or GPU detector.

| TUFLOW device / TLF GPU Device IDs | Physical card | nvidia-smi index |
| --- | --- | --- |
| `-pu0` / `0` | GeForce RTX 3090 | 1 |
| `-pu1` / `1` | GeForce RTX 3080 | 0 |

Separate licensed numbering runs established this reversed mapping using command-line PIDs, GPU UUID observations
and TLFs. UUIDs and process identifiers remain in local evidence rather than this sanitized report. CodeMeter capacity
was four Local licences (product 20), sixteen Local Threads (21), two Multiple Domains (22) and two GPU licences (26).
Both GPU licences were initially available and were available again after all test processes exited.

The exercise used isolated copies of Module 01 with distinct TCF/output names and `Tutorial Model == OFF`.
No licence-free command-line mode was used. A scan of 97 copied control files found no active tutorial/demo
licence-free setting. External occupancy runs used a 0.005-second maximum/initial timestep and 0.001-second minimum
to keep a small model active through the polling cycle; normal queued runs retained the tutorial's ordinary timing.
See the [TUFLOW timestep command reference](https://docs.tuflow.com/classic-hpc/manual/2026.3/80-AppendicesA-TCFCommands.html).
Only test-owned external processes were terminated. Original tutorial and production inputs were unchanged.

| Scenario | Observed result |
| --- | --- |
| Both GPUs free | Two queued simulations launched on separate configured slots and both exited 0; CodeMeter showed two paid GPU allocations. |
| GPU 0 occupied externally | Only GPU 1 launched. One transition ERROR and the red dashboard alert identified GPU 0 and its external PID. |
| GPU 1 occupied externally | Only GPU 0 launched, with the corresponding error/alert; the queued simulation exited 0. |
| Both GPUs occupied | No queued launch through the initial and next 60-second poll; the persistent alert stated that the entire batch was waiting. The unchanged poll emitted INFO rather than another ERROR. |
| One of two occupied GPUs freed | GPU 0 resumed 49.815 seconds after external termination. The alert correctly retained the restriction for still-occupied GPU 1; partial recovery did not clear the remaining warning. |
| Single blocked slot freed | The slot resumed 49.947 seconds after termination, with SUCCESS recovery and the alert cleared. The queued licensed simulation exited 0. |
| One group needs GPUs 0+1 | The group remained blocked after only GPU 0 was freed. It launched 45.347 seconds after GPU 1 was also freed, cleared the alert and exited 0. One PID held two CodeMeter GPU allocations and appeared on both physical UUIDs. |
| External process exits unexpectedly | Test-owned externals were terminated independently of the runner. Subsequent polls restored capacity; external termination was not reported as a queued simulation failure. |
| CodeMeter reporting endpoint unavailable | Five CodeMeter-only and five combined requests to a closed loopback endpoint all failed with no availability result. Local scheduling remained operational and two licensed queued runs completed. This was client reporting-endpoint failure, not shutdown of the shared licence service. |
| Unknown GPU assignment | A real licensed external TUFLOW run without explicit `-puN` was classified unknown. Both explicit groups stayed blocked under `GPU STATUS UNKNOWN - SCHEDULING PAUSED`; no queued process launched. |
| Interrupt while all slots wait | Interrupting the runner's cancellation event returned an interrupted outcome with zero queued launches and no owned children left behind. |

Before/during/after snapshots captured local assignments, nvidia-smi identities, CodeMeter and model logs. CodeMeter
and TLF evidence confirmed paid licence use in the principal occupancy tests, including two simultaneous processes
and two allocations belonging to one multi-GPU process. A snapshot comprises sequential requests: allocations can
change between its product totals and detail pages, so it must not be treated as an atomic licence reservation.

Release snapshots completed 0.797 seconds after the partial release, 0.780 seconds after the single-slot release,
and 0.811 seconds after the final grouped release. They showed the expected remaining GPU allocation counts of 1, 0
and 0 respectively. These are observed upper bounds on reporting release, not precise server release timestamps.
The real 60-second polling interval explains the approximately 45-50 second process-exit-to-launch delays.

### Polling measurements

Five samples per method per controlled state retained individual wall time, CPU time and RSS deltas. The table gives
wall-time median / P95 / maximum in milliseconds. With five samples the nearest-rank P95 equals the maximum; these
small samples characterize this workstation and are not a general latency guarantee.

The initial free-state measurements used `localhost:22352`. An explicit IPv4 endpoint, `127.0.0.1:22352`, returned the
same products much faster and was used for subsequent states. The endpoint effect is observed, not attributed to a
proven networking cause. Do not compare the earlier localhost timing as if it were inherent CodeMeter processing time.

| Controlled state | Local ms | CodeMeter ms | Combined ms |
| --- | ---: | ---: | ---: |
| Both free (localhost) | 95.1 / 110.8 / 110.8 | 12801.4 / 12825.5 / 12825.5 | 12894.2 / 12909.8 / 12909.8 |
| GPU 0 occupied | 130.7 / 137.1 / 137.1 | 538.1 / 585.6 / 585.6 | 663.0 / 780.5 / 780.5 |
| GPU 1 occupied | 134.4 / 144.9 / 144.9 | 659.2 / 899.6 / 899.6 | 821.5 / 980.4 / 980.4 |
| Both occupied | 114.0 / 128.8 / 128.8 | 718.5 / 750.6 / 750.6 | 864.5 / 931.8 / 931.8 |
| Both occupied, grouped slot | 111.6 / 128.8 / 128.8 | 654.7 / 750.9 / 750.9 | 728.2 / 903.9 / 903.9 |
| Single occupied slot | 96.3 / 97.4 / 97.4 | 538.9 / 648.2 / 648.2 | 632.6 / 656.4 / 656.4 |
| Unknown assignment | 99.2 / 110.4 / 110.4 | 554.5 / 569.1 / 569.1 | 649.8 / 688.5 / 688.5 |
| Both occupied, interrupt case | 97.7 / 104.4 / 104.4 | 571.2 / 610.2 / 610.2 | 672.1 / 732.3 / 732.3 |

Across these eight controlled states, all 80 local observations (standalone plus combined) matched the expected
explicit assignments or unknown state: no observed false positives or false negatives in this bounded sample.
CodeMeter supplied allocation/PID information but no physical GPU identity, including the unknown-assignment case.
Combined polling added licence evidence but no additional reliable fact for selecting a physical GPU.

- Local, 40 samples: median CPU 109.375 ms; median RSS delta 22,528 bytes (range 0 to 172,032).
- Codemeter, 40 samples: median CPU 15.625 ms; median RSS delta 86,016 bytes (range -1,429,504 to 778,240).
- Combined, 40 samples: median CPU 140.625 ms; median RSS delta 71,680 bytes (range -1,110,016 to 925,696).

RSS changes include allocator noise and are not per-operation memory requirements. Individual samples, UUID/PID
correlation, rendered ANSI dashboard captures and raw TLFs are retained outside Git in the dedicated local test
location. Only this sanitized summary is committed. All test-owned solver processes were reaped and final CodeMeter
GPU usage was zero.

The remaining scope limits are a real licence-service shutdown/restart, native keyboard Ctrl+C delivery (the test used
the runner cancellation event), and visual inspection in a user's interactive terminal. Occupancy cases captured the
actual dashboard renderer; uncertainty/interruption/recovery cases also ran Rich's live refresh against a captured
console. These limits do not change the observed scheduling outcomes and are not represented as tested.

## Conclusion and remaining risk

Retain local TUFLOW process inspection as the scheduling mechanism. CodeMeter is valuable for proving licence use and
reporting licence availability, but current observations do not support device-specific scheduling. PID-to-TLF
correlation is supporting evidence only, not a timely occupancy authority. The two-GPU observations support keeping
CodeMeter out of the scheduler poll; use it separately for licence reporting.

A race remains between observing a free GPU and launching TUFLOW: another process can claim the device after the check.
The runner reduces conflict risk but cannot reserve a physical GPU atomically. A future robust reservation would need
solver-supported device locking or an agreed host-wide lock respected by every launcher.
