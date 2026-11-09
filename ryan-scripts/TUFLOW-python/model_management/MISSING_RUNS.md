# Infer missing TUFLOW runs

`find_missing_tlf_runs.py` recursively inventories TLF filenames. It reads no log contents and changes no model files.

```powershell
python ryan-scripts/TUFLOW-python/model_management/find_missing_tlf_runs.py "T:\results\04" --no-pause
python ryan-scripts/TUFLOW-python/model_management/find_missing_tlf_runs.py "T:\results\04" --output-csv "missing.csv" --no-pause
```

Alternatively edit `RESULTS_FOLDER` near the top of the script. The default is the launch directory.
CLI values override the editable settings. Relative inputs and outputs use the launch directory. The script does not
change the process working directory; `RESULTS_FOLDER` or the positional folder argument selects what to process.
`--no-recursive` limits discovery to the root. Directory links/junctions are skipped.
An inaccessible directory causes failure. Missing folders and folders without logs also fail.

Every observed underscore-separated field value is crossed with every other observed field value in its filename
family, including scenario fields. Families have the same number of fields and the same positions for recognised
AEP (`01p`, `00.2p`, `1%`, `1AEP`), duration (`060m`, `2h`, `60min`, `2hr`) and TP fields.
Other naming conventions are compared as scenario fields. Values retain their spelling except TP codes, which
normalise to `TP01` form. TP-bearing families expect TP01 through TP10 plus any other observed TP codes.
Families without TP or duration do not acquire those fields. `.tlf` and `.hpc.tlf` pairs count once; identical run
names in different folders also count once. Unrelated models/versions with compatible layouts are crossed too:
choose a scan root containing the run campaign you want to compare.

All gaps are **inferred**. A whole missing ensemble can be identified when its constituent values occur elsewhere.
An AEP, duration or scenario absent everywhere cannot be inferred. This is intended for finding gaps near the end
of a campaign, not defining a simulation plan before runs start. A present log does not prove successful completion.

For TP-bearing filename families, console output shows one AEP-row/duration-column table per scenario combination.
Cells count distinct TPs found, including active runs whose logs already exist: these are not completion counts.
Each scenario table is followed by a compact missing summary. Repeated duration groups share TP ranges, for example
`01p: 060m,120m TP01-03,TP08`; `all TP` denotes an entirely absent set. The scenario prefix is not repeated.
Complete cells are grey and incomplete cells red in terminals that support colour. The caption gives the expected TP
count (normally 10; any additional observed TP codes also become expected). Entirely missing combinations show zero.
Wide grids split into column sections instead of truncating durations. Missing AEP/duration fields get a placeholder.
Without colour, the counts and expected total still identify gaps. Layouts with repeated event fields use the filename
listing instead of an ambiguous grid. Non-TP families also retain a filename listing, including in mixed inventories.
Log lines omit the calling filename/function. Fallback filename listings show at most 50 gaps.

## Optional success verification

Set `VERIFY_SUCCESS = True` or pass `--verify-success` to read the logs and count only simulations with a standalone
`Simulation FINISHED` line. The default is `False`: filename-only scanning. `--no-verify-success` overrides an edited
True setting. The same tables then show **Successful TPs**; compact summaries and CSV include absent runs and runs
without confirmed success. A running, failed or interrupted simulation therefore remains outstanding. This verifies
TUFLOW's completion status, not numerical quality or acceptable mass balance. All observed names still define the
expected dimensions, even when none have succeeded.

For each run, verification uses the newest primary `.tlf` across scanned directories. A primary TLF takes precedence
over its `.hpc.tlf` companion; an old successful companion cannot mask a running or failed primary. If only HPC logs
exist, use the newest HPC log. Files whose size or modification time changes during reading are not confirmed.
An unreadable selected log fails the scan rather than claiming a complete verified inventory. Snapshots can change
while models run; rerun after the campaign ends for final acceptance. Verification performs extra network reads and
is unavailable in legacy tracking-table mode.

```powershell
python ryan-scripts/TUFLOW-python/model_management/find_missing_tlf_runs.py "T:\results\04" --verify-success --no-pause
```

Success verification uses the shared TUFLOW log-summary progress dashboard: enabled by default, ten visible rows,
two refreshes per second, with actual selected log paths supplied for background file-size metadata. The dashboard
covers discovery/latest-log selection, status verification, aggregation and any requested workbook export. Missing
success markers count as SKIP; unreadable logs or export failures finish as FAILED. `--no-live-dashboard` disables
the display and prints periodic progress instead. Redirected output stays plain text. Editable controls are
`USE_LIVE_DASHBOARD`, `LIVE_REFRESH_PER_SECOND` and `LIVE_MAX_ROWS`, with matching CLI overrides.

## Optional standard log-summary export

Set `EXPORT_LOG_SUMMARY = True` or pass `--export-log-summary`, together with success verification. Export is off by
default, and `--no-export-log-summary` overrides an edited True setting. The report uses the existing log-summary
parser, DataFrame assembly/column ordering and protected Excel exporter, with the standard timestamped
`ModellingLog` workbook, `Log Summary` sheet and data dictionary. Existing outputs are never overwritten.

Only the verified scan's selected successful logs are passed to the canonical parser; there is no second folder
discovery pass. The existing parser rereads those logs to extract its normal metadata. An incomplete log is excluded;
if a completion marker exists but the normal parser cannot construct a summary row, a warning identifies the omission.
No workbook is written when there are no summary rows. Parsing errors or export failures fail the operation.
The default output directory is the results root. Set `LOG_SUMMARY_DIRECTORY` or `--log-summary-directory` to select
another existing directory; relative values use the launch directory. This workbook is independent of the optional
missing-run CSV. A filename-only scan never reads log contents or exports a log-summary workbook.

```powershell
python ryan-scripts/TUFLOW-python/model_management/find_missing_tlf_runs.py "T:\results\04" --verify-success --export-log-summary --no-pause
```

`--output-csv` saves all inferred missing run names, without an extension;
the report must not already exist. No report is written by default. The default combination limit is 1,000,000;
`--max-combinations` can change it. Exceeding the limit fails rather than claiming a partial inventory is complete.
Exit status is 0 for a completed analysis (including gaps), 1 for failure, and 2 for invalid CLI arguments.

The legacy `--input-table FILE --sheet 0` route remains table-based with its original TP requirements and automatic
summary export beside the input. It now refuses to overwrite an existing summary CSV. Table summary messages remain
visible at the default SUCCESS console level. It cannot be combined with folder or report options.

The wrapper prints opening and closing script/package versions. It validates effective settings before scanning
or writing reports; failures return 1 without pausing. Only successful interactive runs can pause.

For AI agents, use the maintained CLI with `--no-pause` and an explicit scan root; optionally request a new CSV path.
No separate MCP endpoint is added: filename inference and its scope still need the campaign context documented here.
