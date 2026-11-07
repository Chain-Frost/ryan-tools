# Infer missing TUFLOW runs

`find_missing_tlf_runs.py` recursively inventories TLF filenames. It reads no log contents and changes no model files.

```powershell
python ryan-scripts/tuflow/find_missing_tlf_runs.py "T:\results\04" --no-pause
python ryan-scripts/tuflow/find_missing_tlf_runs.py "T:\results\04" --output-csv "missing.csv" --no-pause
```

Alternatively edit `RESULTS_FOLDER` near the top of the script. The default is the launch directory.
CLI values override the editable settings. Relative inputs and outputs use the launch directory, before any
`--working-directory` change. `--no-recursive` limits discovery to the root. Directory links/junctions are skipped.
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

Console output lists at most 50 gaps. `--output-csv` saves all inferred missing run names, without an extension;
the report must not already exist. No report is written by default. The default combination limit is 1,000,000;
`--max-combinations` can change it. Exceeding the limit fails rather than claiming a partial inventory is complete.
Exit status is 0 for a completed analysis (including gaps), 1 for failure, and 2 for invalid CLI arguments.

The legacy `--input-table FILE --sheet 0` route remains table-based with its original TP requirements and automatic
summary export beside the input. It now refuses to overwrite an existing summary CSV. Table summary messages remain
visible at the default SUCCESS console level. It cannot be combined with folder or report options.

The wrapper prints opening and closing script/package versions. It validates effective settings before changing
directory or writing reports; failures return 1 without pausing. Only successful interactive runs can pause.

For AI agents, use the maintained CLI with `--no-pause` and an explicit scan root; optionally request a new CSV path.
No separate MCP endpoint is added: filename inference and its scope still need the campaign context documented here.
