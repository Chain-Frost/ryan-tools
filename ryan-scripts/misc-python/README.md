# Office utility scripts

## Remove Excel protection

`remove_excel_protection.py` is a maintained standalone entry point using the
installed `ryan-tools` logging and dashboard helpers. It reads `.xlsx` and `.xlsm`
files directly within each selected directory and creates
`unprotected/<name>_unprotected.xlsx` (or `.xlsm`) by default. Original workbooks
and existing destinations are never replaced.

```powershell
python remove_excel_protection.py -i "E:\Workbooks" --no-write --no-pause
python remove_excel_protection.py -i "E:\Workbooks" --no-live-dashboard --no-pause
```

Edit `DEFAULT_INPUT_DIR`, `DEFAULT_OUTPUT_DIR`, `WRITE`, `USE_LIVE_DASHBOARD`,
`LIVE_REFRESH_PER_SECOND`, `LIVE_MAX_ROWS`, `PAUSE_ON_FINISH` and
`CONSOLE_LOG_LEVEL`, `LOG_FILE` and `FILE_LOG_LEVEL` near the top. Write runs
retain DEBUG records in `remove_excel_protection.log` relative to the invocation
directory by default; preview creates no log file. A log cannot alias a selected
workbook or planned workbook output. `--output-directory` overrides the output
setting: a relative folder is based on each input root; an absolute folder is
shared. CLI `--write`/`--no-write` and `--live-dashboard`/`--no-live-dashboard`
override their editable settings in both directions. `--no-pause` disables the
interactive pause. Preview checks paths/collisions, lists destinations and does
not create output folders or inspect workbook contents.

The whole batch is checked for output collisions before copying. ZIP members are
never extracted. Unsafe member names, links, duplicate names, encrypted ZIP
members and XML DTDs are rejected. Only direct spreadsheet-namespace
`workbookProtection` and `sheetProtection` elements are removed; extension XML
and unrelated elements remain. Modified XML is reserialized, preserving namespace
prefixes and content; untouched members retain their exact uncompressed bytes
and ZIP metadata, including macro payloads and relationships.

Each workbook is built and integrity-checked in a private temporary directory.
Publication uses an atomic hard link that refuses existing files, including a
concurrent creator. Filesystems without hard-link support fail with an error;
there is no overwrite/copy fallback. Linked source/output paths (including
Windows junction ancestors) are rejected. Source and destination directories
must remain under the user's control during execution; this is not a sandbox
against an actor replacing parent directories while the process runs.

The dashboard reports each completed copy, failures and interruption; redirected
output has no live animation. Exit codes are 0 for success/no inputs/preview,
1 for invalid configuration or any failed copy, 2 for invalid CLI syntax and
130 for interruption. A failed batch can retain successfully completed copies.
Native Excel/Office fidelity and live network-filesystem acceptance remain
separate from synthetic ZIP/XML and Windows filesystem checks.

See the [issue #199 work record](../../docs/work/2026-11-04-office-protection-safety-199.md).
