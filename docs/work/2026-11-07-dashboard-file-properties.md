# Shared dashboard defaults and background file properties

- Status: Needs review; clean local validation complete.
- Owner: ChatGPT / maintainer.
- Updated: 2026-11-07.
- Next review: 2026-11-14.
- Delivery: included in the current implementation commit on PR #183 branch `feat/batch-dashboard-foundation-180`.
- Next action: review [current-head CI](https://github.com/Chain-Frost/ryan-tools/pull/183/checks) and the implementation. No merge authorized.

## Agreed behavior

The user requested general defaults of enabled dashboards, ten visible rows and
INFO file logging, retaining explicit disabled and DEBUG overrides. Old banner
calls that omit identity/revision arguments warn and continue rather than fail.

The Size column was removed from log summaries in `b05414fa` on 2026-11-01 to
avoid a synchronous per-file stat sweep on cold/network storage. The user wants
file properties generally, and explicitly requested collecting them on another
thread without delaying processing. The log-summary column is restored and the
shared implementation supports other file workflows too.

`LiveWorkflowDashboard` accepts input paths/groups per task. The generic workflow
adapter identifies `Path` items automatically; result processing, raster groups
and protected workbook copies supply their inputs directly. The property helper
uses a single daemon reader and a bounded result queue, starts only inside an
enabled interactive context, and never updates Rich from the background thread.
The coordinator consumes properties during refresh. Shutdown/task replacement
cancel without waiting for an OS stat already in progress. Missing values are
unknown, not zero; grouped values represent summed input-file sizes, not bytes
read or output sizes. Raster waits poll so sizes can appear during long jobs.
The former synchronous result-processing size summary is no longer a startup
gate; its private compatibility helpers remain available.

The dashboard, wrapper and logging standards describe these contracts; AGENTS.md
now states INFO file defaults with explicit DEBUG diagnostics. Basic property
helpers need no MCP tool; existing workflow entries and their revision metadata
remain the discovery routes.

## Validation and boundaries

- Focused property/renderer/workflow checks cover blocked stats, nonblocking
  cancellation, bounded queues, zero/missing/grouped sizes and parent refresh.
- Explicit DEBUG sink preservation remains covered, alongside actual INFO-default
  filtering; notebook expectations now match the requested INFO default.
- Public export compatibility is tested with dashboards explicitly on and off.
- Separate uncommitted `simulation_runner.py` edits appeared during this work.
  They remain outside this commit. Build/tests ran in an isolated local clone
  of implementation `4e7f8b2a`, initializing only the required QGIS/test fixtures.
  The package excludes the unrelated edits. Subsequent changes to this commit
  update only work records, not packaged code or configuration.
- Native UNC/cold-storage performance and licensed solver acceptance are not
  claimed by synthetic blocked-I/O tests. No proprietary project data is used.


## Final local evidence (2026-11-07)

- Clean-checkout affected validation: **461 passed, four skipped** in 70.61 s.
  All skips require Windows symlink privileges unavailable on this machine.
- Ruff lint/format, strict Python 3.14 Pyright and compilation cover the 47
  changed Python files; documentation/index/link and Loguru policy checks pass.
- Built and verified `26.11.7.8` with `--no-bump --skip-pip` from committed code;
  the declared version stayed unchanged. Wheel SHA-256:
  `d50992361c43a30f9ebac07ae72a116d092d078f37b25d9b51e0292a2b9d6b35`.
- Installed that wheel normally, then passed the existing package smoke outside
  both checkouts: discovery, older-wrapper warning, copied CLI/output behavior,
  PO-label export and solver-free TUFLOW TXT export.
- All six copied result-wrapper groups pass the installed-wheel smoke, including
  dashboard on/off parity, selected channels, invalid settings and writer safety.
- Copied property/renderer tests outside both checkouts verify the physical
  installed import origin: **six passed**, including blocked stat/cancellation.
- Hosted status remains independently visible at the PR checks link above;
  earlier green commits are not relabeled as current-head hosted evidence.
