# TUFLOW input checking and runner integration

| Field | Value |
| --- | --- |
| Status | Needs review: cleanup delivered to #159; new-head CI and native Windows acceptance pending |
| Owner | ChatGPT |
| Created / Updated | 2026-10-23 / 2026-10-31 |
| Next review | Windows acceptance / 2026-11-07 |
| Pull request | [ryan-tools #159](https://github.com/Chain-Frost/ryan-tools/pull/159), open and non-draft when checked |
| Branch / delivery base | `feat/tuflow-runner-model-checks`, parent `5810fba8def32aa3570c0684cfa7d99c18caae25`; tested implementation baseline `5edc2f5371da3a55faec4cbab2115e43574f1c43` |
| Delivery | This commit publishes the owner-requested cleanup to the existing #159 branch; no main merge or release |
| Next action | Check GitHub CI on this delivered candidate, resolve failures, then complete any remaining native Windows installer/runner acceptance. |

## Current scope and decisions

Scopes 01-04 are integrated together in #159. The earlier component PRs and
interim benchmark records do not define the current implementation. The
[scope index](2026-10-31-tuflow-integration-scopes/00_scope_index.md) records the
shared contracts; the owner's later decisions below take precedence.

- Consume the accepted public PyTUFLOW API from immutable
  `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b`. Both normal installation routes use
  the same source requirement, GIS bootstrap and post-install verification.
- Input checker findings are terminal-only by default. An explicit report path
  enables an exclusively created issue CSV.
- The Python runner controls `BATCH_COMMANDS`, GPU allocation and launch policy.
  Source batch engine switches and GPU assignments are ignored when preparing
  imported runs. Model selections, effective TCF/cwd/executable and provenance
  remain available. The earlier abbreviated-input P1 classification is withdrawn;
  the owner clarified the authoritative Python setting instead.
- Both command readers use a captured inherited environment and independent
  state per input file. TCF conflicts fail before writes or execution.
- Resume regenerates the full plan and filters only the current launch queue.
  Successful CSV history is retained. Sparse original run indices are valid for
  preflight and presentation.
- Refresh one `<wrapper-stem>.txt` command export beside the supplying Python
  file. TXT is intentional protection against accidental double-click execution.
  Refresh the full plan even when Resume is complete. No timestamped exports,
  backup files or session directories. Never overwrite imported sources or model,
  wrapper or history files, including aliases.
- The runner and command export target Windows. GPU arguments use individual
  `-puN` tokens; comma-separated tokens are rejected. External GPU inspection
  remains once at actual startup, warning-only, with no later activity scans.
- Install Loguru and Colorama explicitly in the focused Windows CSV workflow,
  matching the maintained runner/dashboard imports.
- Use maintained Loguru and live-dashboard helpers; keep subprocess ownership,
  durable completion callbacks and CSV history in their current domain-specific
  boundaries. Share pure argument interpretation without changing valid history
  keys as a side effect of extraction.
- Compose each grouped finding with its original `PreflightIssue` rather than
  copying a reduced set of fields. Preserve public findings and source provenance.
- Remove the unused flags-only parser and 55 retired investigation/evidence files
  added by this workstream. Current user guidance and the scope pack remain.

The new import policy intentionally normalizes engine switches before history
identity is computed. An older imported command can therefore match differently
even when normalization only moves an engine switch. Existing CSV rows remain;
an affected run is pending again. Once normalized, changes to ignored source
engine/GPU switches do not affect identity. No old-parser compatibility shim
or CSV migration is introduced.

Background advisory validation uses one upstream I/O worker to avoid an executor
pool extending interpreter shutdown. Explicit checker/check-only operations
retain their normal parallel settings.

## Ownership

| Boundary | Maintained implementation |
| --- | --- |
| Editable settings, CLI, banners and pause | User-facing checker and runner under `ryan-scripts/TUFLOW-python/model_management` |
| Run parsing and pure argument interpretation | `ryan_library/functions/tuflow/command_plan.py` |
| Public PyTUFLOW validation | `ryan_library/orchestrators/tuflow/input_preflight.py` |
| Multi-TCF grouping and command provenance | `ryan_library/orchestrators/tuflow/command_preflight.py` |
| Scheduling, export and advisory lifecycle | `ryan_library/orchestrators/tuflow/simulation_runner.py` |
| Durable continuation history | `ryan_library/functions/tuflow/execution_history.py` |
| Startup process/GPU snapshot | `ryan_library/functions/tuflow/gpu_occupancy.py` |

The standalone activity tool remains independent. Raster-header/CRS work in
[PyTUFLOW PR #59](https://github.com/Chain-Frost/PyTuflow/pull/59) remains outside
this ryan-tools release and does not restore a local raster validator.

## Validation and delivery

Verified on 2026-10-31 using Linux Python 3.14.8, the real accepted PyTUFLOW
archive at `05e35476c02d6d8ed17c83caf1cb0d185ce51d1b`, and temporary validation
dependencies. No TUFLOW solver or production model was launched.

| Check | Result |
| --- | --- |
| Combined focused pytest | 298 passed, 6 native-Windows cases skipped |
| Strict Pyright 1.1.414, Python 3.14 / Windows target | 21 changed/new Python files; 0 errors, 0 warnings |
| Ruff check / format check | Passed; all 21 files already formatted |
| Repository Loguru formatting policy / Python compilation | Passed for the same 21 files |
| Wheel verification build | Passed; version 26.10.31.1 unchanged |
| Installed wheel and copied wrapper smoke | Passed from a neutral directory without checkout imports |
| Documentation | Seven default documents and eight touched status/guide files checked; remaining-file index coverage passed |
| Whitespace / deletion references | Passed; all 55 retired files absent and no remaining Markdown references found |

The focused test command was:

```bash
PYTHONPATH=/tmp/ryan-tools-pr159-py314-deps /root/.local/bin/python3.14 -m pytest -q -rs \
  tests/functions/tuflow/test_command_plan.py \
  tests/functions/tuflow/test_execution_history.py \
  tests/functions/tuflow/test_gpu_occupancy.py \
  tests/functions/tuflow/test_pytuflow_compatibility.py \
  tests/orchestrators/tuflow/test_input_preflight.py \
  tests/orchestrators/tuflow/test_command_preflight.py \
  tests/orchestrators/tuflow/test_command_preflight_real.py \
  tests/orchestrators/tuflow/test_simulation_runner.py \
  tests/scripts/tuflow/test_command_preflight_wrapper.py \
  tests/scripts/test_run_tuflow_simulations.py \
  tests/scripts/test_tuflow_advisory_runner.py \
  tests/scripts/test_tuflow_csv_continuation.py \
  tests/scripts/test_tuflow_preflight_opt_in.py \
  tests/repo_scripts/test_install_latest_wheel.py \
  tests/repo_scripts/test_install_from_pypi.py \
  tests/scripts/test_codex_mcp_setup.py \
  tests/mcp/test_registry.py
```

Modified-only static checks derive the file set from tracked changes plus new
Python files, excluding deleted files and submodules. The temporary strict
configuration is `/tmp/ryan-tools-pr159-modified-pyright.json`; its 21-file list
is `/tmp/ryan-tools-pr159-modified-python.json`. Includes are relative to the
configuration directory; import paths cover the repository, vendor, installer
helpers and temporary dependencies. No diagnostic rule was relaxed. An earlier
combined typing invocation with invalid absolute include paths was interrupted
and is not counted; the corrected bounded check above completed successfully.

Build command:

```bash
PYTHONPATH=/tmp/ryan-tools-pr159-py314-deps /root/.local/bin/python3.14 repo-scripts/build_library.py --no-bump --skip-pip
```

The verified wheel is `dist/ryan_tools-26.10.31.1-py3-none-any.whl`, SHA-256
`be47b71fd2ce81721a99003573d21d0561462eee78099e1e6fdbb5da6c969368`.
The copied wrappers came from a fresh installation of that wheel. The import
origin was the temporary installed target, and the public capability guard
confirmed actual PyTUFLOW 1.2.7 from the pinned archive. Wheel installation used
`--no-deps` only to isolate this verification target; the normal end-user
installation remains dependency-aware.

The real two-run checker reported the deliberately missing dependency, returned
the expected failure status, and created no default CSV. An explicit report
created only its requested CSV. Export-only returned success twice, refreshed
the same wrapper-named TXT, used Python `BCMD=-b`, discarded imported
`-t/-x/-nlc/-pu7`, and retained the model option `-et1`. No solver, history CSV,
BAT export, timestamped output or leftover temporary output was created.
Temporary evidence is at
`/tmp/ryan-tools-final-wheel-smoke-i39xzrim/smoke-results.json`.

The six skipped cases concern Windows command-path export and associated CSV
failure handling; they remain enabled on Windows. The Linux smoke uses a
portable bare executable token. Native Windows process/priority behavior and
the full Windows GIS bootstrap still require acceptance on the delivered
candidate. Prior Windows CI is not evidence for these local modifications.

Documentation checks use the normal seven-document checks and explicit
`--links-only` checks for touched guides/status files. During the original unstaged validation, the stock central-index check still
saw nine deleted Markdown files in the Git index. Checking its findings against
the exact deletion set confirmed every remaining document was indexed. The
publication tree includes those deletions; the normal index check is required
before delivery and passed after staging the exact publication tree. No checker
rule was relaxed.

Only the required `tests/test_data` and `qgis-resources` submodules were
initialized for testing and packaging. Their pinned revisions are unchanged.
The new installer regression test is included in this delivery. After the
initial local-only handoff, the owner explicitly clarified that the work must
be delivered to GitHub. The complete tested cleanup is published together on
#159, using the latest remote commit as its parent and a checked fast-forward
branch update. No PR merge or release is included.

## Concurrent integration checkpoint

While this cleanup was being verified, remote #159 advanced to documentation-only
commits `624189c3f97a44f690a2c4cd210a07e99e4d7d7a` and
`5810fba8def32aa3570c0684cfa7d99c18caae25`. They change only this status
record and the work-register row; no implementation changed from local baseline
`5edc2f5371da3a55faec4cbab2115e43574f1c43`.

The [remote checkpoint](https://github.com/Chain-Frost/ryan-tools/blob/5810fba8def32aa3570c0684cfa7d99c18caae25/docs/work/2026-10-23-tuflow-runner-model-checks.md)
preserves the restored native merge history: task PRs #170, #172, #173, #174,
#176 and #177 are merged into #159, and all nine checks passed on implementation
head `5edc2f5`. #159 remained open against main. This concise record preserves
that delivery evidence without carrying forward retired investigation logs.
Those earlier checks do not validate this cleanup. Both concurrent documentation
edits have been reconciled into this current record and its register row. New
checks must be assessed on the delivered commit; no main merge or release is
included.

## Retired history

The previous investigation material and detailed interim status are available
in [the reviewed Git revision](https://github.com/Chain-Frost/ryan-tools/tree/5edc2f5371da3a55faec4cbab2115e43574f1c43/docs/work).
They are removed from the final working tree at the owner's request; they are
not outstanding benchmark tasks or additional acceptance gates.
