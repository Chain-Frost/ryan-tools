# Live dashboard logging regression

| Field | Value |
| --- | --- |
| Status | Locally validated; PR delivery and external Ruff/CI pending |
| Owner | ChatGPT |
| Created / Updated | 2026-11-03 |
| Next review | 2026-11-04 |
| Branch | `fix/live-dashboard-console-routing`, based on `b2b07da1` |
| Next action | Open the bug-fix PR; run Ruff and CI on another system as requested. |

The reported copied runner uses wrapper 2026-11-01.5 and library 26.11.2.3.
The shared renderer did not change between that version and the initially inspected
26.11.3.1 checkout. The 1 November runner integration (`99cd3cca`) replaced the older
runner's Rich console messages with Loguru output. A stdout stream captured before
Rich Live starts bypasses its cursor clearing and redraw, matching the partial
headers accumulating at launches and completions.

Main already includes PR #190 (`b2b07da1`, declared library 26.11.3.6), which supplies
a dynamic stdout writer in the new wrapper. Older copied wrappers still use the
library default. This fix makes that default follow current stdout on every write,
without changing explicit writers, file sinks, notebook routing or solver execution.
No new MCP surface is needed: this repairs existing presentation without changing
workflow inputs, outputs or safety semantics.

The focused regression fails on the captured-stream baseline and passes with the
fix. It checks clear-before-message/redraw-after-message sequences, multiline text,
three terminal sizes, file-log isolation, post-Live output, redirected output and
explicit custom sinks. Actual VS Code disconnect/reconnect is not reproduced;
terminal reflow is a separate remaining limitation, not a claimed fix.

The user's running process, project wrapper and simulation outputs remain untouched.
No release or merge is authorised by the request to create a PR.

## Validation and delivery

- Focused logging, dashboard/workflow and simulation-runner tests: **96 passed,
  2 skipped** on Windows Python 3.14.8. The two skips require Windows symlink
  creation privileges; no TUFLOW solver was launched.
- Strict Pyright with explicit `-p pyproject.toml` on both changed Python files:
  **0 errors, 0 warnings**. Explicit configuration avoids an unrelated drive-root
  Python 3.12 configuration discovered by the local Pyright launcher.
- Documentation/index checks, Loguru formatting policy and Git whitespace pass.
- `python repo-scripts/build_library.py --skip-pip` built and verified
  `dist/ryan_tools-26.11.3.7-py3-none-any.whl`; the local ignored wheel is not committed.
  Installed user packages are unchanged.
- Local Ruff execution is denied by Windows. The owner requested logic work here
  and Ruff/CI on another system; those checks remain explicitly pending.
- The local pytest cache directory is inaccessible; focused runs use a temporary
  cache outside the checkout and ignore failed `pytest-cache-files-*` entries.
