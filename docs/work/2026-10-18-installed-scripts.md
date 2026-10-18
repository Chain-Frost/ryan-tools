# Installed scripts and distribution rename

| Field | Value |
| --- | --- |
| Status | Needs review |
| Owner | ChatGPT |
| Updated | 2026-10-18 |
| Next review | 2026-10-19 |
| Branch | `feat/codex-mcp-bootstrap` / PR #120 |
| Delivery | Uncommitted local changes; no push or publication |
| Next action | Review uncommitted PR #120 changes, commit/push when authorised, and check hosted CI before releasing. |

## Scope and decisions

The maintainer requests `ryan-scripts` installed directly beside `ryan_library`, automatic MCP discovery without a
machine-specific checkout path, removal of `ryan_functions/__init__.py`, and renaming the distribution to `ryan-tools`.
The scripts remain authoritative under `ryan-scripts`; the build copies their existing structure and companion files.
Catalogued workflows retain their profiles, safety metadata and external environment requirements.
The import package stays `ryan_library`. The remaining compatibility modules keep their existing support deadlines.

## Implementation

- Stage Python, Markdown, BAT, PowerShell, GMS and XCES files from `ryan-scripts` in the wheel and source manifest.
- Resolve MCP script paths through a selected development checkout, then the installed sibling script directory.
- Report `scripts_directory` in MCP health/listing responses; setup omits/removes the generated checkout override by default.
- Support explicit `--repository-root` / `-RepositoryRoot` overrides during setup.
- Remove the old import shim and exclude its namespace from package discovery and verified wheels.
- Rename distribution metadata, installers, version lookups, artifact patterns and PyPI release endpoints.
- Reject overlapping old/new distribution installs; uninstall `ryan_functions` before installing `ryan-tools`.
- Retain earlier local bytecode-policy work and the separate opt-in user environment BAT.

## Validation evidence

Initial focused validation: 68 tests passed across MCP, packaging and Codex setup; scoped strict Pyright passed.
Initial retained wheel `ryan_tools-26.10.18.1-py3-none-any.whl` verified. Isolated target installation outside the checkout
located all 35 catalogued script targets without repository discovery. Installed `--help` checks passed for culvert,
floodway, QGIS project audit and TUFLOW log summary. The removed alias was absent from the installed target.
Final exact-tree validation on Python 3.14:

- `python -m pytest` scoped to MCP, packaging, setup, wrapper utils, POMM utilities, uncertainty workflow and release helpers:
  **179 passed** (no skips).
- Ruff lint and formatting, strict Pyright and compilation: **26 modified Python files passed**.
- `python repo-scripts/build_library.py --skip-pip` followed by final `--skip-pip --no-bump`: passed.
- `python repo-scripts/verify_wheel.py`: passed; wheel contains **146 script files**, no `ryan_functions/` namespace.
- Retained wheel: `dist/ryan_tools-26.10.18.1-py3-none-any.whl`, **1,375,737 bytes**,
  SHA-256 `a254730bcf238bf3f59d4cc97e2c93736e0822a93460ad86fdaf8208609f0aad`.
- Final isolated target: `C:\Users\Ryan\AppData\Local\Temp\ryan-tools-final-d6a48r9m`.
  Installed-wheel smoke and four representative CLI help checks passed from outside the checkout.
- PyPI/local installers, setup and bytecode BAT dry runs passed. Setup `--install` with an existing legacy distribution
  correctly rejected migration before writing configuration or installing packages.
- Documentation checker: seven default documents plus index coverage, and six explicitly selected documents passed.
- actionlint **v1.7.12** passed all workflows. Hosted CI was not run for these uncommitted changes.
- Existing broken editable engine registrations were replaced with the published `ryan-culverts` and `run-hy8`
  distributions for validation; independent engine source directories were preserved.

No new-name package was installed into the user's normal environment, no live Codex config was changed during this work,
and no commits, pushes or publications were performed. Earlier bytecode work remains unstaged alongside this change.

## Publication and migration

The public PyPI JSON endpoint for `ryan-tools` returned HTTP 404 on 2026-10-18; this does not reserve the name.
The user supplied a PyPI pending-publisher screenshot confirming `ryan-tools`, GitHub repository `Chain-Frost/ryan-tools`,
workflow `pypi-publish.yml` and environment `pypi`. These match the workflow configuration. Account state is user-provided
rather than independently inspected; no account changes were made by the agent and no publication has been performed.
The first new-name release must be published before the PyPI installer can work. Until then, use the local wheel.
The retired distribution and new distribution own common package paths: uninstall the old one first.
Do not uninstall it after installing the new one, since that can delete shared package files.

Unrelated untracked `vendor/run_hy8` and `vendor/ryan_culverts` directories were preserved and not packaged.

## 2026-10-18 ? Bounded MCP discovery follow-up

The maintainer requested smaller on-demand catalogue responses. `list_workflows` now defaults to 10 summaries,
accepts a limit of 1?25 and nonnegative offset, and returns `returned_count`, `has_more` and `next_offset`.
Case-insensitive keyword search matches all words against ID, title, domain and purpose; it combines with domain/profile
filters. Matching domain names are returned to aid exploration. `workflow_count` remains the full match count.
The internal registry listing, health totals and explicitly requested complete catalogue are not paginated.
Startup instructions direct agents to narrow searches and follow pagination before fetching one workflow's details.

Validation superseding the earlier artifact:

- Affected MCP/packaging/setup/wrapper/POMM/uncertainty/release tests: **187 passed**, no skips.
- Ruff lint/format and strict Pyright passed for the three files changed in this follow-up.
- `python repo-scripts/build_library.py --skip-pip`: retained wheel **26.10.18.2**, verified.
- Artifact: `dist/ryan_tools-26.10.18.2-py3-none-any.whl`, **1,376,217 bytes**,
  SHA-256 `e4f25b2e67da067275aa176cdae8a6539ee1f334a59705f8f17df9c5719d5f55`.
- Isolated target: `C:\Users\Ryan\AppData\Local\Temp\ryan-tools-pagination-igeod6lv`.
  Installed smoke passed outside the checkout. Installed MCP returned **10 of 28** default-profile matches;
  the next page had no overlapping IDs; `raster clip` matched one workflow; health and full-catalogue totals agreed.
- MCP documentation links passed. Changes remain unstaged/uncommitted; no hosted run or publication claimed.

## 2026-10-18 ? Retain Python 3.13 support for culverts

The maintainer explicitly keeps Python 3.13 supported, including `ryan-culverts`; users with compatibility problems
can upgrade Python. The current dependency declaration is unconditional (`ryan-culverts>=26.10.16.1`), and the package
baseline stays `>=3.13`. Corrected README, environment and MCP documentation that previously excluded solver features
on 3.13. Runtime missing-module checks remain independent of Python version.

Validation: documentation links passed; wheel `26.10.18.3` built and verified. Its METADATA confirms `Requires-Python:
>=3.13` and an unconditional culvert dependency. Isolated installed-wheel smoke passed on Python 3.14.
No direct Python 3.13 runtime validation was performed in this follow-up.
Focused tests initially reported **83 passed, 1 failed**: `test_removed_script_aliases_are_not_exposed` sees the leftover
source `ryan_library/scripts` directory as an importable namespace after separately requested removal work.
A rerun excluding that unrelated removal check reported **83 passed, 1 deselected**. The source directory was preserved.
Artifact size **1,362,577 bytes**, SHA-256 `05b2af1679a973749c4c1d9fdc1832b76d6b227e3a2388c8e12d230a0be9fce4`.
This follow-up remains uncommitted; the preceding implementation has since been committed independently.

## 2026-10-18 ? Assume mandatory culvert dependency is installed

At the maintainer's request, removed all six repeated `required_modules: ["culvert_solver"]` catalogue declarations
and the generic per-workflow dependency-probing schema/registry code used only by those declarations.
MCP relies on the unconditional `ryan-culverts` distribution dependency. Script discovery checks the script path;
module execution targets still check their own entry module and external console targets still check their executable.
Updated documentation and replaced the removed probing tests with a regression check that mandatory culvert imports
are not probed during script discovery.

Validation: **37 MCP tests passed**; scoped Ruff lint/format and strict Pyright passed; MCP documentation links passed.
Built and verified `ryan_tools-26.10.18.4-py3-none-any.whl` (**1,362,267 bytes**), SHA-256
`0db19b6637c3abcdf6d49a6bcdb64f44e72e88e87e56549409fcee5eb46cc6fa`.
Isolated installed-wheel smoke passed on Python 3.14 outside the checkout.
The previously recorded unrelated source-namespace test gap remains outside this change. No commit/push/publication.

## 2026-10-18 ? Package-and-install migration launchers

The maintainer reported that both package-and-install BATs stopped on the retired distribution guard.
Both now pass `--replace-legacy` to the local wheel installer. The force launcher also forwards caller arguments.
With this explicit migration choice, the installer verifies the local wheel, runs the selected dependency bootstrap
commands, uninstalls `ryan_functions`, and reinstalls the replacement wheel. Migration forces replacement even if the
new distribution's version is already installed, because removing the old owner may remove shared package files.
The force path retains `--no-deps`; the normal path retains dependency installation. Without migration opt-in,
standalone installers keep the existing guard. No old-name checks trigger after the legacy distribution is absent.

Validation: **36 setup/installer tests passed, 1 symlink-permission skip**; Ruff lint/format and strict Pyright passed.
Tests cover migration ordering, no-op when the old distribution is absent, and stopping before uninstall when wheel
verification fails. Both force and normal migration commands were reviewed in dry runs. Documentation links and diff
checks passed. No actual package migration was run in the user's normal environment; no commit or push performed.
The user's pre-existing package version change and retained wheel 26.10.18.6 were preserved.

## 2026-10-18 ? Default migration in the main installers

The maintainer requested that migration belong to the main installer rather than calling BAT files.
Removed `--replace-legacy` from the CLI and both package-and-install BATs. The local-wheel and PyPI installers now
share automatic legacy-distribution migration. Local migration verifies the wheel; PyPI migration resolves the intended
replacement with `--force-reinstall --dry-run` before any uninstall. Both remove the old owner immediately before the
replacement install; no legacy installation means the original install commands run unchanged.
The force BAT retains caller-argument forwarding. Direct pip migration instructions remain documented separately.
This supersedes the preceding opt-in migration design.

Validation: **35 installer/setup tests passed, 1 symlink-permission skip**; Ruff lint/format and strict Pyright passed.
Tests exercise local migration ordering, absence of the old distribution, pre-uninstall verification failure, and default
PyPI migration/preflight ordering. Local force and PyPI installer dry runs passed. Documentation and diff checks passed.
No installation, version bump, wheel rebuild, commit or push performed by this follow-up; concurrent user build/version
changes were preserved.


## 2026-10-18 ? Live installed MCP acceptance and generic user configuration

The maintainer requested a real MCP test and identified a leftover checkout override in the live Codex configuration.
Removed only `RYAN_TOOLS_REPOSITORY_ROOT` from `C:\Users\Ryan\.codex\config.toml`, verified all other parsed
settings were unchanged, and backed up the original as `config.toml.bak.20261018-230000-406532`.

Launched the exact configured Python/module command through an MCP stdio client from the Windows temporary directory,
outside the checkout. Installed `ryan-tools` **26.10.18.8**, MCP SDK **2.3.0**, Python **3.14.6**.
Protocol initialization and enumeration passed: **7 tools**, **2 resources**. Health reported the installed
`site-packages/ryan_library`, sibling `site-packages/ryan-scripts`, `repository_root: null`, profile `create`,
**35 total / 28 visible / 28 available** workflows and no warnings.
Default listing returned **10 of 28**; the following page had no overlapping IDs. `raster clip` search resolved an
existing installed clipping script. Filename parsing returned Q, AEP 01.00p, duration 060m and TP01. Guidance resource
reading passed. All **37 focused MCP tests** passed as well.
This tests a real stdio MCP process using the configured command, not attachment of new tools to this existing chat.
Restart Codex to reload its edited configuration. No package installation, commit, push or publication performed.
