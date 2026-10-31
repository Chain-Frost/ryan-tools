# Ryan Scripts

This folder includes **maintained standalone scripts**, **maintained
library-backed wrappers**, project-specific utilities and historical code.
All maintained entry points follow the [shared principles below](#shared-principles-for-maintained-scripts-and-wrappers);
library-backed wrappers additionally follow
[`WRAPPER_STANDARD.md`](WRAPPER_STANDARD.md). The wrapper standard defines
shared-library use, opening/closing version banners and optional interactive
completion, but does not require an orchestrator.

Read each script's module docstring before running it to confirm its inputs,
outputs, editable settings and whether paths are relative to the working
directory, the script location or another configured base.

## Shared principles for maintained scripts and wrappers

**Maintained** describes a support and safety expectation, not a required code layout.
Choose the simplest appropriate entry point:

- **Maintained standalone script:** self-contained code in one file. No compulsory `ryan_library`,
  wrapper layer, orchestrator, Loguru dependency or installation-time registration.
- **Library-backed wrapper:** a human-editable script calling reusable `ryan_library`
  functions/processors directly. Add an orchestrator only if coordination is
  substantial, reusable or independently testable.
- **Complex workflow:** a wrapper calling an orchestrator where multi-stage
  coordination genuinely belongs in a separate library module.

The following principles apply to **all actively maintained Python entry points**,
including standalone scripts, unless their documented execution context makes
one irrelevant. They do **not** retroactively apply to frozen/archived,
experimental, vendored or versioned legacy files solely because they exist.

1. **Purpose and identity:** a module docstring states purpose, input/output
   effects, dependencies, environment and at least one usable invocation.
   Keep a visible date-based `SCRIPT_VERSION` or `WRAPPER_VERSION`
   (`YYYY-MM-DD.N`) near the top; identify the actual script/version when run,
   without relying on file modification times. An argparse `--version` is useful.
   Library-backed wrappers report both script and installed `ryan-tools` versions.
2. **Complete editable configuration:** expose **every operational CLI
   option** as a visible `DEFAULT_*` constant near the top, including
   modes, paths, output/write controls, logging and pause behaviour. This
   is mandatory for maintained standalone scripts as well as wrappers.
   CLI arguments override the editable defaults in either direction;
   parser omission must remain distinguishable from an explicit `False`,
   `0` or empty value. Built-in `--help` and `--version` are exempt.
   Put the module docstring, minimal imports, version and defaults before
   operational imports wherever practical.
3. **Clear process boundary:** use a guarded `if __name__ == "__main__"` and
   callable `main()` rather than performing filesystem work on import.
   For command-line scripts provide `--help` and meaningful exit codes:
   `0` success, `1` failed processing/validation, `2` malformed arguments.
   Scripts that may pause must provide a headless/no-pause path; never make
   noninteractive automation wait for user input.
4. **Safe and explicit operations:** ship read-only, preview and non-chargeable
   defaults. Allow a deliberate user edit to an exposed `DEFAULT_*` constant,
   or a CLI override, to opt into writing or costly storage retrieval.
   Preserve required output paths, non-overwrite protection, optional dry-runs
   and independent confirmations for destructive operations; a configured
   default must not bypass those safeguards. Report partial failures clearly.
5. **File paths and data integrity:** resolve relative inputs against the
   intended base before changing the working directory; use `Path.absolute()`
   for normal display and mapped-drive/UNC preservation. Use `Path.resolve()`
   only when physical identity or containment actually matters. Respect
   locks, symlinks/junctions, output collisions and original data bytes.
6. **Logging and resources:** give concise user-facing progress, meaningful
   failure diagnostics, and clear output locations. Use parameterised Loguru
   messages when Loguru is already part of the workflow; a lightweight
   standalone script may instead use standard logging or `print`.
   Use context managers and close datasets, files and processes explicitly.
7. **Proportionate checks:** format and lint changed Python using the repository
   Ruff configuration, use Python 3.14 typing and strict Pyright on modified
   maintained code, compile and check `--help`, and run focused disposable
   success/failure tests for changed behaviour. Confirm missing-input and
   no-pause/error-code behaviour. When code depends on an installed
   `ryan_library` wheel, also test a copied wrapper outside the checkout.
   Native GDAL/QGIS/TUFLOW or Windows acceptance is required only when the
   functionality depends on that runtime; document remaining boundaries.

See [the maintained-wrapper standard](WRAPPER_STANDARD.md) for its additional
shared-library, two-banner, package and optional pause requirements.
Only split implementation into the library when reuse or testing warrants it;
do not introduce forwarding modules merely to satisfy a diagram.

## Minimal standard for maintained standalone scripts

A maintained self-contained script follows the **shared principles above**
without adopting the full library-backed wrapper structure. Its processing
can remain inside a well-documented `main()` with small local helpers.
Keep `SCRIPT_VERSION`, editable settings and the guarded execution path.
Display its version and relevant tool/Python versions as appropriate, and
preserve a useful noninteractive invocation. A second closing banner,
`print_wrapper_banner()`, `pause_console()`, `WRAPPER_VERSION`,
`ryan_library` imports, an orchestrator, and MCP registration are **not**
required solely because the script is maintained.

For genuinely interactive or long-running standalone scripts, a completion
banner and explicit `--no-pause` may still be useful. For a narrow read-only
utility, an ordinary argparse CLI plus a clear exit code is sufficient.
Do not demote a maintained script to "legacy" merely because it remains
self-contained. Validation is proportional to what the script actually does.

## QGIS project file audit and repair

`other/audit_qgis_projects.py` inspects QGS/QGZ XML without starting QGIS or
loading referenced geospatial datasets. It has **two explicit audit modes**:

- `--mode existence` (default): check local source-file existence using
  filesystem metadata; **do not open or read their contents**. Network
  metadata requests may still occur.
- `--mode cold-storage-ping` or `DEFAULT_AUDIT_MODE = "cold-storage-ping"`
  (opt-in): open each distinct local source file and request one byte using
  an unbuffered read, to prompt tiered network storage to rehydrate data.
  This can be slow or incur retrieval charges. Empty files cannot provide
  a byte; results depend on storage-server behaviour.

```powershell
python .\ryan-scripts\other\audit_qgis_projects.py --working-directory "Q:\Project\GIS" --mode existence --no-pause
python .\ryan-scripts\other\audit_qgis_projects.py --working-directory "Q:\Project\GIS" --mode cold-storage-ping --no-pause
python .\ryan-scripts\other\audit_qgis_projects.py --project "Q:\Project\GIS\Model.qgz" --auxiliary-report --no-pause
python .\ryan-scripts\other\audit_qgis_projects.py --project "Q:\New\GIS\Model.qgz" --old-project-directory "Q:\Old\GIS" --no-pause
python .\ryan-scripts\other\audit_qgis_projects.py --project "Q:\New\GIS\Model.qgz" --old-project-directory "Q:\Old\GIS" --output "Q:\New\GIS\Model_fixed.qgz" --write --no-pause
```

Path repair is separate from cold-storage ping and is preview-only unless
writing is enabled by `DEFAULT_WRITE = True` or `--write`, **and** a new output
project is selected via `DEFAULT_OUTPUT` or `--output`. CLI `--no-write`
can override the editable write setting. If the output
project is in a different directory, relative data-source paths are calculated
against the **output** project's directory, in both previews and writes.
Where Windows source and destination drives differ, an absolute source path
is retained rather than constructing an invalid relative path. Original
project files are never overwritten. The repair tool preserves embedded
QGZ archive members (including auxiliary SQLite `.qgd` files), or copies
the companion `.qgd` beside a repaired `.qgs`.
`--auxiliary-report` lists auxiliary tables, QGIS layer associations,
column names and row counts; `--sample-rows 1` through `5` explicitly
includes sample stored values. No PyQGIS installation is required. Complex
provider URIs are skipped rather than guessed, and actual QGIS compatibility
still requires representative project validation.

Further project-management ideas are deferred in
[issue #169](https://github.com/Chain-Frost/ryan-tools/issues/169) until
PR #137 merges; TUFLOW TLF log discovery is independent of QGIS projects.

## Installed copies

The wheel currently installs **selected scripts**, not the entire repository script tree, beside `ryan_library`
in `site-packages`. MCP locates installed scripts automatically; no repository path is required. The
[script manifest](../ryan_library/resources/mcp/installed_scripts.json) is a **provisional** install allowlist
pending [individual lifecycle review](../docs/audits/2026-10-19-installed-scripts-inventory.md), and preserves
the original file bytes. Other legacy, experimental and project-specific scripts remain repository-only
until individually reviewed. Only catalogued workflows are exposed through MCP.

After installing `ryan-tools`, use the copyable wrapper helper from any directory:

```powershell
ryan-scripts list
ryan-scripts copy file-management-python/archive_compress_folder.py . --dry-run
ryan-scripts copy file-management-python/archive_compress_folder.py .
```

The destination must already exist. An existing file is protected unless `--overwrite` is explicitly provided.
Replacement uses an atomic destination-directory staging file so a symlink swapped into the
destination after validation is replaced as an entry, not followed to another file.
The helper copies **one original script**, without executing it or copying external dependencies. Inspect its
editable defaults and ensure any required library version or external applications are available before running.
See [MCP setup](../docs/MCP_SETUP.md#installed-scripts-and-development-checkouts).

## Set up Python

The repository targets Python 3.14. From the repository root, install the project and its dependencies into the user's
normal Python installation. Users do not need to create or activate a virtual environment:

```powershell
py -3.14 -m pip install -r requirements.txt
```

Installing the project is important because many wrappers import reusable functions from `ryan_library`. Copying a
wrapper elsewhere without installing the matching package can cause an import error or can run an older installed
version of the library.

## Choose and run a script

Scripts use one of these patterns:

1. **Command-line tool**: ask the script for its supported arguments, then run it with explicit inputs.

   ```powershell
   python .\ryan-scripts\point-cloud-python\clean_xyz.py --help
   ```

2. **Editable wrapper**: open the script, review the user-editable constants near the top or in `main()`, then run it.

   ```powershell
   python .\ryan-scripts\TUFLOW-python\raster_processing\run_asc_to_asc_maximum_searches.py --dry-run
   ```

3. **Folder-driven utility**: place the script with the input files, or change to the intended working directory before
   running it. The module docstring states which location is scanned.

   ```powershell
   Set-Location C:\path\to\model\results
   python E:\Library\Automation\ryan-tools\ryan-scripts\TUFLOW-python\model_management\collect_result_and_check_files.py
   ```

Use quoted paths when a folder or filename contains spaces. Run Python scripts from a terminal when possible so that
log messages, prompts, and failures remain visible.

## Folder guide

| Folder | Typical purpose |
| --- | --- |
| `12D-python` | Convert or thin terrain data and generate 12d culvert geometry. |
| `AutoCAD-python`, `cad-python` | Extract, group, dissolve, and convert CAD/DXF and Surpac geometry. See the observed binary Surpac [STR](../docs/SURPAC_BINARY_STR_FORMAT.md) and [DTM](../docs/SURPAC_BINARY_DTM_FORMAT.md) formats. |
| `file-management-python` | Copy files or folders and rename Outlook MSG files. |
| `gdal-python` | Current library-backed GDAL raster and point-cloud workflows; see its own README. |
| `gdal-bat`, `TUFLOW-bat`, `misc-bat` | Legacy drag-and-drop or command-shell workflows. Prefer maintained Python replacements and review local paths before use. |
| `hydrology-python` | ARF, IFD, RFFE, and related hydrology calculations. |
| `misc-python` | Small maintained utilities that do not belong to a domain-specific workflow. |
| `pdf-python`, `docx-python` | PDF and Word document utilities. |
| `point-cloud-python`, `raster-python` | XYZ/LAS and raster conversion or cleanup. |
| `RORB-python` | RORB rainfall, hydrograph, peak, and closure-period processing. |
| `TUFLOW-python` | TUFLOW result processing, GIS preparation, copying, and model checks; see its README for maintained-wrapper and shared-CLI conventions. |
| `tuflow` | Standard TUFLOW project initialization; see the [project setup guide](tuflow/PROJECT_SETUP.md). |
| `powershell`, `GlobalMapper` | PowerShell helpers and Global Mapper scripts. |
| `other` | Narrow utilities that do not fit a maintained workflow category; review each module docstring and implementation. |
| `python-not-polished` | Experimental or project-specific scripts; treat these as examples, not stable tools. |

Versioned filenames usually indicate older standalone snapshots. Prefer an unversioned, library-backed wrapper when one
exists. Keep project-specific paths, globs, and output names in wrappers; reusable behavior belongs in `ryan_library`.

Maintained root-level hydraulic wrappers include:

- `culvert.py` for culvert solve/analyse/design/rating workflows;
- `tuflow/tuflow_culvert_from_maximums.py` and `tuflow/tuflow_culvert_from_1d_nwk.py` for TUFLOW culvert checks with an explicit `--engine hy8|ryan-culverts` selection; see [the engine guide](../docs/TUFLOW_CULVERT_ENGINES.md);
- `floodway.py` for overtopping floodway formation assessment using a culvert project plus a separate versioned
  floodway formation JSON/TOML definition. Hydrograph/closure-duration processing is intentionally not part of this
  wrapper.

## Safety and verification

- Start destructive or bulk file-management tools in dry-run mode when available, and keep a backup. Some tools rename,
  overwrite, or delete files after confirmation.
- Replace example network credentials and paths before running copy scripts. Do not commit real passwords.
- Check the output folder and a representative output file before processing a full dataset.
- Use `--help` for CLI tools and `--dry-run` where offered. A script without those options may begin work immediately.
- Scripts labelled **legacy**, **experimental**, **in progress**, **not working**, or **superseded** should be reviewed and
  tested on copied data before operational use.

When changing a wrapper, preserve its editable settings and keep the wrapper thin. Changes to shared behavior should be
made in `ryan_library/functions` or an appropriate orchestrator instead.

A new or materially changed maintained wrapper should be assessed for MCP workflow discovery when it offers a stable,
headless workflow that would be useful across projects. If suitable, catalogue the existing wrapper and describe its
mutation and approval semantics; do not create a duplicate package CLI. Basic utilities, narrow project scripts,
interactive tools and experimental scripts are not automatically MCP candidates.
