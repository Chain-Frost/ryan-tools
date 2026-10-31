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
   Each setting must also explain its purpose, permitted values/units,
   `None` meaning, path base and interdependencies where relevant. Include
   a copy-ready commented Python example for non-obvious values such as
   Windows/UNC paths, choice strings, and `tuple[Path, Path]` mappings.
   Document costs and mutation risks beside the setting, not only in `--help`.
   Put the module docstring, minimal imports, version and defaults before
   operational imports wherever practical.
3. **Clear process boundary:** use a guarded `if __name__ == "__main__"` and
   callable `main()` rather than performing filesystem work on import.
   For command-line scripts provide `--help` and meaningful exit codes:
   `0` success, `1` failed processing/validation, `2` malformed arguments.
   Scripts that may pause must provide a headless/no-pause path; never make
   noninteractive automation wait for user input.
4. **Safe and explicit operations:** ship non-destructive, non-chargeable
   defaults. Audits should be read-only; dedicated export/repair scripts may
   create a **new, predictably named file by default** when an input is selected,
   provided original files cannot be changed and existing outputs are never
   overwritten. Offer an explicit preview/no-write override. Deletion,
   in-place replacement, external execution and potentially chargeable retrieval
   require a separate intentional opt-in. Preserve collision checks and
   independent confirmations for destructive operations. Report failures clearly.
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
The [editable-option comment standard](WRAPPER_STANDARD.md#explain-every-editable-option)
applies to **both** standalone scripts and wrappers.
Do not demote a maintained script to "legacy" merely because it remains
self-contained. Validation is proportional to what the script actually does.

## QGIS project source audit (read-only)

[`other/audit_qgis_projects.py`](other/audit_qgis_projects.py) checks QGS/QGZ
data-source references without loading QGIS or opening raster/vector datasets
in its default mode. **This script cannot repair, remap or write projects.**
It supports two editable/CLI audit modes:

- `DEFAULT_AUDIT_MODE = "existence"` (default): use filesystem metadata to
  check whether local file sources exist; no file-content reads. Network
  metadata requests may still occur.
- `DEFAULT_AUDIT_MODE = "cold-storage-ping"` or
  `--mode cold-storage-ping` (opt-in): issue a one-byte unbuffered read per
  distinct local source to request cold-network-storage retrieval. This can
  be slow or chargeable; empty files cannot provide a byte.

Optional `--auxiliary-report` / `DEFAULT_AUXILIARY_REPORT = True` inspects
QGD auxiliary SQLite tables, columns and row counts without modifying them.
`--sample-rows 1` through `5` deliberately includes stored values
(`0` hides them).

```powershell
python .\ryan-scripts\other\audit_qgis_projects.py --working-directory "Q:\Project\GIS" --no-pause
python .\ryan-scripts\other\audit_qgis_projects.py --project "Q:\Project\GIS\Model.qgz" --mode cold-storage-ping --no-pause
python .\ryan-scripts\other\audit_qgis_projects.py --project "Q:\Project\GIS\Model.qgz" --auxiliary-report --no-pause
```

The curated MCP QGIS audit is **read-only** and pins
`--mode existence --no-auxiliary-report --sample-rows 0 --no-pause`,
irrespective of any edited defaults in a copied wrapper.

## QGIS project path repair (separate script)

[`other/repair_qgis_project_paths.py`](other/repair_qgis_project_paths.py)
creates a **new QGS/QGZ copy by default** when source-path changes are needed,
without altering the input project or referenced datasets. It is separately
packaged and copyable, but is **not** exposed as an MCP workflow.
The common processing functions remain in `ryan_library.functions.qgis_project`.

- `--project` / `DEFAULT_PROJECT` selects the single source project.
- `--old-project-directory` / `DEFAULT_OLD_PROJECT_DIRECTORY` interprets
  old relative references after the project file has been moved.
- `--replace-root OLD NEW` / `DEFAULT_REPLACE_ROOT` maps a data-root prefix;
  no referenced data files are moved or modified.
- `--absolute-paths` / `DEFAULT_ABSOLUTE_PATHS` controls whether the new
  references are absolute or relative to the **output** project's directory.
- `DEFAULT_WRITE = True` is the shipped default: write a **new** project
  only when one or more source references change. `--no-write` or
  `DEFAULT_WRITE = False` previews without writing.
- `DEFAULT_OUTPUT = None` automatically names the copy
  `Model_repaired.qgs` / `Model_repaired.qgz` **beside the input project**,
  regardless of the working directory. `--output` or an edited
  `DEFAULT_OUTPUT` selects another new file. Existing projects and QGD
  companions are never overwritten; if the name already exists, the run
  reports an error rather than silently incrementing or replacing it.

```powershell
# Preview path repair without writing a file.
python .\ryan-scripts\other\repair_qgis_project_paths.py --project "Q:\New\GIS\Model.qgz" --old-project-directory "Q:\Old\GIS" --no-write --no-pause

# Write a NEW project automatically at "Q:\New\GIS\Model_repaired.qgz".
python .\ryan-scripts\other\repair_qgis_project_paths.py --project "Q:\New\GIS\Model.qgz" --replace-root "Q:\OldData" "Q:\NewData" --no-pause

# Optionally choose another NEW output file; --write is unnecessary.
python .\ryan-scripts\other\repair_qgis_project_paths.py --project "Q:\New\GIS\Model.qgz" --replace-root "Q:\OldData" "Q:\NewData" --output "Q:\New\GIS\Model_fixed.qgz" --no-pause
```

Both scripts provide user-editable `DEFAULT_*` settings and copy-ready
examples for Windows drive/UNC paths. Relative configurable paths are based on
the configured working directory. Path repair preserves QGZ archive members
(including embedded QGD), or copies a QGS companion `.qgd` if safe, and refuses
existing output collisions or potentially uncheckpointed auxiliary SQLite WAL.
Unknown/complex provider references are skipped instead of guessed. Real QGIS
reopening and representative Windows UNC/mapped-drive acceptance remain
separate operational checks.

Broader project packaging, diff, QA, templates and cartography are tracked in
[issue #169](https://github.com/Chain-Frost/ryan-tools/issues/169),
deferred until PR #137 merges; TUFLOW TLF-run discovery is independent.

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
