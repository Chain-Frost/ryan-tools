# Ryan Scripts

Maintained library-backed wrappers should follow
[`WRAPPER_STANDARD.md`](WRAPPER_STANDARD.md). It defines file layout, embedded
wrapper versions, editable defaults, CLI behaviour, exit codes, headless
pausing and garbage-collection guidance.

This folder contains human-facing entry points for reusable code in `ryan_library`, together with older standalone
utilities retained for reference. Read the module docstring at the top of a Python script before running it: the
docstring identifies whether the script uses command-line arguments, editable settings, the current working directory,
or the folder containing the script.

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
