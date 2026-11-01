# Python wrapper standard

This document applies to **maintained library-backed wrappers**, not every
maintained script. All maintained entry points share the
[common script principles](README.md#shared-principles-for-maintained-scripts-and-wrappers).
A self-contained tool may instead follow the
[maintained standalone standard](README.md#minimal-standard-for-maintained-standalone-scripts)
without importing `ryan_library`. Neither choice requires an orchestrator.
Do not retrofit archived or project-specific scripts solely for architectural uniformity.

## Purpose

A wrapper is a small, human-editable entry point around reusable behaviour in
`ryan_library`. Wrappers may be copied into project folders, so each file must
remain understandable and identifiable without access to its Git history.

Put independently useful algorithms, parsing and focused I/O in
`ryan_library/functions` (or format-specific processors). A wrapper may
**call those functions directly**. Introduce `ryan_library/orchestrators`
only when workflow coordination is itself substantial, shared or independently
testable; a sequence of two function calls is not sufficient reason.
A wrapper normally owns documentation, editable defaults, argument parsing,
configuration resolution, logging, modest workflow dispatch, and the process
boundary. Do not introduce layers that only forward arguments.

## Recommended file order

1. Raw module docstring with purpose, inputs, outputs and examples.
2. `from __future__ import annotations` when annotations require it.
3. Minimal standard-library imports needed to define editable values, commonly
   `Path`, `Literal`, or another annotation/default constructor.
4. Embedded wrapper identity.
5. User-editable defaults.
6. Remaining standard-library imports.
7. Third-party imports.
8. Absolute `ryan_library` imports.
9. Small wrapper-only types such as CLI option dataclasses.
10. `main(...) -> int` and focused wrapper-level dispatch helpers.
11. CLI parser helper.
12. `if __name__ == "__main__"` process boundary.

Minor adjustments are acceptable when Python dependencies require them, but
keep versions and frequently edited settings prominent. Avoid importing the
`ryan_library` package merely to type a default setting near the top.

An exception is a focused configuration import that supplies user-editable
values, such as enums used in `MODE = SRC.TEXTFILES` or `OPERATION = OP.RUN`.
These imports may precede the editable defaults to provide discoverable choices
and editor completion without placing enum definitions above the settings.
Associated setting type aliases may share that import. Keep the supporting
module lightweight and free of operational side effects, and use the installed
`ryan_library` package so copied wrappers still need only one script file.
This exception does not move workflow, logging or other implementation imports
above the settings; wrapper-only implementation types still belong below them.

Operational imports deliberately follow the editable-default block. This keeps
the settings a user is expected to review immediately below the docstring and
minimal supporting imports. Do not defer imports into `main()` merely to alter
startup time; use a local import only for an optional or unusually expensive
dependency on a rarely used code path.

## Embedded identity

Every maintained wrapper must contain a date-based version that travels with a
copied file:

```python
WRAPPER_VERSION = "2026-08-02.1"
```

Use `YYYY-MM-DD.N`, incrementing `N` for another released wrapper revision on
the same day. Update the value whenever defaults, arguments, output naming or
wrapper behaviour changes. Changes confined to the shared library do not
require changing every wrapper version because the installed library version
is reported separately.

At the start of `main`, print both identities:

```python
print_wrapper_banner(wrapper_file=Path(__file__), wrapper_version=WRAPPER_VERSION)
```

This tells a user which copied wrapper file is running and which installed
`ryan-tools` package supplies its implementation.

Print the same two identities again after `main()` returns. The opening banner
records what is about to run; the closing banner remains visible beside the
completion status and interactive pause, even after lengthy console output.

Do not use the file-system modified timestamp as the wrapper identity. Copying,
extracting or synchronising a file can change that timestamp even when its
contents are unchanged; the embedded revision is stable and reviewable.

## Editable defaults

**Every user-facing operational option** in a maintained wrapper must have
a corresponding user-editable constant near the top of its file.
**No naming prefix is required**: `TCF`, `GPU_DEVICES`, `MODE`, `WORKING_DIR`
and existing `DEFAULT_*` names are all valid. Choose clear, concise names
appropriate to the particular script; consistency and discoverability matter
more than a fixed identifier pattern.
This includes paths, input selection, modes, thresholds, output destinations,
write/dry-run flags, logging levels, and pause/headless behaviour, not only
frequently changed settings. A normal run must be configurable by editing
these constants without requiring CLI arguments. Built-in `--help` and
`--version` are not operational settings and do not need defaults.

An explicitly supplied CLI argument always overrides its corresponding
editable constant; otherwise use that constant's value. Do not bury defaults
inside `argparse` or behind unrelated configuration files. Use `None` as the
parser's value when an option is omitted, especially for booleans and zero:
`--write`/`--no-write` must be able to override a changed `WRITE`
in either direction.

```python
INPUT = Path()
RECURSIVE = False
WRITE = False

parser.add_argument("--input", type=Path)
parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=None)
parser.add_argument("--write", action=argparse.BooleanOptionalAction, default=None)

input_path = (args.input if args.input is not None else INPUT).absolute()
recursive = args.recursive if args.recursive is not None else RECURSIVE
write = args.write if args.write is not None else WRITE
```

Existing scripts need not rename their current constants. This rule
specifies observable behaviour and human-editable configuration, not a
repository-wide migration to a particular spelling.

For shared CLI helpers whose parser defaults cannot distinguish omission,
adapt the local parser or add an opposite override (e.g. `--pause` when the
shared helper defines `--no-pause`). Keep the shared helper's other callers
unchanged. Avoid requiring CLI inputs that have reasonable editable defaults;
one-time secrets and typed destructive confirmations are not normal defaults.

**Safe shipped values, deliberate user overrides:** ship non-destructive,
non-chargeable defaults. An auditor normally starts read-only
(`AUDIT_MODE = "existence"`). A dedicated repair/export tool may use
`WRITE = True` to create a **new, automatically named output copy**
when an input is selected, provided it never changes the input or overwrites
any existing destination. Include an explicit `--no-write` or equivalent
preview override. Deletion, in-place replacement, and chargeable retrieval
still require deliberate opt-in. Keep safe exclusive-create, collision checks
and any independent confirmations for destructive operations intact.

### Explain settings where needed

A user should be able to understand and safely edit the visible settings without
reading the implementation. **Do not require an adjacent comment or docstring
for every setting.** If a descriptive name, value and nearby context make both
its purpose and how to change it reliably obvious, leave it uncommented.
Avoid comments that merely restate the variable name.

For example, these settings need no individual explanation:

```python
PAUSE_ON_FINISH = True
MINIMIZE_ON_LAUNCH = False
CONSOLE_LOG_LEVEL = "INFO"
```

Use a concise adjacent comment, a shared comment for related settings, or a
copy-ready example **only where it adds information the user needs**, such as:

- Accepted choices, numeric ranges or units that cannot be inferred from the
  name and value.
- Non-obvious `None`, empty-value or path-base semantics, including relative
  paths, mapped drives and UNC shares.
- Dependencies between settings or effects that are not apparent from the
  names alone.
- Unusual tuple, list or mapping shapes; show a copy-ready Python example
  rather than making the user inspect parsing code.
- Destructive changes, output replacement, external execution, licence/cost
  implications or other meaningful risks. Explain safeguards and required
  opt-in even when the setting's name looks straightforward.

A single shared note is sufficient when several settings follow the same
convention. Comments may be one line or more detailed according to the
complexity and risk. Do not invent commentary solely to meet a documentation
quota.

For example:

```python
RECURSIVE = False

# None allows different imported TCFs; a relative TCF uses WORKING_DIR.
TCF: Path | None = None

# Each inner tuple is one concurrent run; two GPU flags in a group
# assign both GPUs to that run.
# Example: (("-pu0", "-pu1"), ("-pu2",)) for two concurrent runs.
GPU_DEVICES = (("-pu0",), ("-pu1",))
```

This is a usability rule, not a requirement for a prescribed number of
comments. The **effective settings must still be validated**, and missing or
unclear guidance must not weaken file protection, execution consent or other
safety requirements.

### Validate effective user configuration

Accept convenient, obvious editable forms where useful (for example, a path
string or `Path`, a single filename or a list of filenames, and a documented
enum member or its string value). Convert them **once at the wrapper boundary**
into the strict internal types expected by the library. Never interpret a path
string as a sequence of characters, or flatten a grouped concurrency setting
in a way that changes its meaning.

Validate all effective user settings **before** changing files, appending
history, launching processes or performing other side effects. Distinguish a
deliberately omitted setting (`None` or an empty collection where documented)
from an invalid value. Do not silently accept misspelled mode values, blank
paths, unsupported filename extensions, duplicate device IDs, malformed
event/scenario keys, `"False"` in place of `False`, or non-finite numbers.
Raise an actionable error naming the setting, offending value and one
copy-ready valid example.

**File availability is operation-dependent.** Missing inputs required by the
selected operation are errors. When a configured or referenced file is not
required for the chosen operation (such as a model or solver during
command-only export), emit a warning identifying its path instead of
failing the export. Optional checks must never relax safety-critical launch
or overwrite requirements. A hard validation failure must leave existing
export files and execution history unchanged.

Keep cosmetic or highly specific terminal preferences (such as dashboard alternate-screen rendering options) as editable wrapper constants rather than shared CLI arguments. This prevents cluttering the ``--help`` menu across multiple unrelated scripts with rendering flags that users rarely change per execution.

Do not add repository-root `sys.path` changes. Copied wrappers require the
matching `ryan-tools` wheel or editable package to be installed.

## Filesystem path convention

Use `.absolute()` by default for wrapper paths, working directories and user-editable inputs/outputs. Unlike `.resolve()`, this does not dereference symlinks or Windows junctions, so mapped network-drive paths remain in their user-visible form. Normalize relative CLI paths before changing the working directory; when a path is intended relative to the configured `WORKING_DIR`, join it to that directory first. Canonical physical identity and security checks may still require `.resolve()` and must not be weakened.

## Main function and exit codes

`main` returns an integer suitable for `SystemExit`:

```python
def main(*, working_directory: Path | None = None) -> int:
    target_directory = (working_directory or WORKING_DIR).absolute()
    if not change_working_directory(target_dir=target_directory):
        return 1

    try:
        run_shared_workflow(target_directory)
    except Exception:
        logger.exception("Workflow failed.")
        return 1
    return 0
```

Use these process codes consistently for maintained entry points:

| Code | Meaning |
| --- | --- |
| `0` | Processing completed successfully. |
| `1` | Working-directory, validation or processing failure. |
| `2` | Invalid CLI arguments, normally emitted by `argparse`. |

Library helpers must not call `SystemExit` or pause the console. A small
standalone script may keep its implementation inside its guarded `main()`
and does not need a wrapper or orchestrator.

## CLI and process boundary

CLI arguments override editable defaults. Provide descriptive help and at least
one realistic Windows example. Automation-facing wrappers should expose
`--no-pause`; destructive or in-place behaviour must require an explicit
option or confirmation.

```python
if __name__ == "__main__":
    args = _parse_cli_arguments()
    result = main(working_directory=args.working_directory)
    print_wrapper_banner(
        wrapper_file=Path(__file__),
        wrapper_version=WRAPPER_VERSION,
        leading_blank_line=True,
    )
    if not args.no_pause:
        pause_console()
    raise SystemExit(result)
```

`pause_console()` detects redirected or non-interactive standard input and
returns immediately for normal headless runs. On Windows it reads a console
key directly (without invoking `cmd.exe /C PAUSE`), so UNC working directories
do not trigger the Windows CMD network-path warning. `--no-pause` also protects
AI, CI and terminal tools that allocate a pseudo-terminal.

## Garbage collection

Do not add `import gc` and `gc.collect()` directly to each wrapper. The shared
`pause_console()` helper collects unreachable cyclic objects immediately before
an actual interactive wait. At that point `main()` has returned and its local
references are gone, so collection may reduce memory held while a completed
console remains open for a user to inspect.

Headless runs return from `pause_console()` before importing `gc` or collecting.
Pass `collect_before_pause=False` only if a particular lightweight wrapper has
a measured reason to skip the default human-facing behaviour.

Use explicit context managers or library cleanup APIs for files, datasets,
process pools and external resources. Garbage collection does not guarantee
that native GDAL, NumPy or Python allocator caches will immediately return
their reserved memory to Windows.

## Validation checklist

The common [maintained-script checks](README.md#shared-principles-for-maintained-scripts-and-wrappers)
also apply. For a library-backed wrapper:

- Format modified Python files with `ruff format`.
- Run strict Pyright on modified files.
- Compile modified wrapper folders.
- Run `--help` for changed CLI wrappers.
- Verify a missing working directory returns process code `1` without pausing.
- If `ryan_library` changed, rebuild and install the wheel before testing a
  copied wrapper outside the repository.
- For a supported reusable automation workflow, assess whether the existing
  wrapper belongs in the MCP catalogue. Do not catalogue basic, project-specific,
  interactive or experimental scripts merely because they have a CLI.
- Preserve unrelated worktree changes.
