# Python wrapper standard

This document defines the preferred structure for maintained, library-backed
scripts under `ryan-scripts`. It does not apply retroactively to archived or
versioned standalone utilities.

## Purpose

A wrapper is a small, human-editable entry point around reusable behaviour in
`ryan_library`. Wrappers may be copied into project folders, so each file must
remain understandable and identifiable without access to its Git history.

Reusable processing, discovery and validation belong in
`ryan_library/functions` or `ryan_library/orchestrators`. A wrapper should
contain only documentation, editable defaults, argument parsing, configuration
resolution, logging setup and process-boundary behaviour.

## Required file order

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
10. `main(...) -> int`.
11. CLI parser helper.
12. `if __name__ == "__main__"` process boundary.

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
`ryan_functions` package supplies its implementation.

Print the same two identities again after `main()` returns. The opening banner
records what is about to run; the closing banner remains visible beside the
completion status and interactive pause, even after lengthy console output.

Do not use the file-system modified timestamp as the wrapper identity. Copying,
extracting or synchronising a file can change that timestamp even when its
contents are unchanged; the embedded revision is stable and reviewable.

## Editable defaults

Keep project paths, search patterns, output templates and frequently adjusted
settings together near the top of the file. Use uppercase names and explicit
types where useful. A user must be able to configure an ordinary wrapper run by
editing these constants instead of supplying command-line arguments. CLI values
are optional overrides: resolve an explicitly supplied CLI value first, then
fall back to the corresponding editable default.

Do not put the editable value only in `argparse`'s `default=` parameter. Keep
the visible `DEFAULT_*` constant as the source of the wrapper configuration and
normally let the CLI option default to `None`, preserving the distinction
between "not supplied" and an explicit false, zero or empty value:

```python
DEFAULT_INPUT = Path(".")
DEFAULT_RECURSIVE = False

parser.add_argument("--input", type=Path)
parser.add_argument("--recursive", action=argparse.BooleanOptionalAction, default=None)

input_path = Path(args.input if args.input is not None else DEFAULT_INPUT).resolve()
recursive = args.recursive if args.recursive is not None else DEFAULT_RECURSIVE
```

Avoid required positional or optional CLI arguments for values that are
reasonable editable wrapper settings. A required CLI argument is appropriate
only when there is no safe or meaningful hard-coded default, such as a secret,
a one-off destructive confirmation or input that must be chosen for every run.

Keep cosmetic or highly specific terminal preferences (such as dashboard alternate-screen rendering options) as editable wrapper constants rather than shared CLI arguments. This prevents cluttering the ``--help`` menu across multiple unrelated scripts with rendering flags that users rarely change per execution.

Do not add repository-root `sys.path` changes. Copied wrappers require the
matching `ryan-tools` wheel or editable package to be installed.

## Main function and exit codes

`main` returns an integer suitable for `SystemExit`:

```python
def main(*, working_directory: Path | None = None) -> int:
    target_directory = (working_directory or WORKING_DIR).resolve()
    if not change_working_directory(target_dir=target_directory):
        return 1

    try:
        run_shared_workflow(target_directory)
    except Exception:
        logger.exception("Workflow failed.")
        return 1
    return 0
```

Use these process codes consistently:

| Code | Meaning |
| --- | --- |
| `0` | Processing completed successfully. |
| `1` | Working-directory, validation or processing failure. |
| `2` | Invalid CLI arguments, normally emitted by `argparse`. |

Library helpers must not call `SystemExit` or pause the console.

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
returns immediately for normal headless runs. `--no-pause` also protects AI,
CI and terminal tools that allocate a pseudo-terminal.

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

- Format modified Python files with Black.
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
