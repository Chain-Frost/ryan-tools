"""Read a bounded, sequential TUFLOW command plan without executing shell code.

This is NOT a general cmd.exe interpreter. Unsupported flow control, shell
operators, delayed expansion and CALL fail before a partial plan is returned.
Windows path calculations are lexical and can be tested on other platforms.
"""

import ntpath
import posixpath
import re
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path, PurePath, PurePosixPath, PureWindowsPath
from typing import Literal

type Syntax = Literal["batch", "commands"]
_FLAG = re.compile(r"-[es][1-9]", re.IGNORECASE)
_VARIABLE = re.compile(r"%~dp0|%([^%\r\n]+)%", re.IGNORECASE)
_EXE = re.compile(r"tuflow[a-z0-9_-]*(?:\.exe)?", re.IGNORECASE)
_START_OPTIONS = frozenset(
    {
        "/low",
        "/normal",
        "/high",
        "/realtime",
        "/abovenormal",
        "/belownormal",
        "/min",
        "/max",
        "/wait",
        "/b",
    }
)
# Retain these engine/launch switches, but do not reinterpret them as contexts.
_ENGINE_OPTIONS = frozenset({"-b", "-x", "-t", "-nlc"})


class CommandPlanError(ValueError):
    """An incomplete or unsupported command file cannot be certified."""


@dataclass(frozen=True, slots=True)
class PlannedRun:
    """Immutable effective run at its original position, including duplicates."""

    index: int
    source: PurePath
    line_start: int
    line_end: int
    tcf: PurePath
    working_directory: PurePath
    flag_pairs: tuple[tuple[str, str], ...]
    arguments: tuple[str, ...]
    executable: str | None
    # START scheduling hints are provenance only; the Python runner owns priority,
    # process lifetime, concurrency and GPU allocation.
    start_options: tuple[str, ...] = ()

    @property
    def requires_executable_fallback(self) -> bool:
        """True for an abbreviated row that needs the runner's executable."""
        return self.executable is None

    @property
    def flags(self) -> dict[str, str]:
        """Return an independent mapping for the public preflight API."""
        return dict(self.flag_pairs)


@dataclass(frozen=True, slots=True)
class DirectoryRequirement:
    """A directory change must be possible before preflight can claim parity."""

    path: PurePath
    line: int
    source: PurePath


@dataclass(frozen=True, slots=True)
class CommandPlan:
    """All selected runs; construction raises rather than returning partial work."""

    source: PurePath
    runs: tuple[PlannedRun, ...]
    directories: tuple[DirectoryRequirement, ...]
    ignored_statements: int
    syntax: Syntax | Literal["mixed"]
    # 'source' remains the first source for single-file callers; every run and
    # directory requirement carries its own source for multi-file aggregation.
    source_files: tuple[PurePath, ...] = ()

    def manifest(self) -> list[dict[str, object]]:
        """Return private, JSON-serialisable provenance, not benchmark acceptance."""
        return [
            {
                "run_index": run.index,
                "source_file": str(run.source),
                "line_start": run.line_start,
                "line_end": run.line_end,
                "tcf": str(run.tcf),
                "working_directory": str(run.working_directory),
                "flags": run.flags,
                "arguments": list(run.arguments),
                "executable": run.executable,
                "requires_executable_fallback": run.requires_executable_fallback,
                "start_options": list(run.start_options),
            }
            for run in self.runs
        ]


@dataclass(frozen=True, slots=True)
class _Token:
    text: str
    quoted: bool


def _tokens(text: str) -> list[_Token]:
    """Tokenise supported CMD double quotes, not POSIX shell/backslash rules."""
    tokens: list[_Token] = []
    value: list[str] = []
    quoted = False
    started = False
    starts_quoted = False
    for char in text:
        if char == '"':
            if not started:
                starts_quoted = True
            started = True
            quoted = not quoted
        elif not quoted and char in "&|<>()^":
            message = "Shell operators, blocks and inline caret escapes are unsupported"
            raise ValueError(message)
        elif not quoted and char.isspace():
            if started:
                tokens.append(_Token("".join(value), starts_quoted))
                value, started, starts_quoted = [], False, False
        else:
            value.append(char)
            started = True
    if quoted:
        message = "Unclosed double quote"
        raise ValueError(message)
    if started:
        tokens.append(_Token("".join(value), starts_quoted))
    return tokens


def _statements(text: str, syntax: Syntax) -> Iterator[tuple[int, int, str]]:
    """Join unquoted terminal carets, retaining physical source ranges."""
    pending = ""
    continuing = False
    start = 1
    lines = text.splitlines()
    for number, line in enumerate(lines, 1):
        if not continuing:
            start = number
            head = line.lstrip()
            if head.startswith("@"):
                head = head[1:].lstrip()
            if syntax == "commands" and head.startswith(("#", ";", "//")):
                # A list comment is a complete physical line, never a shell
                # continuation capable of swallowing the next active row.
                yield number, number, line
                continue
        pending += line
        comment = pending.lstrip().lstrip("@").lstrip()
        if (comment.startswith("::") or re.match(r"rem(?:\s|$)", comment, re.IGNORECASE)) and pending.endswith("^"):
            message = f"Caret continuation on a comment is unsupported (line {start})"
            raise ValueError(message)
        if pending.endswith("^") and pending.count('"') % 2 == 0:
            pending = pending[:-1]
            continuing = True
            if number == len(lines):
                message = f"Unfinished caret continuation on line {start}"
                raise ValueError(message)
            continue
        yield start, number, pending
        pending = ""
        continuing = False


def _path(value: str, cwd: PurePath) -> PurePath:
    """Apply lexical directory ownership; never dereference network aliases."""
    if not value or any(char in value for char in '%!*?"'):
        message = "Empty, unresolved or wildcard path"
        raise ValueError(message)
    if isinstance(cwd, PureWindowsPath):
        path = PureWindowsPath(value)
        if path.drive and not path.root:
            message = "Drive-relative paths require an unknown per-drive current directory"
            raise ValueError(message)
        joined = cwd / path
        if not joined.is_absolute():
            message = "Path is not absolute in the effective working directory"
            raise ValueError(message)
        return PureWindowsPath(ntpath.normpath(str(joined)))
    if PureWindowsPath(value).drive:
        message = "Windows drive/UNC paths require a Windows working-directory context"
        raise ValueError(message)
    return PurePosixPath(posixpath.normpath(str(cwd / value.replace("\\", "/"))))


def _expand(text: str, environment: Mapping[str, str], source: PurePath, cwd: PurePath) -> str:
    """Single, case-insensitive expansion at the statement's current state."""

    def replace(match: re.Match[str]) -> str:
        if match.group(0).casefold() == "%~dp0":
            separator = "\\" if isinstance(source, PureWindowsPath) else "/"
            return str(source.parent).rstrip("\\/") + separator
        key = match.group(1).casefold()
        if key in environment:
            return environment[key]
        if key == "cd":
            return str(cwd)
        message = f"Undefined or unsupported batch variable %{match.group(1)}%"
        raise ValueError(message)

    result = _VARIABLE.sub(replace, text)
    if "%" in result or "!" in result:
        message = "Unresolved percent expression or delayed expansion is unsupported"
        raise ValueError(message)
    return result


def _simulation_arguments(tokens: list[_Token]) -> tuple[tuple[tuple[str, str], ...], str | None]:
    flags: dict[str, str] = {}
    tcf: str | None = None
    index = 0
    while index < len(tokens):
        token = tokens[index].text
        lower = token.casefold()
        if _FLAG.fullmatch(token):
            index += 1
            if index == len(tokens):
                message = f"Missing value for {token}"
                raise ValueError(message)
            value = tokens[index].text
            if not value or value.startswith("-") or any(char.isspace() for char in value):
                message = f"Invalid/unsupported value for {token}; spaced context values are not supported"
                raise ValueError(message)
            if lower[1:] in flags and flags[lower[1:]] != value:
                message = f"Conflicting duplicate switch {token}"
                raise ValueError(message)
            flags[lower[1:]] = value
        elif lower in _ENGINE_OPTIONS or re.fullmatch(r"-(?:pu\d+(?:,\d+)*|et\d+)", lower):
            pass
        elif lower.endswith(".tcf") and not token.startswith("-"):
            if tcf is not None:
                message = "More than one TCF argument"
                raise ValueError(message)
            tcf = token
        else:
            message = f"Unsupported simulation argument {token!r}"
            raise ValueError(message)
        index += 1
    return tuple(flags.items()), tcf


@dataclass(slots=True)
class _Reader:
    source: PurePath
    cwd: PurePath
    fallback_tcf: PurePath | None
    syntax: Syntax
    environment: dict[str, str]
    runs: list[PlannedRun] = field(default_factory=list[PlannedRun])
    directories: list[DirectoryRequirement] = field(default_factory=list[DirectoryRequirement])
    local_stack: list[dict[str, str]] = field(default_factory=list[dict[str, str]])
    directory_stack: list[PurePath] = field(default_factory=list[PurePath])
    ignored: int = 0
    stopped: bool = False

    def _assignment(self, text: str) -> None:
        value = text.lstrip()
        if value.startswith('"'):
            if not value.rstrip().endswith('"'):
                message = "Unsupported SET quoting or trailing command"
                raise ValueError(message)
            value = value.rstrip()[1:-1]
        key, equal, rhs = value.partition("=")
        if not equal or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", key):
            message = "Only plain SET name=value assignments are supported"
            raise ValueError(message)
        if rhs:
            self.environment[key.casefold()] = rhs
        else:
            self.environment.pop(key.casefold(), None)

    def _change_directory(self, command: str, tokens: list[_Token], line: int) -> None:
        if command == "popd":
            if tokens or not self.directory_stack:
                message = "Unmatched or malformed POPD"
                raise ValueError(message)
            self.cwd = self.directory_stack.pop()
            return
        switch_drive = command == "pushd"
        if tokens and tokens[0].text.casefold() == "/d" and command in {"cd", "chdir"}:
            switch_drive = True
            tokens = tokens[1:]
        if not tokens:
            message = "Directory change requires an explicit path"
            raise ValueError(message)
        if len(tokens) != 1:
            message = "Quote directory paths containing whitespace"
            raise ValueError(message)
        target = _path(tokens[0].text, self.cwd)
        if isinstance(target, PureWindowsPath):
            if target.drive.startswith("\\\\"):
                message = "UNC directory changes require CMD temporary drive mapping; use a mapped drive"
                raise ValueError(message)
            if not switch_drive and target.drive.casefold() != self.cwd.drive.casefold():
                message = "Cross-drive CD without /D depends on per-drive shell state"
                raise ValueError(message)
        if command == "pushd":
            self.directory_stack.append(self.cwd)
        self.cwd = target
        self.directories.append(DirectoryRequirement(target, line, self.source))

    def _start(self, tokens: list[_Token], line: int) -> tuple[list[_Token], PurePath, tuple[str, ...]]:
        """Parse accepted START decorations; never replay shell scheduling."""
        cwd = self.cwd
        title_seen = False
        directory_seen = False
        hints: list[str] = []
        while tokens:
            token = tokens[0]
            option = token.text.casefold()
            if not title_seen and token.quoted:
                title_seen = True
                tokens = tokens[1:]
            elif not token.quoted and option in _START_OPTIONS:
                hints.append(option)
                tokens = tokens[1:]
            elif not token.quoted and option == "/d":
                if directory_seen or len(tokens) < 2 or not tokens[1].text:
                    message = "START /D requires one explicit directory"
                    raise ValueError(message)
                directory_seen = True
                cwd = _path(tokens[1].text, self.cwd)
                if isinstance(cwd, PureWindowsPath) and cwd.drive.startswith("\\\\"):
                    message = "START /D with a UNC working directory requires unsupported CMD mapping"
                    raise ValueError(message)
                self.directories.append(DirectoryRequirement(cwd, line, self.source))
                tokens = tokens[2:]
            elif not token.quoted and option in {"/node", "/affinity", "/separate", "/shared"}:
                message = f"Unsupported START execution modifier {token.text}; runner cannot honor it"
                raise ValueError(message)
            else:
                break
        if not tokens or tokens[0].text.startswith(("-", "/")):
            message = 'START requires a TUFLOW executable; put an empty "" title before a quoted executable'
            raise ValueError(message)
        return tokens, cwd, tuple(hints)

    def _run(self, tokens: list[_Token], start: int, end: int) -> None:
        cwd = self.cwd
        is_start = tokens[0].text.casefold() == "start"
        start_options: tuple[str, ...] = ()
        if is_start:
            tokens, cwd, start_options = self._start(tokens[1:], start)
        bare = tokens[0].text.startswith("-")
        executable: str | None = None
        if not bare:
            executable = tokens[0].text
            if not _EXE.fullmatch(PureWindowsPath(executable).name):
                message = "Unsupported command; expected a TUFLOW executable or bare event/scenario flags"
                raise ValueError(message)
            tokens = tokens[1:]
        if is_start and bare:
            message = "START cannot launch a bare flag row"
            raise ValueError(message)
        flags, explicit = _simulation_arguments(tokens)
        if bare and not flags:
            message = "A bare command row must include event/scenario flags"
            raise ValueError(message)
        selected = explicit
        if selected is None and bare:
            selected = self.environment.get("tcf")
            if selected is None and self.fallback_tcf is not None:
                selected = str(self.fallback_tcf)
        if not selected:
            message = "No effective TCF; supply a TCF argument, SET TCF, or a fallback for bare rows"
            raise ValueError(message)
        tcf = _path(selected, cwd)
        if tcf.suffix.casefold() != ".tcf":
            message = "Effective control file must have a .tcf suffix"
            raise ValueError(message)
        self.runs.append(
            PlannedRun(
                len(self.runs) + 1,
                self.source,
                start,
                end,
                tcf,
                cwd,
                flags,
                tuple(token.text for token in tokens),
                executable,
                start_options,
            )
        )

    def statement(self, raw: str, start: int, end: int) -> None:  # noqa: C901 - single sequential state dispatcher
        text = raw.lstrip()
        if text.startswith("@"):
            text = text[1:].lstrip()
        if not text:
            self.ignored += 1
            return
        if text.startswith("::") or re.match(r"rem(?:\s|$)", text, re.IGNORECASE):
            # Refuse comment/operator combinations instead of silently swallowing
            # a possible second shell command. Ordinary REM run rows are ignored.
            quoted = False
            for char in text:
                if char == '"':
                    quoted = not quoted
                elif not quoted and char in "&|<>":
                    message = "Shell operators in comment statements are unsupported"
                    raise ValueError(message)
            self.ignored += 1
            return
        if self.syntax == "commands" and text.startswith(("#", ";", "//")):
            self.ignored += 1
            return
        if text.startswith(":") and re.fullmatch(r":[\w.-]+\s*", text):
            self.ignored += 1
            return
        expanded = _expand(text, self.environment, self.source, self.cwd)
        tokens = _tokens(expanded)
        if not tokens:
            message = "Expansion produced an empty command"
            raise ValueError(message)
        command = tokens[0].text.casefold()
        rest = tokens[1:]
        if command == "set":
            self._assignment(expanded[3:])
        elif command == "setlocal":
            if any(token.text.casefold() not in {"enableextensions", "disabledelayedexpansion"} for token in rest):
                message = "Only enabled extensions and disabled delayed expansion are supported"
                raise ValueError(message)
            self.local_stack.append(self.environment.copy())
        elif command == "endlocal":
            if rest or not self.local_stack:
                message = "Unmatched or malformed ENDLOCAL"
                raise ValueError(message)
            self.environment = self.local_stack.pop()
        elif command in {"cd", "chdir", "pushd", "popd"}:
            self._change_directory(command, rest, start)
        elif command in {"echo", "echo.", "pause", "title", "cls"}:
            self.ignored += 1
        elif command in {"exit", "goto"}:
            values = tuple(token.text.casefold() for token in rest)
            if (command == "goto" and values == (":eof",)) or (
                command == "exit" and values in {(), ("/b",), ("/b", "0"), ("0",)}
            ):
                self.stopped = True
            else:
                message = "Only normal EXIT [/B] [0] and GOTO :EOF termination are supported"
                raise ValueError(message)
        elif command in {"if", "for", "call", "else", "cmd", "cmd.exe"}:
            message = f"Unsupported shell control flow: {command}; export an explicit ordered run list"
            raise ValueError(message)
        else:
            self._run(tokens, start, end)


def parse_command_plan(
    text: str,
    *,
    source: PurePath,
    initial_directory: PurePath,
    fallback_tcf: str | None = None,
    configuration_directory: PurePath | None = None,
    environment: Mapping[str, str] | None = None,
    syntax: Syntax = "batch",
) -> CommandPlan:
    """Parse the entire supported plan, without filesystem checks or execution.

    ``initial_directory`` is the launch directory, NOT implicitly the BAT's
    folder. Bare flag rows are an intentional command-list extension, including
    in batch mode; they are not executable CMD statements. Environment values
    are supplied explicitly. Unresolved variables do not silently become blank.
    """
    if syntax not in {"batch", "commands"}:
        message = "Unknown command-plan syntax"
        raise CommandPlanError(message)
    if not source.is_absolute() or not initial_directory.is_absolute():
        message = "Source and initial directory must be absolute"
        raise CommandPlanError(message)
    if isinstance(source, PureWindowsPath) != isinstance(initial_directory, PureWindowsPath):
        message = "Source and working-directory path flavours must agree"
        raise CommandPlanError(message)
    if (
        syntax == "batch"
        and isinstance(initial_directory, PureWindowsPath)
        and initial_directory.drive.startswith("\\\\")
    ):
        message = "Use a mapped-drive launch directory for CMD batch semantics; a UNC TCF argument is supported"
        raise CommandPlanError(message)
    config_base = configuration_directory if configuration_directory is not None else initial_directory
    if not config_base.is_absolute() or isinstance(config_base, PureWindowsPath) != isinstance(source, PureWindowsPath):
        message = "Configuration directory must be absolute and match the source path flavour"
        raise CommandPlanError(message)
    try:
        python_tcf = _path(fallback_tcf, config_base) if fallback_tcf is not None else None
    except ValueError as exc:
        raise CommandPlanError(f"Invalid Python TCF configuration {fallback_tcf!r}: {exc}") from exc
    if python_tcf is not None and python_tcf.suffix.casefold() != ".tcf":
        raise CommandPlanError(f"Python TCF must have a .tcf suffix: {python_tcf}")
    reader = _Reader(
        source,
        initial_directory,
        python_tcf,
        syntax,
        {key.casefold(): value for key, value in (environment or {}).items()},
    )
    reader.directories.append(DirectoryRequirement(initial_directory, 0, source))
    start, end = 1, 1
    try:
        for start, end, raw in _statements(text.lstrip("\ufeff"), syntax):
            reader.statement(raw, start, end)
            if reader.stopped:
                break
    except ValueError as exc:
        message = f"{source}:{start}-{end}: {exc}"
        raise CommandPlanError(message) from exc
    if not reader.runs:
        message = f"{source}: No active simulation commands"
        raise CommandPlanError(message)
    if python_tcf is not None:
        for run in reader.runs:
            if run.tcf != python_tcf:
                message = (
                    f"{run.source}:{run.line_start}-{run.line_end}: "
                    f"Python TCF {python_tcf} conflicts with effective imported TCF {run.tcf}"
                )
                raise CommandPlanError(message)
    return CommandPlan(source, tuple(reader.runs), tuple(reader.directories), reader.ignored, syntax, (source,))


def read_command_plan(
    path: Path,
    *,
    initial_directory: Path | None = None,
    fallback_tcf: str | None = None,
    configuration_directory: Path | None = None,
    environment: Mapping[str, str] | None = None,
    syntax: Literal["auto", "batch", "commands"] = "auto",
    encoding: str = "utf-8-sig",
) -> CommandPlan:
    """Read one file atomically; the Python TCF is both fallback and constraint.

    Relative imported paths use each run's effective launch directory. Relative
    Python TCF configuration uses ``configuration_directory`` or the explicitly
    supplied initial directory, never an imported CD/START directory.
    """
    source = path.absolute()
    directory = (initial_directory if initial_directory is not None else Path.cwd()).absolute()
    config_base = (configuration_directory if configuration_directory is not None else directory).absolute()
    selected: Syntax = (
        ("batch" if source.suffix.casefold() in {".bat", ".cmd"} else "commands") if syntax == "auto" else syntax
    )
    try:
        content = source.read_text(encoding=encoding)
    except (OSError, UnicodeError, LookupError) as exc:
        message = f"{source}: Cannot read or decode command input: {type(exc).__name__}: {exc}"
        raise CommandPlanError(message) from exc
    return parse_command_plan(
        content,
        source=source,
        initial_directory=directory,
        fallback_tcf=fallback_tcf,
        configuration_directory=config_base,
        environment=environment,
        syntax=selected,
    )


def read_command_plans(
    paths: Sequence[Path],
    *,
    initial_directory: Path | None = None,
    fallback_tcf: str | None = None,
    configuration_directory: Path | None = None,
    environment: Mapping[str, str] | None = None,
    syntax: Literal["auto", "batch", "commands"] = "auto",
    encoding: str = "utf-8-sig",
) -> CommandPlan:
    """Return one complete, ordered plan from independent BAT/text inputs.

    Each file gets fresh environment and directory stacks. All files are read
    and checked before a plan is returned; no caller should export, validate or
    launch commands while building this plan.
    """
    if not paths:
        raise CommandPlanError("At least one BAT/text input file is required")
    directory = (initial_directory if initial_directory is not None else Path.cwd()).absolute()
    config_base = (configuration_directory if configuration_directory is not None else directory).absolute()
    plans = tuple(
        read_command_plan(
            path,
            initial_directory=directory,
            fallback_tcf=fallback_tcf,
            configuration_directory=config_base,
            environment=environment,
            syntax=syntax,
            encoding=encoding,
        )
        for path in paths
    )
    if len(plans) == 1:
        return plans[0]
    runs = tuple(
        replace(run, index=position)
        for position, run in enumerate((run for plan in plans for run in plan.runs), 1)
    )
    syntaxes = {plan.syntax for plan in plans}
    merged_syntax: Syntax | Literal["mixed"] = plans[0].syntax if len(syntaxes) == 1 else "mixed"
    return CommandPlan(
        source=plans[0].source,
        runs=runs,
        directories=tuple(requirement for plan in plans for requirement in plan.directories),
        ignored_statements=sum(plan.ignored_statements for plan in plans),
        syntax=merged_syntax,
        source_files=tuple(plan.source for plan in plans),
    )
