"""Read TUFLOW selections from BAT/TXT as inert text, never executing shell code.

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
_GPU_OPTION = re.compile(r"-pu\d+", re.IGNORECASE)
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
    duplicates_dropped: int = 0

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


def is_gpu_switch(token: str) -> bool:
    """Recognize one supported GPU selector; multiple devices use repeated -puN."""
    return _GPU_OPTION.fullmatch(token) is not None


def _argument_parts(
    arguments: Sequence[str], *, include_gpu: bool = True, include_batch: bool = True
) -> tuple[tuple[tuple[str, str], ...], str | None, tuple[str, ...], tuple[str, ...]]:
    """Validate once and retain flag occurrences, TCF, ordered arguments and engine switches."""
    flags: list[tuple[str, str]] = []
    selected_flags: dict[str, str] = {}
    model: list[str] = []
    engine: list[str] = []
    tcf: str | None = None
    index = 0
    while index < len(arguments):
        token = arguments[index]
        lower = token.casefold()
        if _FLAG.fullmatch(token):
            index += 1
            if index == len(arguments):
                message = f"Missing value for {token}"
                raise CommandPlanError(message)
            value = arguments[index]
            if not value or value.startswith("-"):
                message = f"Invalid/unsupported value for {token}"
                raise CommandPlanError(message)
            key = lower[1:]
            if key in selected_flags and selected_flags[key] != value:
                message = f"Conflicting duplicate switch {token}"
                raise CommandPlanError(message)
            selected_flags[key] = value
            flags.append((key, value))
            model.extend((token, value))
        elif is_gpu_switch(token) or lower in _ENGINE_OPTIONS or re.fullmatch(r"-et\d+", lower):
            if (include_gpu or not is_gpu_switch(token)) and (include_batch or lower not in _ENGINE_OPTIONS):
                model.append(token)
                engine.append(token)
        elif lower.endswith(".tcf") and not token.startswith("-"):
            if tcf is not None:
                message = "More than one TCF argument"
                raise CommandPlanError(message)
            tcf = token
        else:
            message = f"Unsupported simulation argument {token!r}"
            raise CommandPlanError(message)
        index += 1
    return tuple(flags), tcf, tuple(model), tuple(engine)


def validate_run_arguments(arguments: Sequence[str]) -> tuple[tuple[tuple[str, str], ...], str | None]:
    """Validate arguments without an executable, retaining repeated flag occurrences.

    Imported and generated runs use the same accepted switch/value grammar.
    The returned flag pairs preserve duplicates; a plan's flag mapping may
    collapse identical repeated switches without changing ordered arguments.
    """
    flags, tcf, _, _ = _argument_parts(arguments)
    return flags, tcf


def _planned_argument_parts(
    run: PlannedRun, *, include_gpu: bool, include_batch: bool = True
) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...], tuple[str, ...]]:
    flags, _, ordered, engine = _argument_parts(run.arguments, include_gpu=include_gpu, include_batch=include_batch)
    if dict(flags) != run.flags:
        message = f"Run {run.index} has inconsistent event/scenario arguments"
        raise CommandPlanError(message)
    return flags, ordered, engine


def model_arguments(run: PlannedRun, *, include_gpu: bool = True, include_batch: bool = True) -> tuple[str, ...]:
    """Return ordered model arguments without the explicit TCF token.

    Event/scenario values remain values even when they end in .tcf. Only GPU
    switches in option positions are omitted when runtime allocation is used.
    Disabling include_batch removes source -b/-x/-t/-nlc so a runner can apply
    its own configured policy; model switches such as -etN remain intact.
    """
    _, ordered, _ = _planned_argument_parts(run, include_gpu=include_gpu, include_batch=include_batch)
    return ordered


def partition_model_arguments(run: PlannedRun) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...]]:
    """Return flag occurrences and other switches, excluding TCF/GPU placement.

    Both parts retain their order and case-sensitive values. The history
    consumer owns canonical sorting and case normalization for its run keys.
    """
    flags, _, engine = _planned_argument_parts(run, include_gpu=False)
    return flags, engine


def deduplicate_plan(plan: CommandPlan) -> CommandPlan:
    """Retain only the first occurrence of each effective model selection.

    Source executables, CMD directory changes, START modifiers, engine
    switches and GPUs are not model selections. TCFs, event/scenario values
    and retained -etN switches are relevant. Preserve first source provenance.
    """
    # Plan deduplication uses the parsed TCF path; Resume identifies runs
    # separately by filename and effective simulation flags.
    seen: set[tuple[PurePath, tuple[tuple[str, str], ...], tuple[str, ...]]] = set()
    selected: list[PlannedRun] = []
    for run in plan.runs:
        _, engine = partition_model_arguments(run)
        signature = (
            run.tcf,
            tuple(sorted(run.flags.items())),
            tuple(sorted(option.casefold() for option in engine if option.casefold().startswith("-et"))),
        )
        if signature in seen:
            continue
        seen.add(signature)
        selected.append(replace(run, index=len(selected) + 1))
    return replace(
        plan,
        runs=tuple(selected),
        duplicates_dropped=plan.duplicates_dropped + len(plan.runs) - len(selected),
    )


def effective_executable(run: PlannedRun, *, fallback_executable: Path, base_directory: Path) -> str:
    """Select the source executable or explicitly based fallback without I/O."""
    if run.executable is not None:
        return run.executable
    return str(
        fallback_executable if fallback_executable.is_absolute() else (base_directory / fallback_executable).absolute()
    )


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
        if not tokens or tokens[0].text.startswith("-"):
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
        occurrences, explicit = validate_run_arguments(tuple(token.text for token in tokens))
        flags = tuple(dict(occurrences).items())
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


def _configured_tcf(
    value: str | None,
    *,
    source: PurePath,
    configuration_directory: PurePath,
) -> PurePath | None:
    """Resolve Python's optional model setting without inspecting the filesystem."""
    if not configuration_directory.is_absolute() or isinstance(configuration_directory, PureWindowsPath) != isinstance(
        source, PureWindowsPath
    ):
        message = "Configuration directory must be absolute and match the source path flavour"
        raise CommandPlanError(message)
    if value is None:
        return None
    try:
        tcf = _path(value, configuration_directory)
    except ValueError as exc:
        message = f"Invalid Python TCF configuration {value!r}: {exc}"
        raise CommandPlanError(message) from exc
    if tcf.suffix.casefold() != ".tcf":
        message = f"Python TCF must have a .tcf suffix: {tcf}"
        raise CommandPlanError(message)
    return tcf


def _require_matching_tcf(runs: list[PlannedRun], expected: PurePath | None) -> None:
    """A configured Python TCF constrains every active imported occurrence."""
    if expected is None:
        return
    for run in runs:
        if run.tcf != expected:
            message = (
                f"{run.source}:{run.line_start}-{run.line_end}: "
                f"Python TCF {expected} conflicts with effective imported TCF {run.tcf}"
            )
            raise CommandPlanError(message)


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
    python_tcf = _configured_tcf(fallback_tcf, source=source, configuration_directory=config_base)
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
    _require_matching_tcf(reader.runs, python_tcf)
    return deduplicate_plan(
        CommandPlan(source, tuple(reader.runs), tuple(reader.directories), reader.ignored, syntax, (source,))
    )


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
        message = "At least one BAT/text input file is required"
        raise CommandPlanError(message)
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
        replace(run, index=position) for position, run in enumerate((run for plan in plans for run in plan.runs), 1)
    )
    syntaxes = {plan.syntax for plan in plans}
    merged_syntax: Syntax | Literal["mixed"] = plans[0].syntax if len(syntaxes) == 1 else "mixed"
    return deduplicate_plan(
        CommandPlan(
            source=plans[0].source,
            runs=runs,
            directories=tuple(requirement for plan in plans for requirement in plan.directories),
            ignored_statements=sum(plan.ignored_statements for plan in plans),
            syntax=merged_syntax,
            source_files=tuple(plan.source for plan in plans),
            duplicates_dropped=sum(plan.duplicates_dropped for plan in plans),
        )
    )
