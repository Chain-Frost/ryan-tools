"""Package-native registry for discovering ryan-tools CLI workflows."""

from __future__ import annotations

import importlib.metadata
import json
import os
import re
import shutil
import sys
from collections.abc import Mapping
from importlib.resources import files
from importlib.resources.abc import Traversable
from importlib.util import find_spec
from pathlib import Path
from typing import Any, cast

from ryan_library.mcp.models import (
    CapabilityProfile,
    ExecutionKind,
    ExecutionTarget,
    MutationKind,
    WorkflowSpec,
    profile_allows,
)

DEFAULT_PROFILE: CapabilityProfile = CapabilityProfile.CREATE
PROFILE_ENVIRONMENT_VARIABLE = "RYAN_MCP_PROFILE"
REPOSITORY_ROOT_ENVIRONMENT_VARIABLE = "RYAN_TOOLS_REPOSITORY_ROOT"
_WRAPPER_VERSION_PATTERN: re.Pattern[str] = re.compile(
    pattern=r'^WRAPPER_VERSION\s*=\s*["\']([^"\']+)["\']', flags=re.MULTILINE
)


class WorkflowRegistryError(RuntimeError):
    """Raised when the workflow registry or its configuration is invalid."""


def _looks_like_repository(path: Path) -> bool:
    return (path / "pyproject.toml").is_file() and (path / "ryan-scripts").is_dir()


def resolve_repository_root(explicit_root: Path | None = None, *, discover: bool = True) -> Path | None:
    """Resolve the checkout containing CLI wrappers, if one is available."""
    candidates: list[Path] = []
    if explicit_root is not None:
        candidates.append(explicit_root)

    if discover:
        configured_root: str | None = os.environ.get(REPOSITORY_ROOT_ENVIRONMENT_VARIABLE)
        if configured_root:
            candidates.append(Path(configured_root))

        source_candidate: Path = Path(__file__).resolve().parents[2]
        candidates.append(source_candidate)

        current_directory: Path = Path.cwd()
        candidates.extend((current_directory, *current_directory.parents))

    checked: set[Path] = set()
    for candidate in candidates:
        normalized: Path = candidate.expanduser().absolute()
        if normalized in checked:
            continue
        checked.add(normalized)
        if _looks_like_repository(normalized):
            return normalized
    return None


def _load_json_object(text: str, *, source: str) -> dict[str, Any]:
    try:
        raw: object = json.loads(text)
    except json.JSONDecodeError as exc:
        msg = f"Invalid JSON in {source}: {exc}"
        raise WorkflowRegistryError(msg) from exc
    if not isinstance(raw, dict):
        msg = f"Workflow catalogue must contain a JSON object: {source}"
        raise WorkflowRegistryError(msg)
    return cast("dict[str, Any]", raw)


def installed_scripts_directory() -> Path | None:
    """Locate wheel scripts beside the imported package, independently of a checkout."""
    package_parent = Path(__file__).absolute().parents[2]
    directory = package_parent / "ryan-scripts"
    # A source tree is handled by checkout discovery, not advertised as wheel content.
    return directory if directory.is_dir() and not _looks_like_repository(package_parent) else None


def _load_packaged_workflows() -> tuple[str, str | None, list[WorkflowSpec]]:
    resource: Traversable = files("ryan_library.resources.mcp").joinpath("workflows.json")
    raw_catalogue: dict[str, Any] = _load_json_object(
        text=resource.read_text(encoding="utf-8"),
        source="packaged workflows.json",
    )
    schema_version: Any = raw_catalogue.get("schema_version")
    if schema_version != "1.0":
        msg = f"Unsupported workflow catalogue schema: {schema_version!r}"
        raise WorkflowRegistryError(msg)

    raw_workflows_value: Any = raw_catalogue.get("workflows")
    if not isinstance(raw_workflows_value, list):
        msg = "Packaged workflow catalogue has no valid workflows list."
        raise WorkflowRegistryError(msg)
    raw_workflows: list[object] = cast("list[object]", raw_workflows_value)
    workflows: list[WorkflowSpec] = []
    for raw_workflow in raw_workflows:
        if not isinstance(raw_workflow, Mapping):
            msg = "Every packaged workflow entry must be a JSON object."
            raise WorkflowRegistryError(msg)
        workflows.append(WorkflowSpec.from_mapping(cast("Mapping[str, object]", raw_workflow)))
    catalogue_updated: Any = raw_catalogue.get("catalogue_updated")
    return str(schema_version), str(catalogue_updated) if catalogue_updated is not None else None, workflows


def _gdal_mutation(raw_mutation: str, *, requires_explicit_approval: bool) -> tuple[CapabilityProfile, MutationKind]:
    # An external process must remain privileged even if its output is new
    # rather than in-place. Profile controls discovery, not actual execution.
    if raw_mutation == "executes_external":
        return CapabilityProfile.PRIVILEGED, MutationKind.EXECUTES_EXTERNAL
    # Recompression exposes --replace --yes; an audit-only default does
    # not make the whole catalogue entry safe at create capability.
    if requires_explicit_approval or raw_mutation in {"in_place", "audit_only_unless_explicit_replace"}:
        return CapabilityProfile.PRIVILEGED, MutationKind.IN_PLACE
    if "replace" in raw_mutation:
        return CapabilityProfile.CREATE, MutationKind.CREATES_OR_REPLACES
    return CapabilityProfile.CREATE, MutationKind.CREATES_OUTPUTS


def _load_gdal_workflows() -> list[WorkflowSpec]:
    resource: Traversable = files("ryan_library.resources.mcp").joinpath("gdal_cli_tools.json")
    raw_catalogue: dict[str, Any] = _load_json_object(
        text=resource.read_text(encoding="utf-8"),
        source="packaged gdal_cli_tools.json",
    )

    raw_tools_value: Any = raw_catalogue.get("tools")
    if not isinstance(raw_tools_value, list):
        msg = "Packaged GDAL catalogue has no valid tools list."
        raise WorkflowRegistryError(msg)
    raw_tools: list[object] = cast("list[object]", raw_tools_value)

    workflows: list[WorkflowSpec] = []
    wrapper_versions: Any = raw_catalogue.get("wrapper_versions", {})
    agent_guidance: Any = raw_catalogue.get("agent_guidance", {})
    for raw_tool_value in raw_tools:
        if not isinstance(raw_tool_value, Mapping):
            continue
        raw_tool: Mapping[str, Any] = cast("Mapping[str, Any]", raw_tool_value)
        tool_id: Any = raw_tool.get("id")
        script_name: Any = raw_tool.get("script")
        purpose: Any = raw_tool.get("purpose")
        if not all(isinstance(value, str) and value for value in (tool_id, script_name, purpose)):
            continue

        raw_mutation: str = str(raw_tool.get("mutation", "creates_outputs"))
        requires_approval: bool = bool(raw_tool.get("requires_explicit_approval", False)) or raw_mutation in {
            "audit_only_unless_explicit_replace",
            "executes_external",
        }
        profile, mutation = _gdal_mutation(raw_mutation, requires_explicit_approval=requires_approval)
        metadata: dict[str, Any] = {
            "defaults": raw_tool.get("defaults", {}),
            "help_arguments": raw_tool.get("help_arguments", [script_name, "--help"]),
            "scenarios": raw_tool.get("scenarios", []),
            "agent_guidance": agent_guidance,
            "scenario_program": script_name,
        }
        external_program: Any = raw_tool.get("external_program")
        if isinstance(external_program, str) and external_program.strip():
            metadata["external_program"] = external_program
        if isinstance(wrapper_versions, Mapping):
            typed_wrapper_versions: Mapping[str, Any] = cast("Mapping[str, Any]", wrapper_versions)
            metadata["catalogue_wrapper_version"] = typed_wrapper_versions.get(script_name)

        module_name: Any = raw_tool.get("module")
        execution: ExecutionTarget
        if isinstance(module_name, str) and module_name:
            execution = ExecutionTarget(kind=ExecutionKind.MODULE, value=module_name)
        else:
            execution = ExecutionTarget.from_mapping({"script": f"ryan-scripts/gdal-python/{script_name}"})

        workflows.append(
            WorkflowSpec(
                workflow_id=tool_id,
                title=tool_id.replace("_", " ").title(),
                domain="gdal",
                purpose=purpose,
                execution=execution,
                profile=profile,
                mutation=mutation,
                requires_explicit_approval=requires_approval,
                headless_arguments=("--no-pause",),
                metadata=metadata,
            )
        )
    return workflows


class WorkflowRegistry:
    """Discover CLI workflows without executing them through MCP."""

    def __init__(
        self,
        *,
        configured_profile: CapabilityProfile | str | None = None,
        repository_root: Path | None = None,
        discover_repository: bool = True,
    ) -> None:
        raw_profile: CapabilityProfile | str = configured_profile or os.environ.get(
            PROFILE_ENVIRONMENT_VARIABLE,
            DEFAULT_PROFILE.value,
        )
        try:
            self.configured_profile = CapabilityProfile(raw_profile)
        except ValueError as exc:
            valid_profiles: str = ", ".join(profile.value for profile in CapabilityProfile)
            msg = f"Unknown MCP profile {raw_profile!r}; expected one of: {valid_profiles}"
            raise WorkflowRegistryError(msg) from exc

        self.repository_root: Path | None = resolve_repository_root(repository_root, discover=discover_repository)
        self.scripts_directory: Path | None = installed_scripts_directory()
        self.schema_version, self.catalogue_updated, packaged_workflows = _load_packaged_workflows()
        self.warnings: list[str] = []

        workflows: list[WorkflowSpec] = [*packaged_workflows, *_load_gdal_workflows()]
        if self.repository_root is None and self.scripts_directory is None:
            self.warnings.append(
                "No installed ryan-scripts folder or checkout found. Reinstall the current wheel, or set "
                f"{REPOSITORY_ROOT_ENVIRONMENT_VARIABLE} for a development checkout."
            )

        self._workflows: dict[str, WorkflowSpec] = {}
        for workflow in workflows:
            if workflow.workflow_id in self._workflows:
                msg = f"Duplicate workflow id: {workflow.workflow_id}"
                raise WorkflowRegistryError(msg)
            self._workflows[workflow.workflow_id] = workflow

    def _script_path(self, workflow: WorkflowSpec) -> Path | None:
        if workflow.execution.kind is not ExecutionKind.SCRIPT:
            return None
        relative = Path(workflow.execution.value)
        if self.repository_root is not None:
            return (self.repository_root / relative).absolute()
        if self.scripts_directory is not None:
            return (self.scripts_directory / relative.relative_to("ryan-scripts")).absolute()
        return None

    def _command_prefix(self, workflow: WorkflowSpec) -> list[str] | None:
        match workflow.execution.kind:
            case ExecutionKind.MODULE:
                try:
                    available: bool = find_spec(workflow.execution.value) is not None
                except (ImportError, ModuleNotFoundError, AttributeError, ValueError):
                    available = False
                return [sys.executable, "-m", workflow.execution.value] if available else None
            case ExecutionKind.SCRIPT:
                script_path: Path | None = self._script_path(workflow)
                return [sys.executable, str(script_path)] if script_path is not None and script_path.is_file() else None
            case ExecutionKind.CONSOLE_SCRIPT:
                executable: str | None = shutil.which(workflow.execution.value)
                return [executable] if executable is not None else None

    def _available(self, workflow: WorkflowSpec) -> bool:
        return self._command_prefix(workflow) is not None

    def _maximum_profile(self, requested_profile: CapabilityProfile | str | None) -> CapabilityProfile:
        if requested_profile is None:
            return self.configured_profile
        try:
            requested = CapabilityProfile(requested_profile)
        except ValueError as exc:
            msg = f"Unknown workflow profile: {requested_profile!r}"
            raise WorkflowRegistryError(msg) from exc
        if not profile_allows(configured=self.configured_profile, required=requested):
            msg = f"Profile {requested.value!r} exceeds configured profile {self.configured_profile.value!r}."
            raise WorkflowRegistryError(msg)
        return requested

    def list_workflows(
        self,
        *,
        domain: str | None = None,
        query: str | None = None,
        maximum_profile: CapabilityProfile | str | None = None,
        include_unavailable: bool = False,
    ) -> dict[str, Any]:
        """Return matching summaries for internal use; the MCP tool paginates its response."""
        visible_profile: CapabilityProfile = self._maximum_profile(maximum_profile)
        terms = (query or "").casefold().split()
        summaries: list[dict[str, Any]] = []
        for workflow in sorted(self._workflows.values(), key=lambda item: (item.domain, item.title, item.workflow_id)):
            if domain is not None and workflow.domain.casefold() != domain.casefold():
                continue
            if not profile_allows(configured=visible_profile, required=workflow.profile):
                continue
            searchable = f"{workflow.workflow_id} {workflow.title} {workflow.domain} {workflow.purpose}".casefold()
            if not all(term in searchable for term in terms):
                continue
            available: bool = self._available(workflow)
            if not available and not include_unavailable:
                continue
            summaries.append(workflow.summary(available=available))

        return {
            "configured_profile": self.configured_profile.value,
            "maximum_profile": visible_profile.value,
            "repository_root": str(self.repository_root) if self.repository_root else None,
            "scripts_directory": str(self.scripts_directory) if self.scripts_directory else None,
            "workflow_count": len(summaries),
            "domains": sorted({item["domain"] for item in summaries}),
            "workflows": summaries,
            "warnings": list(self.warnings),
            "next_step": "Call get_workflow with an id, then run its help_command through the client shell.",
        }

    def _script_source(self, workflow: WorkflowSpec) -> str:
        """Identify the selected CLI source without claiming executable readiness."""
        if workflow.execution.kind is not ExecutionKind.SCRIPT:
            return "module_or_console"
        if self.repository_root is not None:
            return "repository_checkout"
        if self.scripts_directory is not None:
            return "installed_distribution"
        return "unavailable"

    @staticmethod
    def _external_prerequisites(workflow: WorkflowSpec) -> dict[str, Any]:
        """Describe declared external execution without guessing binary availability."""
        program = workflow.metadata.get("external_program")
        if isinstance(program, str) and program.strip():
            return {
                "status": "not_verified",
                "declared_program": program,
                "note": "Check the external executable and its version in the target environment before execution.",
            }
        if workflow.mutation is MutationKind.EXECUTES_EXTERNAL:
            return {
                "status": "not_declared",
                "declared_program": None,
                "note": "Workflow invokes an external executable but the catalogue does not identify it.",
            }
        return {
            "status": "not_assessed",
            "declared_program": None,
            "note": "File discovery does not validate optional third-party tools or runtime imports.",
        }

    @staticmethod
    def _installed_distribution_version() -> str | None:
        """Read the distribution version, if installed, without importing any workflow."""
        try:
            return importlib.metadata.version("ryan-tools")
        except importlib.metadata.PackageNotFoundError:
            return None

    def get_workflow(self, workflow_id: str) -> dict[str, Any]:
        """Return one visible workflow and its resolved CLI discovery commands."""
        workflow: WorkflowSpec | None = self._workflows.get(workflow_id)
        if workflow is None or not profile_allows(configured=self.configured_profile, required=workflow.profile):
            return {
                "error": f"Workflow {workflow_id!r} is unknown or not enabled by profile "
                f"{self.configured_profile.value!r}."
            }

        script_path: Path | None = self._script_path(workflow)
        command_prefix: list[str] | None = self._command_prefix(workflow)
        available: bool = command_prefix is not None
        details: dict[str, Any] = workflow.summary(available=available)
        details.update(
            {
                "module_name": workflow.execution.value if workflow.execution.kind is ExecutionKind.MODULE else None,
                "script_relative_path": (
                    workflow.execution.value if workflow.execution.kind is ExecutionKind.SCRIPT else None
                ),
                "script_path": str(script_path) if script_path else None,
                "console_script": (
                    workflow.execution.value if workflow.execution.kind is ExecutionKind.CONSOLE_SCRIPT else None
                ),
                "required_headless_arguments": list(workflow.headless_arguments),
                "metadata": dict(workflow.metadata),
                "script_source": self._script_source(workflow),
                "installed_distribution_version": self._installed_distribution_version(),
                "external_prerequisites": self._external_prerequisites(workflow),
                "availability_scope": "entry_point_only",
            }
        )
        if command_prefix is None:
            if workflow.execution.kind is ExecutionKind.SCRIPT:
                details["error"] = (
                    "CLI script is unavailable. Reinstall the current wheel, or set "
                    f"{REPOSITORY_ROOT_ENVIRONMENT_VARIABLE} to a development checkout."
                )
            else:
                details["error"] = (
                    f"Installed {workflow.execution.kind.value} target is unavailable: {workflow.execution.value}"
                )
            return details

        help_safe: bool = bool(workflow.metadata.get("help_safe", True))
        details["command_prefix"] = command_prefix
        details["help_command"] = [*command_prefix, "--help"] if help_safe else None
        details["wrapper_version"] = self._read_wrapper_version(script_path) if script_path is not None else None
        details["resolved_scenarios"] = self._resolved_scenarios(workflow, command_prefix)
        details["execution_note"] = (
            "The MCP server does not execute this workflow. Run the CLI through the client shell after reviewing help, "
            "paths, mutation metadata, and any approval requirement."
        )
        return details

    @staticmethod
    def _read_wrapper_version(script_path: Path) -> str | None:
        try:
            source: str = script_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError):
            return None
        match: re.Match[str] | None = _WRAPPER_VERSION_PATTERN.search(source)
        return match.group(1) if match else None

    @staticmethod
    def _resolved_scenarios(workflow: WorkflowSpec, command_prefix: list[str]) -> list[dict[str, Any]]:
        raw_scenarios: Any = workflow.metadata.get("scenarios", [])
        if not isinstance(raw_scenarios, list):
            return []

        scenario_program: Any = workflow.metadata.get("scenario_program")
        scenarios: list[dict[str, Any]] = []
        typed_scenarios: list[object] = cast("list[object]", raw_scenarios)
        for raw_scenario_value in typed_scenarios:
            if not isinstance(raw_scenario_value, Mapping):
                continue
            raw_scenario: Mapping[str, Any] = cast("Mapping[str, Any]", raw_scenario_value)
            raw_arguments: Any = raw_scenario.get("arguments")
            if not isinstance(raw_arguments, list):
                continue
            typed_arguments: list[object] = cast("list[object]", raw_arguments)
            if not all(isinstance(item, str) for item in typed_arguments):
                continue
            arguments: list[str] = [cast("str", item) for item in typed_arguments]
            if arguments and isinstance(scenario_program, str) and arguments[0] == scenario_program:
                arguments = arguments[1:]
            scenarios.append(
                {
                    "name": raw_scenario.get("name"),
                    "command": [*command_prefix, *arguments],
                    "note": raw_scenario.get("note"),
                }
            )
        return scenarios

    def catalogue(self) -> dict[str, Any]:
        """Return the complete catalogue visible under the configured profile."""
        listing: dict[str, Any] = self.list_workflows(include_unavailable=True)
        listing.update(
            {
                "schema_version": self.schema_version,
                "catalogue_updated": self.catalogue_updated,
            }
        )
        return listing

    @property
    def total_workflow_count(self) -> int:
        return len(self._workflows)
