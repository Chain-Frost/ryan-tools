"""Package-native registry for discovering ryan-tools CLI workflows."""

from __future__ import annotations

from importlib.resources.abc import Traversable
import json
import os
import re
import sys
from importlib.resources import files
from pathlib import Path
from typing import Any, Mapping, cast

from ryan_library.mcp.models import CapabilityProfile, MutationKind, WorkflowSpec, profile_allows

DEFAULT_PROFILE:CapabilityProfile = CapabilityProfile.CREATE
PROFILE_ENVIRONMENT_VARIABLE = "RYAN_MCP_PROFILE"
REPOSITORY_ROOT_ENVIRONMENT_VARIABLE = "RYAN_TOOLS_REPOSITORY_ROOT"
GDAL_CATALOGUE_RELATIVE_PATH = Path("ryan-scripts/gdal-python/gdal_cli_tools.json")
_WRAPPER_VERSION_PATTERN: re.Pattern[str] = re.compile(pattern=r'^WRAPPER_VERSION\s*=\s*["\']([^"\']+)["\']', flags=re.MULTILINE)


class WorkflowRegistryError(RuntimeError):
    """Raised when the workflow registry or its configuration is invalid."""


def _looks_like_repository(path: Path) -> bool:
    return (path / "pyproject.toml").is_file() and (path / "ryan-scripts").is_dir()


def resolve_repository_root(explicit_root: Path | None = None) -> Path | None:
    """Resolve the checkout containing CLI wrappers, if one is available."""
    candidates: list[Path] = []
    if explicit_root is not None:
        candidates.append(explicit_root)

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
        raise WorkflowRegistryError(f"Invalid JSON in {source}: {exc}") from exc
    if not isinstance(raw, dict):
        raise WorkflowRegistryError(f"Workflow catalogue must contain a JSON object: {source}")
    return cast(dict[str, Any], raw)


def _load_packaged_workflows() -> tuple[str, str | None, list[WorkflowSpec]]:
    resource: Traversable = files("ryan_library.resources.mcp").joinpath("workflows.json")
    raw_catalogue: dict[str, Any] = _load_json_object(
        text=resource.read_text(encoding="utf-8"),
        source="packaged workflows.json",
    )
    schema_version: Any = raw_catalogue.get("schema_version")
    if schema_version != "1.0":
        raise WorkflowRegistryError(f"Unsupported workflow catalogue schema: {schema_version!r}")

    raw_workflows_value: Any = raw_catalogue.get("workflows")
    if not isinstance(raw_workflows_value, list):
        raise WorkflowRegistryError("Packaged workflow catalogue has no valid workflows list.")
    raw_workflows: list[object] = cast(list[object], raw_workflows_value)
    workflows: list[WorkflowSpec] = []
    for raw_workflow in raw_workflows:
        if not isinstance(raw_workflow, Mapping):
            raise WorkflowRegistryError("Every packaged workflow entry must be a JSON object.")
        workflows.append(WorkflowSpec.from_mapping(cast(Mapping[str, object], raw_workflow)))
    catalogue_updated: Any = raw_catalogue.get("catalogue_updated")
    return str(schema_version), str(catalogue_updated) if catalogue_updated is not None else None, workflows


def _gdal_mutation(raw_mutation: str, *, requires_explicit_approval: bool) -> tuple[CapabilityProfile, MutationKind]:
    if requires_explicit_approval or raw_mutation == "in_place":
        return CapabilityProfile.PRIVILEGED, MutationKind.IN_PLACE
    if "replace" in raw_mutation:
        return CapabilityProfile.CREATE, MutationKind.CREATES_OR_REPLACES
    return CapabilityProfile.CREATE, MutationKind.CREATES_OUTPUTS


def _load_gdal_workflows(repository_root: Path) -> tuple[list[WorkflowSpec], str | None]:
    catalogue_path: Path = repository_root / GDAL_CATALOGUE_RELATIVE_PATH
    if not catalogue_path.is_file():
        return [], f"GDAL catalogue not found: {catalogue_path}"

    try:
        raw_catalogue: dict[str, Any] = _load_json_object(
            text=catalogue_path.read_text(encoding="utf-8"),
            source=str(catalogue_path),
        )
    except OSError as exc:
        return [], f"Unable to read GDAL catalogue at {catalogue_path}: {exc}"

    raw_tools_value: Any = raw_catalogue.get("tools")
    if not isinstance(raw_tools_value, list):
        return [], f"GDAL catalogue has no valid tools list: {catalogue_path}"
    raw_tools: list[object] = cast(list[object], raw_tools_value)

    workflows: list[WorkflowSpec] = []
    wrapper_versions: Any = raw_catalogue.get("wrapper_versions", {})
    agent_guidance: Any = raw_catalogue.get("agent_guidance", {})
    for raw_tool_value in raw_tools:
        if not isinstance(raw_tool_value, Mapping):
            continue
        raw_tool: Mapping[str, Any] = cast(Mapping[str, Any], raw_tool_value)
        tool_id: Any = raw_tool.get("id")
        script_name: Any = raw_tool.get("script")
        purpose: Any = raw_tool.get("purpose")
        if not all(isinstance(value, str) and value for value in (tool_id, script_name, purpose)):
            continue

        requires_approval: bool = bool(raw_tool.get("requires_explicit_approval", False))
        raw_mutation: str = str(raw_tool.get("mutation", "creates_outputs"))
        profile, mutation = _gdal_mutation(raw_mutation, requires_explicit_approval=requires_approval)
        metadata: dict[str, Any] = {
            "defaults": raw_tool.get("defaults", {}),
            "help_arguments": raw_tool.get("help_arguments", [script_name, "--help"]),
            "scenarios": raw_tool.get("scenarios", []),
            "agent_guidance": agent_guidance,
        }
        if isinstance(wrapper_versions, Mapping):
            typed_wrapper_versions: Mapping[str, Any] = cast(Mapping[str, Any], wrapper_versions)
            metadata["catalogue_wrapper_version"] = typed_wrapper_versions.get(script_name)

        workflows.append(
            WorkflowSpec(
                workflow_id=tool_id,
                title=tool_id.replace("_", " ").title(),
                domain="gdal",
                purpose=purpose,
                script=f"ryan-scripts/gdal-python/{script_name}",
                profile=profile,
                mutation=mutation,
                requires_explicit_approval=requires_approval,
                headless_arguments=("--no-pause",),
                metadata=metadata,
            )
        )
    return workflows, None


class WorkflowRegistry:
    """Discover CLI workflows without executing them through MCP."""

    def __init__(
        self,
        *,
        configured_profile: CapabilityProfile | str | None = None,
        repository_root: Path | None = None,
    ) -> None:
        raw_profile: CapabilityProfile | str = configured_profile or os.environ.get(
            PROFILE_ENVIRONMENT_VARIABLE,
            DEFAULT_PROFILE.value,
        )
        try:
            self.configured_profile = CapabilityProfile(raw_profile)
        except ValueError as exc:
            valid_profiles: str = ", ".join(profile.value for profile in CapabilityProfile)
            raise WorkflowRegistryError(
                f"Unknown MCP profile {raw_profile!r}; expected one of: {valid_profiles}"
            ) from exc

        self.repository_root: Path | None = resolve_repository_root(repository_root)
        self.schema_version, self.catalogue_updated, packaged_workflows = _load_packaged_workflows()
        self.warnings: list[str] = []

        workflows: list[WorkflowSpec] = list(packaged_workflows)
        if self.repository_root is None:
            self.warnings.append(
                f"No ryan-tools checkout found. Set {REPOSITORY_ROOT_ENVIRONMENT_VARIABLE} to expose CLI scripts."
            )
        else:
            gdal_workflows, gdal_warning = _load_gdal_workflows(self.repository_root)
            workflows.extend(gdal_workflows)
            if gdal_warning:
                self.warnings.append(gdal_warning)

        self._workflows: dict[str, WorkflowSpec] = {}
        for workflow in workflows:
            if workflow.workflow_id in self._workflows:
                raise WorkflowRegistryError(f"Duplicate workflow id: {workflow.workflow_id}")
            self._workflows[workflow.workflow_id] = workflow

    def _script_path(self, workflow: WorkflowSpec) -> Path | None:
        if self.repository_root is None:
            return None
        return (self.repository_root / Path(workflow.script)).absolute()

    def _available(self, workflow: WorkflowSpec) -> bool:
        script_path: Path | None = self._script_path(workflow)
        return script_path is not None and script_path.is_file()

    def _maximum_profile(self, requested_profile: CapabilityProfile | str | None) -> CapabilityProfile:
        if requested_profile is None:
            return self.configured_profile
        try:
            requested = CapabilityProfile(requested_profile)
        except ValueError as exc:
            raise WorkflowRegistryError(f"Unknown workflow profile: {requested_profile!r}") from exc
        if not profile_allows(configured=self.configured_profile, required=requested):
            raise WorkflowRegistryError(
                f"Profile {requested.value!r} exceeds configured profile {self.configured_profile.value!r}."
            )
        return requested

    def list_workflows(
        self,
        *,
        domain: str | None = None,
        maximum_profile: CapabilityProfile | str | None = None,
        include_unavailable: bool = False,
    ) -> dict[str, Any]:
        """Return visible workflow summaries, optionally filtered by domain and profile."""
        visible_profile: CapabilityProfile = self._maximum_profile(maximum_profile)
        summaries: list[dict[str, Any]] = []
        for workflow in sorted(self._workflows.values(), key=lambda item: (item.domain, item.title, item.workflow_id)):
            if domain is not None and workflow.domain.casefold() != domain.casefold():
                continue
            if not profile_allows(configured=visible_profile, required=workflow.profile):
                continue
            available: bool = self._available(workflow)
            if not available and not include_unavailable:
                continue
            summaries.append(workflow.summary(available=available))

        return {
            "configured_profile": self.configured_profile.value,
            "maximum_profile": visible_profile.value,
            "repository_root": str(self.repository_root) if self.repository_root else None,
            "workflow_count": len(summaries),
            "workflows": summaries,
            "warnings": list(self.warnings),
            "next_step": "Call get_workflow with an id, then run its help_command through the client shell.",
        }

    def get_workflow(self, workflow_id: str) -> dict[str, Any]:
        """Return one visible workflow and its resolved CLI discovery commands."""
        workflow: WorkflowSpec | None = self._workflows.get(workflow_id)
        if workflow is None or not profile_allows(configured=self.configured_profile, required=workflow.profile):
            return {
                "error": f"Workflow {workflow_id!r} is unknown or not enabled by profile "
                f"{self.configured_profile.value!r}."
            }

        script_path: Path | None = self._script_path(workflow)
        available: bool = self._available(workflow)
        details: dict[str, Any] = workflow.summary(available=available)
        details.update(
            {
                "script_relative_path": workflow.script,
                "script_path": str(script_path) if script_path else None,
                "required_headless_arguments": list(workflow.headless_arguments),
                "metadata": dict(workflow.metadata),
            }
        )
        if not available or script_path is None:
            details["error"] = (
                f"CLI script is unavailable. Set {REPOSITORY_ROOT_ENVIRONMENT_VARIABLE} to a ryan-tools checkout."
            )
            return details

        help_safe: bool = bool(workflow.metadata.get("help_safe", True))
        command_prefix: list[str] = [sys.executable, str(script_path)]
        details["command_prefix"] = command_prefix
        details["help_command"] = [*command_prefix, "--help"] if help_safe else None
        details["wrapper_version"] = self._read_wrapper_version(script_path)
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
        except OSError, UnicodeError:
            return None
        match: re.Match[str] | None = _WRAPPER_VERSION_PATTERN.search(source)
        return match.group(1) if match else None

    @staticmethod
    def _resolved_scenarios(workflow: WorkflowSpec, command_prefix: list[str]) -> list[dict[str, Any]]:
        raw_scenarios: Any = workflow.metadata.get("scenarios", [])
        if not isinstance(raw_scenarios, list):
            return []

        script_name: str = Path(workflow.script).name
        scenarios: list[dict[str, Any]] = []
        typed_scenarios: list[object] = cast(list[object], raw_scenarios)
        for raw_scenario_value in typed_scenarios:
            if not isinstance(raw_scenario_value, Mapping):
                continue
            raw_scenario: Mapping[str, Any] = cast(Mapping[str, Any], raw_scenario_value)
            raw_arguments: Any = raw_scenario.get("arguments")
            if not isinstance(raw_arguments, list):
                continue
            typed_arguments: list[object] = cast(list[object], raw_arguments)
            if not all(isinstance(item, str) for item in typed_arguments):
                continue
            arguments: list[str] = [cast(str, item) for item in typed_arguments]
            if arguments and arguments[0] == script_name:
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
