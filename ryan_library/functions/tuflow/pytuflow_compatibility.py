"""Lazy capability check for the supported public PyTUFLOW validation API.

The accepted installation source is pinned in repo-scripts/pytuflow-requirement.txt.
Importing this module never imports PyTUFLOW, parses a model, or installs software.
"""

from __future__ import annotations

import importlib
import json
import sys
from dataclasses import dataclass
from importlib import metadata
from pathlib import Path
from typing import Protocol, cast


class ValidationOptionsFactory(Protocol):
    """The public ValidationOptions constructor, independent of its import cost."""

    def __call__(
        self,
        *,
        check_files: bool,
        check_gpkg_layers: bool,
        check_bc_csv_headers: bool,
        io_workers: int,
        maximum_directory_entries: int,
    ) -> object: ...


class BatchValidator(Protocol):
    """The public batch validator; runtime results are checked by the consumer."""

    def __call__(self, model: object, run_flags: list[dict[str, str]], *, options: object) -> object: ...


class PyTuflowCompatibilityError(RuntimeError):
    """Required public PyTUFLOW validation capabilities are unavailable."""


@dataclass(frozen=True, slots=True)
class CompatibilityStatus:
    """Diagnostic that does not require model I/O or version-number guesses."""

    available: bool
    interpreter: str
    version: str
    module_path: str
    source: str
    problem: str

    @property
    def message(self) -> str:
        """Show enough provenance and an exact source remedy to diagnose errors."""
        details = (
            f"Python: {self.interpreter}; PyTUFLOW distribution: {self.version}; "
            f"module: {self.module_path}; source: {self.source}"
        )
        if self.available:
            return f"PyTUFLOW public batch validation available. {details}"
        return (
            f"PyTUFLOW input validation unavailable: {self.problem}. {details}. "
            "Install the accepted pinned PyTUFLOW source using "
            "'python repo-scripts/install_latest_wheel.py' (normal mode), or "
            "'python -m pip install --upgrade --force-reinstall "
            '"pytuflow @ https://github.com/Chain-Frost/PyTuflow/archive/'
            "05e35476c02d6d8ed17c83caf1cb0d185ce51d1b.zip\"'."
        )


@dataclass(frozen=True, slots=True)
class ValidationAPI:
    """Public callables obtained only after checking their capabilities."""

    validate_batch_inputs: BatchValidator
    options_factory: ValidationOptionsFactory
    diagnostic: CompatibilityStatus


def _json_object(raw: str) -> dict[str, object]:
    """Decode JSON metadata into a concrete typed object without trusting contents."""
    decoded: object = json.loads(raw)
    if not isinstance(decoded, dict):
        message = "Expected JSON object in distribution metadata"
        raise TypeError(message)
    return cast(dict[str, object], decoded)


def _source_description(dist: metadata.Distribution) -> str:
    """Record direct-url provenance if pip supplied it, without requiring VCS data."""
    try:
        raw = dist.read_text("direct_url.json")
        if not raw:
            return "not recorded"
        record = _json_object(raw)
        url = record.get("url", "unknown")
        vcs = record.get("vcs_info")
        commit: object = None
        if isinstance(vcs, dict):
            commit = cast(dict[str, object], vcs).get("commit_id")
        return f"{url} (commit {commit})" if commit else str(url)
    except (ValueError, OSError, TypeError):
        return "unreadable direct_url.json"


def _probe() -> tuple[CompatibilityStatus, object | None, object | None]:  # noqa: C901 - capability diagnostics
    interpreter = f"{sys.executable} ({sys.version.split()[0]})"
    version, module_path, source = "not installed", "not importable", "not recorded"
    dist: metadata.Distribution | None = None
    try:
        dist = metadata.distribution("pytuflow")
        version = dist.version
        source = _source_description(dist)
    except metadata.PackageNotFoundError:
        pass

    def status(problem: str = "") -> CompatibilityStatus:
        return CompatibilityStatus(not problem, interpreter, version, module_path, source, problem)

    try:
        root = importlib.import_module("pytuflow")
        module_path = str(getattr(root, "__file__", "unknown") or "unknown")
    except (ImportError, OSError) as exc:
        return status(f"package or transitive import failed ({type(exc).__name__}: {exc})"), None, None

    if dist is None:
        return (
            status("an importable pytuflow module exists without an installed distribution (possible shadowing)"),
            None,
            None,
        )

    # Compare real package locations, not textual spellings: this specifically
    # detects a project checkout shadowing the installed distribution.
    if module_path != "unknown":
        expected = Path(str(dist.locate_file("pytuflow"))).resolve()
        actual = Path(module_path).resolve()
        editable = False
        try:
            raw = dist.read_text("direct_url.json")
            record: dict[str, object] = _json_object(raw) if raw else {}
            dir_info = record.get("dir_info")
            if isinstance(dir_info, dict):
                editable = bool(cast(dict[str, object], dir_info).get("editable", False))
        except (TypeError, ValueError, OSError):
            pass
        if not actual.is_relative_to(expected) and not editable:
            return status(f"import is shadowed by {actual}; installed distribution package is {expected}"), None, None

    try:
        batch = importlib.import_module("pytuflow.batch_validation")
        validation = importlib.import_module("pytuflow.validation")
    except (ImportError, OSError) as exc:
        return status(f"public validation import failed ({type(exc).__name__}: {exc})"), None, None

    validator = getattr(batch, "validate_batch_inputs", None)
    options = getattr(validation, "ValidationOptions", None)
    result_type = getattr(batch, "BatchValidationResult", None)
    required = {"check_files", "check_gpkg_layers", "check_bc_csv_headers", "io_workers", "maximum_directory_entries"}
    fields: object = getattr(options, "__dataclass_fields__", None)
    if not callable(validator):
        return status("missing callable pytuflow.batch_validation.validate_batch_inputs"), None, None
    if not callable(options) or not isinstance(fields, dict) or not required.issubset(cast(dict[str, object], fields)):
        return status("missing supported pytuflow.validation.ValidationOptions inspection fields"), None, None
    issues_property: object = getattr(result_type, "issues", None)
    if not isinstance(issues_property, property) or not callable(issues_property.fget):
        return status("missing public BatchValidationResult.issues property"), None, None
    passed_property: object = getattr(result_type, "passed", None)
    if not isinstance(passed_property, property) or not callable(passed_property.fget):
        return status("missing public BatchValidationResult.passed property"), None, None
    if not callable(getattr(validation, "ValidationFinding", None)):
        return status("missing public ValidationFinding type"), None, None
    return status(), validator, options


def check_validation_capabilities() -> CompatibilityStatus:
    """Diagnose API compatibility without parsing files or installing packages."""
    result, _, _ = _probe()
    return result


def require_validation_api() -> ValidationAPI:
    """Return public APIs or raise an actionable ordinary exception."""
    status, validator, options = _probe()
    if not status.available or validator is None or options is None:
        raise PyTuflowCompatibilityError(status.message)
    return ValidationAPI(cast(BatchValidator, validator), cast(ValidationOptionsFactory, options), status)
