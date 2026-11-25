"""The six legacy reader modules must expose the sole published PyRORB implementation."""

from __future__ import annotations

import importlib
from importlib.metadata import distribution
from pathlib import Path

_EXPORTS: dict[str, tuple[str, ...]] = {
    "associations": (
        "BatchEventKey",
        "Record",
        "RorbDocument",
        "Section",
        "associate_batch_csvs",
        "batch_event_key",
        "compare_output_csv",
        "read_rorb_file",
    ),
    "output_reader": (
        "HydrographSiteColumn",
        "Record",
        "RorbDocument",
        "Section",
        "bind_output_site_columns",
        "exact_number",
        "integer",
        "number",
        "read_output",
        "read_sites",
        "report_metadata",
        "validate_series",
    ),
    "parameter_reader": (
        "RorbDocument",
        "Section",
        "integer",
        "number",
        "read_cmd_parameters",
        "read_saved_settings",
        "resembles_saved_settings",
    ),
    "reader_types": (
        "NUMBER",
        "Diagnostic",
        "Record",
        "RorbDocument",
        "Section",
        "Value",
        "exact_number",
        "integer",
        "number",
        "validate_series",
    ),
    "readers": (
        "MAX_FILE_BYTES",
        "RorbDocument",
        "read_batch",
        "read_cmd_parameters",
        "read_csv",
        "read_output",
        "read_rorb_bytes",
        "read_rorb_file",
        "read_rorb_table_bytes",
        "read_rorb_table_file",
        "read_saved_settings",
        "resembles_saved_settings",
    ),
    "result_readers": (
        "Record",
        "RorbDocument",
        "Section",
        "integer",
        "number",
        "read_batch",
        "read_csv",
        "read_sites",
        "report_metadata",
        "validate_series",
    ),
}


def test_legacy_reader_exports_are_canonical() -> None:
    """Protect the complete reader-owned import surface and class/function identity."""
    package = distribution("pyrorb")
    for module_name, names in _EXPORTS.items():
        legacy = importlib.import_module(f"ryan_library.functions.RORB.{module_name}")
        canonical = importlib.import_module(f"runoff_routing.rorb_readers.{module_name}")
        assert set(names) == {name for name in vars(legacy) if not name.startswith("_")}
        assert canonical.__file__ is not None
        # Physical identity: the canonical source must come from the installed distribution.
        expected = Path(str(package.locate_file(f"runoff_routing/rorb_readers/{module_name}.py")))
        assert Path(canonical.__file__).resolve(strict=True) == expected.resolve(strict=True)
        for name in names:
            assert getattr(legacy, name) is getattr(canonical, name), f"{module_name}.{name}"


def test_published_pyrorb_dependency_metadata() -> None:
    """Check the selected index release has no reverse dependency or direct URL."""
    package = distribution("pyrorb")
    assert package.version == "26.11.24.1"
    assert package.metadata["Requires-Python"] == ">=3.11"
    requirements = package.requires or []
    assert [item for item in requirements if ";" not in item] == ["typing_extensions>=4.12"]
    assert all("@" not in item for item in requirements)
    assert all("ryan-tools" not in item.lower() and "ryan-rorb-readers" not in item.lower() for item in requirements)
