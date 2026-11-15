"""The migrated initialiser exposes its CLI and bundled model templates."""

import subprocess
import sys
from importlib.resources import files


def test_initialiser_model_templates_are_packaged() -> None:
    resource_root = files("ryan_library.resources.tuflow_templates")
    template = resource_root.joinpath("model", "GEOM_01.tgc")
    assert template.is_file()
    assert "Cell Size" in template.read_text(encoding="utf-8")
    run_template = resource_root.joinpath("runs", "Create_Empties.tcf")
    assert run_template.is_file()


def test_initialiser_module_cli_help_exits_without_external_tools() -> None:
    """The installed entry point targets this module and help must not run TUFLOW."""
    result = subprocess.run(
        [sys.executable, "-m", "ryan_library.orchestrators.tuflow.initialiser.cli", "--help"],
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "catchments" in result.stdout
    assert "build" in result.stdout
    assert "init" in result.stdout
