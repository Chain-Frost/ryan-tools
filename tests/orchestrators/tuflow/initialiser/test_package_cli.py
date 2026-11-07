"""The migrated initialiser exposes its CLI and bundled model templates."""

from importlib.resources import files

from ryan_library.orchestrators.tuflow.initialiser.cli import _parser


def test_initialiser_parser_keeps_original_subcommands() -> None:
    help_text = _parser().format_help()
    assert "init" in help_text
    assert "build" in help_text
    assert "catchments" in help_text


def test_initialiser_model_templates_are_packaged() -> None:
    resource_root = files("ryan_library.orchestrators.tuflow.initialiser")
    template = resource_root.joinpath("resources", "tuflow_templates", "model", "GEOM_01.tgc")
    assert template.is_file()
    assert "Cell Size" in template.read_text(encoding="utf-8")
    run_template = resource_root.joinpath("resources", "tuflow_templates", "runs", "Create_Empties.tcf")
    assert run_template.is_file()
