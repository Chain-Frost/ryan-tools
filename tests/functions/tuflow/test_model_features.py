"""Focused synthetic checks for non-blocking TUFLOW model option diagnostics."""

from pathlib import Path

from ryan_library.functions.tuflow.model_features import inspect_model_features


def test_explicit_sgs_target_and_quadtree_in_tcf(tmp_path: Path) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.write_text(
        "! SGS == OFF\nSGS == ON ! actual\nSGS Sample Target Distance == 1.5\n"
        "Quadtree Control File == ..\\model\\grid.qcf\n",
        encoding="utf-8",
    )
    features = inspect_model_features(tcf)
    assert features.sgs == "ON"
    assert features.target_distance.startswith("1.5 ")
    assert features.quadtree == "ON (QCF declared)"


def test_read_file_precedence_and_frequency_override(tmp_path: Path) -> None:
    tcf = tmp_path / "model.tcf"
    included = tmp_path / "settings.trd"
    included.write_text("SGS == OFF\nSGS Sample Target Distance == 2\n", encoding="utf-8")
    tcf.write_text(
        "Read File == settings.trd\nSGS == ON\nSGS Sample Frequency == 11\n"
        "Quadtree Control File == Single Level\n",
        encoding="utf-8",
    )
    features = inspect_model_features(tcf)
    assert features.sgs == "ON"
    assert "SGS frequency 11" in features.target_distance
    assert features.quadtree == "ON (Single Level)"


def test_undeclared_target_is_solver_determined(tmp_path: Path) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.write_text("SGS == ON\n", encoding="utf-8")
    features = inspect_model_features(tcf)
    assert features.sgs == "ON"
    assert "automatic" in features.target_distance
    assert features.quadtree == "OFF (no QCF declared)"


def test_conditionals_are_not_guessed(tmp_path: Path) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.write_text(
        "If Scenario == DEV\nSGS == ON\nQuadtree Control File == grid.qcf\n"
        "Else If Scenario == EXG\nSGS == OFF\nEnd If\n",
        encoding="utf-8",
    )
    features = inspect_model_features(tcf)
    assert features.sgs.startswith("UNKNOWN")
    assert features.quadtree.startswith("UNKNOWN")


def test_missing_control_file_or_include_never_blocks(tmp_path: Path) -> None:
    tcf = tmp_path / "model.tcf"
    missing = inspect_model_features(tcf)
    assert missing.sgs.startswith("UNKNOWN")
    assert missing.quadtree.startswith("UNKNOWN")
    tcf.write_text("Read File == missing.trd\nSGS == ON\n", encoding="utf-8")
    included = inspect_model_features(tcf)
    assert included.sgs == "ON"
    assert included.target_distance.startswith("UNKNOWN")
    assert included.quadtree.startswith("UNKNOWN")


def test_cycles_do_not_recurse_indefinitely(tmp_path: Path) -> None:
    tcf = tmp_path / "model.tcf"
    tcf.write_text("Read File == model.tcf\n", encoding="utf-8")
    assert inspect_model_features(tcf).quadtree.startswith("UNKNOWN")
