"""Setuptools entry point and wheel resource staging."""

import shutil
from pathlib import Path

from setuptools import setup  # pyright: ignore[reportMissingModuleSource]
from setuptools.command.build_py import build_py  # pyright: ignore[reportMissingModuleSource]


class _BuildPyWithQgisStyles(build_py):
    """Stage QGIS styles and the standalone script tree in the wheel."""

    def run(self) -> None:
        """Build Python modules, then stage QML files from the resource submodule."""
        super().run()

        project_root: Path = Path(__file__).absolute().parent
        source_dir: Path = project_root / "qgis-resources" / "styles" / "TUFLOW"
        qml_files: list[Path] = sorted(source_dir.glob("*.qml"))
        if not qml_files:
            message: str = (
                f"No TUFLOW QML styles found at {source_dir}. "
                "Initialize the qgis-resources submodule before building the wheel."
            )
            raise RuntimeError(message)

        target_dir: Path = Path(self.build_lib) / "ryan_library" / "resources" / "qgis" / "tuflow"
        target_dir.mkdir(parents=True, exist_ok=True)
        for qml_file in qml_files:
            shutil.copy2(src=qml_file, dst=target_dir / qml_file.name)

        scripts_source = project_root / "ryan-scripts"
        suffixes = {".py", ".md", ".bat", ".ps1", ".gms", ".xces"}
        for source in sorted(scripts_source.rglob("*")):
            if source.is_file() and source.suffix.casefold() in suffixes and "__pycache__" not in source.parts:
                destination = Path(self.build_lib) / "ryan-scripts" / source.relative_to(scripts_source)
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, destination)


if __name__ == "__main__":
    setup(cmdclass={"build_py": _BuildPyWithQgisStyles})
