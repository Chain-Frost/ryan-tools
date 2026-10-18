"""Setuptools entry point and wheel resource staging."""

import json
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
        manifest = project_root / "ryan_library" / "resources" / "mcp" / "installed_scripts.json"
        policy = json.loads(manifest.read_text(encoding="utf-8"))
        entries = policy.get("installed_files")
        if policy.get("schema_version") != 1 or not isinstance(entries, list) or not entries:
            msg = f"Invalid installed script manifest: {manifest}"
            raise RuntimeError(msg)
        if not all(isinstance(item, str) for item in entries) or len(entries) != len(set(entries)):
            msg = f"Duplicate or invalid script paths in {manifest}"
            raise RuntimeError(msg)

        # A previous build may have staged additional scripts. Remove only the
        # build output copy, never the authoritative source tree.
        target_root = Path(self.build_lib) / "ryan-scripts"
        if target_root.exists():
            shutil.rmtree(target_root)
        for item in entries:
            relative = Path(item)
            if relative.is_absolute() or not relative.parts or ".." in relative.parts:
                msg = f"Unsafe installed script manifest path: {item}"
                raise RuntimeError(msg)
            source = scripts_source / relative
            if not source.is_file() or source.is_symlink():
                msg = f"Missing or symlinked installed script: {source}"
                raise RuntimeError(msg)
            destination = target_root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, destination)


if __name__ == "__main__":
    setup(cmdclass={"build_py": _BuildPyWithQgisStyles})
