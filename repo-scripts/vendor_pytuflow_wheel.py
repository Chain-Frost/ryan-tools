"""Validate and stage the temporary accepted PyTuflow wheel.

This is a dependency artifact, never package data in the ryan-tools wheel.
Only the exact accepted bytes can be installed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import zipfile
from importlib import metadata
from pathlib import Path

SOURCE_COMMIT = "887534ea8c15b80d185a38a7ead1d42ab42a7167"
SOURCE_REPOSITORY = "https://github.com/Chain-Frost/PyTuflow"
SOURCE_ARCHIVE = f"{SOURCE_REPOSITORY}/archive/{SOURCE_COMMIT}.zip"
WHEEL_NAME = "pytuflow-1.3.0-py3-none-any.whl"
WHEEL_SHA256 = "4bfcd24734349f9e0dcf60776ee06a2e17953ac342f40e3611b0e2aae33b30e3"
WHEEL_BYTES = 976_447
WHEEL_DIR = Path(__file__).absolute().parent / "wheels" / "pytuflow" / SOURCE_COMMIT
WHEEL_PATH = WHEEL_DIR / WHEEL_NAME
MANIFEST_PATH = WHEEL_DIR / "provenance.json"
ACCEPTANCE_JOBS = (
    "https://github.com/Chain-Frost/ryan-tools/actions/runs/42453824703/job/128648480823",
    "https://github.com/Chain-Frost/ryan-tools/actions/runs/42453824693/job/128648567535",
    "https://github.com/Chain-Frost/ryan-tools/actions/runs/42453824703/job/128648861058",
)


def verify_wheel(path: Path = WHEEL_PATH, *, manifest: bool = True) -> None:
    """Fail closed on missing bytes, wrong digest or incomplete wheel contents."""
    if not path.is_file():
        raise FileNotFoundError(f"Accepted PyTuflow wheel is missing: {path}")
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if len(content) != WHEEL_BYTES or digest != WHEEL_SHA256:
        raise ValueError(
            f"PyTuflow wheel is corrupt or from another revision: {path}; "
            f"size={len(content)}, sha256={digest}; expected {WHEEL_BYTES} and {WHEEL_SHA256}"
        )
    with zipfile.ZipFile(path) as archive:
        bad_member = archive.testzip()
        if bad_member is not None:
            raise ValueError(f"PyTuflow wheel member has invalid checksum: {bad_member}")
        names = set(archive.namelist())
        dist = "pytuflow-1.3.0.dist-info/"
        required = {
            "pytuflow/__init__.py",
            "pytuflow/py.typed",
            dist + "METADATA",
            dist + "WHEEL",
            dist + "entry_points.txt",
            dist + "RECORD",
        }
        if missing := required - names:
            raise ValueError(f"PyTuflow wheel lacks required files: {sorted(missing)}")
        if not any(name.startswith(dist) and "license" in name.casefold() for name in names):
            raise ValueError("PyTuflow wheel lacks its license notice")
        for resource_dir in ("pytuflow/_outputs/data/", "pytuflow/project/data/"):
            if not any(name.startswith(resource_dir) and name.endswith(".json") for name in names):
                raise ValueError(f"PyTuflow wheel lacks runtime JSON resources: {resource_dir}")
        dist_metadata = archive.read(dist + "METADATA").decode("utf-8")
        entry_points = archive.read(dist + "entry_points.txt").decode("utf-8")
        if "Name: pytuflow\n" not in dist_metadata or "Version: 1.3.0\n" not in dist_metadata:
            raise ValueError("PyTuflow wheel has incorrect package metadata")
        if "pytuflow-project" not in entry_points or "pytuflow-arr" not in entry_points:
            raise ValueError("PyTuflow wheel lacks supported CLI entry points")
    if manifest:
        if not MANIFEST_PATH.is_file():
            raise FileNotFoundError(f"PyTuflow provenance manifest is missing: {MANIFEST_PATH}")
        record = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        for key, expected in (
            ("source_repository", SOURCE_REPOSITORY),
            ("source_commit", SOURCE_COMMIT),
            ("source_archive", SOURCE_ARCHIVE),
            ("wheel_filename", WHEEL_NAME),
            ("wheel_sha256", WHEEL_SHA256),
            ("wheel_bytes", WHEEL_BYTES),
        ):
            if record.get(key) != expected:
                raise ValueError(f"PyTuflow provenance has incorrect {key}: {record.get(key)!r}")


def stage_wheel(candidate: Path) -> None:
    """Write a provenance record only after verifying the accepted wheel bytes."""
    verify_wheel(candidate, manifest=False)
    WHEEL_DIR.mkdir(parents=True, exist_ok=True)
    if candidate.absolute() != WHEEL_PATH.absolute():
        shutil.copyfile(candidate, WHEEL_PATH)
    record = {
        "source_repository": SOURCE_REPOSITORY,
        "source_commit": SOURCE_COMMIT,
        "source_archive": SOURCE_ARCHIVE,
        "wheel_filename": WHEEL_NAME,
        "wheel_sha256": WHEEL_SHA256,
        "wheel_bytes": WHEEL_BYTES,
        "build_command": "python -m pip wheel --no-deps --no-cache-dir --no-build-isolation --wheel-dir <temp> <source_archive>",
        "build_python": sys.version.split()[0],
        "build_pip": metadata.version("pip"),
        "build_hatchling": metadata.version("hatchling"),
        "acceptance_jobs": list(ACCEPTANCE_JOBS),
    }
    MANIFEST_PATH.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    verify_wheel()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stage", type=Path, help="Import a reproduced exact-hash candidate")
    args = parser.parse_args(argv)
    try:
        if args.stage is not None:
            stage_wheel(args.stage)
        else:
            verify_wheel()
    except (OSError, ValueError, zipfile.BadZipFile, metadata.PackageNotFoundError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Verified accepted PyTuflow wheel: {WHEEL_PATH} ({WHEEL_SHA256})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
