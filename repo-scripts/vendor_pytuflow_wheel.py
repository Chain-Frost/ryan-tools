"""Verify and stage the exact accepted temporary PyTuflow dependency wheel.

The artifact is repository-side installation input, never ryan-tools package data.
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
from typing import cast
from urllib.parse import urlsplit
from urllib.request import url2pathname

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


def _json_object(raw: str) -> dict[str, object]:
    decoded: object = json.loads(raw)
    if not isinstance(decoded, dict):
        message = "Expected an object in the PyTuflow provenance metadata"
        raise ValueError(message)
    return cast(dict[str, object], decoded)


def _verify_contents(archive: zipfile.ZipFile) -> None:
    bad_member = archive.testzip()
    if bad_member is not None:
        message = f"PyTuflow wheel member has invalid checksum: {bad_member}"
        raise ValueError(message)
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
        message = f"PyTuflow wheel lacks required files: {sorted(missing)}"
        raise ValueError(message)
    if not any(name.startswith(dist) and "license" in name.casefold() for name in names):
        message = "PyTuflow wheel lacks its license notice"
        raise ValueError(message)
    for resource_dir in ("pytuflow/_outputs/data/", "pytuflow/project/data/"):
        if not any(name.startswith(resource_dir) and name.endswith(".json") for name in names):
            message = f"PyTuflow wheel lacks runtime JSON resources: {resource_dir}"
            raise ValueError(message)
    dist_metadata = archive.read(dist + "METADATA").decode("utf-8")
    entry_points = archive.read(dist + "entry_points.txt").decode("utf-8")
    if "Name: pytuflow\n" not in dist_metadata or "Version: 1.3.0\n" not in dist_metadata:
        message = "PyTuflow wheel has incorrect package metadata"
        raise ValueError(message)
    if "pytuflow-project" not in entry_points or "pytuflow-arr" not in entry_points:
        message = "PyTuflow wheel lacks supported CLI entry points"
        raise ValueError(message)


def _verify_manifest() -> None:
    if not MANIFEST_PATH.is_file():
        message = f"PyTuflow provenance manifest is missing: {MANIFEST_PATH}"
        raise FileNotFoundError(message)
    record = _json_object(MANIFEST_PATH.read_text(encoding="utf-8"))
    for key, expected in (
        ("source_repository", SOURCE_REPOSITORY),
        ("source_commit", SOURCE_COMMIT),
        ("source_archive", SOURCE_ARCHIVE),
        ("wheel_filename", WHEEL_NAME),
        ("wheel_sha256", WHEEL_SHA256),
        ("wheel_bytes", WHEEL_BYTES),
    ):
        if record.get(key) != expected:
            message = f"PyTuflow provenance has incorrect {key}: {record.get(key)!r}"
            raise ValueError(message)


def verify_wheel(path: Path = WHEEL_PATH, *, manifest: bool = True) -> None:
    """Fail closed on missing bytes, wrong digest and incomplete wheel contents."""
    if not path.is_file():
        message = f"Accepted PyTuflow wheel is missing: {path}"
        raise FileNotFoundError(message)
    content = path.read_bytes()
    digest = hashlib.sha256(content).hexdigest()
    if len(content) != WHEEL_BYTES or digest != WHEEL_SHA256:
        message = (
            f"PyTuflow wheel is corrupt or from another revision: {path}; "
            f"size={len(content)}, sha256={digest}; expected {WHEEL_BYTES} and {WHEEL_SHA256}"
        )
        raise ValueError(message)
    with zipfile.ZipFile(path) as archive:
        _verify_contents(archive)
    if manifest:
        _verify_manifest()


def verify_installed() -> None:
    """Require pip's installed local archive URL and digest to match the vendored file."""
    verify_wheel()
    installed = metadata.distribution("pytuflow")
    if installed.version != "1.3.0":
        message = f"Expected installed PyTuflow 1.3.0, found {installed.version}"
        raise ValueError(message)
    raw = installed.read_text("direct_url.json")
    if not raw:
        message = "Installed PyTuflow lacks direct_url.json; expected the accepted local wheel"
        raise ValueError(message)
    record = _json_object(raw)
    url = record.get("url")
    archive_info = record.get("archive_info")
    if not isinstance(url, str) or not isinstance(archive_info, dict):
        message = "Installed PyTuflow has no local archive URL and digest"
        raise ValueError(message)
    info = cast(dict[str, object], archive_info)
    hashes = info.get("hashes")
    sha = cast(dict[str, object], hashes).get("sha256") if isinstance(hashes, dict) else None
    if sha != WHEEL_SHA256 and info.get("hash") != f"sha256={WHEEL_SHA256}":
        message = "Installed PyTuflow archive digest differs from the accepted wheel"
        raise ValueError(message)
    parts = urlsplit(url)
    if parts.scheme != "file":
        message = f"Installed PyTuflow is not the accepted local wheel: {url}"
        raise ValueError(message)
    installed_path = Path(url2pathname(parts.path))
    if parts.netloc not in ("", "localhost"):
        installed_path = Path(f"//{parts.netloc}{url2pathname(parts.path)}")
    # Resolve deliberately here: installed-wheel provenance must identify the same physical file.
    if installed_path.resolve() != WHEEL_PATH.resolve():
        message = f"Installed PyTuflow archive differs: {installed_path} != {WHEEL_PATH}"
        raise ValueError(message)


def stage_wheel(candidate: Path) -> None:
    """Record exact reproduced bytes without substituting an unreviewed build."""
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
    parser.add_argument("--installed", action="store_true", help="Check installed direct-URL source and SHA-256")
    args: argparse.Namespace = parser.parse_args(argv)
    if args.stage is not None and args.installed:
        parser.error("--stage and --installed are mutually exclusive")
    try:
        if args.stage is not None:
            stage_wheel(Path(args.stage))
        elif args.installed:
            verify_installed()
        else:
            verify_wheel()
    except (OSError, ValueError, zipfile.BadZipFile, metadata.PackageNotFoundError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    print(f"Verified accepted PyTuflow wheel: {WHEEL_PATH} ({WHEEL_SHA256})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
