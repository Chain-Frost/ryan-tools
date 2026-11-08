"""Recover bounded GitHub asset download failures in the Windows CI GIS bootstrap."""

import argparse
import hashlib
import json
import re
import subprocess
import sys
import time
from collections.abc import Iterator
from dataclasses import dataclass
from http.client import HTTPException
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Protocol, cast
from urllib.parse import quote, urlsplit
from urllib.request import Request, urlopen

# Reuse the installer's exact command builders for CI-only transport recovery.
from install_latest_wheel import _gis_dependency_command, _pip_install_command  # pyright: ignore[reportPrivateUsage]

GITHUB_REPOSITORY = "cgohlke/geospatial-wheels"
API_ROOT = f"https://api.github.com/repos/{GITHUB_REPOSITORY}"
MAX_BOOTSTRAP_ATTEMPTS = 4
COMMAND_TIMEOUT_SECONDS = 600
NETWORK_TIMEOUT_SECONDS = 30
METADATA_BUDGET_SECONDS = 60
DOWNLOAD_BUDGET_SECONDS = 120
MAX_METADATA_BYTES = 2 * 1024 * 1024
MAX_WHEEL_BYTES = 256 * 1024 * 1024
DOWNLOAD_CHUNK_BYTES = 64 * 1024
TRANSIENT_FAILURE = re.compile(r"^\s*ERROR: HTTP error (?:502|503|504) while getting (\S+)", re.MULTILINE)
OFFICIAL_WHEEL = re.compile(
    r"https://github\.com/cgohlke/geospatial-wheels/releases/download/"
    r"(?P<tag>[A-Za-z0-9][A-Za-z0-9._-]*)/"
    r"(?P<filename>(?P<distribution>fiona|rasterio|gdal)-[A-Za-z0-9_.+-]+\.whl)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class FailedWheel:
    """Exact official wheel selected by the failed normal pip resolution."""

    url: str
    tag: str
    filename: str
    distribution: str


@dataclass(frozen=True, slots=True)
class ReleaseAsset:
    """Identity and integrity data obtained from the official public release API."""

    api_url: str
    filename: str
    size: int
    sha256: str


class ResponseReader(Protocol):
    """A read1 call makes at most one underlying socket read."""

    def read1(self, size: int, /) -> bytes: ...


def failed_official_wheel(output: str) -> FailedWheel | None:
    """Recover only one unambiguous transient HTTP error for an approved GIS wheel."""
    urls = set(TRANSIENT_FAILURE.findall(output))
    if len(urls) != 1:
        return None
    url = urls.pop()
    match = OFFICIAL_WHEEL.fullmatch(url)
    if match is None:
        return None
    return FailedWheel(url, match["tag"], match["filename"], match["distribution"].lower())


def _request(url: str, accept: str) -> Request:
    """Use public GitHub endpoints without forwarding any repository credentials."""
    endpoint = re.escape(API_ROOT) + r"/releases/(?:tags/[A-Za-z0-9][A-Za-z0-9._-]*|assets/[1-9][0-9]*)"
    if re.fullmatch(endpoint, url) is None:
        message = "Recovery requires an exact official GitHub release API endpoint."
        raise ValueError(message)
    return Request(  # noqa: S310 - The complete HTTPS API endpoint is allowlisted above.
        url,
        headers={
            "Accept": accept,
            "User-Agent": "ryan-tools-ci-gis-bootstrap",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )


def _check_response_url(url: str) -> None:
    """Accept HTTPS API responses and GitHub's normal release-asset redirects."""
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or parsed.hostname
        not in {"api.github.com", "release-assets.githubusercontent.com", "objects.githubusercontent.com"}
        or parsed.username is not None
        or parsed.password is not None
        or parsed.port not in {None, 443}
    ):
        message = "Recovery response did not come from an approved HTTPS GitHub endpoint."
        raise ValueError(message)


def _object(value: object, description: str) -> dict[str, object]:
    """Reject malformed API data before interpreting its fields."""
    if not isinstance(value, dict):
        message = f"Expected a JSON object for {description}."
        raise ValueError(message)
    return cast(dict[str, object], value)


def _positive_integer(value: object, description: str, maximum: int) -> int:
    if type(value) is not int or not 0 < value <= maximum:
        message = f"Invalid or oversized {description}: {value!r}."
        raise ValueError(message)
    return value


def _bounded_chunks(response: ResponseReader, maximum: int, deadline: float) -> Iterator[bytes]:
    """Bound slow-progress responses by bytes and a deadline plus one socket timeout."""
    total = 0
    while True:
        if time.monotonic() >= deadline:
            message = "GitHub response exceeded the recovery time budget."
            raise TimeoutError(message)
        block = response.read1(min(DOWNLOAD_CHUNK_BYTES, maximum - total + 1))
        if time.monotonic() >= deadline:
            message = "GitHub response exceeded the recovery time budget."
            raise TimeoutError(message)
        if not block:
            return
        total += len(block)
        if total > maximum:
            message = "GitHub response exceeded the recovery size limit."
            raise ValueError(message)
        yield block


def release_asset(failure: FailedWheel) -> ReleaseAsset:
    """Resolve the failed tag and filename without choosing another wheel or version."""
    url = f"{API_ROOT}/releases/tags/{quote(failure.tag, safe='')}"
    deadline = time.monotonic() + METADATA_BUDGET_SECONDS
    with urlopen(_request(url, "application/vnd.github+json"), timeout=NETWORK_TIMEOUT_SECONDS) as response:  # noqa: S310 - Constructed HTTPS API URL; no arbitrary URL input.
        _check_response_url(str(response.geturl()))
        payload = b"".join(_bounded_chunks(cast(ResponseReader, response), MAX_METADATA_BYTES, deadline))
    release = _object(json.loads(payload), "release")
    if release.get("tag_name") != failure.tag:
        message = "GitHub returned a different release tag."
        raise ValueError(message)
    assets = release.get("assets")
    if not isinstance(assets, list):
        message = "GitHub release has no asset list."
        raise ValueError(message)
    candidates = [
        _object(item, "release asset")
        for item in cast(list[object], assets)
        if _object(item, "release asset").get("name") == failure.filename
    ]
    if len(candidates) != 1:
        message = "Expected exactly one matching official wheel asset."
        raise ValueError(message)
    asset = candidates[0]
    asset_id = _positive_integer(asset.get("id"), "asset ID", sys.maxsize)
    api_url = f"{API_ROOT}/releases/assets/{asset_id}"
    if (
        asset.get("browser_download_url") != failure.url
        or asset.get("url") != api_url
        or asset.get("state") != "uploaded"
    ):
        message = "GitHub asset identity does not match the failed official download."
        raise ValueError(message)
    size = _positive_integer(asset.get("size"), "asset size", MAX_WHEEL_BYTES)
    digest = asset.get("digest")
    if not isinstance(digest, str) or re.fullmatch(r"sha256:[0-9a-fA-F]{64}", digest) is None:
        message = "The official asset does not provide a valid SHA-256 digest."
        raise ValueError(message)
    return ReleaseAsset(api_url, failure.filename, size, digest.removeprefix("sha256:").lower())


def download_asset(asset: ReleaseAsset, directory: Path) -> Path:
    """Download one bounded asset, then require its exact published size and digest."""
    destination = directory / asset.filename
    digest = hashlib.sha256()
    total = 0
    deadline = time.monotonic() + DOWNLOAD_BUDGET_SECONDS
    with urlopen(_request(asset.api_url, "application/octet-stream"), timeout=NETWORK_TIMEOUT_SECONDS) as response:  # noqa: S310 - API URL was constructed and verified against official metadata.
        _check_response_url(str(response.geturl()))
        with destination.open("xb") as wheel:
            for block in _bounded_chunks(cast(ResponseReader, response), asset.size, deadline):
                total += len(block)
                digest.update(block)
                wheel.write(block)
    if total != asset.size or digest.hexdigest() != asset.sha256:
        message = "Downloaded wheel failed official size/SHA-256 verification."
        raise ValueError(message)
    print(f"Verified {asset.filename}: {total} bytes; sha256={asset.sha256}; source={asset.api_url}", flush=True)
    return destination


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    """Retain pip diagnostics and return its real exit status, with a process bound."""
    print(f"Command: {subprocess.list2cmdline(command)}", flush=True)
    result = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=COMMAND_TIMEOUT_SECONDS,
    )
    print(result.stdout, end="", flush=True)
    print(result.stderr, end="", file=sys.stderr, flush=True)
    return result


def bootstrap() -> int:
    """Allow one recovery per GIS distribution and always finish normal resolution."""
    command = _gis_dependency_command()
    recovered: set[str] = set()
    for attempt in range(1, MAX_BOOTSTRAP_ATTEMPTS + 1):
        print(f"CI GIS bootstrap attempt {attempt}/{MAX_BOOTSTRAP_ATTEMPTS}", flush=True)
        result = run_command(command)
        if result.returncode == 0:
            return 0
        failure = failed_official_wheel(result.stdout + "\n" + result.stderr)
        if failure is None:
            return result.returncode
        if failure.distribution in recovered or attempt == MAX_BOOTSTRAP_ATTEMPTS:
            print("ERROR: GIS download recovery limit reached; retaining the bootstrap failure.", file=sys.stderr)
            return result.returncode
        print(f"Recovering the same official wheel via GitHub's asset API: {failure.url}", flush=True)
        asset = release_asset(failure)
        with TemporaryDirectory(prefix="ryan-ci-gis-") as temporary:
            wheel = download_asset(asset, Path(temporary))
            install = run_command([*_pip_install_command(), "--force-reinstall", "--no-deps", str(wheel)])
            if install.returncode != 0:
                return install.returncode
        recovered.add(failure.distribution)
        print("Re-running the original GIS bootstrap with full dependency resolution.", flush=True)
    return 1


def main() -> int:
    """Run the CI-only recovery without changing normal user installation commands."""
    argparse.ArgumentParser(description=__doc__).parse_args()
    try:
        return bootstrap()
    except (OSError, ValueError, HTTPException, subprocess.TimeoutExpired) as error:
        print(f"ERROR: CI GIS bootstrap recovery failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
