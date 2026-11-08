"""Keep CI download recovery bounded, provenance-checked and dependency-aware."""

import hashlib
import importlib.util
import io
import json
import subprocess
import sys
from email.message import Message
from pathlib import Path
from types import ModuleType
from typing import cast
from urllib.error import HTTPError
from urllib.request import Request

import pytest

TAG = "v2026.8.20"
API = "https://api.github.com/repos/cgohlke/geospatial-wheels"
BASE = f"https://github.com/cgohlke/geospatial-wheels/releases/download/{TAG}"
PAYLOAD = b"official wheel fixture bytes"


@pytest.fixture
def bootstrap_module(monkeypatch: pytest.MonkeyPatch) -> ModuleType:
    path = Path(__file__).absolute().parents[2] / "repo-scripts" / "ci_gis_bootstrap.py"
    monkeypatch.setattr(sys, "path", [str(path.parent), *sys.path])
    spec = importlib.util.spec_from_file_location("ci_gis_bootstrap_for_test", path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, spec.name, module)
    spec.loader.exec_module(module)
    monkeypatch.setattr(sys, "argv", [str(path)])
    return module


def _filename(distribution: str = "fiona") -> str:
    return f"{distribution}-1.0-cp314-cp314-win_amd64.whl"


def _failure(distribution: str = "fiona", *, code: int = 504, url: str | None = None) -> str:
    source = url or f"{BASE}/{_filename(distribution)}"
    return f"  ERROR: HTTP error {code} while getting {source} (from https://gisidx.github.io/gwi/{distribution}/)\n"


def _metadata(*, asset_changes: dict[str, object] | None = None) -> dict[str, object]:
    assets: list[dict[str, object]] = []
    for identifier, distribution in enumerate(("fiona", "rasterio", "gdal"), 1):
        record: dict[str, object] = {
            "id": identifier,
            "name": _filename(distribution),
            "browser_download_url": f"{BASE}/{_filename(distribution)}",
            "url": f"{API}/releases/assets/{identifier}",
            "state": "uploaded",
            "size": len(PAYLOAD),
            "digest": f"sha256:{hashlib.sha256(PAYLOAD).hexdigest()}",
        }
        if distribution == "fiona" and asset_changes:
            record.update(asset_changes)
        assets.append(record)
    return {"tag_name": TAG, "assets": assets}


class _Response(io.BytesIO):
    def __init__(self, payload: bytes, url: str) -> None:
        super().__init__(payload)
        self.url = url

    def geturl(self) -> str:
        return self.url


def _stub_api(
    module: ModuleType,
    monkeypatch: pytest.MonkeyPatch,
    *,
    metadata: dict[str, object] | None = None,
    payload: bytes = PAYLOAD,
) -> list[str]:
    calls: list[str] = []
    record = metadata if metadata is not None else _metadata()

    def open_url(request: Request, *, timeout: int) -> _Response:
        assert 0 < timeout <= 30
        assert request.get_header("Authorization") is None
        calls.append(request.full_url)
        if request.full_url == f"{API}/releases/tags/{TAG}":
            assert request.get_header("Accept") == "application/vnd.github+json"
            return _Response(json.dumps(record).encode(), request.full_url)
        assert request.full_url in {f"{API}/releases/assets/{identifier}" for identifier in (1, 2, 3)}
        assert request.get_header("Accept") == "application/octet-stream"
        return _Response(payload, "https://release-assets.githubusercontent.com/fixture.whl")

    monkeypatch.setattr(module, "urlopen", open_url)
    return calls


def _stub_commands(
    module: ModuleType, monkeypatch: pytest.MonkeyPatch, results: list[tuple[int, str]]
) -> list[list[str]]:
    calls: list[list[str]] = []
    outcomes = iter(results)

    def run(command: list[str]) -> subprocess.CompletedProcess[str]:
        calls.append(list(command))
        if "--no-deps" in command:
            assert "--force-reinstall" in command
            assert Path(command[-1]).read_bytes() == PAYLOAD
        code, output = next(outcomes)
        return subprocess.CompletedProcess(command, code, stdout="", stderr=output)

    monkeypatch.setattr(module, "run_command", run)
    return calls


@pytest.mark.parametrize("code", [502, 503, 504])
def test_recognises_only_exact_official_transient_downloads(bootstrap_module: ModuleType, code: int) -> None:
    failure = bootstrap_module.failed_official_wheel(_failure(code=code))
    assert failure is not None
    assert failure.distribution == "fiona"
    assert failure.filename == _filename()
    assert failure.tag == TAG
    assert failure.url == f"{BASE}/{_filename()}"


@pytest.mark.parametrize(
    "url",
    [
        f"http://github.com/cgohlke/geospatial-wheels/releases/download/{TAG}/{_filename()}",
        f"https://github.com.evil.invalid/cgohlke/geospatial-wheels/releases/download/{TAG}/{_filename()}",
        f"https://github.com@evil.invalid/cgohlke/geospatial-wheels/releases/download/{TAG}/{_filename()}",
        f"https://github.com/someone/geospatial-wheels/releases/download/{TAG}/{_filename()}",
        f"{BASE}/../../{_filename()}",
        f"{BASE}/pandas-1.0-cp314-cp314-win_amd64.whl",
        f"{BASE}/{_filename()}?mirror=1",
        f"{BASE}/{_filename()}#fragment",
        "file:///tmp/fiona-1.0-py3-none-any.whl",
    ],
)
def test_rejects_foreign_or_ambiguous_urls(bootstrap_module: ModuleType, url: str) -> None:
    assert bootstrap_module.failed_official_wheel(_failure(url=url)) is None


@pytest.mark.parametrize("code", [400, 401, 403, 404, 408, 429, 500])
def test_other_http_failures_are_not_recovered(bootstrap_module: ModuleType, code: int) -> None:
    assert bootstrap_module.failed_official_wheel(_failure(code=code)) is None


def test_multiple_failed_urls_are_not_guessed(bootstrap_module: ModuleType) -> None:
    assert bootstrap_module.failed_official_wheel(_failure() + _failure("gdal")) is None
    assert bootstrap_module.failed_official_wheel(_failure() * 2) is not None


def test_success_runs_original_bootstrap_once(bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _stub_commands(bootstrap_module, monkeypatch, [(0, "")])
    network = _stub_api(bootstrap_module, monkeypatch)
    assert bootstrap_module.main() == 0
    assert calls == [bootstrap_module._gis_dependency_command()]
    assert network == []


@pytest.mark.parametrize(
    "output", ["ERROR: ResolutionImpossible", "ERROR: No matching distribution", _failure(code=404)]
)
def test_deterministic_failure_preserves_exit_code_without_retry(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch, output: str
) -> None:
    calls = _stub_commands(bootstrap_module, monkeypatch, [(17, output)])
    network = _stub_api(bootstrap_module, monkeypatch)
    assert bootstrap_module.main() == 17
    assert len(calls) == 1
    assert network == []


def test_recovers_each_distribution_once_then_resolves_all_dependencies(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    outcomes = [
        (1, _failure("fiona")),
        (0, ""),
        (1, _failure("rasterio")),
        (0, ""),
        (1, _failure("gdal")),
        (0, ""),
        (0, ""),
    ]
    calls = _stub_commands(bootstrap_module, monkeypatch, outcomes)
    network = _stub_api(bootstrap_module, monkeypatch)
    assert bootstrap_module.main() == 0
    normal = bootstrap_module._gis_dependency_command()
    assert calls[::2] == [normal] * 4
    assert len(network) == 6
    for command, distribution in zip(calls[1::2], ("fiona", "rasterio", "gdal"), strict=True):
        assert command[:-1] == [*bootstrap_module._pip_install_command(), "--force-reinstall", "--no-deps"]
        assert Path(command[-1]).name == _filename(distribution)
        assert not Path(command[-1]).exists(), "Temporary recovery wheels must be cleaned up."


def test_repeated_distribution_failure_stops_without_a_second_recovery(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = _stub_commands(bootstrap_module, monkeypatch, [(1, _failure()), (0, ""), (23, _failure())])
    network = _stub_api(bootstrap_module, monkeypatch)
    assert bootstrap_module.main() == 23
    assert len(calls) == 3
    assert len(network) == 2


def test_failure_after_all_three_recoveries_is_retained(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    outcomes = [
        (1, _failure("fiona")),
        (0, ""),
        (1, _failure("rasterio")),
        (0, ""),
        (1, _failure("gdal")),
        (0, ""),
        (29, _failure()),
    ]
    calls = _stub_commands(bootstrap_module, monkeypatch, outcomes)
    network = _stub_api(bootstrap_module, monkeypatch)
    assert bootstrap_module.main() == 29
    assert len(calls) == 7
    assert len(network) == 6


@pytest.mark.parametrize("install_code", [0, 19])
def test_recovery_install_or_final_resolution_failure_cannot_report_success(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch, install_code: int
) -> None:
    outcomes = [(1, _failure()), (install_code, ""), (31, "ERROR: ResolutionImpossible")]
    calls = _stub_commands(bootstrap_module, monkeypatch, outcomes)
    network = _stub_api(bootstrap_module, monkeypatch)
    assert bootstrap_module.main() == (31 if install_code == 0 else 19)
    assert len(calls) == (3 if install_code == 0 else 2)
    assert len(network) == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"name": "different.whl"},
        {"browser_download_url": "https://example.invalid/foreign.whl"},
        {"url": "https://example.invalid/foreign.whl"},
        {"state": "new"},
        {"id": True},
        {"size": 0},
        {"size": True},
        {"size": 256 * 1024 * 1024 + 1},
        {"digest": None},
        {"digest": "sha1:" + "0" * 40},
    ],
)
def test_invalid_asset_metadata_fails_before_download_or_install(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch, changes: dict[str, object]
) -> None:
    calls = _stub_commands(bootstrap_module, monkeypatch, [(1, _failure())])
    network = _stub_api(bootstrap_module, monkeypatch, metadata=_metadata(asset_changes=changes))
    assert bootstrap_module.main() == 1
    assert len(calls) == len(network) == 1


@pytest.mark.parametrize("payload", [PAYLOAD[:-1], PAYLOAD + b"!", PAYLOAD[:-1] + b"!"])
def test_size_or_hash_mismatch_never_reaches_pip(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch, payload: bytes
) -> None:
    calls = _stub_commands(bootstrap_module, monkeypatch, [(1, _failure())])
    network = _stub_api(bootstrap_module, monkeypatch, payload=payload)
    assert bootstrap_module.main() == 1
    assert len(calls) == 1
    assert len(network) == 2


@pytest.mark.parametrize("failed_request", [1, 2])
def test_api_failure_is_not_retried_or_hidden(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch, failed_request: int
) -> None:
    calls = _stub_commands(bootstrap_module, monkeypatch, [(1, _failure())])
    network = _stub_api(bootstrap_module, monkeypatch)
    original = bootstrap_module.urlopen

    def fail_api(request: Request, *, timeout: int) -> _Response:
        if len(network) + 1 == failed_request:
            network.append(request.full_url)
            raise HTTPError(request.full_url, 503, "Service unavailable", Message(), None)
        return cast(_Response, original(request, timeout=timeout))

    monkeypatch.setattr(bootstrap_module, "urlopen", fail_api)
    assert bootstrap_module.main() == 1
    assert len(calls) == 1
    assert len(network) == failed_request


@pytest.mark.parametrize("payload", [b"x", b""])
def test_deadline_is_checked_after_each_read_including_eof(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch, payload: bytes
) -> None:
    moments = iter([1.0, 11.0])
    monkeypatch.setattr(bootstrap_module.time, "monotonic", lambda: next(moments))
    with pytest.raises(TimeoutError, match="time budget"):
        list(bootstrap_module._bounded_chunks(io.BytesIO(payload), 20, 10.0))


def test_slow_progress_and_oversized_metadata_are_bounded(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(bootstrap_module, "DOWNLOAD_CHUNK_BYTES", 1)
    moments = iter([1.0, 2.0, 3.0, 4.0, 5.0, 11.0])
    monkeypatch.setattr(bootstrap_module.time, "monotonic", lambda: next(moments))
    with pytest.raises(TimeoutError, match="time budget"):
        list(bootstrap_module._bounded_chunks(io.BytesIO(b"abcdef"), 20, 10.0))
    monkeypatch.setattr(bootstrap_module.time, "monotonic", lambda: 0.0)
    with pytest.raises(ValueError, match="size limit"):
        list(bootstrap_module._bounded_chunks(io.BytesIO(b"abcdef"), 3, 10.0))


def test_process_exit_code_and_diagnostics_are_preserved(
    bootstrap_module: ModuleType, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def run(command: list[str], **options: object) -> subprocess.CompletedProcess[str]:
        assert options["check"] is False
        assert 0 < cast(int, options["timeout"]) <= 600
        return subprocess.CompletedProcess(command, 43, stdout="normal output\n", stderr="failure detail\n")

    monkeypatch.setattr(bootstrap_module.subprocess, "run", run)
    result = bootstrap_module.run_command([sys.executable, "-m", "pip", "check"])
    assert result.returncode == 43
    output = capsys.readouterr()
    assert "normal output" in output.out
    assert "failure detail" in output.err
