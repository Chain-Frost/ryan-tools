"""Tests for ryan_library.functions.misc_functions."""

from pathlib import Path
import sys

# Ensure the project root is importable when tests are run directly.
PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pytest

from ryan_library.functions import misc_functions


def test_calculate_pool_size_single_core(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure a single CPU core always yields one worker."""

    monkeypatch.setattr(misc_functions.multiprocessing, "cpu_count", lambda: 1)

    assert misc_functions.calculate_pool_size(num_files=100) == 1


def test_calculate_pool_size_respects_available_minus_one(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pool size should be limited by available cores minus one when splits are larger."""

    monkeypatch.setattr(misc_functions.multiprocessing, "cpu_count", lambda: 4)

    assert misc_functions.calculate_pool_size(num_files=30) == 3
    assert misc_functions.calculate_pool_size(num_files=6) == 2


def test_calculate_pool_size_caps_high_cpu_counts(monkeypatch: pytest.MonkeyPatch) -> None:
    """The helper should cap CPU counts at twenty before subtracting one."""

    monkeypatch.setattr(misc_functions.multiprocessing, "cpu_count", lambda: 32)

    assert misc_functions.calculate_pool_size(num_files=120) == 19


def test_calculate_pool_size_never_returns_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure the minimum split logic never returns zero workers."""

    monkeypatch.setattr(misc_functions.multiprocessing, "cpu_count", lambda: 4)

    assert misc_functions.calculate_pool_size(num_files=2) == 1
