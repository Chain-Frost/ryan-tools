"""Focused tests for the vendored 3D transform and hydrograph import boundary."""

from __future__ import annotations

import subprocess
import sys

import numpy as np
import pytest

from ryan_library.functions.geometry import find_similarity_transform

SOURCE = np.array(
    [[0, 0, 0], [0, 2, 2], [2, 3, 1], [3, 1, 2], [1, 1, 3]],
    dtype=np.float64,
)
TARGET = np.array(
    [[3, 7, 5], [6, 7, 2], [4.5, 4, 0.5], [6, 2.5, 3.5], [7.5, 5.5, 3.5]],
    dtype=np.float64,
)


def test_similarity_solver_recovers_known_transform() -> None:
    """Reconstruct the upstream worked example without altering the algorithm."""
    scale, rotation, translation = find_similarity_transform(SOURCE, TARGET)
    assert scale == pytest.approx(1.5, abs=1e-10)
    assert rotation.shape == (3, 3)
    assert translation.shape == (3, 1)
    np.testing.assert_allclose((scale * rotation @ SOURCE.T + translation).T, TARGET, atol=1e-10)


def test_similarity_fixed_scale_and_weights() -> None:
    """Forward caller options to the source numerical implementation."""
    scale, rotation, translation = find_similarity_transform(
        SOURCE, TARGET, weights=[100, 20, 2, 20, 50], estimate_scale=False, initial_scale=1.25
    )
    assert scale == pytest.approx(1.25)
    assert np.isfinite(rotation).all()
    assert np.isfinite(translation).all()


def test_similarity_rejects_insufficient_control_points() -> None:
    """Preserve upstream input validation without replacing its solver."""
    with pytest.raises(ValueError, match="two distinct source points"):
        find_similarity_transform([[1.0, 2.0, 3.0]], [[2.0, 3.0, 4.0]])


@pytest.mark.parametrize(
    "source_points",
    [
        [[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]],
        [[0.0, 0.0, 0.0], [1.0, 1.0, 1.0], [2.0, 2.0, 2.0]],
    ],
)
def test_similarity_accepts_distinct_points_with_matching_xyz_axes(source_points: list[list[float]]) -> None:
    """Distinct XYZ controls must not be confused with equal coordinate axes."""
    source = np.asarray(source_points, dtype=np.float64)
    target = source + np.array([3.0, 7.0, 5.0])

    multiplier, rotation, translation = find_similarity_transform(
        source, target, estimate_scale=False, initial_scale=1.0
    )

    assert multiplier == pytest.approx(1.0)
    np.testing.assert_allclose((multiplier * rotation @ source.T + translation).T, target, atol=1e-10)


@pytest.mark.parametrize("count", [2, 3])
def test_similarity_rejects_duplicate_source_points_before_eigensolver(count: int) -> None:
    """Identical complete control points fail consistently in the vendor and public APIs."""
    source = [[1.0, 2.0, 3.0]] * count
    target = [[2.0 + i, 3.0 + i, 4.0 + i] for i in range(count)]
    from vendor.simil import find as vendor_find

    with pytest.raises(ValueError, match="two distinct source points"):
        vendor_find(source, target)

    with pytest.raises(ValueError, match="two distinct source points"):
        find_similarity_transform(source, target)


@pytest.mark.parametrize(
    ("weights", "match"),
    [
        ([1.0, 0.0], "two distinct source points must have positive"),
        ([0.0, 0.0], "two distinct source points must have positive"),
        ([-1.0, 1.0], "weights must be finite and nonnegative"),
        ([float("nan"), 1.0], "weights must be finite and nonnegative"),
        ([float("inf"), 1.0], "weights must be finite and nonnegative"),
        ([float("-inf"), 1.0], "weights must be finite and nonnegative"),
    ],
)
def test_similarity_rejects_invalid_two_point_weights(weights: list[float], match: str) -> None:
    """Reject an underdetermined or invalid weighted fit in both import paths."""
    from vendor.simil import find as vendor_find

    source = [[0.0, 0.0, 0.0], [1.0, 1.0, 1.0]]
    target = [[2.0, 3.0, 4.0], [3.0, 4.0, 5.0]]
    with pytest.raises(ValueError, match=match):
        vendor_find(source, target, alpha_0=weights)
    with pytest.raises(ValueError, match=match):
        find_similarity_transform(source, target, weights=weights)


def test_similarity_rejects_duplicate_positive_weighted_controls() -> None:
    """An ignored distinct control does not rescue repeated active coordinates."""
    from vendor.simil import find as vendor_find

    source = [[1.0, 2.0, 3.0], [1.0, 2.0, 3.0], [2.0, 3.0, 4.0]]
    target = [[4.0, 5.0, 6.0], [4.0, 5.0, 6.0], [5.0, 6.0, 7.0]]
    weights = [1.0, 2.0, 0.0]
    with pytest.raises(ValueError, match="two distinct source points must have positive"):
        vendor_find(source, target, alpha_0=weights)
    with pytest.raises(ValueError, match="two distinct source points must have positive"):
        find_similarity_transform(source, target, weights=weights)


def test_similarity_accepts_zero_weight_controls_with_sufficient_active_geometry() -> None:
    """Unused controls are valid when enough distinct positively weighted points remain."""
    from vendor.simil import find as vendor_find

    weights = [1.0, 0.0, 1.0, 0.0, 1.0]
    for solver in (
        lambda: vendor_find(SOURCE, TARGET, alpha_0=weights),
        lambda: find_similarity_transform(SOURCE, TARGET, weights=weights),
    ):
        scale, rotation, translation = solver()
        assert scale == pytest.approx(1.5, abs=1e-10)
        np.testing.assert_allclose((scale * rotation @ SOURCE.T + translation).T, TARGET, atol=1e-10)


def test_vendor_imports_are_independent_and_pyhma_runs() -> None:
    """Only explicit HMA access should load its additional scientific stack."""
    code = """
import sys
from vendor.simil import find
assert callable(find)
assert 'vendor.PyHMA' not in sys.modules
from vendor import HMA
import numpy as np
assert HMA is __import__('vendor.PyHMA', fromlist=['HMA']).HMA
q = np.array([1., 2., 4., 3., 1.])
score = HMA(q, q).calc_dense2()
assert np.isclose(score, 1.0), score
"""
    subprocess.run([sys.executable, "-c", code], check=True, capture_output=True, text=True, timeout=60)
