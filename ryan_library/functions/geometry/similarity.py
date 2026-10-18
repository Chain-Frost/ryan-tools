"""Fit a 3D similarity transform using the retained third-party simil solver."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
from numpy.typing import NDArray

from vendor.simil import find

type FloatArray = NDArray[np.float64]
type PointRows = Sequence[Sequence[float]] | FloatArray


def find_similarity_transform(
    source_points: PointRows,
    target_points: PointRows,
    *,
    weights: Sequence[float] | FloatArray | None = None,
    estimate_scale: bool = True,
    initial_scale: float = 1.0,
) -> tuple[float, FloatArray, FloatArray]:
    """Fit a 3D Helmert/similarity transform and return scale, rotation and translation.

    Input point rows must each have three coordinates, with at least two
    distinct source points. Corresponding source/target points share a row.
    If `weights` is supplied, provide one nonnegative weight per control
    point. The upstream solver validates weight dimensions but does not
    guarantee rejection of all nonfinite or negative weights.

    Returns a dimensionless multiplier, a (3, 3) rotation matrix and a
    (3, 1) translation vector. Transform an (n, 3) source array with
    `(multiplier * rotation @ source.T + translation).T`.

    With `estimate_scale=False`, `initial_scale` fixes the multiplier.
    A negative initial scale requests reflection as supported upstream.
    The returned coordinates and translation use the target frame; input
    points use the source frame. Each point set must have consistent units.
    This is not a CRS transformation and does not perform reprojection.

    The numerical method is delegated to `vendor.simil`; no duplicate
    implementation is maintained in `ryan_library`.
    """
    return find(
        source_points,
        target_points,
        alpha_0=weights,
        scale=estimate_scale,
        lambda_0=initial_scale,
    )
