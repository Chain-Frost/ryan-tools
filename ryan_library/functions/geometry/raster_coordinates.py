"""Pixel-centre coordinate conversion without deprecated Affine tuple multiplication."""

import rasterio


def pixel_centre(transform: rasterio.Affine, row: int, col: int) -> tuple[float, float]:
    """Map zero-based D8 row/column indices to the affine pixel centre.

    Computes the standard six-coefficient affine transformation explicitly.
    Avoids version-dependent Affine tuple-multiplication deprecation warnings
    without assuming the @ operator is supported by installed versions.
    """
    x = float(col) + 0.5
    y = float(row) + 0.5
    return (
        transform.a * x + transform.b * y + transform.c,
        transform.d * x + transform.e * y + transform.f,
    )
