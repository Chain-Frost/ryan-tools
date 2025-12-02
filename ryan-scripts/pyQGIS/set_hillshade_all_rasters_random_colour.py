from __future__ import annotations

import random

from qgis.core import QgsProject, QgsRasterLayer, QgsHillshadeRenderer
from qgis.PyQt.QtGui import QColor


def _rand_nice_color() -> QColor:
    # Avoid muddy/dark colors: random hue, high saturation, high value
    return QColor.fromHsv(random.randint(0, 359), 200, 255)


def _make_hillshade_renderer(layer: QgsRasterLayer, band: int, z_factor: float, azimuth: float, altitude: float):
    """
    QgsHillshadeRenderer ctor signature varies across QGIS builds.
    Try common signatures.
    """
    provider = layer.dataProvider()

    # QGIS 3.38+ docs: (input, band, lightAzimuth, lightAltitude)
    try:
        r = QgsHillshadeRenderer(provider, band, azimuth, altitude)
        if hasattr(r, "setZFactor"):
            r.setZFactor(z_factor)
        return r
    except TypeError:
        pass

    # Older builds: (input, band, zFactor, azimuth, altitude)
    try:
        r = QgsHillshadeRenderer(provider, band, z_factor, azimuth, altitude)
        return r
    except TypeError as e:
        raise TypeError(f"Cannot construct QgsHillshadeRenderer on this QGIS build: {e}") from e


def set_all_rasters_to_hillshade_and_random_color(
    z_factor: float = 5.0,
    azimuth: float = 315.0,
    altitude: float = 45.0,
    colorize_strength: int = 70,  # 0..100
) -> None:
    for lyr in QgsProject.instance().mapLayers().values():
        if not isinstance(lyr, QgsRasterLayer):
            continue
        if not lyr.isValid():
            continue

        band = 1  # adjust if you need something else
        hs = _make_hillshade_renderer(lyr, band, z_factor, azimuth, altitude)
        lyr.setRenderer(hs)

        # Colorize the *rendered* output (works well with hillshade)
        c = _rand_nice_color()
        f = lyr.hueSaturationFilter()
        f.setColorizeOn(True)
        f.setColorizeColor(c)
        f.setColorizeStrength(int(max(0, min(100, colorize_strength))))

        lyr.triggerRepaint()


set_all_rasters_to_hillshade_and_random_color(z_factor=5.0, colorize_strength=70)
