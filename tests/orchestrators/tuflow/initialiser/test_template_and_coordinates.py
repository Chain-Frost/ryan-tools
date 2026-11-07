"""TUFLOW template commands and affine conversions for Windows compatibility."""

from pathlib import Path

import pytest
from rasterio.transform import from_origin

from ryan_library.orchestrators.tuflow.initialiser.coordinates import pixel_centre

_TEMPLATE_ROOT = Path(__file__).resolve().parents[4] / "ryan_library" / "orchestrators" / "tuflow" / "initialiser" / "resources" / "tuflow_templates"


def test_ensemble_event_source_keyword_is_valid() -> None:
    text = (_TEMPLATE_ROOT / "model" / "EVENTS_01.tef").read_text(encoding="utf-8")
    block = text.split("Define Event == EnsembleILCL", maxsplit=1)[1].split("End Define", maxsplit=1)[0]
    assert "BC Event Source == ~CC~ | Near-term_SSP3-7.0" in block
    assert "BC BC Event Source" not in text


def test_check_files_reference_configured_outputroot() -> None:
    templates = tuple((_TEMPLATE_ROOT / "runs").glob("*.tcf"))
    assert templates
    matching = 0
    for path in templates:
        text = path.read_text(encoding="utf-8")
        if "Write Check Files ==" not in text:
            continue
        matching += 1
        assert "<<OutputRoot>>\\check\\" in text
        assert "OutputRoot>>\\check\\" not in text.replace("<<OutputRoot>>", "")
    assert matching > 0


def test_pixel_centre_matches_map_coordinates_on_rectangular_grid() -> None:
    transform = from_origin(100, 200, 2, 3)
    assert pixel_centre(transform, 0, 0) == pytest.approx((101, 198.5))
    assert pixel_centre(transform, 3, 7) == pytest.approx((115, 189.5))
