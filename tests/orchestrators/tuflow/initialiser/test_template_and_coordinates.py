"""TUFLOW template commands and affine conversions for Windows compatibility."""

import re
from pathlib import Path

import pytest
from rasterio.transform import from_origin

from ryan_library.functions.geometry.raster_coordinates import pixel_centre

_TEMPLATE_ROOT = Path(__file__).resolve().parents[4] / "ryan_library" / "resources" / "tuflow_templates"


def test_climate_event_is_independent_of_rainfall_method() -> None:
    text = (_TEMPLATE_ROOT / "model" / "EVENTS_01.tef").read_text(encoding="utf-8")
    block = text.split("Define Event == EnsembleILCL", maxsplit=1)[1].split("End Define", maxsplit=1)[0]
    assert "BC Event Source == ~EnsembleILCL~ | EnsembleILCL" in block
    assert "~CC~" not in block
    climate = text.split("Define Event == CCNear", maxsplit=1)[1].split("End Define", maxsplit=1)[0]
    assert "BC Event Source == ~CC~ | Near-term_SSP3-7.0" in climate
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


def test_standard_template_has_active_boundary_read() -> None:
    text = (_TEMPLATE_ROOT / "model" / "BOUND_01.tbc").read_text(encoding="utf-8")
    assert "    Read GIS BC ==" in text
    assert "<<BC_READ_COMMENT>>" not in text


def test_pixel_centre_matches_map_coordinates_on_rectangular_grid() -> None:
    transform = from_origin(100, 200, 2, 3)
    assert pixel_centre(transform, 0, 0) == pytest.approx((101, 198.5))
    assert pixel_centre(transform, 3, 7) == pytest.approx((115, 189.5))


@pytest.mark.parametrize(
    "minutes",
    [15, 30, 45, 60, 90, 120, 150, 180, 240, 270, 300, 360, 540, 720, 1080, 1440, 1800, 2160, 2880, 4320, 5760, 7200],
)
def test_duration_aliases_select_same_end_time(minutes: int) -> None:
    events = (_TEMPLATE_ROOT / "model" / "EVENTS_01.tef").read_text(encoding="utf-8")
    run_times = (_TEMPLATE_ROOT / "runs" / "run_times_01.trd").read_text(encoding="utf-8")
    long_flag = f"{minutes:05}m"
    short_flag = f"{minutes:04}m"
    for flag in (long_flag, short_flag):
        blocks = re.findall(rf"^Define Event == {flag}\n(.*?)End Define", events, flags=re.MULTILINE | re.DOTALL)
        assert len(blocks) == 1
        normalized = "\n".join(" ".join(line.split()) for line in blocks[0].splitlines())
        assert f"BC Event Source == ~DUR5~ | {long_flag}" in normalized
        assert f"BC Event Source == ~DUR4~ | {short_flag}" in normalized
    assert f"If Event == {long_flag} | {short_flag}" in run_times
    if minutes == 540:
        assert "If Event == 00540m | 0540m\n\tEnd Time == 18 ! hours" in run_times


def test_output_paths_share_base_and_separate_climate_selection() -> None:
    path = next((_TEMPLATE_ROOT / "runs").glob("__project__*.tcf"))
    assert "~e5~" in path.name
    commands = {
        line.split("==", maxsplit=1)[0].strip(): line.split("==", maxsplit=1)[1].strip()
        for line in path.read_text(encoding="utf-8").splitlines()
        if "==" in line and not line.lstrip().startswith("!")
    }
    assert commands["Set Variable OutputBase"] == ".."
    assert commands["Set Variable OutputRoot"].endswith("\\<<~e5~>>")
    assert commands["Output Folder"] == "<<OutputBase>>\\<<OutputRoot>>"
    assert commands["Log Folder"] == "<<OutputBase>>\\<<OutputRoot>>\\Log"
    assert commands["Write Check Files"] == "<<OutputBase>>\\<<OutputRoot>>\\check\\"
    assert commands["Store Maximums and Minimums"] == "ON MAXIMUMS ONLY"
    assert commands["Pause"] == "No rainfall input selected"
