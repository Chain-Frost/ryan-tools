"""In-process native PyTUFLOW 1.3.1 SSP acceptance with synthetic ARR layer schemas.

The genuine pinned ArrEngine assembles events and losses. Only the TP source
is replaced by one deterministic point pattern to avoid network downloads.
This is not acceptance against a hydraulic executable or a real Data Hub site.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any, cast

import pytest

from ryan_library.functions.hydrology.arr_user_climate_losses import (
    native_omitted_preburst_adjustments,
    user_loss_adjustments,
)
from ryan_library.functions.hydrology.rainfall_loss_screening import LossSpec, ScreeningStatus
from ryan_library.orchestrators.hydrology.rainfall_loss_workflow import assemble_arr_events


class _OfflinePatterns:
    """One unmodified 60-minute point TP; no downloads or file access."""

    def patterns(
        self, duration: float, aep_name: str, output_notation: str = "aep", **_kwargs: object
    ) -> list[SimpleNamespace]:
        assert duration == 60
        assert aep_name == "1%"
        assert output_notation == "aep"
        return [
            SimpleNamespace(
                event_id=1,
                tp_number=1,
                timestep=60.0,
                increments=[100.0],
                source="point",
                region="synthetic",
            )
        ]


def _response(preburst_ratio: float) -> Any:
    """ARR Data Hub-shaped arrays; all numbers chosen expressly for the regression."""
    api = pytest.importorskip("pytuflow.arr.api_client")
    return api.ArrApiResponse(
        {
            "layers": {
                "RecIFD": {
                    "Default Historical (1961-1990) Baseline": {
                        "index": [60, 120],
                        "columns": [1, 50],
                        "data": [[100.0, 100.0], [120.0, 120.0]],
                    },
                    "Default Current (2030) Baseline": {
                        "index": [60, 120],
                        "columns": [1, 50],
                        "data": [[110.0, 110.0], [132.0, 132.0]],
                    },
                },
                "ARFParams": {
                    "a": 0.327,
                    "b": 0.241,
                    "c": 0.448,
                    "d": 0.36,
                    "e": 0.00096,
                    "f": 0.48,
                    "g": -0.21,
                    "h": 0.012,
                    "i": -0.0013,
                },
                "StormLossesNonNSW": {
                    "Storm Initial Losses (mm)": 100.0,
                    "Storm Continuing Losses (mm/h)": 0.0,
                },
                "Preburst50": {
                    "index": [60, 120],
                    "columns": [1, 50],
                    "data": [
                        [preburst_ratio, preburst_ratio],
                        [preburst_ratio, preburst_ratio],
                    ],
                },
                "ClimateChange": {
                    "rainfall_factors": {
                        "SSP2-4.5": {
                            "index": [2030, 2050],
                            "columns": ["<1 hour", "12 Hours"],
                            "data": [[1.0, 1.0], [1.2, 1.2]],
                        }
                    },
                    "loss_factors": {
                        "Initial_Loss": {
                            "index": [2030, 2050],
                            "columns": ["Losses SSP2-4.5"],
                            "data": [[0.9], [0.8]],
                        },
                        "Continuing_Loss": {
                            "index": [2030, 2050],
                            "columns": ["Losses SSP2-4.5"],
                            "data": [[1.1], [1.2]],
                        },
                    },
                },
            }
        }
    )


@pytest.mark.parametrize("preburst_ratio", [0.005, 0.05])
def test_pinned_native_arr_engine_ssp_loss_parity_and_pb_safety(preburst_ratio: float) -> None:
    """Real ArrEngine.run IL/CL output must be consumed without double-scaling."""
    module = pytest.importorskip("pytuflow.arr.engine")
    config_module = pytest.importorskip("pytuflow.arr.config")
    cfg = config_module.ArrConfig.from_dict(
        {
            "site": {
                "name": "synthetic native parity",
                "latitude": -27.389,
                "longitude": 152.858,
                "catchment_area": 0.0,
            },
            "ifd": {"baseline_year": 1990},
            "events": {"aep": ["1%"], "duration": [60], "output_notation": "aep"},
            "complete_storm": True,
            "preburst": {"percentile": "50%", "pattern_method": "constant", "pattern_duration": 1.0},
            "losses": {
                "user_initial_loss": 100.0,
                "user_continuing_loss": 0.0,
                "climate_change_method": "storm",
            },
            "arf": {"arf_override": 0.6625},
            "climate_change": {
                "enabled": True,
                "scenarios": [{"baseline_year": 2050, "ssp": "SSP2"}],
            },
            "output": {"path": "unused-synthetic"},
        }
    )
    response = _response(preburst_ratio)
    engine = module.ArrEngine(cfg, response)
    # Replace only the source of temporal patterns; exercise the real ARR engine.
    engine._tp_set = cast(Any, _OfflinePatterns())
    events = engine.run()
    assert len(events) == 2
    baseline, cc = events
    assert baseline.cc_scenario is None
    assert cc.cc_scenario == "2050_SSP2"
    assert cc.depth_point == pytest.approx(120)
    assert cc.depth_areal == pytest.approx(79.5)
    assert cc.continuing_loss == pytest.approx(0.0)

    if preburst_ratio == 0.005:
        assert cc.preburst is None
        # Native suppressed-PB event is burst-scaled (100 - 100*0.005)*0.8.
        assert cc.initial_loss == pytest.approx(79.6)
    else:
        assert cc.preburst is not None
        assert cc.initial_loss == pytest.approx(80.0)
        assert cc.preburst.depth == pytest.approx(6.0)

    base_loss = LossSpec(100.0, continuing_loss_mm_per_hour=0.0)
    factors = user_loss_adjustments(
        events,
        default_losses=base_loss,
        losses_by_aep={},
        reference_year=1990,
        factor_provider=cast(Any, response.climate_change_loss_factors),
        complete_storm=True,
    )
    cc_key = ("1%", 60, "2050_SSP2")
    assert factors[cc_key].effective.initial_loss_mm == pytest.approx(80.0)
    selected = native_omitted_preburst_adjustments(events, factors)
    assert (cc_key in selected) is (preburst_ratio == 0.005)

    ratios = {("1%", 60, None): preburst_ratio, cc_key: preburst_ratio}
    result = assemble_arr_events(
        events,
        losses_by_aep={},
        default_losses=None,
        event_loss_adjustments=selected,
        source_preburst_ratios=ratios,
        continuing_loss_timing="full_increment",
        complete_storm=True,
    )
    assert result.patterns[0].status == ScreeningStatus.ZERO
    assert result.patterns[1].status == ScreeningStatus.POSITIVE
    if preburst_ratio == 0.005:
        assert result.patterns[1].excess_mm == pytest.approx(0.1)
        assert result.loss_adjustments[cc_key].effective.initial_loss_mm == 80.0
    else:
        # CC preburst (6 mm) + areal storm (79.5 mm) exceeds full storm IL 80.
        assert result.patterns[1].excess_mm == pytest.approx(5.5)
        assert cc_key not in result.loss_adjustments
