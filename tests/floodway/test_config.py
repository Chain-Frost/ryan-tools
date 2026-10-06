"""Tests for versioned floodway formation JSON/TOML configuration."""

import json
from pathlib import Path

from ryan_library.classes.floodway import FloodwayZone
from ryan_library.functions.floodway import (
    export_floodway_formation_json,
    load_floodway_formation,
    load_floodway_formation_json,
)


def test_floodway_formation_json_round_trip_retains_supported_inputs(tmp_path: Path) -> None:
    source = tmp_path / "floodway.json"
    source.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "name": "Seven Mile style formation",
                "crest_flow_length_m": 9.0,
                "two_d_verification_reason": "Skewed approach flow requires spatial verification.",
                "zones": [
                    {
                        "zone": "D",
                        "slope": 0.03,
                        "roughness_manning_n": 0.015,
                        "label": "Pavement",
                    },
                    {
                        "zone": "C",
                        "elevation_m": 99.865,
                        "label": "Downstream shoulder",
                    },
                    {
                        "zone": "B",
                        "slope": 1.0 / 3.0,
                        "roughness_manning_n": 0.04,
                        "hec23_riprap": {
                            "selected_d50_m": 0.15,
                            "uniformity_coefficient": 2.1,
                            "porosity": 0.45,
                        },
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    formation = load_floodway_formation_json(source)

    assert formation.crest_flow_length == 9.0
    assert formation.two_d_verification_reason == "Skewed approach flow requires spatial verification."
    assert formation.get_zone(FloodwayZone.PAVEMENT) is not None
    batter = formation.get_zone(FloodwayZone.DOWNSTREAM_BATTER)
    assert batter is not None
    assert batter.hec23_riprap is not None
    assert batter.hec23_riprap.specific_gravity == 2.65

    exported = export_floodway_formation_json(formation, tmp_path / "exported.json")
    assert load_floodway_formation_json(exported) == formation


def test_floodway_formation_toml_supports_minimal_mrwa_geometry(tmp_path: Path) -> None:
    source = tmp_path / "floodway.toml"
    source.write_text(
        """
schema_version = 1
name = "Floodway"
crest_flow_length_m = 9.0

[[zones]]
zone = "D"
slope = 0.03
roughness_manning_n = 0.015

[[zones]]
zone = "C"
elevation_m = 100.0

[[zones]]
zone = "B"
slope = 0.3333333333333333
roughness_manning_n = 0.04
""".strip(),
        encoding="utf-8",
    )

    formation = load_floodway_formation(source)

    assert formation.name == "Floodway"
    assert formation.get_zone(FloodwayZone.DOWNSTREAM_SHOULDER) is not None
