"""Tests for floodway machine-readable event-envelope exports."""

import csv
import json
from pathlib import Path

from ryan_library.classes.floodway import (
    FloodwayApplicabilityStatus,
    FloodwayAssessmentLayer,
    FloodwayZone,
    FloodwayZoneDemand,
    GoverningFloodwayDemand,
)
from ryan_library.functions.floodway.envelope import build_floodway_event_envelope
from ryan_library.functions.floodway.export import (
    export_floodway_envelope_json,
    export_floodway_governors_csv,
)


def _candidate() -> GoverningFloodwayDemand:
    return GoverningFloodwayDemand(
        scenario_name="2% AEP",
        aep_percent=2.0,
        demand=FloodwayZoneDemand(
            zone=FloodwayZone.PAVEMENT,
            unit_discharge=1.8,
            velocity=3.2,
            dynamic_pressure_pa=5120.0,
            momentum_flux_per_width_npm=5760.0,
            specific_energy_m=1.1,
            applicability=FloodwayApplicabilityStatus.LEGACY_REPRODUCTION,
            layer=FloodwayAssessmentLayer.MRWA_COMPLIANCE,
            source_id="MRWA-FLOODWAY-DESIGN-GUIDE-2006-EQ4-7",
        ),
    )


def test_json_export_retains_candidates_governors_and_provenance(tmp_path: Path) -> None:
    envelope = build_floodway_event_envelope((_candidate(),))

    target = export_floodway_envelope_json(envelope, tmp_path / "floodway.json")
    payload = json.loads(target.read_text(encoding="utf-8"))

    assert payload["candidates"][0]["scenario"] == "2% AEP"
    assert payload["candidates"][0]["demand"]["zone"] == "D"
    assert payload["candidates"][0]["demand"]["source_id"] == "MRWA-FLOODWAY-DESIGN-GUIDE-2006-EQ4-7"
    assert {item["metric"] for item in payload["governors"]} == {
        "velocity",
        "dynamic_pressure",
        "momentum_flux",
    }


def test_csv_export_is_compact_governor_summary(tmp_path: Path) -> None:
    envelope = build_floodway_event_envelope((_candidate(),))

    target = export_floodway_governors_csv(envelope, tmp_path / "floodway.csv")
    with target.open("r", encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))

    assert len(rows) == 3
    assert {row["metric"] for row in rows} == {"velocity", "dynamic_pressure", "momentum_flux"}
    assert all(row["zone"] == "D" for row in rows)
    assert all(row["scenario"] == "2% AEP" for row in rows)
