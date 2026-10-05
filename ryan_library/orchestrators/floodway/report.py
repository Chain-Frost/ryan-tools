"""Reviewable Markdown reporting for floodway assessment results."""

from pathlib import Path

from ...classes.floodway import FloodwayEventEnvelope, FloodwayScenarioAssessment


def _format_optional(value: float | None, *, decimals: int = 3) -> str:
    return "—" if value is None else f"{value:.{decimals}f}"


def _format_bool(value: bool) -> str:
    return "yes" if value else "no"


def render_floodway_scenario_markdown(assessment: FloodwayScenarioAssessment) -> str:
    """Render one scenario with MRWA demand and enhanced protection evidence kept distinct."""
    hydraulics = assessment.hydraulics
    lines = [
        "# Floodway scenario assessment",
        "",
        f"Scenario: {hydraulics.scenario_name}",
        f"AEP (%): {_format_optional(hydraulics.aep_percent)}",
        f"Headwater elevation (m): {hydraulics.headwater_elevation:.3f}",
        f"Tailwater elevation (m): {hydraulics.tailwater_elevation:.3f}",
        f"Roadway discharge (m³/s): {hydraulics.roadway_discharge:.3f}",
        "",
        "## MRWA / hydraulic demand assessment",
        "",
        "| Zone | Station (m) | Flow state | Applicability | q (m²/s) | V (m/s) | Source | Message |",
        "| --- | ---: | --- | --- | ---: | ---: | --- | --- |",
    ]

    for item in assessment.zone_assessments:
        demand = item.demand
        lines.append(
            "| "
            + " | ".join(
                (
                    item.zone.value,
                    f"{item.integration_station:.3f}",
                    item.flow_state.value,
                    item.applicability.value,
                    "—" if demand is None else f"{demand.unit_discharge:.3f}",
                    "—" if demand is None else f"{demand.velocity:.3f}",
                    "—" if demand is None or not demand.source_id else demand.source_id,
                    item.message or "—",
                )
            )
            + " |"
        )

    protection_rows = [
        item
        for item in assessment.zone_assessments
        if item.protection_result is not None
    ]
    if protection_rows:
        lines.extend(
            [
                "",
                "## Enhanced protection assessment",
                "",
                "HEC-23 DG5 results are enhanced engineering checks and are not relabelled as MRWA compliance.",
                "",
                "| Zone | Station (m) | Source | q (m²/s) | Minimum d50 (m) | Selected d50 (m) | Meets d50 | Recommended thickness (m) | Sufficient |",
                "| --- | ---: | --- | ---: | ---: | ---: | --- | ---: | --- |",
            ]
        )
        for item in protection_rows:
            result = item.protection_result
            assert result is not None
            lines.append(
                "| "
                + " | ".join(
                    (
                        item.zone.value,
                        f"{item.integration_station:.3f}",
                        result.source_id,
                        f"{result.unit_discharge:.3f}",
                        f"{result.minimum_d50_m:.3f}",
                        f"{result.selected_d50_m:.3f}",
                        _format_bool(result.meets_minimum_d50),
                        _format_optional(result.recommended_thickness_m),
                        _format_bool(result.is_sufficient),
                    )
                )
                + " |"
            )

    lines.append("")
    return "\n".join(lines)


def render_floodway_envelope_markdown(envelope: FloodwayEventEnvelope) -> str:
    """Render a review-oriented envelope summary without recalculating results."""
    lines = [
        "# Floodway event-envelope assessment",
        "",
        f"Candidate demand states: {len(envelope.candidates)}",
        "",
        "## Governing demands",
        "",
    ]

    if not envelope.governors:
        lines.extend(
            [
                "No active floodway demand states were supplied.",
                "",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "| Zone | Metric | Scenario | AEP (%) | q (m²/s) | V (m/s) | Dynamic pressure (Pa) | Momentum flux (N/m) | Applicability | Layer | Source |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
        ]
    )
    for governor in envelope.governors:
        demand = governor.demand
        lines.append(
            "| "
            + " | ".join(
                (
                    governor.zone.value,
                    governor.metric.value,
                    governor.scenario_name,
                    _format_optional(governor.aep_percent),
                    f"{demand.unit_discharge:.3f}",
                    f"{demand.velocity:.3f}",
                    f"{demand.dynamic_pressure_pa:.1f}",
                    f"{demand.momentum_flux_per_width_npm:.1f}",
                    demand.applicability.value,
                    demand.layer.value,
                    demand.source_id or "—",
                )
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "Velocity, dynamic pressure and momentum flux are reported as separate demand measures; this report does not combine them into a generic floodway force.",
            "",
        ]
    )
    return "\n".join(lines)


def export_floodway_scenario_markdown(assessment: FloodwayScenarioAssessment, path: Path) -> Path:
    """Write a Markdown floodway scenario report and return the output path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_floodway_scenario_markdown(assessment), encoding="utf-8")
    return target


def export_floodway_envelope_markdown(envelope: FloodwayEventEnvelope, path: Path) -> Path:
    """Write a Markdown floodway envelope report and return the output path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_floodway_envelope_markdown(envelope), encoding="utf-8")
    return target
