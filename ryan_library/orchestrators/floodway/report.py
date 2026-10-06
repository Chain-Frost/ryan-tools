"""Reviewable Markdown reporting for floodway assessment results."""

from pathlib import Path

from ...classes.floodway import FloodwayEventEnvelope, FloodwayScenarioAssessment
from ...functions.floodway.mrwa_protection import (
    MRWA_CURRENT_FLOODWAY_GUIDANCE,
    MRWA_CURRENT_FLOODWAY_GUIDANCE_SOURCE_ID,
)


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
        f"Total discharge (m³/s): {_format_optional(hydraulics.total_discharge)}",
        f"Roadway discharge (m³/s): {hydraulics.roadway_discharge:.3f}",
        "",
        "## MRWA / hydraulic demand assessment",
        "",
        "| Zone | Station (m) | Flow state | Applicability | q (m²/s) | Depth (m) | V (m/s) | Fr | V²/2g (m) | Specific energy (m) | Source | Message |",
        "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |",
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
                    "—" if demand is None else _format_optional(demand.depth_m),
                    "—" if demand is None else f"{demand.velocity:.3f}",
                    "—" if demand is None else _format_optional(demand.froude_number),
                    "—" if demand is None else _format_optional(demand.velocity_head_m),
                    "—" if demand is None else _format_optional(demand.specific_energy_m),
                    "—" if demand is None or not demand.source_id else demand.source_id,
                    item.message or "—",
                )
            )
            + " |"
        )

    mrwa_protection_rows = [
        item
        for item in assessment.zone_assessments
        if item.mrwa_protection_result is not None
    ]
    if mrwa_protection_rows:
        lines.extend(
            [
                "",
                "## MRWA legacy rock slope protection",
                "",
                "Table 5.1 dumped-rock selections are reported as legacy MRWA compliance evidence.",
                "",
                "| Zone | Station (m) | Velocity (m/s) | Rock class | Section thickness (m) | Applicability | Source |",
                "| --- | ---: | ---: | --- | ---: | --- | --- |",
            ]
        )
        for item in mrwa_protection_rows:
            result = item.mrwa_protection_result
            if result is None:
                continue
            lines.append(
                "| "
                + " | ".join(
                    (
                        item.zone.value,
                        f"{item.integration_station:.3f}",
                        f"{result.velocity_ms:.3f}",
                        result.rock_class,
                        _format_optional(result.section_thickness_m),
                        result.applicability.value,
                        result.source_id,
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
            if result is None:
                continue
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
        lines.extend(
            [
                "## Current MRWA guidance requiring project-level confirmation",
                "",
                f"Source: {MRWA_CURRENT_FLOODWAY_GUIDANCE_SOURCE_ID}",
                "",
                *[f"- {requirement}" for _, requirement in MRWA_CURRENT_FLOODWAY_GUIDANCE],
                "",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "| Zone | Metric | Scenario | AEP (%) | Total Q (m³/s) | Roadway Q (m³/s) | HW (m) | TW (m) | Flow state | Station (m) | q (m²/s) | Depth (m) | V (m/s) | Fr | V²/2g (m) | Specific energy (m) | Dynamic pressure (Pa) | Momentum flux (N/m) | Applicability | Layer | Source |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |",
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
                    _format_optional(governor.total_discharge),
                    _format_optional(governor.roadway_discharge),
                    _format_optional(governor.headwater_elevation),
                    _format_optional(governor.tailwater_elevation),
                    "—" if governor.flow_state is None else governor.flow_state.value,
                    f"{governor.integration_station:.3f}",
                    f"{demand.unit_discharge:.3f}",
                    _format_optional(demand.depth_m),
                    f"{demand.velocity:.3f}",
                    _format_optional(demand.froude_number),
                    _format_optional(demand.velocity_head_m),
                    _format_optional(demand.specific_energy_m),
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
            "## Current MRWA guidance requiring project-level confirmation",
            "",
            f"Source: {MRWA_CURRENT_FLOODWAY_GUIDANCE_SOURCE_ID}",
            "",
            *[f"- {requirement}" for _, requirement in MRWA_CURRENT_FLOODWAY_GUIDANCE],
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
