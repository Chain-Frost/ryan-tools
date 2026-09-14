"""Reviewable Markdown reporting for floodway event-envelope results."""

from pathlib import Path

from ...classes.floodway import FloodwayEventEnvelope


def _format_optional(value: float | None, *, decimals: int = 3) -> str:
    return "—" if value is None else f"{value:.{decimals}f}"


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


def export_floodway_envelope_markdown(envelope: FloodwayEventEnvelope, path: Path) -> Path:
    """Write a Markdown floodway envelope report and return the output path."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(render_floodway_envelope_markdown(envelope), encoding="utf-8")
    return target
