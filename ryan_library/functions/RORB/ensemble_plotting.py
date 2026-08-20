"""Create peak-flow and exceedance-duration plots for ensemble results."""

# moved from unsorted, not tested in production yet - 2026-08-20

import math
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
from loguru import logger
from matplotlib.axes import Axes
from matplotlib.patches import Patch

_SET1_COLORS = ("#E41A1C", "#377EB8", "#4DAF4A", "#984EA3", "#FF7F00", "#FFFF33", "#A65628", "#F781BF", "#999999")


def _require_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing plotting columns: {missing}")


def _draw_vertical_boxplots(
    ax: Axes,
    df: pd.DataFrame,
    category_col: str,
    value_col: str,
    hue_col: str | None = None,
    single_color: str = "steelblue",
) -> tuple[list[Patch], list[str]]:
    """Draw categorical boxplots with Matplotlib's current orientation API."""
    category_order = list(pd.unique(df[category_col]))
    hue_order = list(pd.unique(df[hue_col])) if hue_col is not None else [None]
    hue_count = len(hue_order)
    group_width = 0.8
    box_width = min(0.7, group_width / max(hue_count, 1))

    for category_index, category in enumerate(category_order):
        category_mask = df[category_col].eq(category)
        for hue_index, hue in enumerate(hue_order):
            mask = (
                category_mask
                if hue_col is None
                else category_mask & df[hue_col].eq(hue)  # pyright: ignore[reportArgumentType]
            )
            values = pd.to_numeric(df.loc[mask, value_col], errors="coerce").dropna().tolist()
            if not values:
                continue

            position = category_index + (hue_index - (hue_count - 1) / 2) * box_width
            color = single_color if hue_col is None else _SET1_COLORS[hue_index % len(_SET1_COLORS)]
            ax.boxplot(  # pyright: ignore[reportUnknownMemberType]
                values,
                positions=[position],
                widths=box_width * 0.9,
                patch_artist=True,
                whis=1000.0,
                orientation="vertical",
                manage_ticks=False,
                boxprops={"facecolor": color, "edgecolor": "#333333", "linewidth": 0.8},
                medianprops={"color": "#333333", "linewidth": 1.0},
                whiskerprops={"color": "#333333", "linewidth": 0.8},
                capprops={"color": "#333333", "linewidth": 0.8},
            )

    ax.set_xticks(  # pyright: ignore[reportUnknownMemberType]
        range(len(category_order)), labels=[str(category) for category in category_order]
    )
    ax.set_xlim(-0.5, len(category_order) - 0.5)
    ax.grid(axis="y", color="#D9D9D9", linewidth=0.8)  # pyright: ignore[reportUnknownMemberType]
    ax.set_axisbelow(True)

    if hue_col is None:
        return [], []
    handles = [
        Patch(facecolor=_SET1_COLORS[index % len(_SET1_COLORS)], edgecolor="#333333") for index in range(hue_count)
    ]
    return handles, [str(hue) for hue in hue_order]


def plot_peak_flow_distribution(
    df: pd.DataFrame,
    output_path: Path,
    location: str,
    aep_col: str = "AEP",
    peak_flow_col: str = "PeakFlow",
    duration_col: str = "Duration",
) -> None:
    """
    Plots the distribution of Peak Flows across different AEPs and Durations.
    """
    if df.empty:
        logger.warning("No data to plot for location {}", location)
        return
    _require_columns(df, [aep_col, peak_flow_col, duration_col])

    plt.clf()
    fig, ax = plt.subplots(figsize=(10, 8))
    fig.subplots_adjust(hspace=0.5)

    legend_handles, legend_labels = _draw_vertical_boxplots(ax, df, aep_col, peak_flow_col, duration_col)

    ax.set(
        xlabel="AEP (1 in X years)",
        ylabel=r"Peak Flow ($m^3/s$)",
        title=f"Location: {location} - Peak Flow Distribution",
    )

    max_flow = df[peak_flow_col].max()
    if pd.notna(max_flow):
        plt.ylim(0, math.ceil(max_flow / 100) * 100)

    # Shrink current axis's height by 10% on the bottom to fit legend
    box = ax.get_position()
    ax.set_position((box.x0, box.y0 + box.height * 0.2, box.width, box.height * 0.8))

    # Put a legend below current axis
    ax.legend(  # pyright: ignore[reportUnknownMemberType]
        legend_handles,
        legend_labels,
        loc="upper center",
        bbox_to_anchor=(0.5, -0.15),
        ncol=5,
        title="Duration (hours)",
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=400, bbox_inches="tight")  # pyright: ignore[reportUnknownMemberType]
    plt.close(fig)
    logger.info("Saved plot: {}", output_path)


def plot_exceedance_duration(
    df: pd.DataFrame,
    output_path: Path,
    location: str,
    aep_col: str = "AEP",
    duration_col: str = "ClosureTime",
    hue_col: str = "CC",
) -> None:
    """
    Plots the distribution of exceedance durations (closure times) across AEPs.
    """
    if df.empty:
        logger.warning("No data to plot for location {}", location)
        return
    _require_columns(df, [aep_col, duration_col])

    plt.clf()
    fig, ax = plt.subplots(figsize=(12, 10))

    if hue_col in df.columns and df[hue_col].nunique() > 1:
        legend_handles, legend_labels = _draw_vertical_boxplots(ax, df, aep_col, duration_col, hue_col)
        ax.legend(legend_handles, legend_labels, title=hue_col)  # pyright: ignore[reportUnknownMemberType]
    else:
        _draw_vertical_boxplots(ax, df, aep_col, duration_col)

    ax.set(
        xlabel="AEP (1 in X years)",
        ylabel="Exceedance Time (hours)",
        title=f"Location: {location} - Exceedance Duration",
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=400, bbox_inches="tight")  # pyright: ignore[reportUnknownMemberType]
    plt.close(fig)
    logger.info("Saved plot: {}", output_path)
