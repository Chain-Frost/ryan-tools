import math
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from loguru import logger


def _require_columns(df: pd.DataFrame, columns: list[str]) -> None:
    missing = [column for column in columns if column not in df.columns]
    if missing:
        raise ValueError(f"Missing plotting columns: {missing}")


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
    sns.set_context("paper")
    sns.set_style("whitegrid")

    fig, ax = plt.subplots(figsize=(10, 8))
    fig.subplots_adjust(hspace=0.5)

    box_plot = sns.boxplot(
        x=aep_col,
        y=peak_flow_col,
        hue=duration_col,
        data=df,
        palette="Set1",
        linewidth=0.8,
        whis=1000.0,
        orient="v",
        ax=ax,
    )

    box_plot.set(
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
        loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=5, title="Duration (hours)"
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
    sns.set_theme(rc={"figure.figsize": (12, 10)})
    sns.set_style("whitegrid")

    fig, ax = plt.subplots()

    if hue_col in df.columns and df[hue_col].nunique() > 1:
        box_plot = sns.boxplot(
            data=df,
            x=aep_col,
            y=duration_col,
            hue=hue_col,
            palette="Set1",
            linewidth=0.8,
            whis=1000.0,
            orient="v",
            ax=ax,
        )
    else:
        box_plot = sns.boxplot(
            data=df,
            x=aep_col,
            y=duration_col,
            color="steelblue",
            linewidth=0.8,
            whis=1000.0,
            orient="v",
            ax=ax,
        )

    box_plot.set(
        xlabel="AEP (1 in X years)",
        ylabel="Exceedance Time (hours)",
        title=f"Location: {location} - Exceedance Duration",
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=400, bbox_inches="tight")  # pyright: ignore[reportUnknownMemberType]
    plt.close(fig)
    logger.info("Saved plot: {}", output_path)
