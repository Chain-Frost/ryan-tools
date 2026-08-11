from pathlib import Path

from loguru import logger

from ryan_library.functions.plotting.ensemble_boxplot import plot_exceedance_duration, plot_peak_flow_distribution
from ryan_library.processors.rorb.ensemble_reader import (
    calculate_closure_times,
    calculate_peak_flows,
    read_rorb_parquet,
)


def orchestrate_ensemble_plotting(
    input_path: Path,
    output_dir: Path,
    locations: list[str] | None = None,
    capacity_threshold: float | None = None,
    source: str = "rorb",
) -> None:
    """
    Coordinates reading ensemble model results and plotting peak flows and closure times.
    """
    logger.info(f"Starting ensemble plotting orchestration for source: {source}")

    if source.lower() == "rorb":
        # Columns based on standard RORB parquet exports
        expected_cols = ["Time", "Flow", "Location", "CC", "Model", "Method", "AEP", "Duration", "TP"]
        
        # In a generic implementation, we would inspect the parquet columns or catch errors
        try:
            df = read_rorb_parquet(input_path, columns=expected_cols)
        except Exception:
            # Fallback if specific columns are missing
            logger.warning("Failed to load specific columns, attempting to load entire parquet")
            df = read_rorb_parquet(input_path)
            
        # Filter by locations if specified
        if locations:
            df = df[df["Location"].isin(locations)]
            
        group_cols = ["Model", "Location", "Method", "CC", "AEP", "Duration", "TP"]
        
        # Ensure only existing grouping columns are used
        group_cols = [col for col in group_cols if col in df.columns]
        
        if not group_cols:
            raise ValueError("No valid grouping columns found in the data.")
            
        # 1. Plot Peak Flows
        logger.info("Processing Peak Flows")
        peak_df = calculate_peak_flows(df, group_cols=group_cols, flow_col="Flow", peak_flow_col_name="PeakFlow")
        
        for loc in peak_df["Location"].unique():
            loc_df = peak_df[peak_df["Location"] == loc]
            out_plot = output_dir / f"PeakFlow_{loc}.png"
            
            # Aggregate over TPs (temporal patterns). The box plot uses 'Duration' as hue and 'AEP' as x
            # and takes the distribution over the other variables (like TP, Method, CC).
            plot_peak_flow_distribution(
                df=loc_df,
                output_path=out_plot,
                location=str(loc),
                aep_col="AEP",
                peak_flow_col="PeakFlow",
                duration_col="Duration",
            )
            
        # 2. Plot Closure Times if capacity threshold is provided
        if capacity_threshold is not None:
            logger.info(f"Processing Closure Times with threshold {capacity_threshold}")
            closure_df = calculate_closure_times(
                df,
                threshold=capacity_threshold,
                group_cols=group_cols,
                time_col="Time",
                flow_col="Flow",
                closure_col_name="ClosureTime",
            )
            
            for loc in closure_df["Location"].unique():
                loc_df = closure_df[closure_df["Location"] == loc]
                out_plot = output_dir / f"ClosureTime_{loc}.png"
                
                # Plot closure time distribution over AEP, with CC as hue
                hue_col = "CC" if "CC" in closure_df.columns else "Duration"
                
                plot_exceedance_duration(
                    df=loc_df,
                    output_path=out_plot,
                    location=str(loc),
                    aep_col="AEP",
                    duration_col="ClosureTime",
                    hue_col=hue_col,
                )
    else:
        raise NotImplementedError(f"Data source '{source}' is not supported yet.")

    logger.info("Ensemble plotting orchestration complete.")
