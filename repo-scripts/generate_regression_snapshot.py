"""Generate a golden snapshot of processed EG02 data for regression testing."""

from pathlib import Path
import pandas as pd
from loguru import logger
from pandas import DataFrame

from ryan_library.functions.tuflow.tuflow_common import collect_files
from ryan_library.functions.tuflow.pomm_utils import process_files_in_parallel
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.functions.file_utils import ensure_output_directory
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

# Constants
SOURCE_DIR = Path("tests/test_data/tuflow/TUFLOW_Example_Model_Dataset/EG02")
SNAPSHOT_DIR = Path("tests/regression/data/snapshot")


def generate_snapshot() -> None:
    """Process EG02 data and save aggregated Parquet files."""

    source_path: Path = Path.cwd() / SOURCE_DIR
    output_path: Path = Path.cwd() / SNAPSHOT_DIR

    ensure_output_directory(output_dir=output_path)

    logger.info(f"Generating regression snapshot from: {source_path}")
    logger.info(f"Saving snapshot to: {output_path}")

    with setup_logger(console_log_level="INFO") as log_queue:
        # 1. Collect all files
        suffixes_config: SuffixesConfig = SuffixesConfig.get_instance()
        # We want to test all supported types found in EG02
        # Based on file listing: _1d_H, _1d_V, _1d_Q, _1d_MB, _PO, _POMM, etc.
        # We'll let collect_files find everything it can match.
        # Passing None to include_data_types might default to something specific in collect_files?
        # Let's check collect_files signature or just pass a broad list if needed.
        # Actually collect_files defaults to None which might mean "all known suffixes"?
        # Let's check tuflow_common.py if needed, but for now assuming None means all.

        # Checking collect_files implementation via memory/search if needed,
        # but usually it requires a list. Let's provide a comprehensive list based on EG02 contents.
        include_types: list[str] = [
            "H",
            "V",
            "Q",
            "MB",
            "PO",
            "POMM",
            "Chan",
            "EOF",
            "Cmx",
            "Nmx",
            "ccA",
            "CF",
            "RLL_Qmx",
        ]

        csv_files: list[Path] = collect_files(
            paths_to_process=[source_path], include_data_types=include_types, suffixes_config=suffixes_config
        )

        if not csv_files:
            logger.error("No files found to process!")
            return

        logger.info(f"Found {len(csv_files)} files to process.")
        for f in csv_files:
            logger.debug(f"Collected file: {f}")

        # 2. Process files
        results: ProcessorCollection = process_files_in_parallel(file_list=csv_files, log_queue=log_queue)

        if not results.processors:
            logger.error("No processors returned results.")
            return

        # 3. Aggregate and Save
        # We'll group by dataformat (Timeseries, POMM, Maximums, etc.)
        # But actually, we want to save by specific data type (H, V, Q) to keep schemas clean.
        # Or we can use the ProcessorCollection's combine methods.

        # Let's try to group by the 'data_type' attribute of the processors.
        processors_by_type = {}
        for p in results.processors:
            p_type: str = p.data_type
            if p_type not in processors_by_type:
                processors_by_type[p_type] = []
            processors_by_type[p_type].append(p)

        # Prepare Excel writer
        excel_path = output_path / "snapshot.xlsx"
        with pd.ExcelWriter(excel_path, engine="openpyxl") as writer:
            for p_type, procs in processors_by_type.items():
                logger.info(f"Aggregating {len(procs)} processors for type: {p_type}")

                # Create a temporary collection for this type to use its combine methods if applicable
                # or just concat the dfs if they are simple.
                # Timeseries processors have a specific combine method.
                # POMM has a specific combine method.

                temp_collection = ProcessorCollection()
                for p in procs:
                    temp_collection.add_processor(p)

                combined_df = pd.DataFrame()

                # Determine how to combine based on type/format
                # This is a bit heuristic.
                first_proc = procs[0]
                if first_proc.dataformat == "Timeseries":
                    combined_df: DataFrame = temp_collection.combine_1d_timeseries()
                elif first_proc.dataformat == "POMM":
                    combined_df = temp_collection.pomm_combine()
                elif first_proc.dataformat == "Maximums":
                    combined_df = temp_collection.combine_1d_maximums()
                else:
                    # Fallback: just concat
                    dfs = [p.df for p in procs if not p.df.empty]
                    if dfs:
                        combined_df = pd.concat(dfs, ignore_index=True)

                if not combined_df.empty:
                    # Sort for determinism
                    # Try to sort by common columns if they exist
                    sort_cols: list[str] = [
                        c for c in ["Time", "Chan ID", "Location", "internalName"] if c in combined_df.columns
                    ]
                    if sort_cols:
                        combined_df = combined_df.sort_values(sort_cols).reset_index(drop=True)

                    # Save Parquet (one file per type)
                    output_file: Path = output_path / f"{p_type}.parquet"
                    combined_df.to_parquet(output_file, index=False)
                    logger.info(f"Saved {output_file}")

                    # Save to Excel sheet
                    # Sheet name length limit is 31 chars
                    sheet_name = p_type[:31]
                    combined_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    logger.info(f"Added sheet '{sheet_name}' to {excel_path}")
                else:
                    logger.warning(f"No data to save for type {p_type}")

        logger.info(f"Saved combined Excel file: {excel_path}")


if __name__ == "__main__":
    generate_snapshot()
