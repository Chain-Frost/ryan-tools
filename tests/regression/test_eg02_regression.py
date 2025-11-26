"""Regression tests for EG02 dataset."""

from pathlib import Path
import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from ryan_library.functions.tuflow.tuflow_common import collect_files
from ryan_library.functions.tuflow.pomm_utils import process_files_in_parallel
from ryan_library.classes.suffixes_and_dtypes import SuffixesConfig
from ryan_library.functions.loguru_helpers import setup_logger
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

# Constants
SOURCE_DIR = Path("tests/test_data/tuflow/TUFLOW_Example_Model_Dataset/EG02")
SNAPSHOT_DIR = Path("tests/regression/data/snapshot")


@pytest.fixture(scope="module")
def processed_results() -> dict[str, pd.DataFrame]:
    """Process EG02 data once for the module and return aggregated DataFrames by type."""
    source_path = Path.cwd() / SOURCE_DIR

    with setup_logger(console_log_level="INFO") as log_queue:
        suffixes_config = SuffixesConfig.get_instance()
        include_types = ["H", "V", "Q", "MB", "PO", "POMM", "Chan", "EOF", "Cmx", "Nmx", "ccA", "CF", "RLL_Qmx"]

        csv_files = collect_files(
            paths_to_process=[source_path], include_data_types=include_types, suffixes_config=suffixes_config
        )

        if not csv_files:
            pytest.fail("No files found to process for regression test!")

        results: ProcessorCollection = process_files_in_parallel(file_list=csv_files, log_queue=log_queue)

        if not results.processors:
            pytest.fail("No processors returned results for regression test!")

        # Aggregate results by type
        processors_by_type = {}
        for p in results.processors:
            p_type = p.data_type
            if p_type not in processors_by_type:
                processors_by_type[p_type] = []
            processors_by_type[p_type].append(p)

        aggregated_dfs = {}
        for p_type, procs in processors_by_type.items():
            temp_collection = ProcessorCollection()
            for p in procs:
                temp_collection.add_processor(p)

            combined_df = pd.DataFrame()
            first_proc = procs[0]

            if first_proc.dataformat == "Timeseries":
                combined_df = temp_collection.combine_1d_timeseries()
            elif first_proc.dataformat == "POMM":
                combined_df = temp_collection.pomm_combine()
            elif first_proc.dataformat == "Maximums":
                combined_df = temp_collection.combine_1d_maximums()
            else:
                dfs = [p.df for p in procs if not p.df.empty]
                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)

            if not combined_df.empty:
                sort_cols = [c for c in ["Time", "Chan ID", "Location", "internalName"] if c in combined_df.columns]
                if sort_cols:
                    combined_df = combined_df.sort_values(sort_cols).reset_index(drop=True)
                aggregated_dfs[p_type] = combined_df

        return aggregated_dfs


@pytest.mark.parametrize(
    "data_type", ["H", "V", "Q", "PO", "POMM", "Chan", "Cmx", "Nmx", "ccA", "CF", "EOF", "RLL_Qmx"]
)
def test_regression_against_snapshot(data_type: str, processed_results: dict[str, pd.DataFrame]):
    """Compare processed results against the golden snapshot for each data type."""
    snapshot_path = Path.cwd() / SNAPSHOT_DIR / f"{data_type}.parquet"

    if not snapshot_path.exists():
        pytest.fail(f"Snapshot file not found: {snapshot_path}")

    if data_type not in processed_results:
        pytest.fail(f"No processed results found for type: {data_type}")

    expected_df = pd.read_parquet(snapshot_path)
    observed_df = processed_results[data_type]

    # Ensure column order matches for comparison
    # Align columns to expected
    common_cols = [c for c in expected_df.columns if c in observed_df.columns]

    # Check for missing columns
    missing_cols = set(expected_df.columns) - set(observed_df.columns)
    if missing_cols:
        pytest.fail(f"Missing columns in observed data: {missing_cols}")

    extra_cols = set(observed_df.columns) - set(expected_df.columns)
    if extra_cols:
        # This might be okay if we added new features, but for strict regression we might want to know
        pass

    observed_df = observed_df[expected_df.columns]

    # Sort again just to be sure, although fixture should have sorted
    sort_cols = [c for c in ["Time", "Chan ID", "Location", "internalName"] if c in expected_df.columns]
    if sort_cols:
        expected_df = expected_df.sort_values(sort_cols).reset_index(drop=True)
        observed_df = observed_df.sort_values(sort_cols).reset_index(drop=True)

    # Clear columns.name metadata as it may not be preserved perfectly in Parquet roundtrip
    # and is not critical for regression testing of data values
    expected_df.columns.name = None
    observed_df.columns.name = None

    assert_frame_equal(observed_df, expected_df)
