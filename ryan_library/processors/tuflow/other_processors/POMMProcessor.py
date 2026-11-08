# ryan_library/processors/tuflow/other_processors/POMMProcessor.py

__lazy_modules__ = ["pandas"]

import pandas as pd
from loguru import logger

from ....functions.tuflow.sample_validity import clean_hydraulic_samples, is_water_level_measurement
from ..base_processor import BaseProcessor, DataValidationError

POMM_RENAME_COLUMNS: dict[str, str] = {
    "Location": "Type",
    "Time": "Location",
    "Maximum (Extracted from Time Series)": "Max",
    "Time of Maximum": "Tmax",
    "Minimum (Extracted From Time Series)": "Min",
    "Time of Minimum": "Tmin",
}


class POMMProcessor(BaseProcessor):
    """Processor for POMM output CSVs (post-transpose, extract run-code parts, derive AbsMax/SignedAbsMax)."""

    # True only when a structurally valid POMM input has no selected locations.
    filtered_empty: bool = False

    def __post_init__(self) -> None:
        # Let the BaseProcessor __post_init__ do its thing (sets self.name_parser, data_type, etc).
        super().__post_init__()

    def _valid_extrema(self) -> pd.DataFrame:
        """Numerical extrema excluding level no-data, without changing source Max/Min."""
        valid = self.df[["Max", "Min"]].copy()
        levels = self.df["Type"].astype("string").map(lambda name: is_water_level_measurement(str(name)))
        for column in ("Max", "Min"):
            dry = levels & valid[column].eq(-99999).fillna(False)
            valid.loc[dry, column] = float("nan")
        return valid

    def _derive_abs_metrics(self) -> None:
        """Derive absolute extrema without allowing source level sentinels to govern."""
        if not {"Max", "Min"}.issubset(self.df.columns):
            return
        valid = self._valid_extrema()
        absolute_values = valid.abs()
        self.df["AbsMax"] = absolute_values.max(axis=1)
        use_max = (absolute_values["Max"] >= absolute_values["Min"]) | absolute_values["Min"].isna()
        self.df["SignedAbsMax"] = valid["Max"].where(use_max, valid["Min"])

    def _remove_invalid_extrema(self) -> None:
        """Mask dry-level sentinels and discard rows without an actual numeric extreme."""
        level_rows = self.df["Type"].astype("string").map(lambda name: is_water_level_measurement(str(name)))
        for column in ("Max", "Min"):
            numeric, rejected = clean_hydraulic_samples(self.df[column], measurement="")
            no_data = level_rows & numeric.eq(-99999).fillna(False)
            count = int(no_data.sum())
            if count:
                rejected["no_data"] = rejected.get("no_data", 0) + count
            # Raw Max/Min are provenance columns, including the source -99999.
            # Only derived AbsMax/SignedAbsMax must exclude level no-data.
            self.df[column] = numeric
            self.record_rejections(rejected)

        all_missing = self._valid_extrema().isna().all(axis=1)
        if all_missing.any():
            self.record_rejections({"all_missing_extrema": int(all_missing.sum())})
            # Preserve unusable event rows with NaN maxima for TP population
            # accounting in pomm_utils; they must never govern a peak.
            if all_missing.all():
                self.input_health = "INVALID"

    def process(self) -> None:
        """Read the raw POMM CSV, transpose + promote row 1 to header,
        rename the key columns, calculate AbsMax and SignedAbsMax,
        extract TP, Duration, AEP, and finally add all 'common' columns.
        """
        self.reset_processing_state()
        try:
            # 1) Load the CSV without headers (header=None)
            raw_df: pd.DataFrame = pd.read_csv(filepath_or_buffer=self.file_path, header=None)
            self.raw_df = raw_df

            # 2) Extract run_code from top-left cell
            file_run_code: str = str(raw_df.iloc[0, 0]).strip()
            filename_run_code: str = self.name_parser.raw_run_code
            if file_run_code != filename_run_code:
                logger.warning(
                    "{}: POMM run code '{}' in the file does not match filename-derived run code '{}'.",
                    self.file_name,
                    file_run_code,
                    filename_run_code,
                )

            # 3) Drop the first column and transpose
            transposed: pd.DataFrame = raw_df.drop(columns=0).T

            # 4) Promote row-0 (of transposed) to headers
            transposed.columns = pd.Index(transposed.iloc[0], dtype=str)
            transposed = transposed.drop(index=transposed.index[0])

            # 5) Rename the core columns to consistent names
            #    We expect header names like:
            #        ['Location', 'Time', 'Maximum (Extracted from Time Series)', 'Time of Maximum',
            #         'Minimum (Extracted From Time Series)', 'Time of Minimum']
            #    They don't 100% match up to data for the Time/Location headers.
            #    We will rename them to:
            #        Location → 'Type'
            #        Time → 'Location'
            #        Maximum (Extracted from Time Series) → 'Max'
            #        Time of Maximum → 'Tmax'
            #        Minimum (Extracted From Time Series) → 'Min'
            #        Time of Minimum → 'Tmin'
            #        Velocity → 'Velocity'
            # Define new column names and their data types
            headers: list[str] = transposed.columns.tolist()
            if self.expected_in_header and not self.check_headers_match(headers):
                msg = f"{self.log_path}: Header mismatch for POMM data. Got {headers}"
                raise DataValidationError(msg)

            rename_map: dict[str, str] = POMM_RENAME_COLUMNS

            missing_sources: list[str] = [col for col in rename_map if col not in headers]
            if missing_sources:
                msg = f"{self.log_path}: Missing expected columns {missing_sources} after transpose."
                raise DataValidationError(msg)

            transposed = transposed.rename(columns=rename_map)
            ordered_columns = list(rename_map.values())
            transposed = transposed.loc[:, ordered_columns]

            self.df = transposed

            self._remove_invalid_extrema()
            if self.df.empty:
                self.input_health = "INVALID"
                return

            # Rename columns and cast to appropriate data types
            base_dtype_map: dict[str, str] = {
                column: self.output_columns[column] for column in ordered_columns if column in self.output_columns
            }
            if base_dtype_map:
                self.apply_dtype_mapping(dtype_mapping=base_dtype_map, context="pomm_base_columns")

            if not {"Max", "Min"}.issubset(self.df.columns):
                msg = f"{self.log_path}: Required columns 'Max' and 'Min' not available after renaming."
                raise DataValidationError(msg)

            # 10) Finally, apply the dtype mapping from output_columns (so that everything
            #     matches your JSON's "output_columns" keys & dtypes).  This is the call that
            #     looks at Config.get_instance().data_types["POMM"].output_columns and does .astype(...)

            # 7) Derive AbsMax and SignedAbsMax once the Max/Min columns exist.
            #    AbsMax = max(|Max|, |Min|); SignedAbsMax keeps the sign of the larger magnitude value.
            self._derive_abs_metrics()
            had_locations = not self.df.empty
            self.apply_entity_filter()
            if had_locations and self.applied_entity_filter and self.df.empty:
                self.filtered_empty = True
                logger.info("{}: No requested POMM locations found; skipping file.", self.file_name)
                return
            if not self.df["AbsMax"].notna().any():
                self.input_health = "INVALID"
            self.add_common_columns()
            self.apply_output_transformations()
            if not self.validate_data():
                self.input_health = "INVALID"
                self.df = pd.DataFrame()
                return
            # Mark success
            self.processed = True
            logger.info(f"{self.log_path}: POMM processed successfully.")

        except Exception:
            logger.exception(f"{self.log_path}: Failed in POMMProcessor.process()")
            self.input_health = "INVALID"
            self.df = pd.DataFrame()
            return
