from pathlib import Path
from loguru import logger
import pandas as pd
from ryan_library.functions.misc_functions import ExcelExporter, ExportContent
from ryan_library.processors.tuflow.base_processor import BaseProcessor


class ProcessorCollection:
    """
    A collection of BaseProcessor instances, allowing combined export.

    This class can be used to hold one or more processed BaseProcessor instances.
    It provides methods to combine their DataFrames and export the consolidated result.
    """

    def __init__(self) -> None:
        """
        Initialize an empty ProcessorCollection.
        """
        self.processors: list[BaseProcessor] = []
        self.excel_exporter = ExcelExporter()  # Initialize ExcelExporter

    def add_processor(self, processor: BaseProcessor) -> None:
        """
        Add a processed BaseProcessor instance to the collection.

        Args:
            processor (BaseProcessor): A processed BaseProcessor instance.
        """
        self.processors.append(processor)
        logger.debug(f"Added processor: {processor}")

    def export_to_excel(
        self,
        file_name_prefix: str = "Export",
        sheet_name: str = "CombinedData",
    ) -> None:
        """
        Export the combined DataFrame of all processors to a single Excel file.

        Args:
            output_path (Path): The directory where the Excel file should be saved.
            file_name_prefix (str): Prefix for the resulting Excel filename.
            sheet_name (str): Name of the sheet in the Excel file.
        """
        if not self.processors:
            logger.warning("No processors to export.")
            return

        output_path: Path = Path.cwd()
        logger.info("Starting export to Excel.")

        # Combine all DataFrames
        combined_df = self.combine_data()
        if combined_df.empty:
            logger.warning("Combined DataFrame is empty. Nothing to export.")
            return

        # Prepare export content
        export_content: dict[str, ExportContent] = {
            file_name_prefix: {"dataframes": [combined_df], "sheets": [sheet_name]}
        }

        # Perform the export
        self.excel_exporter.export_dataframes(export_content)

        logger.info(
            f"Exported combined data to {output_path / f'{file_name_prefix}.xlsx'}"
        )

    def export_to_csv(self, output_path: Path) -> None:
        """
        Export the combined DataFrame of all processors to a single CSV file.

        Args:
            output_path (Path): The path where the combined CSV should be saved.
        """
        if not self.processors:
            logger.warning("No processors to export.")
            return
        print("some processors")
        # Combine all DataFrames vertically
        combined_df = self.combine_data()
        combined_df.to_csv(output_path / "export.csv", index=False)
        logger.info(f"Exported combined data to {output_path}")

    def combine_data(self) -> pd.DataFrame:
        """
        Combine the DataFrames from all processors into a single DataFrame.

        Returns:
            pd.DataFrame: Combined DataFrame.
        """
        if not self.processors:
            return pd.DataFrame()

        for p in self.processors:
            print(p.df.head())
            print("")

        combined_df: pd.DataFrame = pd.concat(
            [p.df for p in self.processors], ignore_index=True
        )
        return combined_df

    def get_processors_by_data_type(
        self, data_types: list[str] | str
    ) -> "ProcessorCollection":
        """
        Retrieve processors matching a specific data_type or list of data_types.

        Args:
            data_types (list[str] | str): A data_type or list of data_types to match.

        Returns:
            ProcessorCollection: A new collection of processors with matching data_type(s).
        """
        # Ensure it's always a list for uniform processing
        if isinstance(data_types, str):
            data_types = [data_types]

        filtered_collection = ProcessorCollection()
        for processor in self.processors:
            if processor.data_type in data_types:
                filtered_collection.add_processor(processor)

        return filtered_collection
