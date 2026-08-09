# ryan_library/processors/tuflow/other_processors/TLFProcessor.py

from pathlib import Path
from typing import Any
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.functions.path_stuff import convert_to_relative_path
from ryan_library.functions.parse_tlf import (
    search_for_completion,
    is_complete_tlf,
    process_top_lines,
    finalise_data,
    get_log_lines,
)


class TLFProcessor(BaseProcessor):
    """
    Processor for TUFLOW Log Files (.tlf).

    This processor parses TUFLOW simulation log files to extract
    timing, configuration, and completion metadata. It leverages
    efficient tail-reading for large files to minimise network transfer.
    """

    def process(self) -> None:
        """
        Process the TLF file and populate the internal DataFrame (self.df).

        Reads the log file (using efficient tail-reading for files > 10MB).
        If the file has successfully finished computing, it scans the header
        to extract build info, start time, variables, and scenario/event
        combinations.
        """
        sim_complete: int = 0
        success: int = 0
        spec_events: bool = False
        spec_scen: bool = False
        spec_var: bool = False
        data_dict: dict[str, Any] = {}
        current_section: str | None = None

        file_size: int = self.file_path.stat().st_size
        is_large_file: bool = file_size > 10 * 1024 * 1024  # 10 MB

        # get_log_lines returns the entire file for small files (lines),
        # and only the tail (last_lines) for large files to avoid memory/network spikes.
        lines: list[str]
        last_lines: list[str]
        lines, last_lines = get_log_lines(
            logfile_path=self.file_path,
            is_large_file=is_large_file,
        )

        if not lines and not last_lines:
            self.processed = False
            return

        runcode: str = self.file_path.stem
        relative_logfile_path: Path = convert_to_relative_path(user_path=self.file_path)

        # 1. Search for simulation completion in the last 100 lines
        for line in last_lines:
            data_dict, sim_complete, current_section = search_for_completion(
                line=line,
                data_dict=data_dict,
                sim_complete=sim_complete,
                current_section=current_section,
            )
            # Break early if the final metric has been captured
            if sim_complete == 2:
                data_dict["Runcode"] = runcode
                break

        # 2. If complete, parse the top header lines for run configuration
        if is_complete_tlf(data_dict=data_dict, sim_complete=sim_complete):
            data_dict, success, spec_events, spec_scen, spec_var = process_top_lines(
                logfile_path=self.file_path,
                lines=lines,
                data_dict=data_dict,
                success=success,
                spec_events=spec_events,
                spec_scen=spec_scen,
                spec_var=spec_var,
                is_large_file=is_large_file,
                runcode=runcode,
                relative_logfile_path=relative_logfile_path,
            )

            # 3. Finalise and store data if we successfully found all 4 key sections
            if success == 4:
                self.df = finalise_data(
                    runcode=runcode,
                    data_dict=data_dict,
                    logfile_path=self.file_path,
                )
                if not self.df.empty:
                    self.processed = True
