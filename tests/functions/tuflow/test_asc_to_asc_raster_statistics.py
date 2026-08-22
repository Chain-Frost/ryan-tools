"""Integration tests for ASC-to-ASC discovery using synthetic raster data."""

# pyright: reportMissingTypeStubs=false, reportUnknownVariableType=false, reportUnknownMemberType=false, reportUnknownArgumentType=false

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import numpy as np
import pytest

from ryan_library.classes.tuflow_string_classes import TuflowStringParser
from ryan_library.functions.gdal.raster_processing import read_raster_band
from ryan_library.functions.tuflow.tuflow_result_naming import replace_filename_component
from ryan_library.orchestrators.tuflow.asc2asc_max_by_search import (
    build_max_searches,
    discover_max_jobs as discover_search_max_jobs,
)
from ryan_library.orchestrators.tuflow.asc2asc_mean_then_max_by_search import (
    MeanJobDetails,
    discover_max_jobs,
    discover_mean_jobs,
    discover_rasters,
)

SCENARIOS = ("EXG", "DEV")
RESULT_TYPES = ("d_HR_Max", "h_HR_Max", "V_Max")


@pytest.mark.parametrize("separator_directory", ["underscore", "plus"])
def test_parser_and_replacement_support_both_separators(raster_test_data: Path, separator_directory: str) -> None:
    fixture = next((raster_test_data / "tuflow_statistics" / "separator_cases" / separator_directory).glob("*.tif"))
    parser = TuflowStringParser(fixture)

    assert parser.aep is not None and parser.aep.numeric_value == pytest.approx(1.0)
    assert parser.duration is not None and parser.duration.numeric_value == pytest.approx(30.0)
    assert parser.tp is not None and parser.tp.numeric_value == 1
    assert parser.data_type == "d_HR_Max"
    tp_text = parser.tp.original_text
    duration_text = parser.duration.original_text
    assert tp_text is not None
    assert duration_text is not None

    mean_name = replace_filename_component(
        filename=fixture.name,
        old_component=tp_text,
        new_component="TPMean",
    )
    maximum_name = replace_filename_component(
        filename=mean_name,
        old_component=duration_text,
        new_component=None,
    )
    separator = "+" if separator_directory == "plus" else "_"
    assert f"{separator}TPMean{separator}" in mean_name
    assert duration_text not in maximum_name
    assert f"{separator}TPMean{separator}" in maximum_name


def test_mean_then_max_discovery_uses_nested_mixed_separator_fixtures(raster_test_data: Path) -> None:
    search_root = raster_test_data / "tuflow_statistics" / "mean_then_max"
    parsed = discover_rasters(
        search_root=search_root,
        input_glob="*.tif",
        scenarios=SCENARIOS,
        result_types=RESULT_TYPES,
    )
    mean_jobs, incomplete = discover_mean_jobs(
        rasters=parsed,
        output_root=search_root / "test_outputs",
        expected_tps=frozenset(range(1, 11)),
        write_source=True,
    )
    max_jobs = discover_max_jobs(mean_jobs=mean_jobs, output_root=search_root / "test_outputs", write_source=True)

    assert len(parsed) == 120
    assert any("+" in raster.path.name for raster in parsed)
    assert {raster.grid_directory for raster in parsed} == {
        search_root / "layout_alpha" / "results" / "grids",
        search_root / "layout_beta" / "nested" / "model_outputs" / "grids",
    }
    assert len(mean_jobs) == 12
    assert len(max_jobs) == 6
    assert incomplete == []
    assert all(len(details.job.input_files) == 10 for details in mean_jobs)
    assert all(details.job.write_source for details in mean_jobs)
    assert all(len(job.input_files) == 2 for job, _ in max_jobs)
    assert all(job.write_source for job, _ in max_jobs)
    assert all(job.original_input_groups is not None for job, _ in max_jobs)
    assert all(len(group) == 10 for job, _ in max_jobs for group in (job.original_input_groups or ()))
    assert any("+TPMean+" in details.job.output_file.name for details in mean_jobs)
    assert any("+TPMean-DurMax+" in job.output_file.name for job, _ in max_jobs)


def test_discovered_mean_inputs_match_expected_pixel_values(raster_test_data: Path) -> None:
    search_root = raster_test_data / "tuflow_statistics" / "mean_then_max"
    parsed = discover_rasters(
        search_root=search_root,
        input_glob="*.tif",
        scenarios=SCENARIOS,
        result_types=RESULT_TYPES,
    )
    mean_jobs, _ = discover_mean_jobs(
        rasters=parsed,
        output_root=search_root / "test_outputs",
        expected_tps=frozenset(range(1, 11)),
    )
    selected: MeanJobDetails = next(
        details
        for details in mean_jobs
        if details.scenario == "DEV" and details.duration == "00060m" and details.result_type == "V_Max"
    )
    arrays: list[np.ndarray] = []
    for input_file in selected.job.input_files:
        arrays.append(read_raster_band(input_file))
    calculated = np.mean(np.stack(arrays), axis=0)

    expected_document = cast(
        dict[str, object], json.loads((raster_test_data / "expected.json").read_text(encoding="utf-8"))
    )
    statistics = cast(dict[str, dict[str, dict[str, dict[str, float]]]], expected_document["tuflow_statistics"])
    expected = statistics["DEV/V_Max"]["00060m"]
    assert calculated[0, 0] == pytest.approx(expected["cell_0_0_mean"])
    assert calculated[3, 3] == pytest.approx(expected["cell_3_3_mean"])


def test_configurable_max_search_discovers_both_filename_styles(raster_test_data: Path) -> None:
    searches = build_max_searches(
        input_glob_template="{scenario}/{event}/grids/*_{result_type}.tif",
        output_filename_template="Synthetic_{scenario}_{event}_{result_type}.tif",
        template_axes={"scenario": SCENARIOS, "event": ("PMP",), "result_type": RESULT_TYPES},
    )
    jobs = discover_search_max_jobs(
        search_root=raster_test_data / "tuflow_statistics" / "max_search",
        searches=searches,
    )

    assert len(jobs) == 6
    assert all(len(job.input_files) == 2 for job in jobs)
    assert any("+" in path.name for job in jobs for path in job.input_files)
