"""Starter example for embedding the TUFLOW-results-to-HY-8 workflow.

The script first runs the same result-discovery and combination workflow as
``culvert_results/combine_culvert_maximums.py``. It then retains both the typed maximums record
and the generated HY-8 crossing so project-specific filtering and modification
can happen before the final project is written.

The model-to-culvert selection is loaded from a CSV with one permitted pair per
row. ``R03`` is the model name parsed by ``TuflowStringParser`` and ``Chan ID``
is the culvert name::

    R03,Chan ID
    M1,CULVERT_001
    M1,CULVERT_020
    M2,CULVERT_002
    M2,CULVERT_003


"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import pandas as pd  # pyright: ignore[reportMissingTypeStubs]
from run_hy8 import CulvertCrossing, Hy8FileWriter, Hy8Project

from ryan_library.functions.hy8 import (
    CulvertMaximumRecord,
    Hy8CulvertOptions,
    build_crossing_from_record,
)
from ryan_library.functions.tuflow.notebook_helpers import CulvertMaximumsResult, run_culvert_maximums
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection

# Replace these values with configuration owned by your calling script. Each
# result path is scanned recursively for the selected TUFLOW result types.
TUFLOW_RESULT_PATHS: tuple[Path, ...] = (Path(r"Q:\path\to\tuflow\results"),)
CULVERT_SELECTION_CSV = Path(r"Q:\path\to\culverts_to_include.csv")
OUTPUT_HY8 = Path("filtered_culverts.hy8")
PROJECT_TITLE = "Filtered TUFLOW Culverts"
TUFLOW_DATA_TYPES: tuple[str, ...] = ("Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF")


@dataclass(slots=True)
class ConvertedCulvert:
    """Keep a source record and its generated HY-8 object together."""

    source: CulvertMaximumRecord
    crossing: CulvertCrossing


@dataclass(slots=True)
class CulvertWorkflowResult:
    """All useful outputs from the embedded workflow."""

    project: Hy8Project
    processor_collection: ProcessorCollection
    maximums: pd.DataFrame
    raw_data: pd.DataFrame
    retained: list[ConvertedCulvert]
    rejected: list[ConvertedCulvert]


def convert_culverts(
    maximums: pd.DataFrame,
    *,
    options: Hy8CulvertOptions | None = None,
) -> list[ConvertedCulvert]:
    """Convert valid rows while retaining their typed source records."""

    conversion_options: Hy8CulvertOptions = options or Hy8CulvertOptions()
    rows = cast(
        list[dict[str, Any]],
        maximums.to_dict(orient="records"),  # pyright: ignore[reportUnknownMemberType]
    )
    converted: list[ConvertedCulvert] = []

    for row_index, row in enumerate(rows):
        source: CulvertMaximumRecord | None = CulvertMaximumRecord.from_mapping(row, row_index=row_index)
        if source is None:
            # The bridge logs why an invalid source row was skipped.
            continue

        crossing: CulvertCrossing = build_crossing_from_record(source, options=conversion_options)
        converted.append(ConvertedCulvert(source=source, crossing=crossing))

    return converted


def _normalise_names(values: Collection[str]) -> frozenset[str]:
    """Return stripped, case-insensitive values for exact comparisons."""

    return frozenset(value.strip().casefold() for value in values if value.strip())


def load_culverts_by_model(csv_path: Path) -> dict[str, frozenset[str]]:
    """Load permitted ``R03``/``Chan ID`` pairs from a CSV file."""

    culverts_by_model: dict[str, set[str]] = {}
    with csv_path.open(mode="r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        required_columns: set[str] = {"R03", "Chan ID"}
        available_columns: set[str] = set(reader.fieldnames or ())
        missing_columns: set[str] = required_columns - available_columns
        if missing_columns:
            missing_text = ", ".join(sorted(missing_columns))
            raise ValueError(f"{csv_path} is missing required column(s): {missing_text}")

        for row_number, row in enumerate(reader, start=2):
            model_name = (row.get("R03") or "").strip()
            culvert_name = (row.get("Chan ID") or "").strip()
            if not model_name or not culvert_name:
                raise ValueError(f"{csv_path} row {row_number} must contain both R03 and Chan ID values.")
            culverts_by_model.setdefault(model_name, set()).add(culvert_name)

    if not culverts_by_model:
        raise ValueError(f"{csv_path} contains no model/culvert selections.")

    return {model_name: frozenset(culvert_names) for model_name, culvert_names in culverts_by_model.items()}


def _normalise_model_culverts(
    culverts_by_model: Mapping[str, Collection[str]],
) -> dict[str, frozenset[str]]:
    """Normalise model names and culvert names for exact comparisons."""

    return {
        model_name.strip().casefold(): _normalise_names(culvert_names)
        for model_name, culvert_names in culverts_by_model.items()
        if model_name.strip()
    }


def keep_culvert(
    culvert: ConvertedCulvert,
    *,
    culverts_by_model: Mapping[str, Collection[str]],
) -> bool:
    """Apply the parsed ``R03`` model's permitted ``Chan ID`` selection."""

    source: CulvertMaximumRecord = culvert.source
    crossing: CulvertCrossing = culvert.crossing
    allowed_by_model: dict[str, frozenset[str]] = _normalise_model_culverts(culverts_by_model)
    model_name: str = str(source.raw.get("R03", "")).strip().casefold()
    allowed_culvert_names: frozenset[str] | None = allowed_by_model.get(model_name)
    if allowed_culvert_names is None or source.chan_id.casefold() not in allowed_culvert_names:
        return False

    # Generated HY-8 fields remain available for any additional rules.
    if not crossing.culverts:
        return False

    return True


def build_filtered_project(
    maximums: pd.DataFrame,
    *,
    project_title: str,
    options: Hy8CulvertOptions | None = None,
    culverts_by_model: Mapping[str, Collection[str]],
) -> tuple[Hy8Project, list[ConvertedCulvert], list[ConvertedCulvert]]:
    """Return the project plus retained and rejected culvert objects."""

    if "R03" not in maximums.columns:
        raise ValueError("Cannot filter by model name because the combined maximums data has no 'R03' column.")

    converted: list[ConvertedCulvert] = convert_culverts(maximums, options=options)
    retained: list[ConvertedCulvert] = []
    rejected: list[ConvertedCulvert] = []

    for culvert in converted:
        should_keep = keep_culvert(
            culvert,
            culverts_by_model=culverts_by_model,
        )
        destination: list[ConvertedCulvert] = retained if should_keep else rejected
        destination.append(culvert)

    conversion_options: Hy8CulvertOptions = options or Hy8CulvertOptions()
    project = Hy8Project(
        title=project_title,
        units=conversion_options.units,
        crossings=[culvert.crossing for culvert in retained],
    )
    return project, retained, rejected


def run_workflow(
    result_paths: Sequence[str | Path],
    output_hy8: Path,
    *,
    project_title: str,
    culvert_selection_csv: Path,
    parallel: bool | None = None,
) -> CulvertWorkflowResult:
    """Discover TUFLOW results, combine, filter, and write an HY-8 project."""

    maximums_result: CulvertMaximumsResult = run_culvert_maximums(
        paths=result_paths,
        data_types=TUFLOW_DATA_TYPES,
        parallel=parallel,
        log_level="INFO",
    )
    maximums: pd.DataFrame = maximums_result.maximums
    raw_data: pd.DataFrame = maximums_result.raw_data
    processor_collection: ProcessorCollection = maximums_result.processor_collection
    if maximums.empty:
        raise ValueError("No combined TUFLOW culvert maximums were produced from the supplied result paths.")

    culverts_by_model: dict[str, frozenset[str]] = load_culverts_by_model(culvert_selection_csv)
    options = Hy8CulvertOptions()
    project, retained, rejected = build_filtered_project(
        maximums,
        project_title=project_title,
        options=options,
        culverts_by_model=culverts_by_model,
    )
    if not retained:
        raise ValueError("No culverts remain after applying the culvert-name and R03 model-name filters.")

    # Objects can be changed here before validation and serialization.
    for culvert in retained:
        culvert.crossing.notes += "; selected by custom workflow"

    print(f"Retained {len(retained)} culverts; rejected {len(rejected)}.")
    Hy8FileWriter(project).write(output_path=output_hy8)
    return CulvertWorkflowResult(
        project=project,
        processor_collection=processor_collection,
        maximums=maximums,
        raw_data=raw_data,
        retained=retained,
        rejected=rejected,
    )


if __name__ == "__main__":
    workflow_result: CulvertWorkflowResult = run_workflow(
        result_paths=TUFLOW_RESULT_PATHS,
        output_hy8=OUTPUT_HY8,
        project_title=PROJECT_TITLE,
        culvert_selection_csv=CULVERT_SELECTION_CSV,
    )
    print(workflow_result.project.describe())
