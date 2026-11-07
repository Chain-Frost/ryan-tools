"""Real-fixture and synthetic TLF model diagnostics and optional HPC history (#231)."""

from pathlib import Path

import pytest

from ryan_library.functions.parse_tlf import search_for_completion, search_from_top
from ryan_library.functions.tuflow.hpc_companion import read_hpc_companion_metrics
from ryan_library.orchestrators.tuflow.tuflow_logsummary import process_log_file
from ryan_library.processors.tuflow.other_processors.TLFProcessor import TLFProcessor

DATA_ROOT = Path(__file__).resolve().parents[1] / "test_data" / "tuflow"
EXAMPLE_LOG = DATA_ROOT / "TUFLOW_Example_Model_Dataset" / "log"
MODULE11_LOG = DATA_ROOT / "tutorials" / "Module_11" / "runs" / "log"


@pytest.mark.parametrize(
    ("line", "field", "expected"),
    [
        ("TUFLOW 1D Negative Depths: 4", "TLF_1D_Negative_Depths", 4),
        ("HPC HCN Repeated Timesteps: 0  ! HCN is High Control Numbers", "TLF_HPC_HCN_Repeated_Timesteps", 0),
        ("HPC NaN Repeated Timesteps: 12", "TLF_HPC_NaN_Repeated_Timesteps", 12),
        ("HPC NaN WARNING 2550: 3", "TLF_HPC_NaN_Warnings_2550", 3),
        ("WARNINGs prior to simulation:     6  [0 not in _messages layer]", "TLF_Warnings_Prior", 6),
        ("WARNINGs during simulation:       2", "TLF_Warnings_During", 2),
        ("CHECKs prior to simulation:       21", "TLF_Checks_Prior", 21),
        ("CHECKs during simulation:         1", "TLF_Checks_During", 1),
    ],
)
def test_main_footer_counters(line: str, field: str, expected: int) -> None:
    data, _, _ = search_for_completion(line, {}, 0, "final")
    assert data[field] == expected


@pytest.mark.parametrize(
    ("kind", "field", "value"),
    [
        ("cells (ZC)", "TLF_Quadtree_ZC_Cells", 21847),
        ("u-faces (ZU)", "TLF_Quadtree_ZU_Faces", 21709),
        ("v-faces (ZV)", "TLF_Quadtree_ZV_Faces", 21682),
        ("corners (ZH)", "TLF_Quadtree_ZH_Corners", 22359),
    ],
)
def test_quadtree_mesh_counts(kind: str, field: str, value: int) -> None:
    line = f"Quadtree Mesh number of {kind}:           {value:,}"
    data, _, _, _, _ = search_from_top(line, {}, 0, False, False, False)
    assert data[field] == value


def test_main_diagnostics_omit_missing_values() -> None:
    data, _, _ = search_for_completion("No additional errors were encountered", {}, 1, "final")
    assert not any(key.startswith(("TLF_", "HPC_")) for key in data)


@pytest.mark.parametrize(
    ("filename", "field", "expected"),
    [
        ("EG15_003.tlf", "TLF_Quadtree_ZC_Cells", 21847),
        ("EG01_007.tlf", "TLF_Warnings_Prior", 0),
        ("EG01_007.tlf", "TLF_Checks_During", 0),
        ("EG01_007.tlf", "TLF_1D_Negative_Depths", 0),
        ("EG01_007.tlf", "TLF_HPC_HCN_Repeated_Timesteps", 0),
    ],
)
def test_real_main_tlf_diagnostics_in_processor_and_report(filename: str, field: str, expected: int) -> None:
    source = EXAMPLE_LOG / filename
    if not source.is_file():
        pytest.skip("Retained TUFLOW test-data submodule not checked out")
    processor = TLFProcessor(file_path=source)
    processor.process()
    summary = process_log_file(source)
    assert processor.processed
    assert len(processor.df) == len(summary) == 1
    assert int(processor.df.iloc[0][field]) == expected
    assert int(summary.iloc[0][field]) == expected


@pytest.mark.parametrize(
    ("runcode", "active_cells", "sealed_wall_cells", "samples", "observed_max"),
    [
        ("M11_5m_001", 19166, 16330, 136, 6046),
        ("M11_5m_002", 19142, 16354, 131, 5167),
    ],
)
def test_real_hpc_observation_values_and_public_opt_in(
    runcode: str, active_cells: int, sealed_wall_cells: int, samples: int, observed_max: int
) -> None:
    source = MODULE11_LOG / f"{runcode}.tlf"
    if not source.is_file():
        pytest.skip("Retained TUFLOW test-data submodule not checked out")
    observed = read_hpc_companion_metrics(source)
    assert observed["HPC_Companion_Status"] == "observed"
    assert observed["HPC_Active_2D_Cells"] == active_cells
    assert observed["HPC_Sealed_Wall_Cells"] == sealed_wall_cells
    assert observed["HPC_Groundwater_Active_Cells"] == active_cells
    assert observed["HPC_Groundwater_Sealed_Wall_Cells"] == sealed_wall_cells
    assert observed["HPC_nWet_Sampled_Records"] == samples
    assert observed["HPC_nWet_Observed_Max"] == observed_max
    sampled_max = observed["HPC_nWet_Observed_Max"]
    active_cells_reported = observed["HPC_Active_2D_Cells"]
    assert isinstance(sampled_max, int)
    assert isinstance(active_cells_reported, int)
    assert sampled_max <= active_cells_reported
    assert "HPC_Groundwater_Cells" not in observed

    standard = process_log_file(source)
    enriched = process_log_file(source, include_hpc_companion_metrics=True)
    assert len(standard) == len(enriched) == 1
    assert "HPC_Active_2D_Cells" not in standard.columns
    assert int(enriched.iloc[0]["HPC_Active_2D_Cells"]) == active_cells
    assert int(enriched.iloc[0]["HPC_nWet_Observed_Max"]) == observed_max

    processor = TLFProcessor(file_path=source)
    processor.process(include_hpc_companion_metrics=True)
    assert processor.processed
    assert int(processor.df.iloc[0]["HPC_nWet_Sampled_Records"]) == samples


def test_absent_companion_never_creates_or_assumes_zero(tmp_path: Path) -> None:
    main = tmp_path / "M11_5m_010.tlf"
    main.write_text("Synthetic main log", encoding="utf-8")
    outcome = read_hpc_companion_metrics(main)
    assert outcome["HPC_Companion_Status"] == "missing"
    assert outcome["HPC_Companion_Path"] == str(tmp_path / "M11_5m_010.hpc.tlf")
    assert "HPC_Active_2D_Cells" not in outcome
    assert "HPC_nWet_Observed_Max" not in outcome


def test_multiple_domain_headers_are_not_combined(tmp_path: Path) -> None:
    main = tmp_path / "Q01.tlf"
    companion = tmp_path / "Q01.hpc.tlf"
    companion.write_text(
        "Number active cells = 100\n"
        "Number sealed wall cells = 10\n"
        "iStep time maxNu maxNc maxNd nWet vol dt Eff\n"
        "100 0:00:01 0.1 0.3 0.2 25 100 1.2 99.1%\n"
        "Number active cells = 200\n"
        "Number sealed wall cells = 20\n"
        "100 0:00:01 0.1 0.3 0.2 60 100 1.2 99.1%\n",
        encoding="utf-8",
    )
    outcome = read_hpc_companion_metrics(main)
    assert outcome["HPC_Companion_Status"] == "multiple_domains"
    assert "HPC_Active_2D_Cells" not in outcome
    assert "HPC_nWet_Observed_Max" not in outcome


def test_truncated_companion_only_returns_complete_samples(tmp_path: Path) -> None:
    main = tmp_path / "S1.tlf"
    companion = tmp_path / "S1.hpc.tlf"
    companion.write_bytes(
        b"Number active cells = 100\n"
        b"iStep time maxNu maxNc maxNd nWet vol dt Eff\n"
        b"100 0:00:01 0.1 0.3 0.2 0 100 1.2 99.1%\n"
        b"200 0:00:02 0.1 0.3 0.2 75"
    )
    observed = read_hpc_companion_metrics(main)
    assert observed["HPC_Companion_Status"] == "incomplete"
    assert observed["HPC_Active_2D_Cells"] == 100
    assert observed["HPC_nWet_Sampled_Records"] == 1
    assert observed["HPC_nWet_Observed_Max"] == 0
def test_signed_model_clocks_contribute_to_sampled_wet_max(tmp_path: Path) -> None:
    main = tmp_path / "S2.tlf"
    companion = tmp_path / "S2.hpc.tlf"
    companion.write_text(
        "Number active cells = 100\n"
        "iStep time maxNu maxNc maxNd nWet vol dt Eff\n"
        "100 -0:00:57 0.1 0.3 0.2 90 100 1.2 99.1%\n"
        "200 +0:00:01 0.1 0.3 0.2 25 100 1.2 99.1%\n"
        "300 0:00:02.5 0.1 0.3 0.2 30 100 1.2 99.1%\n"
        "400 -0:60:00 0.1 0.3 0.2 99 100 1.2 99.1%\n",
        encoding="utf-8",
    )
    observed = read_hpc_companion_metrics(main)
    assert observed["HPC_Companion_Status"] == "observed"
    assert observed["HPC_nWet_Sampled_Records"] == 3
    assert observed["HPC_nWet_Observed_Max"] == 90
