"""WP04: independent processor lifecycle, canonical selection and sentinel regressions."""

from pathlib import Path
from typing import cast
from unittest.mock import patch

import pandas as pd
import pytest

from ryan_library.functions.tuflow.po_timeseries_checks import (
    PeakCheckConfig,
    StabilityCheckConfig,
    analyze_peak_csv,
    analyze_stability_csv,
    analyze_stability_q_csv,
)
from ryan_library.functions.tuflow.pomm_utils import find_aep_dur_max
from ryan_library.functions.tuflow.tuflow_common import _result_status  # pyright: ignore[reportPrivateUsage]
from ryan_library.mcp.server import inspect_tuflow_collection, inspect_tuflow_result
from ryan_library.orchestrators.tuflow.peak_check_po_csvs import (
    _file_outcome as _peak_file_outcome,  # pyright: ignore[reportPrivateUsage]
)
from ryan_library.orchestrators.tuflow.tuflow_timeseries_stability import (
    _file_outcome as _stability_file_outcome,  # pyright: ignore[reportPrivateUsage]
)
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.maximums_1d.NmxProcessor import NmxProcessor
from ryan_library.processors.tuflow.other_processors.POMMProcessor import POMMProcessor
from ryan_library.processors.tuflow.other_processors.POProcessor import POProcessor
from ryan_library.processors.tuflow.other_processors.TLFProcessor import TLFProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.processors.tuflow.timeseries_1d.HProcessor import HProcessor
from ryan_library.processors.tuflow.timeseries_1d.QProcessor import QProcessor

RUN = "Selection_01.00p_00120m_TP01"


def _file(folder: Path, suffix: str, content: str) -> Path:
    path = folder / f"{RUN}{suffix}"
    path.write_text(content, encoding="utf-8")
    return path


def _cmx(folder: Path) -> Path:
    return _file(
        folder,
        "_1d_Cmx.csv",
        "Chan ID,Time Qmax,Qmax,Vmax,Time Vmax\nC1,0.5,5.0,1.5,0.75\nC2,0.5,-4.0,2.0,0.75\n",
    )


def _nmx(folder: Path) -> Path:
    return _file(
        folder,
        "_1d_Nmx.csv",
        "Node ID,Hmax,Time Hmax\nC1.1,10,1\nC1.2,9,1\nC2.1,20,1\nC2.2,19,1\n",
    )


@pytest.mark.parametrize("kind", ["Cmx", "Nmx"])
def test_factory_filter_uses_canonical_channel_ids_after_transform(tmp_path: Path, kind: str) -> None:
    path = _cmx(tmp_path) if kind == "Cmx" else _nmx(tmp_path)
    processor = BaseProcessor.from_file(path, entity_filter=[" C1 "])
    processor.process()
    assert processor.processed
    assert processor.applied_entity_filter == frozenset({"C1"})
    assert set(processor.df["Chan ID"].astype(str)) == {"C1"}
    if kind == "Cmx":
        assert processor.df["Q"].dropna().tolist() == [5.0]
        assert processor.df["V"].dropna().tolist() == [1.5]
    else:
        assert processor.df["US_h"].tolist() == [10.0]
        assert processor.df["DS_h"].tolist() == [9.0]
    collection = ProcessorCollection()
    collection.add_processor(processor)
    assert set(collection.combine_raw()["Chan ID"].astype(str)) == {"C1"}


def test_direct_nmx_no_match_is_visible_skip_not_full_data(tmp_path: Path) -> None:
    processor = NmxProcessor(file_path=_nmx(tmp_path), entity_filter=frozenset({"NO MATCH"}))
    processor.process()
    assert not processor.processed
    assert processor.filtered_empty
    assert processor.df.empty
    assert processor.applied_entity_filter == frozenset({"NO MATCH"})
    collection = ProcessorCollection()
    collection.add_processor(processor)
    assert not collection.processors


def test_q_success_failure_recovery_and_invalid_time_axes(tmp_path: Path) -> None:
    path = _file(tmp_path, "_1d_Q.csv", "Descriptor,Time,Q C1\nrow,0,0\nrow,1,-3\n")
    processor = QProcessor(file_path=path)
    processor.process()
    assert processor.processed
    assert processor.df["Q"].tolist() == [0.0, -3.0]

    path.write_text("Descriptor,Time,Q C1\nrow,0,\nrow,1,\n", encoding="utf-8")
    processor.process()
    assert not processor.processed
    assert processor.df.empty
    assert processor.raw_df.empty
    assert processor.input_health == "INVALID"

    for times in ("row,1,2\nrow,1,3\n", "row,2,2\nrow,1,3\n", "row,inf,2\nrow,1,3\n"):
        path.write_text("Descriptor,Time,Q C1\n" + times, encoding="utf-8")
        processor.process()
        assert not processor.processed
        assert processor.df.empty

    path.write_text("Descriptor,Time,Q C1\nrow,0,0\nrow,1,7\n", encoding="utf-8")
    processor.process()
    assert processor.processed
    assert processor.input_health == "OK"
    assert processor.df["Q"].tolist() == [0.0, 7.0]


def _pomm_path(
    folder: Path, *, upstream_max: str = "40.9482", upstream_min: str = "-99999", run_code: str = RUN
) -> Path:
    headings = [
        "Location",
        "Time",
        "Maximum (Extracted from Time Series)",
        "Time of Maximum",
        "Minimum (Extracted From Time Series)",
        "Time of Minimum",
    ]
    raw = pd.DataFrame(
        {
            0: [run_code] * 6,
            1: headings,
            2: ["U/S Structure Water Level", "Bridge", upstream_max, "1", upstream_min, "2"],
            3: ["D/S Structure Water Level", "Bridge", "40.7525", "1", "-99999", "2"],
            4: ["Flow", "Bridge", "-5", "1", "0", "2"],
        }
    )
    path = folder / f"{run_code}_POMM.csv"
    raw.to_csv(path, index=False, header=False)
    return path


def test_pomm_sentinel_does_not_govern_extrema_and_negative_flow_is_retained(tmp_path: Path) -> None:
    processor = POMMProcessor(file_path=_pomm_path(tmp_path))
    processor.process()
    assert processor.processed
    assert processor.input_health == "PARTIAL"
    assert processor.rejection_reasons["no_data"] == 2
    results = processor.df.set_index("Type")
    upstream = results.loc["U/S Structure Water Level"]
    downstream = results.loc["D/S Structure Water Level"]
    flow = results.loc["Flow"]
    assert cast("float", upstream["AbsMax"]) == pytest.approx(40.9482)
    assert cast("float", upstream["SignedAbsMax"]) == pytest.approx(40.9482)
    assert cast("float", upstream["Min"]) == -99999
    assert cast("float", downstream["AbsMax"]) == pytest.approx(40.7525)
    assert cast("float", downstream["SignedAbsMax"]) == pytest.approx(40.7525)
    assert cast("float", flow["AbsMax"]) == 5.0
    assert cast("float", flow["SignedAbsMax"]) == -5.0


def test_pomm_all_missing_extrema_not_accepted_as_success(tmp_path: Path) -> None:
    path = _pomm_path(tmp_path, upstream_max="-99999", upstream_min="-99999")
    processor = POMMProcessor(file_path=path)
    processor.process()
    assert processor.processed  # The other level and flow remain valid.
    assert len(processor.df) == 3
    assert processor.input_health == "PARTIAL"
    assert processor.df.loc[processor.df["Type"] == "U/S Structure Water Level", "AbsMax"].isna().all()

    raw = pd.read_csv(path, header=None)
    for column in (2, 3, 4):
        raw.iloc[2, column] = "-99999"
        raw.iloc[4, column] = "-99999"
    raw.iloc[2, 4] = "**********"
    raw.iloc[4, 4] = "**********"
    raw.to_csv(path, index=False, header=False)
    processor.process()
    # Structurally readable observations remain in the collection for
    # all-invalid event population counts, but the file is a FAIL, not OK.
    assert processor.processed
    assert processor.input_health == "INVALID"
    assert _result_status(processor) == "FAIL"
    assert processor.df["AbsMax"].isna().all()


def test_pomm_keeps_unusable_tp_provenance_for_population_counts(tmp_path: Path) -> None:
    valid = POMMProcessor(file_path=_pomm_path(tmp_path))
    invalid = POMMProcessor(
        file_path=_pomm_path(
            tmp_path,
            run_code=RUN.replace("TP01", "TP02"),
            upstream_max="-99999",
            upstream_min="-99999",
        )
    )
    valid.process()
    invalid.process()
    assert valid.processed
    assert invalid.processed
    collection = ProcessorCollection()
    collection.add_processor(valid)
    collection.add_processor(invalid)

    raw = collection.pomm_combine()
    upstream = raw.loc[raw["Type"] == "U/S Structure Water Level"]
    assert len(upstream) == 2
    assert upstream["AbsMax"].notna().sum() == 1
    selected = find_aep_dur_max(raw)
    winning = selected.loc[selected["Type"] == "U/S Structure Water Level"].iloc[0]
    assert winning["AbsMax"] == pytest.approx(40.9482)
    assert winning["count_TP"] == 1
    assert winning["count_unusable_TP"] == 1
    assert winning["tp_population_status"] == "PARTIAL"


def _pomm_two_location_path(folder: Path) -> Path:
    """One wholly unusable and one usable level at distinct POMM locations."""
    path = folder / f"{RUN}_POMM.csv"
    pd.DataFrame(
        {
            0: [RUN] * 6,
            1: [
                "Location",
                "Time",
                "Maximum (Extracted from Time Series)",
                "Time of Maximum",
                "Minimum (Extracted From Time Series)",
                "Time of Minimum",
            ],
            2: ["U/S Structure Water Level", "Dry", "-99999", "1", "-99999", "2"],
            3: ["U/S Structure Water Level", "Wet", "41", "1", "40", "2"],
        }
    ).to_csv(path, index=False, header=False)
    return path


def test_pomm_selection_reclassifies_only_unusable_locations(tmp_path: Path) -> None:
    path = _pomm_two_location_path(tmp_path)
    selected = POMMProcessor(file_path=path, entity_filter=frozenset({"Dry"}))
    selected.process()
    assert selected.processed
    assert selected.input_health == "INVALID"
    assert _result_status(selected) == "FAIL"
    assert selected.df["AbsMax"].isna().all()
    assert len(selected.df) == 1

    missing = POMMProcessor(file_path=path, entity_filter=frozenset({"Absent"}))
    missing.process()
    assert not missing.processed
    assert missing.filtered_empty
    assert _result_status(missing) == "SKIP"


def test_collection_copy_preserves_invalid_pomm_health_and_tp_evidence(tmp_path: Path) -> None:
    path = _pomm_two_location_path(tmp_path)
    selected = POMMProcessor(file_path=path, entity_filter=frozenset({"Dry"}))
    selected.process()
    assert selected.processed
    assert _result_status(selected) == "FAIL"
    collection = ProcessorCollection()
    collection.add_processor(selected)
    copied = collection.copy().processors[0]
    assert copied is not selected
    assert copied.processed
    assert copied.input_health == "INVALID"
    assert copied.rejected_samples == selected.rejected_samples
    assert copied.rejection_reasons == selected.rejection_reasons
    assert copied.df["AbsMax"].isna().all()
    assert _result_status(copied) == "FAIL"


def test_public_filter_reclassifies_unusable_pomm_location(tmp_path: Path) -> None:
    path = _pomm_two_location_path(tmp_path)
    original = POMMProcessor(file_path=path)
    original.process()
    assert original.processed
    assert original.input_health == "PARTIAL"
    assert original.df["AbsMax"].notna().sum() == 1
    collection = ProcessorCollection()
    collection.add_processor(original)

    selected = collection.copy()
    selected.filter_locations(["Dry"])
    assert len(selected.processors) == 1
    invalid = selected.processors[0]
    assert invalid.processed
    assert len(invalid.df) == 1
    assert invalid.df["AbsMax"].isna().all()
    assert invalid.input_health == "INVALID"
    assert _result_status(invalid) == "FAIL"

    usable = collection.copy()
    usable.filter_locations(["Wet"])
    assert len(usable.processors) == 1
    assert _result_status(usable.processors[0]) == "OK"


def test_mcp_exposes_invalid_health_without_losing_raw_population(tmp_path: Path) -> None:
    path = _pomm_two_location_path(tmp_path)
    single = inspect_tuflow_result(file_path=str(path), locations=["Dry"])
    assert single["processed"] is True
    assert single["workflow_status"] == "FAIL"
    assert single["input_health"] == "INVALID"
    assert single["rejected_samples"] == 2
    assert single["rejection_reasons"]["no_data"] == 2
    assert single["rejection_reasons"]["all_missing_extrema"] == 1
    collection = inspect_tuflow_collection(file_paths=[str(path)], locations=["Dry"])
    assert len(collection["processors"]) == 1
    report = collection["processors"][0]
    assert report["processed"] is True
    assert report["workflow_status"] == "FAIL"
    assert report["rejected_samples"] == single["rejected_samples"]


def test_selected_wholly_invalid_q_is_fail_not_absent_entity_skip(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_1d_Q.csv",
        "Descriptor,Time,Q C1,Q C2\nrow,0,inf,1\nrow,1,inf,2\n",
    )
    existing = QProcessor(file_path=path, entity_filter=frozenset({"C1"}))
    existing.process()
    assert not existing.processed
    assert not existing.filtered_empty
    assert existing.input_health == "INVALID"
    assert _result_status(existing) == "FAIL"
    assert existing.rejection_reasons["nonfinite"] == 2

    absent = QProcessor(file_path=path, entity_filter=frozenset({"C3"}))
    absent.process()
    assert not absent.processed
    assert absent.filtered_empty
    assert _result_status(absent) == "SKIP"

    valid = QProcessor(file_path=path, entity_filter=frozenset({"C2"}))
    valid.process()
    assert valid.processed
    assert valid.df["Q"].tolist() == [1.0, 2.0]


def test_h_selection_distinguishes_blank_present_channel_from_absent(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_1d_H.csv",
        "Descriptor,Time,H C1.1,H C1.2,H C2.1,H C2.2\nrow,0,,,10,9\nrow,1,,,11,10\n",
    )
    present = HProcessor(file_path=path, entity_filter=frozenset({"C1"}))
    present.process()
    assert not present.processed
    assert not present.filtered_empty
    assert present.input_health == "INVALID"
    assert _result_status(present) == "FAIL"

    absent = HProcessor(file_path=path, entity_filter=frozenset({"C3"}))
    absent.process()
    assert not absent.processed
    assert absent.filtered_empty
    assert _result_status(absent) == "SKIP"

    usable = HProcessor(file_path=path, entity_filter=frozenset({"C2"}))
    usable.process()
    assert usable.processed
    assert usable.df["US_H"].tolist() == [10.0, 11.0]
    assert usable.df["DS_H"].tolist() == [9.0, 10.0]


def test_po_mask_sentinel_before_absolute_values_with_valid_zero_and_negative(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_PO.csv",
        "Dummy,Time,HU,Flow\nDummy,Time,Bridge,Bridge\nrow,0,40.9482,-5\nrow,1,-99999,0\nrow,2,40.0,2\n",
    )
    processor = POProcessor(file_path=path)
    processor.process()
    assert processor.processed
    assert processor.input_health == "PARTIAL"
    assert processor.rejection_reasons["no_data"] == 1
    levels = processor.df[processor.df["Type"] == "HU"]
    assert levels["Value"].tolist() == [40.9482, -99999.0, 40.0]
    assert levels["AbsValue"].dropna().tolist() == [40.9482, 40.0]
    flows = processor.df[processor.df["Type"] == "Flow"]
    assert flows["Value"].tolist() == [-5.0, 0.0, 2.0]
    assert flows["AbsValue"].tolist() == [5.0, 0.0, 2.0]


def test_po_selection_distinguishes_rejected_location_from_absent(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_PO.csv",
        "Dummy,Time,Q,Q\nDummy,Time,C1,C2\nrow,0,inf,1\nrow,1,inf,2\n",
    )
    present = POProcessor(file_path=path, entity_filter=frozenset({"C1"}))
    present.process()
    assert not present.processed
    assert not present.filtered_empty
    assert present.input_health == "INVALID"
    assert _result_status(present) == "FAIL"
    assert present.rejection_reasons["nonfinite"] == 2

    absent = POProcessor(file_path=path, entity_filter=frozenset({"C3"}))
    absent.process()
    assert not absent.processed
    assert absent.filtered_empty
    assert _result_status(absent) == "SKIP"

    usable = POProcessor(file_path=path, entity_filter=frozenset({"C2"}))
    usable.process()
    assert usable.processed
    assert usable.df["Value"].tolist() == [1.0, 2.0]


def test_po_public_filter_reclassifies_dry_only_location(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_PO.csv",
        "Dummy,Time,HU,HU\nDummy,Time,Dry,Wet\nrow,0,-99999,39\nrow,1,-99999,41\n",
    )
    unfiltered = POProcessor(file_path=path)
    unfiltered.process()
    assert unfiltered.processed
    assert unfiltered.input_health == "PARTIAL"
    assert unfiltered.df["Value"].eq(-99999).sum() == 2
    assert unfiltered.df["AbsValue"].notna().sum() == 2

    direct = POProcessor(file_path=path, entity_filter=frozenset({"Dry"}))
    direct.process()
    assert direct.processed
    assert direct.input_health == "INVALID"
    assert _result_status(direct) == "FAIL"
    assert direct.df["Value"].tolist() == [-99999.0, -99999.0]

    collection = ProcessorCollection()
    collection.add_processor(unfiltered)
    selected = collection.copy()
    selected.filter_locations(["Dry"])
    assert len(selected.processors) == 1
    invalid = selected.processors[0]
    assert invalid.processed
    assert invalid.input_health == "INVALID"
    assert _result_status(invalid) == "FAIL"
    assert invalid.df["Value"].tolist() == [-99999.0, -99999.0]
    assert invalid.df["AbsValue"].isna().all()

    usable = collection.copy()
    usable.filter_locations(["Wet"])
    assert len(usable.processors) == 1
    assert _result_status(usable.processors[0]) == "OK"
    assert usable.processors[0].df["AbsValue"].tolist() == [39.0, 41.0]


def test_po_public_filter_preserves_rejected_source_location(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_PO.csv",
        "Dummy,Time,Q,Q\nDummy,Time,Dry,Wet\nrow,0,inf,1\nrow,1,inf,2\n",
    )
    original = POProcessor(file_path=path)
    original.process()
    assert original.processed
    assert original.input_health == "PARTIAL"
    assert original.source_locations == frozenset({"Dry", "Wet"})
    assert original.df["Location"].astype(str).tolist() == ["Wet", "Wet"]

    collection = ProcessorCollection()
    collection.add_processor(original)
    selected = collection.copy()
    selected.filter_locations(["Dry"])
    assert len(selected.processors) == 1
    invalid = selected.processors[0]
    assert invalid.processed
    assert invalid.df.empty
    assert invalid.selected_unusable
    assert not invalid.filtered_empty
    assert invalid.input_health == "INVALID"
    assert invalid.rejection_reasons["nonfinite"] == 2
    assert _result_status(invalid) == "FAIL"

    by_type = selected.get_processors_by_data_type("PO")
    assert len(by_type.processors) == 1
    assert _result_status(by_type.processors[0]) == "FAIL"

    absent = collection.copy()
    absent.filter_locations(["Absent"])
    assert not absent.processors

    wet = collection.copy()
    wet.filter_locations(["Wet"])
    assert len(wet.processors) == 1
    assert _result_status(wet.processors[0]) == "OK"
    assert wet.processors[0].df["Value"].tolist() == [1.0, 2.0]

    mixed = collection.copy()
    mixed.filter_locations(["Dry", "Wet"])
    assert len(mixed.processors) == 1
    assert _result_status(mixed.processors[0]) == "OK"


def test_po_source_location_evidence_resets_when_file_reprocessed(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_PO.csv",
        "Dummy,Time,Q,Q\nDummy,Time,Dry,Wet\nrow,0,inf,1\nrow,1,inf,2\n",
    )
    processor = POProcessor(file_path=path)
    processor.process()
    assert processor.source_locations == frozenset({"Dry", "Wet"})

    path.write_text("Dummy,Time,Q\nDummy,Time,Wet\nrow,0,3\nrow,1,4\n", encoding="utf-8")
    processor.process()
    assert processor.processed
    assert processor.source_locations == frozenset({"Wet"})
    assert processor.input_health == "OK"
    processor.filter_locations(["Dry"])
    assert processor.filtered_empty
    assert not processor.selected_unusable



@pytest.mark.parametrize(
    ("suffix", "content"),
    [
        (
            "_1d_Q.csv",
            "Descriptor,Time,Q C1,Q C2\nrow,0,inf,1\nrow,1,inf,2\n",
        ),
        (
            "_1d_H.csv",
            "Descriptor,Time,H C1.1,H C1.2,H C2.1,H C2.2\n"
            "row,0,inf,inf,10,9\nrow,1,inf,inf,11,10\n",
        ),
    ],
)
def test_timeseries_public_filter_preserves_all_rejected_channel(
    tmp_path: Path, suffix: str, content: str
) -> None:
    path = _file(tmp_path, suffix, content)
    processor = BaseProcessor.from_file(path)
    processor.process()
    assert processor.processed
    assert processor.input_health == "PARTIAL"
    assert set(processor.source_channels) == {"C1", "C2"}

    source = ProcessorCollection()
    source.add_processor(processor)

    rejected = source.copy()
    rejected.filter_locations(["C1"])
    assert len(rejected.processors) == 1
    invalid = rejected.processors[0]
    assert invalid.df.empty
    assert invalid.selected_unusable
    assert invalid.input_health == "INVALID"
    assert not invalid.filtered_empty
    assert _result_status(invalid) == "FAIL"

    selected_type = rejected.get_processors_by_data_type(processor.data_type)
    assert len(selected_type.processors) == 1
    assert _result_status(selected_type.processors[0]) == "FAIL"

    absent = source.copy()
    absent.filter_locations(["C3"])
    assert not absent.processors

    usable = source.copy()
    usable.filter_locations(["C2"])
    assert len(usable.processors) == 1
    assert _result_status(usable.processors[0]) == "OK"
    assert set(usable.processors[0].df["Chan ID"].astype(str)) == {"C2"}


def test_timeseries_source_channel_evidence_resets_on_reprocess(tmp_path: Path) -> None:
    path = _file(tmp_path, "_1d_Q.csv", "Descriptor,Time,Q C1,Q C2\nrow,0,inf,1\nrow,1,inf,2\n")
    processor = QProcessor(file_path=path)
    processor.process()
    assert processor.source_channels == frozenset({"C1", "C2"})

    path.write_text("Descriptor,Time,Q C2\nrow,0,3\nrow,1,4\n", encoding="utf-8")
    processor.process()
    assert processor.processed
    assert processor.source_channels == frozenset({"C2"})
    assert processor.input_health == "OK"
    processor.filter_locations(["C1"])
    assert processor.filtered_empty
    assert not processor.selected_unusable


def test_tlf_retry_does_not_expose_previous_finished_status(tmp_path: Path) -> None:
    path = _file(tmp_path, ".tlf", "placeholder\n")
    processor = TLFProcessor(file_path=path)
    with (
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.get_log_lines",
            return_value=(["head"], ["tail"]),
        ),
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.search_for_completion",
            return_value=({}, 2, None),
        ),
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.is_complete_tlf",
            return_value=True,
        ),
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.process_top_lines",
            return_value=({}, 4, False, False, False),
        ),
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.finalise_data",
            return_value=pd.DataFrame([{"EndStatus": "Simulation FINISHED"}]),
        ),
    ):
        processor.process()
    assert processor.processed
    assert processor.df.iloc[0]["EndStatus"] == "Simulation FINISHED"

    path.write_text("initialising\n", encoding="utf-8")
    with patch(
        "ryan_library.processors.tuflow.other_processors.TLFProcessor.get_log_lines",
        return_value=(["initialising"], ["initialising"]),
    ):
        processor.process()
    assert not processor.processed
    assert processor.df.empty
    assert processor.raw_df.empty

    with (
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.get_log_lines",
            return_value=(["head"], ["tail"]),
        ),
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.search_for_completion",
            return_value=({}, 2, None),
        ),
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.is_complete_tlf",
            return_value=True,
        ),
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.process_top_lines",
            return_value=({}, 4, False, False, False),
        ),
        patch(
            "ryan_library.processors.tuflow.other_processors.TLFProcessor.finalise_data",
            return_value=pd.DataFrame([{"EndStatus": "Simulation FINISHED"}]),
        ),
    ):
        processor.process()
    assert processor.processed
    assert len(processor.df) == 1


def _stability_config() -> StabilityCheckConfig:
    return StabilityCheckConfig(
        datatype_include=["HU", "Q"],
        datatype_case_sensitive=False,
        location_include=[],
        location_exclude=[],
        location_case_sensitive=False,
        flat_tol=0.01,
        diff_rel_tol=0.05,
        diff_abs_tol=0.01,
        max_sign_changes=2,
        min_points=2,
    )


def _peak_config() -> PeakCheckConfig:
    return PeakCheckConfig(
        datatype_include=["HU", "Q"],
        datatype_case_sensitive=False,
        location_include=[],
        location_exclude=[],
        location_case_sensitive=False,
        warn_2hours=2.0,
        warn_1hour=1.0,
        flat_tol=0.01,
    )


def test_qa_partial_hu_sentinel_preserves_actual_range_and_hydraulic_status(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_PO.csv",
        "Dummy,Time,HU,Q\n,(h),Bridge,Bridge\n0,0,40,-5\n1,1,-99999,0\n2,2,41,1\n",
    )
    level = next(result for result in analyze_stability_csv(path, _stability_config()) if result.datatype == "HU")
    assert level.status == "PARTIAL_DATA"
    assert level.input_health == "PARTIAL"
    assert level.hydraulic_status == "OK"
    assert level.value_range == 1.0
    assert level.rejected_samples == 1
    assert level.rejection_reasons == "no_data=1"
    peak = next(result for result in analyze_peak_csv(path, _peak_config()) if result.datatype == "HU")
    assert peak.status == "PARTIAL_DATA"
    assert peak.peak_value == 41.0
    assert peak.rejected_samples == 1
    flow = next(result for result in analyze_stability_csv(path, _stability_config()) if result.datatype == "Q")
    assert flow.rejected_samples == 0
    assert flow.value_min == -5.0
    assert flow.value_max == 1.0


@pytest.mark.parametrize(
    ("csv_data", "expected"),
    [
        ("Dummy,Time,Q\n,(h),C1\n0,0,1\n1,0,2\n", "INVALID_TIME_AXIS"),
        ("Dummy,Time,Q\n,(h),C1\n0,2,1\n1,1,2\n", "INVALID_TIME_AXIS"),
        ("Dummy,Time,Q\n,(h),C1\n0,inf,1\n1,1,2\n", "INVALID_TIME_AXIS"),
        ("Dummy,Time,Q\n,(h),C1\n0,0,1\n1,1,2,EXTRA\n", "MALFORMED_ROW"),
    ],
)
def test_qa_rejects_bad_po_time_and_row_width(tmp_path: Path, csv_data: str, expected: str) -> None:
    path = _file(tmp_path, "_PO.csv", csv_data)
    result = analyze_stability_csv(path, _stability_config())
    assert len(result) == 1
    assert result[0].status == expected
    assert result[0].input_health == "INVALID"
    assert _peak_file_outcome([{"status": expected, "input_health": "INVALID"}]) == "FAIL"
    assert _stability_file_outcome([{"status": expected, "input_health": "INVALID"}]) == "FAIL"


def test_q_qa_rejects_nonmonotonic_and_malformed_input(tmp_path: Path) -> None:
    path = _file(tmp_path, "_1d_Q.csv", "Time,Q C1\n1,5\n0,2\n")
    result = analyze_stability_q_csv(path, _stability_config())
    assert len(result) == 1
    assert result[0].status == "INVALID_TIME_AXIS"
    assert result[0].input_health == "INVALID"


def test_qa_all_invalid_level_samples_fail_not_skip(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_PO.csv",
        "Dummy,Time,HU\nDummy,Time,Bridge\n0,0,-99999\n1,1,**********\n",
    )
    peaks = analyze_peak_csv(path, _peak_config())
    stability = analyze_stability_csv(path, _stability_config())
    assert len(peaks) == 1
    assert len(stability) == 1
    assert peaks[0].status == "NO_DATA"
    assert peaks[0].input_health == "INVALID"
    assert stability[0].status == "NO_DATA"
    assert stability[0].input_health == "INVALID"
    assert _peak_file_outcome([{"status": peaks[0].status, "input_health": peaks[0].input_health}]) == "FAIL"
    stability_outcome = _stability_file_outcome(
        [{"status": stability[0].status, "input_health": stability[0].input_health}]
    )
    assert stability_outcome == "FAIL"


def test_qa_rejects_invalid_tolerance_configuration(tmp_path: Path) -> None:
    path = _file(tmp_path, "_PO.csv", "Dummy,Time,Q\n,(h),C1\n0,0,1\n1,1,2\n")
    config = _stability_config()
    from dataclasses import replace

    with pytest.raises(ValueError, match="finite"):
        analyze_stability_csv(path, replace(config, diff_abs_tol=float("inf")))
    with pytest.raises(ValueError, match="one-hour"):
        analyze_peak_csv(path, replace(_peak_config(), warn_1hour=3.0))
