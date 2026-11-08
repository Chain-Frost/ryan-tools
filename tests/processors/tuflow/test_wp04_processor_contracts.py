"""WP04: independent processor lifecycle, canonical selection and sentinel regressions."""

from pathlib import Path
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
from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.maximums_1d.CmxProcessor import CmxProcessor
from ryan_library.processors.tuflow.maximums_1d.NmxProcessor import NmxProcessor
from ryan_library.processors.tuflow.other_processors.POMMProcessor import POMMProcessor
from ryan_library.processors.tuflow.other_processors.POProcessor import POProcessor
from ryan_library.processors.tuflow.other_processors.TLFProcessor import TLFProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
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
        "Chan ID,Time Qmax,Qmax,Vmax,Time Vmax\n"
        "C1,0.5,5.0,1.5,0.75\nC2,0.5,-4.0,2.0,0.75\n",
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


def _pomm_path(folder: Path, *, upstream_max: str = "40.9482", upstream_min: str = "-99999") -> Path:
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
            0: [RUN] * 6,
            1: headings,
            2: ["U/S Structure Water Level", "Bridge", upstream_max, "1", upstream_min, "2"],
            3: ["D/S Structure Water Level", "Bridge", "40.7525", "1", "-99999", "2"],
            4: ["Flow", "Bridge", "-5", "1", "0", "2"],
        }
    )
    path = folder / f"{RUN}_POMM.csv"
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
    assert upstream["AbsMax"] == pytest.approx(40.9482)
    assert upstream["SignedAbsMax"] == pytest.approx(40.9482)
    assert pd.isna(upstream["Min"])
    assert downstream["AbsMax"] == pytest.approx(40.7525)
    assert downstream["SignedAbsMax"] == pytest.approx(40.7525)
    assert flow["AbsMax"] == 5.0
    assert flow["SignedAbsMax"] == -5.0


def test_pomm_all_missing_extrema_not_accepted_as_success(tmp_path: Path) -> None:
    path = _pomm_path(tmp_path, upstream_max="-99999", upstream_min="-99999")
    processor = POMMProcessor(file_path=path)
    processor.process()
    assert processor.processed  # The other level and flow remain valid.
    assert len(processor.df) == 2
    assert processor.input_health == "PARTIAL"

    raw = pd.read_csv(path, header=None)
    for column in (2, 3, 4):
        raw.iloc[2, column] = "-99999"
        raw.iloc[4, column] = "-99999"
    raw.iloc[2, 4] = "**********"
    raw.iloc[4, 4] = "**********"
    raw.to_csv(path, index=False, header=False)
    processor.process()
    assert not processor.processed
    assert processor.df.empty


def test_po_mask_sentinel_before_absolute_values_with_valid_zero_and_negative(tmp_path: Path) -> None:
    path = _file(
        tmp_path,
        "_PO.csv",
        "Dummy,Time,HU,Flow\nDummy,Time,Bridge,Bridge\n"
        "row,0,40.9482,-5\nrow,1,-99999,0\nrow,2,40.0,2\n",
    )
    processor = POProcessor(file_path=path)
    processor.process()
    assert processor.processed
    assert processor.input_health == "PARTIAL"
    assert processor.rejection_reasons["no_data"] == 1
    levels = processor.df[processor.df["Type"] == "HU"]
    assert levels["AbsValue"].tolist() == [40.9482, 40.0]
    flows = processor.df[processor.df["Type"] == "Flow"]
    assert flows["Value"].tolist() == [-5.0, 0.0, 2.0]
    assert flows["AbsValue"].tolist() == [5.0, 0.0, 2.0]


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
        "Dummy,Time,HU,Q\n,(h),Bridge,Bridge\n"
        "0,0,40,-5\n1,1,-99999,0\n2,2,41,1\n",
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


def test_q_qa_rejects_nonmonotonic_and_malformed_input(tmp_path: Path) -> None:
    path = _file(tmp_path, "_1d_Q.csv", "Time,Q C1\n1,5\n0,2\n")
    result = analyze_stability_q_csv(path, _stability_config())
    assert len(result) == 1
    assert result[0].status == "INVALID_TIME_AXIS"


def test_qa_rejects_invalid_tolerance_configuration(tmp_path: Path) -> None:
    path = _file(tmp_path, "_PO.csv", "Dummy,Time,Q\n,(h),C1\n0,0,1\n1,1,2\n")
    config = _stability_config()
    from dataclasses import replace

    with pytest.raises(ValueError, match="finite"):
        analyze_stability_csv(path, replace(config, diff_abs_tol=float("inf")))
    with pytest.raises(ValueError, match="one-hour"):
        analyze_peak_csv(path, replace(_peak_config(), warn_1hour=3.0))
