"""Processed channel selection retains values and preserves EOF ambiguity decisions."""

from collections.abc import Collection, Sequence
from pathlib import Path

import pandas as pd
import pytest

from ryan_library.processors.tuflow.base_processor import BaseProcessor
from ryan_library.processors.tuflow.maximums_1d.CmxProcessor import CmxProcessor
from ryan_library.processors.tuflow.maximums_1d.NmxProcessor import NmxProcessor
from ryan_library.processors.tuflow.other_processors.EOFProcessor import EOFProcessor
from ryan_library.processors.tuflow.processor_collection import ProcessorCollection
from ryan_library.processors.tuflow.timeseries_1d.QProcessor import QProcessor

RUN_CODE = "Selection_01.00p_00120m_TP01"


def _process_file[ProcessorT: BaseProcessor](
    directory: Path, suffix: str, content: str, processor_type: type[ProcessorT]
) -> ProcessorT:
    path = directory / f"{RUN_CODE}{suffix}"
    path.write_text(content, encoding="utf-8")
    processor = processor_type(file_path=path)
    processor.process()
    assert processor.processed
    assert not processor.df.empty
    return processor


def _process_cmx(directory: Path, channels: Sequence[str]) -> CmxProcessor:
    rows = [f"{channel},0.5,{index * 5.0},{index * 1.5},0.75" for index, channel in enumerate(channels, 1)]
    content = "Chan ID,Time Qmax,Qmax,Vmax,Time Vmax\n" + "\n".join(rows) + "\n"
    return _process_file(directory, "_1d_Cmx.csv", content, CmxProcessor)


def _process_nmx(directory: Path, channels: Sequence[str]) -> NmxProcessor:
    rows = [
        f"{channel}.{node},{index * 10.0 + node},0.5" for index, channel in enumerate(channels, 1) for node in (1, 2)
    ]
    content = "Node ID,Hmax,Time Hmax\n" + "\n".join(rows) + "\n"
    return _process_file(directory, "_1d_Nmx.csv", content, NmxProcessor)


def _process_q(directory: Path, channels: Sequence[str]) -> QProcessor:
    header = "Descriptor,Time," + ",".join(f"Q {channel}" for channel in channels)
    row = "row,0.5," + ",".join(str(index * 5.0) for index, _ in enumerate(channels, 1))
    return _process_file(directory, "_1d_Q.csv", f"{header}\n{row}\n", QProcessor)


def _process_eof(directory: Path, channels: Sequence[tuple[str, float]]) -> EOFProcessor:
    header = (
        "Channel  Type  Num_barrels  US_Invert  DS_Invert  US_Obvert  DS_Obvert  Length  Slope  "
        "Mannings_n  Diam_Width  Height  Inlet_Height  Inlet_Width  Entry Loss  Exit Loss  Fixed Loss  Ent/Exit Losses"
    )
    rows = [
        f"{channel}  C  1  10  9  12  11  100  0.01  0.013  2  {height}  2  2  0.5  1  0  1.5"
        for channel, height in channels
    ]
    content = "CULVERT AND PIPE DATA\n" + header + "\n" + "\n".join(rows) + "\n\n"
    return _process_file(directory, ".eof", content, EOFProcessor)


def _collection(*processors: BaseProcessor) -> ProcessorCollection:
    collection = ProcessorCollection()
    for processor in processors:
        collection.add_processor(processor)
    return collection


def _channels(frame: pd.DataFrame) -> set[str]:
    return set(frame["Chan ID"].astype(str).tolist())


def test_real_cmx_and_nmx_selection_preserves_processed_rows_and_is_idempotent(tmp_path: Path) -> None:
    cmx = _process_cmx(tmp_path, ["C01", "C02"])
    nmx = _process_nmx(tmp_path, ["C01", "C02"])
    collection = _collection(cmx, nmx)
    expected = [processor.df.loc[processor.df["Chan ID"] == "C01"].copy() for processor in (cmx, nmx)]

    assert collection.filter_locations([" C01 ", ""]) == frozenset({"C01"})
    assert len(collection.processors) == 2
    for processor, frame in zip((cmx, nmx), expected, strict=True):
        pd.testing.assert_frame_equal(processor.df, frame)
        assert processor.applied_location_filter == frozenset({"C01"})
        assert processor.processed
    assert cmx.df["Q"].dropna().tolist() == [5.0]
    assert cmx.df["V"].dropna().tolist() == [1.5]
    assert nmx.df["US_h"].tolist() == [11.0]
    assert nmx.df["DS_h"].tolist() == [12.0]

    selected_frames = [processor.df for processor in (cmx, nmx)]
    for locations in (["C01"], [" C01 ", "C01"], [], None):
        collection.filter_locations(locations)
        for processor, frame in zip((cmx, nmx), selected_frames, strict=True):
            assert processor.df is frame


@pytest.mark.parametrize("column", ["Location", "Chan ID", "Node ID", "ID"])
@pytest.mark.parametrize("dtype", ["string", "category"])
def test_identifier_selection_is_exact_and_preserves_original_values_and_dtypes(
    tmp_path: Path, column: str, dtype: str
) -> None:
    processor = CmxProcessor(file_path=tmp_path / f"{RUN_CODE}_1d_Cmx.csv")
    processor.df = pd.DataFrame(
        {column: pd.Series([" 001 ", "0010", "1", "c01"], dtype=dtype), "value": pd.Series([1, 2, 3, 4], dtype="Int32")}
    )
    expected = processor.df.iloc[[0]].copy()

    assert processor.filter_locations(["001", "C01"]) == frozenset({"001", "C01"})

    pd.testing.assert_frame_equal(processor.df, expected)


def test_numeric_entity_ids_remain_numeric_after_text_selection(tmp_path: Path) -> None:
    processor = CmxProcessor(file_path=tmp_path / f"{RUN_CODE}_1d_Cmx.csv")
    processor.df = pd.DataFrame({"ID": pd.Series([1, 10, None], dtype="Int64"), "value": [2.0, 3.0, 4.0]})
    expected = processor.df.iloc[[0]].copy()

    processor.filter_locations([" 1 "])

    pd.testing.assert_frame_equal(processor.df, expected)


def test_location_keeps_priority_when_channel_column_is_also_present(tmp_path: Path) -> None:
    processor = CmxProcessor(file_path=tmp_path / f"{RUN_CODE}_1d_Cmx.csv")
    processor.df = pd.DataFrame({"Location": ["L1", "L2"], "Chan ID": ["L2", "L1"], "Q": [5.0, 50.0]})
    expected = processor.df.iloc[[0]].copy()

    processor.filter_locations(["L1"])

    pd.testing.assert_frame_equal(processor.df, expected)


@pytest.mark.parametrize("locations", [None, [], ["", " "]])
def test_empty_selection_leaves_processed_collection_unchanged(
    tmp_path: Path, locations: Collection[str] | None
) -> None:
    cmx = _process_cmx(tmp_path, ["C01", "C02"])
    nmx = _process_nmx(tmp_path, ["C01", "C02"])
    collection = _collection(cmx, nmx)
    frames = [processor.df for processor in (cmx, nmx)]

    assert collection.filter_locations(locations) == frozenset()

    assert len(collection.processors) == 2
    for processor, frame in zip((cmx, nmx), frames, strict=True):
        assert processor.df is frame
        assert processor.applied_location_filter is None


def test_unmatched_selection_removes_valid_processors_without_marking_them_failed(tmp_path: Path) -> None:
    cmx = _process_cmx(tmp_path, ["C01", "C02"])
    nmx = _process_nmx(tmp_path, ["C01", "C02"])
    collection = _collection(cmx, nmx)

    assert collection.filter_locations(["missing"]) == frozenset({"missing"})

    assert collection.processors == []
    assert collection.combine_1d_maximums().empty
    for processor in (cmx, nmx):
        assert processor.df.empty
        assert processor.processed
        assert processor.applied_location_filter == frozenset({"missing"})


def test_unique_eof_labels_align_before_full_channel_selection(tmp_path: Path) -> None:
    selected = "Bridge 159.975"
    excluded = "Bridge 202.231"
    cmx = _process_cmx(tmp_path, [selected, excluded])
    nmx = _process_nmx(tmp_path, [selected, excluded])
    q = _process_q(tmp_path, [selected, excluded])
    eof = _process_eof(tmp_path, [("Bridge 159.97", 2.0), ("Bridge 202.23", 20.0)])
    expected_geometry = eof.df.iloc[[0]].drop(columns="Chan ID").copy()
    identifier_dtype = eof.df["Chan ID"].dtype
    collection = _collection(cmx, nmx, q, eof)

    collection.filter_locations([selected])

    assert len(collection.processors) == 4
    assert _channels(eof.df) == {selected}
    assert eof.df["Chan ID"].dtype == identifier_dtype
    pd.testing.assert_frame_equal(eof.df.drop(columns="Chan ID"), expected_geometry)
    for combined in (collection.combine_1d_maximums(), collection.combine_1d_timeseries()):
        assert _channels(combined) == {selected}
        assert combined["Height"].tolist() == [2.0]
        assert combined["Q"].tolist() == [5.0]
    assert collection.align_eof_channel_ids() == 0


@pytest.mark.parametrize("keep_literal", [False, True], ids=["full-id-only", "explicit-short-literal"])
def test_ambiguous_eof_is_never_reassigned_after_references_are_filtered(tmp_path: Path, keep_literal: bool) -> None:
    selected = "Bridge 159.975"
    conflicting = "Bridge 159.979"
    excluded = "Bridge 202.231"
    literal = "Bridge 159.97"
    cmx = _process_cmx(tmp_path, [selected, conflicting, excluded])
    q = _process_q(tmp_path, [selected, conflicting, excluded])
    eof = _process_eof(tmp_path, [(literal, 2.0), ("Bridge 202.23", 20.0)])
    collection = _collection(cmx, q, eof)
    locations = [selected, literal] if keep_literal else [selected]

    collection.filter_locations(locations)

    assert _channels(cmx.df) == {selected}
    assert _channels(q.df) == {selected}
    assert _channels(eof.df) == ({literal} if keep_literal else set())
    assert any(processor is eof for processor in collection.processors) is keep_literal
    retained_eof = eof.df.copy()

    # Repeating selection or either combine must not resolve the now-absent
    # conflict by attaching the explicitly selected literal's geometry to A.
    collection.filter_locations(locations)
    assert collection.align_eof_channel_ids() == 0
    maximums = collection.combine_1d_maximums()
    timeseries = collection.combine_1d_timeseries()
    assert _channels(maximums) == ({selected, literal} if keep_literal else {selected})
    assert _channels(timeseries) == {selected}
    for combined in (maximums, timeseries):
        full_channel = combined.loc[combined["Chan ID"] == selected]
        assert full_channel["Q"].tolist() == [5.0]
        if keep_literal:
            assert full_channel["Height"].isna().all()
        else:
            assert "Height" not in full_channel.columns
    if keep_literal:
        literal_row = maximums.loc[maximums["Chan ID"] == literal]
        assert literal_row["Height"].tolist() == [2.0]
        assert literal_row["Q"].isna().all()
    pd.testing.assert_frame_equal(eof.df, retained_eof)
    pd.testing.assert_frame_equal(collection.combine_1d_maximums(), maximums)
    pd.testing.assert_frame_equal(collection.combine_1d_timeseries(), timeseries)
