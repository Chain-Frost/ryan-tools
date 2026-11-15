"""Synthetic format contracts and failure cases; no proprietary fixtures or engine execution."""

import ast
from decimal import Decimal
from pathlib import Path
from typing import cast

import pytest

from ryan_library.functions.RORB.associations import associate_batch_csvs, batch_event_key, compare_output_csv
from ryan_library.functions.RORB.read_rorb_files import parse_batch_output, read_hydrograph_csv
from ryan_library.functions.RORB.readers import read_rorb_bytes, read_rorb_file, read_rorb_table_bytes


def batch_text(row: str = "1 24 hour 20% 2 100 0.9 12") -> str:
    return (
        "RORBWin Batch Run Summary\nProgram version 6.45\n"
        "Parameters: kc = 10 m = 0.8\n"
        "Loss parameters Initial loss (mm) Cont. loss (mm/h)\n20 2\n"
        "Peak Description\n01 Calculated hydrograph: Outlet\n\n"
        " Run Duration AEP TPat Rain(mm) ARF Peak0001\n"
        + row
        + "\nRun, Representative hydrograph\n1 dur24hour_aep20tp2.out\n"
        "Elapsed Run Time (hh:mm:ss) = 00:00:01\n"
    )


def csv_text(name: str = " aep20_du24hourtp2", values: str = "0,1,0\n1,2,12\n2,3,0\n") -> str:
    return (
        "RORBWin CSV Output File\n"
        f"Refer output file for complete listing of inputs and outputs: {name}.out\n"
        "Inc,Time (hrs),Calculated hydrograph: Outlet\n" + values
    )


def output_text(rows: str = "0 0 0\n1 1 12\n2 2 0\n") -> str:
    return (
        "RORBWin Output File\nProgram version 6.45\n"
        "Time increment (hours)= 1\nDuration of calculations: 2\n"
        "Hydrograph summary\nSite Description\n01 Calculated hydrograph, Outlet\n\n"
        "Inc Time Hyd0001\n" + rows
    )


def cmd_text(loss: str = "1", bursts: str = "1") -> str:
    return (
        "# BEGIN\nCat file :model.catg\nStm file :storm.stm\nLumped kc:T\nVerbosity:3\n"
        f"Lossmodel:{loss}\nNum ISA  :1\nISA 1    :10,0.8\nNum burst:{bursts}\nISA 1    :20,2\n# END\n"
    )


def write(tmp_path: Path, content: str, name: str = "model_batch.out") -> Path:
    path = tmp_path / name
    path.write_text(content, encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "bad",
    [
        "1 BAD hour 20% 2 100 0.9 12",
        "1 24 bananas 20% 2 100 0.9 12",
        "1 24 hour 20% 2 100 0.9 12garbage",
        "1 24 hour 20% 2 100 0.9 NaN",
        "1 24 hour 20% 2 100 0.9 ********",
        "1 24 hour 20% 2 100",
        "1 24 hour 200% 2 100 0.9 12",
        "1 24 hour 20 2 100 0.9 12",
    ],
)
def test_bad_batch_rows_are_diagnosed_not_silently_dropped(tmp_path: Path, bad: str) -> None:
    path = write(tmp_path, batch_text("1 24 hour 20% 1 100 0.9 12\n" + bad))
    document = read_rorb_file(path)
    assert document.status == "invalid"
    assert not document.section("runs").valid
    assert any(d.code == "run_record" and d.line == 11 for d in document.diagnostics)
    with pytest.raises(ValueError):
        parse_batch_output(path)


def test_batch_retains_metadata_sites_and_frequency_semantics(tmp_path: Path) -> None:
    path = write(tmp_path, batch_text("1 90 minute 0.2EY 2 100 0.9 12"))
    document = read_rorb_file(path)
    row = document.section("runs").rows[0]
    assert row["Duration"] == 1.5
    assert row["aep_kind"] == "EY"
    assert row["aep_raw"] == "0.2EY"
    assert document.section("sites").rows[0]["name"] == "Outlet"
    assert document.section("representatives").rows[0]["reference"] == "dur24hour_aep20tp2.out"
    assert document.metadata["program_version"] == "6.45"
    assert all("ROC" not in r for r in document.section("parameters").rows)


def test_missing_parameters_are_null_not_defaults(tmp_path: Path) -> None:
    content = batch_text().replace("Parameters: kc = 10 m = 0.8\n", "")
    path = write(tmp_path, content)
    table = parse_batch_output(path)
    assert table.loc[0, "kc"] is None
    assert table.loc[0, "ROC"] is None
    assert table.loc[0, "csv"] is None


@pytest.mark.parametrize("mutation", ["truncate", "duplicate", "mapping", "header"])
def test_batch_boundaries_and_identity(tmp_path: Path, mutation: str) -> None:
    text = batch_text()
    if mutation == "truncate":
        text = text.split("Run, Representative")[0]
    elif mutation == "duplicate":
        text = text.replace("1 24 hour 20% 2 100 0.9 12", "1 24 hour 20% 2 100 0.9 12\n1 24 hour 20% 2 100 0.9 12")
    elif mutation == "mapping":
        text = text.replace("Peak0001", "Peak0002")
    else:
        text = text.replace("Run Duration AEP", "Run Duration ARI")
    assert read_rorb_file(write(tmp_path, text)).status == "invalid"


def test_unsigned_table_is_unsupported(tmp_path: Path) -> None:
    path = write(tmp_path, batch_text().replace("RORBWin Batch Run Summary\n", ""))
    assert read_rorb_file(path).status == "unsupported"


def test_association_unprefixed_and_ambiguous(tmp_path: Path) -> None:
    path = write(tmp_path, batch_text())
    csv_path = write(tmp_path, csv_text(), " aep20_du24hourtp2.csv")
    before = csv_path.read_bytes()
    result = associate_batch_csvs(read_rorb_file(path))
    assert result.valid
    assert result.rows[0]["path"] == str(csv_path.absolute())
    assert csv_path.read_bytes() == before
    write(tmp_path, csv_text("model_ aep20_du24hourtp2"), "model_ aep20_du24hourtp2.csv")
    result = associate_batch_csvs(read_rorb_file(path))
    assert not result.valid
    assert result.rows[0]["status"] == "ambiguous"
    assert result.rows[0]["path"] is None


@pytest.mark.parametrize(
    ("batch_peak", "csv_peak", "expected_status"),
    [
        ("55.3", "55.340", "corroborated"),
        ("55.3", "55.351", "mismatch"),
        ("55.3401", "55.340", "corroborated"),
        ("55.3401", "55.341", "mismatch"),
        ("5.53E1", "55.340", "corroborated"),
    ],
)
def test_association_peak_rounding_uses_reported_source_precision(
    tmp_path: Path, batch_peak: str, csv_peak: str, expected_status: str
) -> None:
    batch_path = write(tmp_path, batch_text(f"1 24 hour 20% 2 100 0.9 {batch_peak}"))
    write(tmp_path, csv_text(values=f"0,1,0\n1,2,{csv_peak}\n2,3,0\n"), " aep20_du24hourtp2.csv")

    result = associate_batch_csvs(read_rorb_file(batch_path))

    assert result.rows[0]["status"] == expected_status
    assert result.valid is (expected_status == "corroborated")


@pytest.mark.parametrize("content", [csv_text(values="0,1,0\n1,2,99\n"), csv_text("another_run"), "not a RORB file"])
def test_association_rejects_wrong_values_references_and_signatures(tmp_path: Path, content: str) -> None:
    path = write(tmp_path, batch_text())
    write(tmp_path, content, " aep20_du24hourtp2.csv")
    assert associate_batch_csvs(read_rorb_file(path)).rows[0]["status"] == "mismatch"


@pytest.mark.parametrize(
    "rows",
    [
        "0,0,0\n1,1,NaN\n",
        "0,0,0\n1,1,12junk\n",
        "0,0,0\n1,1\n",
        "0,0,0\n2,1,12\n",
        "0,1,0\n1,0,12\n",
        "0,0,0\n1,1,1e999\n",
    ],
)
def test_csv_rejects_bad_records(tmp_path: Path, rows: str) -> None:
    path = write(tmp_path, csv_text(values=rows), "hydro.csv")
    assert read_rorb_file(path).status == "invalid"
    with pytest.raises(ValueError):
        read_hydrograph_csv(path)


def test_csv_preserves_increment_and_time_without_certifying_completion(tmp_path: Path) -> None:
    path = write(tmp_path, csv_text(), "hydro.csv")
    doc = read_rorb_file(path)
    assert doc.status == "partial"
    assert doc.section("hydrographs").rows[0]["Time (hrs)"] == 1
    assert doc.section("hydrographs").rows[0]["Inc"] == 0
    assert "Inc" not in read_hydrograph_csv(path).columns


def test_csv_signature_alone_is_insufficient(tmp_path: Path) -> None:
    text = "RORBWin CSV Output File\nanything\nfoo,bar\n1,2\n"
    assert read_rorb_file(write(tmp_path, text)).status == "invalid"


@pytest.mark.parametrize("rows", ["0 0 0\n1 1 12\n", "0 0 0\n1 1 ********\n2 2 0\n", "0 0 0\n1 1 12\n3 3 0\n"])
def test_output_rejects_truncated_and_malformed_summaries(tmp_path: Path, rows: str) -> None:
    doc = read_rorb_file(write(tmp_path, output_text(rows), "run.out"))
    assert doc.status == "invalid"
    assert not doc.section("hydrographs_1").valid


@pytest.mark.parametrize(
    "rows",
    [
        "0 0 0\n1 100 12\n2 101 0\n",
        "0 0.000 0\n1 0.320 12\n2 0.667 0\n",
    ],
)
def test_output_rejects_time_spacing_inconsistent_with_report(tmp_path: Path, rows: str) -> None:
    text = output_text(rows)
    if "0.320" in rows:
        text = text.replace("Time increment (hours)= 1", "Time increment (hours)= 0.333333")
    document = read_rorb_file(write(tmp_path, text, "run.out"))
    section = document.section("hydrographs_1")

    assert document.status == "invalid"
    assert not section.valid
    assert section.completeness == "reported_increment_count"
    assert any(
        diagnostic.code == "time_increment_mismatch" and diagnostic.line == section.rows[1]["line"]
        for diagnostic in document.diagnostics
    )


@pytest.mark.parametrize("reported_increment", ["0.333333", "0.333", "3.33D-1"])
def test_output_accepts_rounding_to_reported_time_precision(tmp_path: Path, reported_increment: str) -> None:
    rows = "0 0.000 0\n1 0.333 12\n2 0.667 0\n"
    text = output_text(rows).replace("Time increment (hours)= 1", f"Time increment (hours)= {reported_increment}")
    document = read_rorb_file(write(tmp_path, text, "run.out"))
    section = document.section("hydrographs_1")

    assert document.status == "partial"
    assert section.valid
    assert section.completeness == "reported_increment_count"
    assert [row["Time"] for row in section.rows] == [0.0, 0.333, 0.667]
    assert not any(diagnostic.code == "time_increment_mismatch" for diagnostic in document.diagnostics)


def test_output_validates_increment_reported_after_summary(tmp_path: Path) -> None:
    text = output_text("0 0 0\n1 100 12\n2 101 0\n")
    text = text.replace("Time increment (hours)= 1\n", "") + "\nTime increment (hours)= 1\n"
    document = read_rorb_file(write(tmp_path, text, "run.out"))
    assert document.status == "invalid"
    assert any(diagnostic.code == "time_increment_mismatch" for diagnostic in document.diagnostics)


def test_output_csv_time_discrepancy_is_not_repaired(tmp_path: Path) -> None:
    out_path = write(tmp_path, output_text(), "run.out")
    csv_path = write(tmp_path, csv_text("run"), "run.csv")
    result = compare_output_csv(read_rorb_file(out_path), read_rorb_file(csv_path))
    assert result["flows_match"] is True
    assert result["time_matches"] is False
    assert result["csv_minus_output_hours"] == 1
    assert result["status"] == "mismatch"


@pytest.mark.parametrize(("model", "key"), [("1", "CL"), ("2", "ROC")])
def test_cmd_documented_subset_is_not_engine_acceptance(tmp_path: Path, model: str, key: str) -> None:
    doc = read_rorb_file(write(tmp_path, cmd_text(model), "model.par"))
    assert doc.status == "ok"
    assert doc.section("losses").rows[0][key] == 2
    assert doc.metadata["engine_compatibility"] == "not_checked"
    assert any(d.code == "engine_capabilities_unverified" for d in doc.diagnostics)


@pytest.mark.parametrize(
    "text", [cmd_text("3"), cmd_text(bursts="2"), cmd_text().replace("# END", "Extra     :5\n# END")]
)
def test_cmd_variants_are_partial_not_silently_accepted(tmp_path: Path, text: str) -> None:
    doc = read_rorb_file(write(tmp_path, text, "model.par"))
    assert doc.status == "partial"
    assert any(d.code == "cmd_variant" for d in doc.diagnostics)


@pytest.mark.parametrize(
    "text",
    [
        cmd_text().replace("# END\n", ""),
        cmd_text().replace("10,0.8", "NaN,0.8"),
        cmd_text().replace("Lumped kc:T", "Lumped kc:X"),
    ],
)
def test_cmd_malformed_records_fail(tmp_path: Path, text: str) -> None:
    assert read_rorb_file(write(tmp_path, text, "model.par")).status == "invalid"


def test_saved_settings_candidates_remain_explicitly_unverified() -> None:
    path = Path(__file__).parents[2] / "test_data/RORB/example-par-file.par"
    doc = read_rorb_file(path)
    assert doc.kind == "saved_settings"
    assert doc.status == "partial"
    assert doc.section("parameter_candidates").rows[0]["active"] is None
    assert any("Do not use" in d.message for d in doc.diagnostics)


def test_encoding_limit_and_no_mutation(tmp_path: Path) -> None:
    path = tmp_path / "model.out"
    raw = (
        batch_text()
        .replace("Program version 6.45", "Program version 6.45\nRainfall location: km\u00b2")
        .encode("cp1252")
    )
    path.write_bytes(raw)
    doc = read_rorb_file(path)
    assert doc.encoding == "cp1252"
    assert any(d.code == "encoding_fallback" for d in doc.diagnostics)
    assert path.read_bytes() == raw
    with pytest.raises(ValueError, match="limit"):
        read_rorb_file(path, max_bytes=10)
    with pytest.raises(UnicodeDecodeError):
        read_rorb_file(path, encoding="utf-8")


def test_linked_path_is_preserved(tmp_path: Path) -> None:
    original = write(tmp_path, batch_text())
    link = tmp_path / "linked_batch.out"
    try:
        link.symlink_to(original)
    except OSError:
        pytest.skip("Creating symlinks requires Windows permission")
    document = read_rorb_file(link)
    assert document.path == link.absolute()
    assert document.sha256 == read_rorb_file(original).sha256


def test_distributed_cmd_routing_and_losses_keep_isa_identity(tmp_path: Path) -> None:
    text = cmd_text().replace("Lumped kc:T", "Lumped kc:F").replace("Num ISA  :1", "Num ISA  :2")
    text = text.replace("ISA 1    :10,0.8", "ISA 1    :10,0.8\nISA 2    :15,0.9")
    text = text.replace("ISA 1    :20,2", "ISA 1    :20,2\nISA 2    :30,3")
    doc = read_rorb_file(write(tmp_path, text, "model.par"))
    assert doc.status == "ok"
    assert [r["isa"] for r in doc.section("routing").rows] == [1, 2]
    assert [r["kc"] for r in doc.section("routing").rows] == [10, 15]
    assert [r["CL"] for r in doc.section("losses").rows] == [2, 3]


def test_unknown_saved_settings_layout_is_not_guessed(tmp_path: Path) -> None:
    source = Path(__file__).parents[2] / "test_data/RORB/example-par-file.par"
    text = source.read_text().replace("EnsARR16", "AnotherMode")
    document = read_rorb_file(write(tmp_path, text, "another.par"))
    assert document.status == "unsupported"
    assert not document.sections


def test_one_based_cmd_report_has_exact_origin_tokens_and_bytes(tmp_path: Path) -> None:
    """An original CMD-style 1..N+1 report is complete without a clock correction."""
    raw = (
        output_text("1 0.50 0.000\n2 1.00 12.345000\n3 1.50 0.000\n")
        .replace("Program version 6.45", "Program version 6.44\nCopyright \u00a9 test")
        .replace("Time increment (hours)= 1", "Time increment (hours)= 0.50")
        .replace("\n", "\r\n")
        .encode("cp1252")
    )
    path = tmp_path / "native.out"
    path.write_bytes(raw)
    by_file = read_rorb_file(path)
    by_bytes = read_rorb_bytes(raw, path=path)

    assert by_file.original_bytes == by_bytes.original_bytes == raw
    assert by_file.size_bytes == len(raw)
    assert by_file.sha256 == by_bytes.sha256
    assert by_file.lines == by_bytes.lines
    assert by_file.encoding == "cp1252"
    assert any(d.code == "encoding_fallback" for d in by_file.diagnostics)
    section = by_file.section("hydrographs_1")
    assert by_file.status == "partial"
    assert section.valid
    assert section.completeness == "reported_increment_count"
    assert section.increment_origin == 1
    assert [r["Inc"] for r in section.rows] == [1, 2, 3]
    assert [r["Time"] for r in section.rows] == [0.5, 1.0, 1.5]
    assert section.source_tokens[1]["Hyd0001"] == "12.345000"
    assert section.source_decimal(1, "Hyd0001") == Decimal("12.345000")
    assert section.source_decimal(0, "Time") == Decimal("0.50")
    assert by_file.metadata["time_increment_raw"] == "0.50"
    assert by_file.metadata["reported_duration_increments"] == 2
    assert by_file.metadata["engine_identity"] == "unverified_from_report_banner"


def test_zero_based_report_origin_is_unchanged(tmp_path: Path) -> None:
    document = read_rorb_file(write(tmp_path, output_text(), "zero.out"))
    section = document.section("hydrographs_1")
    assert document.status == "partial"
    assert section.increment_origin == 0
    assert [row["Inc"] for row in section.rows] == [0, 1, 2]
    assert section.source_decimal(1, "Time") == Decimal(1)


def test_report_rejects_uncharacterised_increment_origin(tmp_path: Path) -> None:
    document = read_rorb_file(write(tmp_path, output_text("2 1 0\n3 2 12\n4 3 0\n"), "bad.out"))
    assert document.status == "invalid"
    assert document.section("hydrographs_1").increment_origin == 2
    assert any(d.code == "increment_origin" for d in document.diagnostics)


def test_batch_and_csv_preserve_numeric_tokens_including_exponents() -> None:
    batch = read_rorb_bytes(batch_text("1 90 minute 0.2EY 2 100 0.9 5.53D1").encode())
    row = batch.section("runs")
    assert batch.status == "partial"
    assert row.source_tokens[0]["AEP"] == "0.2EY"
    assert row.source_decimal(0, "Peak0001") == Decimal("55.3")
    assert row.source_tokens[0]["Duration"] == "90"
    hydro = read_rorb_bytes(csv_text(values="0,1.000,0.000\n1,2.000,5.53D1\n2,3.000,0\n").encode())
    assert hydro.status == "partial"
    assert hydro.section("hydrographs").source_tokens[1]["Calculated hydrograph: Outlet"] == "5.53D1"
    assert hydro.section("hydrographs").source_decimal(1, "Calculated hydrograph: Outlet") == Decimal("55.3")


def test_bytes_reader_rejects_over_limit_and_invalid_numeric_tokens() -> None:
    with pytest.raises(ValueError, match="exceeds"):
        read_rorb_bytes(b"1234", max_bytes=3)
    document = read_rorb_bytes(output_text("0 0 0\n1 1 12junk\n2 2 0\n").encode())
    assert document.status == "invalid"
    assert any(d.code == "output_record" for d in document.diagnostics)


def test_content_recognition_and_scalar_tokens_without_report_banner() -> None:
    source = (
        "Time increment (hours)= 0.50\n"
        "Duration of calculations: 1\n"
        "Peak discharge,m³/s 1.000\n"
        "Time to peak,h 0.50\n"
        "Volume,m³ 1.800D+3\n"
        "Hydrograph summary\nSite Description\n01 Synthetic outlet\n"
        "Inc Time Hyd001\n1 0.50 0.000\n2 1.00 1.000\n"
        "End of synthetic hydrograph\n"
    )
    document = read_rorb_bytes(source.encode("utf-8"))
    assert document.kind == "standard_output"
    assert document.status == "partial"
    assert any(item.code == "missing_report_banner" for item in document.diagnostics)
    hydro = document.section("hydrographs_1")
    assert hydro.valid
    assert hydro.increment_origin == 1
    assert hydro.completeness == "reported_increment_count"
    assert [r["Inc"] for r in hydro.rows] == [1, 2]
    scalars = document.section("reported_scalars")
    assert [r["family"] for r in scalars.rows] == ["Peak discharge", "Time to peak", "Volume"]
    assert [r["unit"] for r in scalars.rows] == ["m³/s", "h", "m³"]
    assert [r["ordinal"] for r in scalars.rows] == [1, 1, 1]
    assert [r["token"] for r in scalars.rows] == ["1.000", "0.50", "1.800D+3"]
    assert scalars.source_decimal(2, "value") == Decimal(1800)
    assert document.section("sites").rows[0]["description"] == "Synthetic outlet"


def test_incomplete_print_site_description_is_not_invalid_numeric_data(tmp_path: Path) -> None:
    contents = output_text("0 0 0 0\n1 1 12 10\n2 2 0 0\n").replace("Inc Time Hyd0001", "Inc Time Hyd0001 Hyd0002")
    document = read_rorb_file(write(tmp_path, contents, "partial-site.out"))
    assert document.status == "partial"
    assert document.section("hydrographs_1").valid
    assert any(d.code == "site_mapping" and d.severity == "warning" for d in document.diagnostics)


def test_invalid_scalar_token_is_retained_in_canonical_provenance() -> None:
    source = (
        "RORBWin Output File\nProgram version 6.44\n"
        "Time increment (hours)= 0.5\nDuration of calculations: 1\n"
        "Peak discharge,m³/s NaN\n"
        "Inc Time Hyd001\n1 0.50 0.000\n2 1.00 1.000\nEnd\n"
    )
    document = read_rorb_bytes(source.encode())
    assert document.status == "invalid"
    scalar = document.section("reported_scalars")
    assert not scalar.valid
    assert scalar.rows[0]["token"] == source.splitlines()[4].split()[-1]
    assert scalar.source_tokens[0]["value"] == "NaN"
    assert any(d.code == "reported_scalar_value" and d.severity == "error" for d in document.diagnostics)
    hydro = document.section("hydrographs_1")
    assert hydro.valid
    assert hydro.completeness == "reported_increment_count"
    assert [row["Inc"] for row in hydro.rows] == [1, 2]


def test_explicit_bare_table_api_retains_provenance_and_same_numeric_grammar() -> None:
    raw = b"Inc Time Hyd001 Hyd002\r\n1 0 0 0\r\n2 1 3.50000 0\r\nEnd\r\n"
    default = read_rorb_bytes(raw)
    assert default.kind == "unsupported"
    assert default.status == "unsupported"
    document = read_rorb_table_bytes(raw)
    assert document.kind == "isolated_hydrograph_table"
    assert document.status == "partial"
    assert document.original_bytes == raw
    assert any(d.code == "unverified_table_source" for d in document.diagnostics)
    assert not any(d.code == "unsupported_format" for d in document.diagnostics)
    table = document.section("hydrographs_1")
    assert table.valid
    assert table.increment_origin == 1
    assert table.completeness == "terminated"
    assert table.columns == ["line", "Inc", "Time", "Hyd001", "Hyd002"]
    assert table.source_tokens[1]["Hyd001"] == "3.50000"
    assert table.source_decimal(1, "Hyd001") == Decimal("3.50000")


def test_explicit_bare_table_rejects_incomplete_and_invalid_rows() -> None:
    truncated = read_rorb_table_bytes(b"Inc Time Hyd001 Hyd002\n1 0 1 2\n2 1 3\nEnd\n")
    assert truncated.status == "invalid"
    assert not truncated.section("hydrographs_1").valid
    assert any(d.code == "output_record" for d in truncated.diagnostics)
    malformed = read_rorb_table_bytes(b"Inc Time Hyd001\n1 0 1\n2 1 NaN\nEnd\n")
    assert malformed.status == "invalid"
    assert any(d.code == "output_record" for d in malformed.diagnostics)
    rejected = read_rorb_table_bytes(b"not a characterised report\n")
    assert rejected.status == "unsupported"


def test_explicit_companion_overrides_resolve_only_selected_event(tmp_path: Path) -> None:
    """A repeated Run requires original frequency/duration and TP identity."""
    batch_path = write(tmp_path, batch_text("1 24 hour 20% 2 100 0.9 12\n1 24 hour 20% 3 100 0.9 12"))
    first = write(tmp_path, csv_text("custom_event"), "custom_event.csv")
    second = write(tmp_path, csv_text("other_event"), "other_event.csv")
    identity = (1, "24", "hour", "20%", 2)
    document = read_rorb_file(batch_path)
    assert batch_event_key(document.section("runs").rows[0]) == identity
    assert batch_event_key(document.section("runs").rows[1]) == (1, "24", "hour", "20%", 3)
    outcome = associate_batch_csvs(document, overrides={identity: first})
    assert outcome.rows[0]["status"] == "corroborated"
    assert outcome.rows[0]["path"] == str(first.absolute())
    assert "Explicit override" in str(outcome.rows[0]["reason"])
    assert outcome.rows[1]["status"] == "missing"
    assert outcome.rows[1]["path"] is None
    assert second.is_file()
    with pytest.raises(ValueError, match="unknown run/duration"):
        associate_batch_csvs(document, overrides={(99, "24", "hour", "20%", 2): first})
    # Python callers may bypass annotations; the importer still rejects that input.
    with pytest.raises(ValueError, match=r"pathlib\.Path"):
        associate_batch_csvs(document, overrides={identity: cast(Path, "custom_event.csv")})


def test_explicit_override_is_fail_closed_and_can_break_ambiguity(tmp_path: Path) -> None:
    batch_path = write(tmp_path, batch_text())
    original = write(tmp_path, csv_text(), " aep20_du24hourtp2.csv")
    prefixed = write(tmp_path, csv_text("model_ aep20_du24hourtp2"), "model_ aep20_du24hourtp2.csv")
    identity = (1, "24", "hour", "20%", 2)
    assert associate_batch_csvs(read_rorb_file(batch_path)).rows[0]["status"] == "ambiguous"
    selected = associate_batch_csvs(read_rorb_file(batch_path), overrides={identity: prefixed})
    assert selected.valid
    assert selected.rows[0]["path"] == str(prefixed.absolute())
    missing = associate_batch_csvs(read_rorb_file(batch_path), overrides={identity: tmp_path / "absent.csv"})
    assert missing.rows[0]["status"] == "missing"
    assert missing.rows[0]["path"] is None
    mismatched = write(tmp_path, csv_text("wrong_name"), "mismatched.csv")
    failed = associate_batch_csvs(read_rorb_file(batch_path), overrides={identity: mismatched})
    assert failed.rows[0]["status"] == "mismatch"
    assert failed.rows[0]["path"] is None
    assert original.is_file()


def test_minimal_reader_sources_remain_python_313_syntax() -> None:
    """The 3.14 whole-repository formatter must not break the >=3.13 reader wheel."""
    source_root = Path(__file__).parents[3] / "ryan_library" / "functions" / "RORB"
    modules = (
        "reader_types.py",
        "readers.py",
        "output_reader.py",
        "result_readers.py",
        "parameter_reader.py",
        "associations.py",
    )
    for name in modules:
        source = source_root / name
        ast.parse(source.read_text(encoding="utf-8"), filename=str(source), feature_version=(3, 13))
