"""Synthetic tests for optional, vertex-based XMDF control diagnostics."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import cast

import fiona
import numpy as np
import pandas as pd
import pytest

from ryan_library.functions.tuflow.xmdf_hotspots import (
    resolve_xmdf_types,
    selected_xmdf_times,
    validate_xmdf_surface,
)
from ryan_library.orchestrators.tuflow.xmdf_hotspots import (
    XmdfHotspotConfig,
    run_xmdf_hotspot_workflow,
)


class FakeXMDF:
    def __init__(
        self, *, dt: bool = False, misalign: bool = False, shift_after_first: bool = False
    ) -> None:
        self.with_dt = dt
        self.misalign = misalign
        self.shift_after_first = shift_after_first
        self.names = ("Courant Number", "Shallow Wave Celerity Number", "Diffusion Number", "Depth")
        if dt:
            self.names = (*self.names, "dt")

    def data_types(self, filter_by: str | None = None) -> list[str]:  # noqa: ARG002 - Protocol keyword must match.
        return list(self.names)

    def times(self, filter_by: str | None = None) -> list[float]:  # noqa: ARG002 - Protocol keyword must match.
        return [0.0, 0.0833333333333333, 0.16666666666666666]

    def surface(self, data_type: str, time: float) -> pd.DataFrame:
        high = np.float32(0.9 if time < 0.1 else 0.95)
        values: dict[str, list[float]] = {
            "Courant Number": [float(high), 0.15, 0.99],
            "Shallow Wave Celerity Number": [0.05, 0.92, 0.1],
            "Diffusion Number": [0.05, 0.28, 0.05],
            "Depth": [0.2, 3.1, 7.2],
            "dt": [2.0, 3.0, 0.1],
        }
        x = [100.0, 110.0, 120.0]
        if self.shift_after_first and time > 0:
            x[1] = 111.0
        if self.misalign and data_type == "Shallow Wave Celerity Number":
            x[1] = 111.0
        return pd.DataFrame(
            {
                "x": x,
                "y": [200.0, 200.0, 200.0],
                "value": values[data_type],
                # Node 2 deliberately inactive despite large values.
                "active": [True, True, False],
            }
        )


def fake_reader(_path: Path, _mesh: Path | None) -> FakeXMDF:
    return FakeXMDF()


def fake_reader_with_dt(_path: Path, _mesh: Path | None) -> FakeXMDF:
    return FakeXMDF(dt=True)


def fake_reader_misaligned(_path: Path, _mesh: Path | None) -> FakeXMDF:
    return FakeXMDF(misalign=True)


def fake_reader_shifted(_path: Path, _mesh: Path | None) -> FakeXMDF:
    return FakeXMDF(shift_after_first=True)


def make_file(tmp_path: Path) -> Path:
    source = tmp_path / "synthetic.xmdf"
    source.touch()
    return source


def properties(path: Path, layer: str) -> list[dict[str, object]]:
    with fiona.open(path, layer=layer) as source:
        return [dict(feature["properties"]) for feature in source]


def test_dataset_names_are_explicit_and_unambiguous() -> None:
    names = resolve_xmdf_types(["Courant Number", "Shallow Wave Celerity Number", "Diffusion Number"])
    assert names == {
        "Nu": "Courant Number",
        "Nc": "Shallow Wave Celerity Number",
        "Nd": "Diffusion Number",
    }
    with pytest.raises(ValueError, match="Required XMDF dataset"):
        resolve_xmdf_types(["Nc", "Nd"])
    with pytest.raises(ValueError, match="Ambiguous"):
        resolve_xmdf_types(["Nu", "Courant Number", "Nc", "Nd"])


def test_selected_times_preserve_subhourly_outputs() -> None:
    reader = FakeXMDF()
    names = resolve_xmdf_types(reader.data_types())
    times = selected_xmdf_times(reader, names, start_time_hr=0, end_time_hr=0.17, every=1)
    assert len(times) == 3
    assert times[1] == pytest.approx(1 / 12)


def test_missing_dt_is_not_fabricated(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.orchestrators.tuflow import xmdf_hotspots as workflow

    monkeypatch.setattr(workflow, "_reader", fake_reader)
    source = make_file(tmp_path)
    destination = tmp_path / "xmdf.gpkg"
    report = run_xmdf_hotspot_workflow(XmdfHotspotConfig((source,), destination, "EPSG:7851", max_points_per_type=100))
    assert report.dt_sources == 0
    assert report.dt_features == 0
    assert report.snapshots == 3
    assert report.control_features > 0
    assert len(properties(destination, "xmdf_dt_hotspots")) == 0
    details = properties(destination, "xmdf_metadata")
    assert details[0]["dt_available"] == 0
    assert details[0]["spatial_support"] == "mesh_vertices"
    controls = properties(destination, "xmdf_control_hotspots")
    assert all(row["dt_at_node"] is None for row in controls)
    assert all(row["node_index"] != 2 for row in controls)
    assert any(row["parameter"] == "Nu" and row["value"] == pytest.approx(float(np.float32(0.9))) for row in controls)
    assert {row["time_hr"] for row in controls} == set(FakeXMDF().times())


def test_optional_dt_reports_actual_mesh_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.orchestrators.tuflow import xmdf_hotspots as workflow

    monkeypatch.setattr(workflow, "_reader", fake_reader_with_dt)
    source = make_file(tmp_path)
    output = tmp_path / "dt.gpkg"
    report = run_xmdf_hotspot_workflow(XmdfHotspotConfig((source,), output, "EPSG:7851", start_time_hr=0.1))
    assert report.dt_sources == 1
    assert report.snapshots == 1
    dt = properties(output, "xmdf_dt_hotspots")
    assert {row["node_index"] for row in dt} == {0, 1}
    assert {row["dt_snapshot_s"] for row in dt} == {2.0, 3.0}
    assert all(row["nu_at_node"] is not None for row in dt)
    control = properties(output, "xmdf_control_hotspots")
    assert any(row["dt_at_node"] == 2.0 for row in control)


def test_no_fake_area_or_raster_connectivity(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.orchestrators.tuflow import xmdf_hotspots as workflow

    monkeypatch.setattr(workflow, "_reader", fake_reader)
    source = make_file(tmp_path)
    output = tmp_path / "nodal.gpkg"
    run_xmdf_hotspot_workflow(XmdfHotspotConfig((source,), output, "EPSG:7851"))
    with fiona.open(output, layer="xmdf_control_hotspots") as result:
        properties = cast("Mapping[str, str]", result.schema["properties"])
        columns = set(properties)
    assert "area_ha" not in columns
    assert "component" not in columns
    assert "node_index" in columns


def test_dry_run_checks_metadata_without_writing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.orchestrators.tuflow import xmdf_hotspots as workflow

    monkeypatch.setattr(workflow, "_reader", fake_reader)
    source = make_file(tmp_path)
    output = tmp_path / "existing.gpkg"
    output.write_bytes(b"original")
    report = run_xmdf_hotspot_workflow(XmdfHotspotConfig((source,), output, "EPSG:7851", dry_run=True))
    assert report.dry_run
    assert output.read_bytes() == b"original"


def test_misaligned_surface_fails_without_overwriting(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.orchestrators.tuflow import xmdf_hotspots as workflow

    monkeypatch.setattr(workflow, "_reader", fake_reader_misaligned)
    source = make_file(tmp_path)
    output = tmp_path / "existing.gpkg"
    output.write_bytes(b"original")
    with pytest.raises(ValueError, match="node coordinates/order"):
        run_xmdf_hotspot_workflow(XmdfHotspotConfig((source,), output, "EPSG:7851", overwrite=True))
    assert output.read_bytes() == b"original"


def test_point_cap_retains_total_and_worst_values(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ryan_library.orchestrators.tuflow import xmdf_hotspots as workflow

    monkeypatch.setattr(workflow, "_reader", fake_reader)
    source = make_file(tmp_path)
    output = tmp_path / "capped.gpkg"
    run_xmdf_hotspot_workflow(XmdfHotspotConfig((source,), output, "EPSG:7851", max_points_per_type=1, end_time_hr=0))
    nu = [row for row in properties(output, "xmdf_control_hotspots") if row["parameter"] == "Nu"]
    assert len(nu) == 1
    assert nu[0]["near_nodes_total"] == 1
    assert properties(output, "xmdf_metadata")[0]["max_points_per_type"] == 1


def test_validation_rejects_unknown_crs(tmp_path: Path) -> None:
    source = make_file(tmp_path)
    with pytest.raises(ValueError, match="Invalid XMDF CRS"):
        run_xmdf_hotspot_workflow(XmdfHotspotConfig((source,), tmp_path / "out.gpkg", "NOT_A_CRS"))


def test_surface_validation_rejects_missing_active() -> None:
    frame = pd.DataFrame({"x": [1.0], "y": [2.0], "value": [0.99]})
    with pytest.raises(ValueError, match="missing"):
        validate_xmdf_surface(frame, parameter="Nu", reference=None)

def test_node_identity_must_remain_stable_across_output_times(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ryan_library.orchestrators.tuflow import xmdf_hotspots as workflow

    monkeypatch.setattr(workflow, "_reader", fake_reader_shifted)
    source = make_file(tmp_path)
    output = tmp_path / "existing.gpkg"
    output.write_bytes(b"previous analysis")
    with pytest.raises(ValueError, match="changed between output timestamps"):
        run_xmdf_hotspot_workflow(XmdfHotspotConfig((source,), output, "EPSG:7851", overwrite=True))
    assert output.read_bytes() == b"previous analysis"
