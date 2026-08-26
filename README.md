# ryan-tools

`ryan-tools` is a collection of Python utilities for TUFLOW, RORB, 12D, GDAL, QGIS and general data-processing
workflows. Reusable code lives in `ryan_library`; files under `ryan-scripts` are human-facing wrappers and standalone
utilities.

The distribution is named `ryan_functions`, while maintained imports normally use `ryan_library`.

## Project status

The repository targets Python 3.14. Maintained library code, orchestrators and unversioned wrappers follow the current
repository standards; older standalone, versioned and compatibility files remain where they still support migration or
narrow workflows.

Start with:

- [Development guide](docs/DEVELOPMENT_GUIDE.md) for architecture, code categories, lifecycle terms and validation.
- [Environment guide](docs/ENVIRONMENTS.md) for Python, VS Code, installed-wheel and QGIS/OSGeo4W setup.
- [Ryan Scripts guide](ryan-scripts/README.md) for choosing and safely running scripts.
- [Maintained wrapper standard](ryan-scripts/WRAPPER_STANDARD.md) when changing a library-backed wrapper.
- [Project documentation index](docs/README.md) for the complete topic map, including specialised guides kept beside
  scripts, processors, examples and maintenance tools.
- [Examples](examples/README.md) for direct library use when an existing wrapper is not the right fit.

## Repository map

```text
ryan-tools/
|-- ryan_library/              # Maintained Python package
|   |-- classes/               # Configuration, metadata and filename parsing
|   |-- functions/             # Reusable algorithms and focused I/O helpers
|   |-- orchestrators/         # Complete workflow controllers
|   |-- processors/            # Stateful processors for supported result formats
|   `-- scripts/               # Deprecated import-compatibility wrappers
|-- ryan-scripts/              # Human-facing wrappers and standalone utilities
|-- docs/                      # Development guidance, plans and setup documentation
|-- examples/                  # Example notebooks and supporting demonstrations
|-- repo-scripts/              # Build, environment and repository maintenance tools
|-- tests/                     # Unit, integration and regression tests
|   `-- test_data/             # Required synthetic test-data submodule
|-- vendor/
|   `-- run_hy8/               # HY-8 submodule
|-- excel-resources/           # Excel workbook resources submodule
|-- qgis-resources/            # QGIS resources submodule
|-- unsorted/                  # Separate holding-area submodule
|-- pyproject.toml             # Package metadata and tool configuration
`-- setup.py                   # Setuptools hook that stages QGIS styles into wheels
```

## Set up the repository

This repository uses Git submodules and Git LFS for workbook resources. Install Git LFS, then clone recursively:

```powershell
git lfs install
git clone --recurse-submodules https://github.com/Chain-Frost/ryan-tools.git
cd ryan-tools
git lfs pull
git -C qgis-resources lfs pull
git -C excel-resources lfs pull
```

For an existing clone:

```powershell
git submodule update --init --recursive
git lfs pull
git -C qgis-resources lfs pull
git -C excel-resources lfs pull
```

The test suite requires `tests/test_data`; it does not download or substitute those fixtures automatically.

Install the repository requirements into the user's normal Python 3.14 installation. `ryan-tools` does not require or
assume that users know how to create or activate a virtual environment:

```powershell
py -3.14 -m pip install --upgrade pip
py -3.14 repo-scripts\install_latest_wheel.py --dependencies-only
py -3.14 -m pip install -r requirements.txt
```

`requirements.txt` installs the checkout and development tools into that Python installation. Installing the project is
important when running wrappers copied outside the repository because they import the shared implementation from the
installed `ryan_functions` distribution. The dependency bootstrap installs binary Fiona, Rasterio and GDAL packages
from the configured geospatial wheel index, avoiding a local source build on Windows.

## Build and install

After changing `ryan_library` or package metadata, rebuild from the repository root:

```powershell
python repo-scripts/build_library.py
```

The build script updates the version in `pyproject.toml` and creates the wheel under `dist/`. Use `--skip-pip` when the
build dependency is already installed, or `--skip-artifacts` in an environment that cannot create or retain wheel
artifacts. Wheel builds require the QGIS resource submodule because `setup.py` stages the pinned TUFLOW QML styles into
the package.

Windows convenience entry points are:

```powershell
.\package_and_install.bat
.\install-latest-wheel.bat
```

The first builds and installs the package; the second installs the newest existing wheel.

## Test changes

Use focused pytest commands for a bounded change. Keep temporary files under the repository on this Windows checkout:

```powershell
python -m pytest tests\path\to\test_file.py --basetemp=.pytest_cache\basetemp
```

Run the complete suite through the repository runner:

```powershell
cmd.exe /C repo-scripts\run_tests.bat
```

The runner configures the source and bundled HY-8 import paths, uses a repository-local base temporary directory and
generates terminal, HTML and XML coverage reports. See the [development guide](docs/DEVELOPMENT_GUIDE.md#validation-by-change-type)
for proportional validation expectations.

## Choose a workflow

| Need | Start here |
| --- | --- |
| Run or adapt a human-facing script | [`ryan-scripts/README.md`](ryan-scripts/README.md) |
| Process TUFLOW results with maintained wrappers | [`ryan-scripts/TUFLOW-python/README.md`](ryan-scripts/TUFLOW-python/README.md) |
| Extend the TUFLOW processor framework | [`ryan_library/processors/tuflow/README.md`](ryan_library/processors/tuflow/README.md) |
| Use reusable Python APIs directly | [`examples/README.md`](examples/README.md) |
| Run maintained GDAL workflows | [`ryan-scripts/gdal-python/README.md`](ryan-scripts/gdal-python/README.md) |
| Convert geospatial, point-cloud, CAD or model files | [Supported file converters and formats](#supported-file-converters-and-formats) |
| Map a remaining legacy GDAL BAT file to its replacement | [`ryan-scripts/gdal-bat/README.md`](ryan-scripts/gdal-bat/README.md) |
| Connect the repository MCP server | [`docs/MCP_SETUP.md`](docs/MCP_SETUP.md) |

### Parse a TUFLOW result filename

`TuflowStringParser` extracts the configured data type and run metadata from a result path:

```python
from pathlib import Path

from ryan_library.classes.tuflow_string_classes import TuflowStringParser

parser = TuflowStringParser(Path("M11_01p_00120m_TP01_1d_Q.csv"))

print(parser.data_type)
print(parser.raw_run_code)
print(parser.clean_run_code)
print(parser.aep)
print(parser.duration)
print(parser.tp)
```

The authoritative suffix registry is
[`ryan_library/classes/tuflow_results_validation_and_datatypes.json`](ryan_library/classes/tuflow_results_validation_and_datatypes.json).
It contains both processor-backed tabular types and raster classifications used for discovery.

### Load TUFLOW data for exploration

`load_tuflow_data()` handles discovery, logging, serial or parallel processing and optional location filtering:

```python
from ryan_library.functions.tuflow.notebook_helpers import load_tuflow_data

collection = load_tuflow_data(
    paths=["results"],
    data_types=["Q", "V", "H", "Nmx", "Cmx", "Chan", "EOF"],
    parallel=True,
    locations=["Culvert_01"],
)

timeseries = collection.combine_1d_timeseries()
maximums = collection.combine_1d_maximums()
```

Processor-backed types currently include `POMM`, `PO`, `Cmx`, `Nmx`, `Chan`, `ccA`, `RLL_Qmx`, `Q`, `H`, `CF`,
`V`, `EOF` and `TLF`. Use the processor development notes for combination behavior, caching and extension guidance.

### Run a ready-made TUFLOW workflow

Orchestrators are callable workflow controllers; wrappers add editable defaults, CLI handling, banners, exit codes and
optional pauses:

```python
from pathlib import Path

from ryan_library.orchestrators.tuflow.tuflow_culverts_merge import main_processing

main_processing(
    paths_to_process=[Path("results")],
    include_data_types=["Nmx", "Cmx", "Chan", "ccA", "RLL_Qmx", "EOF"],
    locations_to_include=["Culvert_01"],
    output_dir=Path("outputs"),
    export_mode="both",
)
```

Representative orchestrators cover culvert maximums and time series, PO/POMM combination, closure durations, peak and
stability checks, TUFLOW log summaries and result styling. The corresponding maintained wrappers are under
`ryan-scripts/TUFLOW-python`; use that folder's README and each wrapper's `--help` output as the current interface.

### Use other library helpers

RORB hydrograph discovery and parsing:

```python
from pathlib import Path

from ryan_library.functions.RORB.read_rorb_files import find_batch_files, parse_batch_output

batch_files = find_batch_files([Path("rorb_outputs")])
runs = [parse_batch_output(path) for path in batch_files]
```

12D culvert export processing:

```python
from pathlib import Path

from ryan_library.functions.process_12D_culverts import get_combined_df_from_files

culverts = get_combined_df_from_files(Path("12d_exports"))
```

For GDAL raster conversion, mosaics, flood extents, footprints, metadata and point-cloud conversion, use the
[maintained GDAL Python wrappers](ryan-scripts/gdal-python/README.md). The BAT files that remain under
`ryan-scripts/gdal-bat` are legacy migration references and should not be selected for new work.

### Supported file converters and formats

The library provides Python-native and GDAL-backed conversion functions, batch orchestrators, and maintained wrappers across geospatial, point-cloud, CAD, hydrologic, and tabular formats:

| Domain | Source formats | Destination formats | Key library APIs and entry points | Notes |
| --- | --- | --- | --- | --- |
| **Raster translation & compression** | Any GDAL raster (`.flt`, `.asc`, `.rst`, `.xyz`, `.tif`, `.ecw`, etc.) | `.tif` (GeoTIFF), `.ovr` (pyramid overviews) | [`translate_to_geotiff()`](ryan_library/functions/gdal/raster_processing.py), [`convert_rasters()`](ryan_library/orchestrators/gdal/raster_workflows.py), [`gdal_translate_TIF_ovr.py`](ryan-scripts/gdal-python/README.md#raster-conversion-and-compression) | Lossless conversion with choice of `tuflow` (DEFLATE) or `efficient` (tiled ZSTD) profiles. |
| **Raster mosaics & virtual datasets** | Multiple raster tiles (`.tif`, `.xyz`, `.asc`, `.flt`) | `.tif` (GeoTIFF mosaic), `.vrt` (VRT dataset) | [`merge_directory()`](ryan_library/orchestrators/gdal/raster_merge.py), [`create_grouped_mosaics()`](ryan_library/orchestrators/gdal/raster_mosaic.py), [`gdal_merge.py`](ryan-scripts/gdal-python/README.md#mosaics), [`build_VRT.py`](ryan-scripts/gdal-python/README.md#grouped-tuflow-result-mosaics) | Supports vector extent clipping, assigned CRS/NoData, and grouped TUFLOW result sets. |
| **Vector dataset translation** | `.gpkg`, `.shp`, `.fgb`, `.geojson`, `.sqlite` | `.gpkg` (GeoPackage), `.shp` (ESRI Shapefile), `.fgb` (FlatGeobuf), `.geojson` (GeoJSON), `.sqlite` (SQLite) | [`translate_vector_dataset()`](ryan_library/functions/gdal/vector_conversion.py), [`require_vector_driver()`](ryan_library/functions/gdal/vector_conversion.py) | Atomic vector translation via GDAL; multi-layer support for GPKG and SQLite. |
| **File Geodatabase export** | `.gdb` (ESRI File Geodatabase directories) | `.gpkg`, `.shp`, `.fgb`, `.geojson`, `.sqlite` | [`export_file_geodatabase()`](ryan_library/orchestrators/gdal/file_geodatabase_export.py), [`discover_file_geodatabases()`](ryan_library/orchestrators/gdal/file_geodatabase_export.py) | Batch GDB discovery; exports each layer to separate files or one multi-layer database. |
| **Raster to vector classification** | Depth, water level, or DEM rasters (`.tif`, `.ecw`, etc.) | `.gpkg`, `.shp`, `.geojson`, `.fgb`, `.sqlite` (flood extent polygons, raster footprints) | [`main_processing()`](ryan_library/orchestrators/gdal/gdal_flood_extent.py), [`polygonize_flood_extent()`](ryan_library/functions/gdal/raster_processing.py), [`gdal_flood_extent.py`](ryan-scripts/gdal-python/README.md#flood-extents), [`gdal_raster_footprint.py`](ryan-scripts/gdal-python/README.md#metadata-footprints-and-point-clouds) | Thresholding, band selection, optional GDAL sieve filtering, and boundary vectorization. |
| **LiDAR & point clouds** | `.laz` (compressed LiDAR) | `.las` (uncompressed ASPRS LAS point clouds) | [`convert_laz_to_las()`](ryan_library/functions/lidar_processing.py), [`convert_laz_directory()`](ryan_library/functions/lidar_processing.py), [`laz_to_las.py`](ryan-scripts/gdal-python/README.md#metadata-footprints-and-point-clouds) | Memory-efficient chunked streaming preserving headers, point formats, scales, offsets, and VLRs. |
| **Elevation rasters to tables/points** | `.tif` (GeoTIFF DEMs) | Tabular `X, Y, Z` DataFrames, `.csv`, `.xyz`, `.las` | [`read_geotiff()`](ryan_library/functions/terrain_processing.py), [`tile_data()`](ryan_library/functions/terrain_processing.py), [`thin-raster-terrain-for-12D_v5.py`](ryan-scripts/12D-python/thin-raster-terrain-for-12D_v5.py), [`tif-to-LAS-valid-only_v6.py`](ryan-scripts/12D-python/tif-to-LAS-valid-only_v6.py) | Spatial tiling, NoData masking, and point thinning for 12D Model or tabular workflows. |
| **CAD, mining & 12D geometry** | `.str` (Surpac binary/text strings), `.dtm` (Surpac binary/text DTM meshes), `.dxf` (polyface meshes), 12D culvert export text | `.gpkg` (3D lines & polygon meshes), `.parquet` (geometry tables), DataFrames | [`get_combined_df_from_files()`](ryan_library/functions/process_12D_culverts.py), [`dtm_str_converter_to_gpkg.py`](ryan-scripts/cad-python/dtm_str_converter_to_gpkg.py), [`extract_dxf_polyface.py`](ryan-scripts/cad-python/extract_dxf_polyface.py), [`polyface_parquet_to_gpkg.py`](ryan-scripts/cad-python/polyface_parquet_to_gpkg.py) | Translates mining CAD and 12D export formats into modern GIS/Parquet geometries. |
| **Hydrologic & model results** | TUFLOW 1D/2D CSVs (`_POMM`, `_PO`, `_Cmx`, `_Nmx`, `_Chan`, `_ccA`, `_RLL_Qmx`, `_Q`, `_H`, etc.), `.tlf` logs, RORB `.out` files | `.xlsx` (multi-sheet styled workbooks), `.parquet`, `.csv`, DataFrames | [`load_tuflow_data()`](ryan_library/functions/tuflow/notebook_helpers.py), [`parse_batch_output()`](ryan_library/functions/RORB/read_rorb_files.py), [`ExcelExporter`](ryan_library/functions/excel_export.py), [`save_to_excel()`](ryan_library/functions/excel_export.py) | Ingests simulation outputs into consolidated tabular datasets, summary reports, and Excel exports. |


## Excel and QGIS resources

The resource submodules contain project templates and application assets:

- `excel-resources/workbooks/`: Excel templates for hydrology, frequency and culvert workflows.
- `qgis-resources/processing-models/tuflow/`: QGIS models for common TUFLOW input layers.
- `qgis-resources/processing-models/tuflow/supporting-workbooks/`: formula-driven supporting workbooks used by relevant models.
- `qgis-resources/styles/`: QML styles, QPT layouts and supporting spatial assets.
- `qgis-resources/scripts/`: QGIS Python console and PyQGIS utilities.

Except for the TUFLOW QML files staged into wheel builds, treat these resources as project templates rather than files
installed with the Python package. The parent repository pins each submodule to a reviewed commit.

## Repository automation

- [Pull request template](.github/pull_request_template.md) records summaries, validation and review checklists.
- [Code-review instructions](.github/code_review_instructions.md) describe repository-specific review expectations.
- [Development guide](docs/DEVELOPMENT_GUIDE.md) is the canonical architecture and validation reference.
- [MCP setup](docs/MCP_SETUP.md) documents focused inspection tools and staged CLI workflow discovery for AI clients.
