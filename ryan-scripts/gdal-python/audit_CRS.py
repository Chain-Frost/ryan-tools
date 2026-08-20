from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path

from osgeo import gdal, ogr, osr


ROOTS: list[Path] = [
    Path(r"Q:\path\path]"),
    
]

OUTPUT_CSV: Path = Path(__file__).resolve().parent / "higginsville_crs_audit.csv"

EXTENSIONS: set[str] = {".tif", ".tiff", ".flt", ".gpkg"}


def describe_srs(srs: osr.SpatialReference | None) -> dict[str, str]:
    if srs is None:
        return {
            "crs_name": "",
            "authority": "",
            "epsg": "",
            "crs_type": "",
            "wkt": "",
            "status": "MISSING CRS",
        }

    # Work on a clone because AutoIdentifyEPSG can modify the SRS.
    srs = srs.Clone()

    try:
        srs.AutoIdentifyEPSG()
    except RuntimeError:
        pass

    if srs.IsProjected():
        crs_type = "Projected"
        authority_node = "PROJCS"
    elif srs.IsGeographic():
        crs_type = "Geographic"
        authority_node = "GEOGCS"
    else:
        crs_type = "Other"
        authority_node = None

    authority:str|None = ""
    epsg:str |None= ""

    if authority_node is not None:
        authority = srs.GetAuthorityName(authority_node) or ""
        epsg = srs.GetAuthorityCode(authority_node) or ""

    # Sometimes GDAL attaches authority information at the root instead.
    if not authority:
        authority = srs.GetAuthorityName(None) or ""

    if not epsg:
        epsg = srs.GetAuthorityCode(None) or ""

    crs_name:str|None = (
        srs.GetName()
        or srs.GetAttrValue("PROJCS")
        or srs.GetAttrValue("GEOGCS")
        or ""
    )

    return {
        "crs_name": crs_name,
        "authority": authority,
        "epsg": epsg,
        "crs_type": crs_type,
        "wkt": srs.ExportToWkt(),
        "status": "OK",
    }


def audit_raster(path: Path) -> list[dict[str, str]]:
    ds = gdal.Open(str(path), gdal.GA_ReadOnly)

    if ds is None:
        return [{
            "file": str(path),
            "file_type": path.suffix.lower(),
            "layer": "",
            "crs_name": "",
            "authority": "",
            "epsg": "",
            "crs_type": "",
            "wkt": "",
            "status": "FAILED TO OPEN",
        }]

    projection = ds.GetProjectionRef()

    if projection:
        srs = osr.SpatialReference()
        srs.ImportFromWkt(projection)
    else:
        srs = None

    result: dict[str, str] = {
        "file": str(path),
        "file_type": path.suffix.lower(),
        "layer": "",
        **describe_srs(srs),
    }

    ds = None
    return [result]


def audit_gpkg(path: Path) -> list[dict[str, str]]:
    ds = ogr.Open(str(path), 0)

    if ds is None:
        return [{
            "file": str(path),
            "file_type": ".gpkg",
            "layer": "",
            "crs_name": "",
            "authority": "",
            "epsg": "",
            "crs_type": "",
            "wkt": "",
            "status": "FAILED TO OPEN",
        }]

    rows: list[dict[str, str]] = []

    for i in range(ds.GetLayerCount()):
        layer = ds.GetLayerByIndex(i)

        if layer is None:
            continue

        srs = layer.GetSpatialRef()

        rows.append({
            "file": str(path),
            "file_type": ".gpkg",
            "layer": layer.GetName(),
            **describe_srs(srs),
        })

    # An empty GeoPackage is still worth reporting.
    if not rows:
        rows.append({
            "file": str(path),
            "file_type": ".gpkg",
            "layer": "",
            "crs_name": "",
            "authority": "",
            "epsg": "",
            "crs_type": "",
            "wkt": "",
            "status": "NO LAYERS",
        })

    ds = None
    return rows


def find_files() -> list[Path]:
    files: list[Path] = []

    for root in ROOTS:
        if not root.exists():
            print(f"WARNING: folder does not exist: {root}")
            continue

        files.extend(
            path
            for path in root.rglob("*")
            if path.is_file() and path.suffix.lower() in EXTENSIONS
        )

    return sorted(files)


def main() -> None:
    gdal.UseExceptions()

    files: list[Path] = find_files()

    print(f"Found {len(files):,} files to inspect.")

    rows: list[dict[str, str]] = []

    for number, path in enumerate(files, start=1):
        print(f"[{number:>5}/{len(files)}] {path}")

        try:
            if path.suffix.lower() == ".gpkg":
                rows.extend(audit_gpkg(path))
            else:
                rows.extend(audit_raster(path))

        except Exception as exc:
            rows.append({
                "file": str(path),
                "file_type": path.suffix.lower(),
                "layer": "",
                "crs_name": "",
                "authority": "",
                "epsg": "",
                "crs_type": "",
                "wkt": "",
                "status": f"ERROR: {exc}",
            })

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    fields: list[str] = [
        "file",
        "file_type",
        "layer",
        "crs_name",
        "authority",
        "epsg",
        "crs_type",
        "status",
        "wkt",
    ]

    with OUTPUT_CSV.open(mode="w", newline="", encoding="utf-8-sig") as f:
        writer: csv.DictWriter[str] = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    print()
    print("=" * 80)
    print("CRS SUMMARY")
    print("=" * 80)

    crs_counts = Counter()

    for row in rows:
        if row["status"] != "OK":
            key: str = row["status"]
        elif row["epsg"]:
            key = f"{row['authority']}:{row['epsg']} - {row['crs_name']}"
        elif row["crs_name"]:
            key = f"NO EPSG - {row['crs_name']}"
        else:
            key = "MISSING CRS"

        crs_counts[key] += 1

    for crs, count in crs_counts.most_common():
        print(f"{count:>6}  {crs}")

    print()
    print(f"Audit written to:")
    print(OUTPUT_CSV)


if __name__ == "__main__":
    main()