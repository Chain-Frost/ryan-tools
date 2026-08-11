# Observed binary Surpac STR format

## Status and scope

This document records an empirically derived interpretation of a binary GEOVIA Surpac STR variant. It is intended to
make the findings reviewable and extendable because no authoritative public binary layout specification was identified
during the investigation.

The interpretation was developed on 10 August 2026 from five binary STR files produced for a real project, then tested
against a larger supplied corpus on 11 August 2026. It was validated by consuming files through their final byte,
reconstructing point and break records, comparing corresponding binary and ASCII STR semantics, and creating and
reopening 3D GeoPackages. Project files, paths and identifying header content are deliberately not included here.

This is an observed format, not a claim about every Surpac binary STR file. The maintained implementation is
[`dtm_str_converter_to_gpkg.py`](../ryan-scripts/cad-python/dtm_str_converter_to_gpkg.py). Unknown variants must be
rejected clearly rather than decoded by guessing.

## What is confirmed externally

Dassault Systèmes states that Surpac 6.9 and later save files in binary form by default and that the preference can be
changed when text STR files are required. The source does not describe the binary record layout:
[Displaying Surpac Data in Google Earth](https://blog.3ds.com/brands/geovia/displaying-surpac-data-in-google-earth/).

No authoritative public description of the byte layout below was found in the official material checked on 10 August
2026. If an official specification becomes available, it should take precedence over these observations and this
document and parser should be reviewed against it.

## Observed file envelope

All multibyte numeric values in the examined files are big-endian.

```text
+--------------------------------+
| ASCII header line 1 + CR/LF    |
+--------------------------------+
| ASCII header line 2 + CR/LF    |
+--------------------------------+
| 5 x NUL bytes                  |  Purpose unknown
+--------------------------------+
| Point and segment-break records|
+--------------------------------+
| ASCII bytes "END" + NUL        |
+--------------------------------+
```

The two headers must decode as ASCII. The five-NUL prefix and final `END\0` marker were identical in all examined files,
but their formal meaning is unknown.

Detection should use the complete structure, not the `.str` suffix alone. ASCII and binary Surpac files use the same
extension.

## Observed record layout

Each normal record contains the following fields:

| Field | Encoding | Bytes |
| --- | --- | ---: |
| String number | Signed integer, big-endian (`>i`) | 4 |
| Y / northing | IEEE-754 double, big-endian (`>d`) | 8 |
| X / easting | IEEE-754 double, big-endian (`>d`) | 8 |
| Z / elevation | IEEE-754 double, big-endian (`>d`) | 8 |
| Description | NUL-terminated ASCII | Variable |

The fixed portion is 28 bytes. A normal empty description still has a NUL terminator, so the smallest normal record is
29 bytes.

The equivalent Python unpacking operations are:

```python
string_number = struct.unpack_from(">i", raw, offset)[0]
y, x, z = struct.unpack_from(">ddd", raw, offset + 4)
```

The coordinate order is Surpac order: `string, Y, X, Z, descriptions`. GIS geometries must therefore be constructed as
`Point(X, Y, Z)`.

### Description fields

The binary record contains one ASCII description string. Commas inside it retain the normal ASCII STR meaning of
separating description fields. For example, the stored string `BW (4),27` corresponds to `d1 = "BW (4)"` and
`d2 = "27"` in the converter output.

### Segment breaks

An ordinary observed segment-break record contains:

```text
string_number = 0
y = 0.0
x = 0.0
z = 0.0
description = ""
```

Nonzero coordinates or a nonempty description on string zero are treated as malformed.

The final segment break in the examined files has a special encoding: it is exactly the 28-byte all-zero fixed portion
immediately before `END\0`, with no description terminator. The parser accepts this only when all four fixed fields are
zero and it is the final record. Any other 28-byte tail is rejected as truncated input.

## STR semantics preserved by the converter

Binary records are converted to the same DataFrame schema used by the existing ASCII parser:

```text
point_number, group, string, y, x, z, d1, d2, ...
```

The existing point-number behavior is significant for matching DTM vertex references:

- the second ASCII header record consumes point number zero;
- the first point therefore receives point number one;
- every string-zero break consumes another point number;
- break records increment the geometry group and are not emitted as points.

The binary parser deliberately preserves those rules. Comma-separated descriptions, negative or zero coordinates,
large nonnegative string numbers and the full precision of binary doubles are retained.

An ASCII row ending in a comma contains an explicit empty description field, while the equivalent binary record can
have an empty description and therefore no `d1` value. Pandas represents these as `""` and `None` respectively. This is
a representation difference only: point number, group, string and geometry are unchanged.

## Validation and failure behavior

A file is accepted as this binary variant only when all applicable checks pass:

- two newline-terminated ASCII header records are present;
- the observed five-NUL prefix follows header line 2;
- the file ends with `END\0`;
- every normal record has its complete 28-byte fixed portion and a description terminator;
- the only permitted unterminated record is the final 28-byte all-zero break described above;
- string numbers are nonnegative;
- X, Y and Z values are finite;
- string-zero records contain no nonzero coordinates or description; and
- parsing consumes the complete payload without ignored bytes.

Failures report the input path and byte offset where applicable. Arbitrary control bytes are not decoded with
replacement characters, and unsupported layouts are not tested under alternative endianness heuristics.

## Known limits and unsupported formats

- The observations cover the supplied corpus, not every Surpac version or producer.
- Only big-endian records were observed.
- Only ASCII headers and descriptions were observed.
- The purposes of the five-NUL prefix and unterminated final break are unknown.
- String numbers from zero into the hundreds were present, but the parser does not impose that sample-specific range.
- Binary DTM support is based on a separate empirical investigation; see the
  [observed binary DTM format](SURPAC_BINARY_DTM_FORMAT.md).
- Supporting these STR and DTM variants must not be interpreted as support for other binary Surpac file types.

## CRS and GeoPackage export

STR coordinates do not identify their CRS. A local engineering grid must not be labelled as its underlying map-grid
EPSG CRS unless the coordinates have actually been transformed.

The converter accepts an authority code, literal WKT, or `.prj` path through `--crs`. GeoPackages are written with
Fiona because the default writer path can attempt a lossy WKT1 conversion and fail for WKT2 `DERIVEDPROJCRS`
definitions. Validation should reopen the output and check the CRS name, feature counts, coordinate bounds and Z
coordinates.

Example:

```powershell
python ryan-scripts/cad-python/dtm_str_converter_to_gpkg.py `
    --str "input.str" `
    --crs "project_local_grid.prj" `
    --no-dtm `
    --no-pause
```

Verbose output reports whether `ASCII STR` or `binary STR` was detected.

## Evidence and tests

The original five-file investigation produced 281 point features and 52 linestring features. The maintained tests use
synthetic fixtures rather than project data and cover:

- equivalent ASCII and binary DataFrames;
- point-number and group behavior;
- multiple strings and segments;
- empty, populated and comma-containing descriptions;
- negative and zero coordinates;
- 3D precision and geometry;
- the unterminated terminal all-zero break;
- malformed headers, prefix, marker, records, descriptions, numbers, coordinates and breaks;
- independent STR and DTM format dispatch; and
- `.prj` loading and Fiona export routing.

The five original binary files were also parsed directly during implementation, and a resulting GeoPackage was
reopened with the expected 3D point and line counts and WKT2 local-grid CRS.

The broader robustness audit parsed 82 manageable STR files (43 binary and 39 ASCII). Corresponding Local-grid binary
and MGA-grid ASCII files had identical point-number, group and string sequences; their only non-coordinate differences
were the explicit-empty versus absent description values described above. All 36 manageable binary STR files paired
with DTMs supplied every vertex needed to construct 1,740,259 3D triangles.

The remaining approximately 214 MB binary topography STR was audited without constructing a DataFrame. Its 7,745,535
string records matched the corresponding MGA ASCII string-number sequence exactly. The layout is supported, but full
conversion at that size remains limited by the converter's all-in-memory DataFrame and geometry pipeline; see the DTM
document for the paired surface result.

A second robustness corpus added 121 successfully parsed STR files (62 binary and 59 ASCII), containing 1,220,064
emitted point records. Its 42 DTM-paired STR files supplied 1,147,154 points and every vertex needed to construct
2,092,916 3D triangles.

A third robustness corpus added 16 successfully parsed STR files (11 binary and five ASCII), containing 774,783 emitted
point records. Eight self-contained DTM/STR pairs supplied 758,304 points and every vertex needed to construct 1,516,050
3D triangles. One additional STR parsed correctly but its named DTM retained incompatible external point numbering; the
DTM document records why that pair remains rejected.

## Extending this knowledge

Contributions describing other variants are welcome. Do not commit proprietary STR files. Instead, provide the
smallest synthetic or anonymised fixture that reproduces the structure and record:

- Surpac version and relevant save preferences, if known;
- how the file was produced or exported;
- header termination and the bytes between headers and records;
- byte order and evidence used to determine it;
- point, break and end-marker encodings;
- description encoding and termination;
- whether every byte can be accounted for; and
- the exact parser error and byte offset from the current implementation.

A parser change should update this document, add a synthetic regression test, preserve strict rejection of ambiguous
layouts, and demonstrate that existing ASCII STR and documented binary STR behavior remains unchanged.
