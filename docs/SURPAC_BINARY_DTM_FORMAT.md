# Observed binary Surpac DTM format

## Status and scope

This document records an empirically derived interpretation of binary GEOVIA Surpac DTM files. It exists so the
findings can be reviewed and extended: no authoritative public description of this byte layout was identified during
the investigation.

The interpretation was developed on 10-11 August 2026. It began with one matched binary DTM, binary STR and ASCII DXF
triplet, then was tested against a larger supplied corpus. The maintained implementation is
[`dtm_str_converter_to_gpkg.py`](../ryan-scripts/cad-python/dtm_str_converter_to_gpkg.py). See also the independently
inferred [binary STR format](SURPAC_BINARY_STR_FORMAT.md).

This is an observed family of formats, not a claim about every Surpac binary DTM. Unknown layouts are deliberately
rejected instead of being guessed.

## External information

Dassault Systèmes states that Surpac 6.9 and later save files in binary form by default, but its public article does not
describe the representation:
[Displaying Surpac Data in Google Earth](https://blog.3ds.com/brands/geovia/displaying-surpac-data-in-google-earth/).

If an official specification becomes available, it should take precedence and this document and parser should be
reviewed against it.

## Byte order and file envelope

All observed multibyte integers and floating-point values are big-endian. A file consists of:

```text
+----------------------------------------+
| ASCII header line + line ending        |
+----------------------------------------+
| direct or embedded-coordinate prefix   |
+----------------------------------------+
| one or more mesh/submesh blocks        |
+----------------------------------------+
| 8 x 0xFF bytes                         |  Final marker; purpose unknown
+----------------------------------------+
```

Observed headers resemble:

```text
<matching-str-name>,<numeric-value>;algorithm=standard;fields=x,y
```

The matching STR filename is useful corroboration, but is not sufficient by itself to identify the layout.

### Direct prefix

Thirty-four of the 36 manageable binary samples used:

```text
33 x NUL
ASCII "END" + NUL
```

### Embedded-coordinate prefix

Two samples instead used:

```text
4 x NUL
repeating 29-byte embedded point records
58 x NUL
ASCII "END" + NUL
```

Each embedded point record has the unpacking format `>iBddd`:

| Field | Encoding | Observed value or meaning |
| --- | --- | --- |
| Reserved | Signed integer (`>i`) | `0` |
| Record type | Unsigned byte (`B`) | `1` |
| X | Big-endian double (`>d`) | Finite coordinate |
| Y | Big-endian double (`>d`) | Finite coordinate |
| Z | Big-endian double (`>d`) | Finite coordinate |

The role of these coordinates is not yet established. Triangle vertices still resolve against the matching STR point
numbers, so the converter validates this preamble but does not use it to construct geometry.

## Mesh and submesh blocks

The first mesh for a string begins with a 17-byte full header (`>iiiBi`):

| Field | Encoding | Observed value | Interpretation |
| --- | --- | ---: | --- |
| Block flag | Signed integer | `1` | Full-header marker |
| String number | Signed integer | Positive | Matches the STR string number |
| Reserved | Signed integer | `0` | Unknown |
| Record type | Unsigned byte | `2` | Mesh record |
| Mesh number | Signed integer | Positive | Often `1`; an observed string begins at `4` |

A solid can contain more than one disconnected mesh under the same STR string. Subsequent meshes use a compact 9-byte
header (`>iBi`):

| Field | Encoding | Observed value |
| --- | --- | ---: |
| Reserved | Signed integer | `0` |
| Record type | Unsigned byte | `2` |
| Mesh number | Signed integer | Greater than the preceding mesh number |

Mesh identifiers normally increment by one, but observed staged-surface files retain gaps such as `1, 2, 3, 6, 7, 8`.
They are therefore identifiers, not an inferred block count. Compact-header IDs must increase, but need not be
consecutive. A new full header can introduce a new string with any positive mesh ID. Triangle and neighbour numbering
restart at one in each mesh. The converter retains both `string` and `mesh` columns so these local references remain
unambiguous. Up to 33 meshes in one string were observed.

Each full or compact header is followed by a NUL-terminated ASCII metadata string. The observed required keys are:

```text
neighbours, validated, algorithm
```

`closed` and `direction` are optional. `direction` is present for closed solids and absent in observed open surfaces;
one older surface omitted both fields. Examples include:

```text
neighbours=yes,validated=true,closed=yes,direction=solid,algorithm=legacy
neighbours=yes,validated=true,closed=no,algorithm=legacy
neighbours=yes,validated=true,closed=no,algorithm=retriangulation
neighbours=yes,validated=false,algorithm=legacy
```

The parser requires exactly the known required keys plus optional `closed` and `direction`, and requires
`neighbours=yes`. It does not assign undocumented semantics to the other values.

## Triangle records

A triangle has eight big-endian signed integers followed, normally, by one NUL byte:

| Field | Bytes | Observed rule |
| --- | ---: | --- |
| Vertex count | 4 | Always `3` |
| Triangle number | 4 | Starts at `1` in each mesh and increments |
| Vertex 1 | 4 | Positive STR point number |
| Vertex 2 | 4 | Positive STR point number |
| Vertex 3 | 4 | Positive STR point number |
| Neighbour 1 | 4 | Triangle across edge V1-V2 |
| Neighbour 2 | 4 | Triangle across edge V2-V3 |
| Neighbour 3 | 4 | Triangle across edge V3-V1 |
| Record terminator | 1 | Normally NUL |

The eight integer fields use `>8i`. The final triangle immediately before a compact submesh header omits its NUL
terminator in the observed multi-mesh files. This exception is accepted only when the following nine bytes form the
exact next sequential compact header.

Vertex numbers refer to the generated STR `point_number`, not a zero-based array and not the DataFrame row number after
string-zero separators are removed. Preserving Surpac's source numbering is therefore essential.

There is no observed triangle-count field. A mesh ends when the next bytes no longer begin with the big-endian integer
`3`; the following bytes must form a valid full header, the next compact header, or the final marker. Sequential triangle
numbers, valid terminators, positive distinct vertices and topology checks prevent this from becoming a permissive scan.

## Neighbour topology

For a triangle `(V1, V2, V3)`:

```text
neighbour1 -> edge V1-V2
neighbour2 -> edge V2-V3
neighbour3 -> edge V3-V1
```

Observed open-surface boundaries use neighbour `0`. Every positive neighbour must:

- refer to a triangle number in the same mesh;
- contain the expected shared edge; and
- refer back to the original triangle.

The parser also accepts `-1` as a boundary sentinel for compatibility with the established ASCII DTM convention, though
`-1` was not observed in this binary corpus.

## Validation evidence

The initial matched triplet contained 2,696 triangles. Every byte was accounted for, every vertex resolved against the
binary STR, all neighbour relationships were reciprocal, and every reconstructed triangle matched the corresponding
DXF `3DFACE` in the same vertex order with zero coordinate difference. A GeoPackage round trip retained all 2,696 3D
polygons and the supplied local-grid WKT2 CRS.

The broader robustness corpus contained 36 manageable Local-grid binary DTM/STR pairs and corresponding MGA-grid ASCII
DTM/STR representations. It covered closed solids, open surfaces, legacy and retriangulation metadata, direct and
embedded-coordinate prefixes, and strings containing 1 to 33 meshes.

- All 36 binary DTMs parsed successfully.
- Their 1,740,259 triangle rows matched the corresponding ASCII DTM rows exactly across triangle number, vertices and
  neighbours.
- All 36 matching binary STR files supplied every referenced vertex.
- Constructing the complete 3D geometry produced all 1,740,259 polygons, with no missing or two-dimensional triangles.
- Five representative GeoPackages were written with Fiona and reopened successfully. They covered 24 to 100,064
  triangles and included open, multi-mesh and embedded-prefix variants.

The remaining Local-grid topography pair was too large for the converter's current all-in-memory architecture, but a
memory-mapped audit established that it uses the same supported layout. All 7,745,535 binary STR string records matched
the corresponding ASCII string sequence, and all 15,477,710 binary DTM triangle/topology records matched the ASCII DTM
exactly. Its embedded prefix contained 13,359 point records. This separates format support from the outstanding
large-output scalability work.

A second robustness corpus was then checked on 11 August 2026. All 42 DTM files parsed (36 binary and six ASCII), and
all matching STR files supplied the vertices needed to construct 2,092,916 3D polygons from 1,147,154 points. This
corpus exposed the non-consecutive mesh identifiers described above. Two affected binary pairs were also written to
temporary Fiona GeoPackages and reopened with exact feature counts and Z geometries.

A third robustness corpus contained nine DTM files. Eight self-contained pairs parsed and constructed 1,516,050 3D
polygons from 758,304 points; these comprised four binary and four ASCII DTMs. It exposed the optional `closed` metadata
field and a valid `validated=false` surface. That binary case was written to a temporary Fiona GeoPackage and reopened
with all 406 triangles and Z geometries intact.

The ninth DTM was deliberately left rejected. Its 172-triangle mesh contains vertex references 101-198 while its named
STR supplies only point numbers 1-99, and its neighbour fields contain references up to 223. Inferring a point offset or
silently treating the dangling neighbours as boundaries could create plausible but unverified geometry. This is treated
as a non-self-contained or stale-topology source pair, not evidence for another automatically decodable layout.

The source corpus is proprietary and is not committed. Synthetic tests encode the structural variants without retaining
project coordinates or names.

## Parser rejection rules

The maintained parser rejects, with a path and byte offset where applicable:

- a missing or non-ASCII header;
- a malformed direct or embedded-coordinate prefix;
- non-finite or incorrectly tagged embedded points;
- a missing eight-byte final marker;
- invalid full or compact mesh headers, nonpositive mesh IDs or non-increasing compact mesh IDs;
- missing, non-ASCII, malformed or unsupported metadata;
- a mesh without triangles;
- a triangle whose vertex count is not three;
- non-sequential triangle numbering;
- a missing terminator except at the precisely validated compact-header boundary;
- nonpositive or repeated vertices;
- an out-of-range neighbour; or
- a neighbour that does not share the expected edge or reference the triangle reciprocally.

Detection uses the complete structure, not the `.dtm` suffix alone. The parser does not try alternative endianness, field
widths or marker interpretations after a validation failure.

## Known limits

- Only the variants represented by this corpus have been examined.
- Only big-endian records and `neighbours=yes` metadata were observed.
- The meanings of several reserved fields, prefix padding and the final marker remain unknown.
- Embedded-prefix coordinates are validated but their formal purpose is unknown.
- Coordinate reconstruction still depends on the matching STR file.
- The current implementation reads complete files and materialises records, DataFrames and geometry in memory. This was
  not suitable for the Local-grid topography inputs in the robustness folder (approximately 214 MB STR and 487 MB DTM,
  expanding to about 7.7 million points and 15.5 million polygons); a future streaming/chunked conversion path is needed
  for full GeoPackage export at that scale.
- Other Surpac releases or save options may contain additional metadata, record types, fields, topology or markers.
- Some exported or split surfaces can retain vertex or neighbour numbering from a larger source model. Those files need
  corrected source data or explicit external mapping evidence; the parser does not guess offsets.

## Extending this knowledge

Contributions describing other variants are welcome. Do not commit proprietary DTM, STR or DXF files. Instead, provide
the smallest synthetic or anonymised fixture that reproduces the structure and record:

- Surpac version and save preferences, if known;
- whether the DTM represents an open surface or closed solid;
- the matching STR relationship;
- header, prefix, mesh header, metadata and final-marker bytes;
- whether triangle numbers restart per mesh;
- boundary-neighbour representation;
- byte order and field widths;
- whether every byte can be accounted for; and
- the exact parser error and byte offset from the current implementation.

A parser change should update this document, add a synthetic regression test, retain strict rejection of ambiguous
layouts, and compare reconstructed triangles with an independent representation such as ASCII DTM or DXF where possible.
