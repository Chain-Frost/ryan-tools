# Single-file Parquet bundles for ryan-tools TUFLOW processor collections

**Decision:** 8 November 2026
**Stack:** draft #251; no main merge or release authorised.
**Status:** Implementation for independent CI/Codex/native review.

## Purpose

After retiring unused HDF5 snapshots, retain the ability to save and later
restore a collection of *heterogeneous* TUFLOW processors. A standard Parquet
file has one table schema; concatenating heterogeneous processor DataFrames
loses their distinct types, filter state and class identity. The supported
collection archive is therefore one **.rtparquet ZIP container**, with one
`processors/NNNNNN.parquet` member per processor and a `manifest.json`.
The compacted basic-info lookup has its own optional Parquet member.

The format is designed for native `ryan-tools` processing only. It does not
use or substitute PyTuflow result readers and does not add a mandatory Dask
or PyTables dependency. PyArrow is already a runtime dependency.

## Contract

- `collection.to_parquet("results.rtparquet")`: atomically stage then
  publish a single-file snapshot, refusing overwrite by default.
- `ProcessorCollection.from_parquet("results.rtparquet")`: validate all
  declared members, SHA-256 hashes for loaded frames, processor registry,
  class identity, dataformat and run identity; reconstruct trusted classes
  with the current `BaseProcessor.from_file` factory rather than importing
  arbitrary paths from serialized input.
- Restore processed frames, retained pandas dtypes and indexes, entity and
  location filter state, canonical source identities, and optional
  compacted `basic_info_lookup`. Do not reparse original CSV data.
- Filter by result `data_types` before reading frame members; apply optional
  `locations` through current collection filtering after restore.
- By default verify source existence/size/mtime (and GeoPackage WAL
  presence/size/mtime). `verify_sources=False` is an explicit archived
  snapshot mode, not a live cache-freshness claim.
- Fail on missing, extra, duplicated, corrupt or mismatched members.
  Reject input archive traversal and class-indirection semantics by fixed
  member names and an allowlisted current factory.
- Store Parquet-compressed members with ZIP_STORED; never wrap the frames
  in an additional compressed pickle or opaque HDF5 layer.
- The snapshot can be copied as one file but is **not** a native .parquet
  file. Direct DataFrame exports continue to use ordinary .parquet.

The archive is not a bounded-memory analytics engine: restoring all
processors materialises their DataFrames. For the representative full 300-run
time-series workload, WP11/#229 incremental partitioned Parquet and
per-run reduction remain the higher-scale strategy.

## Acceptance

Run focused real on-disk Q/PO/POMM round-trips, mixed and nullable dtypes,
compacted lookup and selection, source freshness, incomplete/corrupt archive
handling, no-clobber/overwrite/partial-write cleanup, strict typing, Ruff,
six hosted CI checks, and native Windows Python 3.14 source/wheel acceptance.
No main merge, automatic release or issue closure is authorised.
