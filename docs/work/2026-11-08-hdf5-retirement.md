# Retire unused HDF5 processor snapshots — breaking change

**Decision:** 8 November 2026
**Notice retained through:** 31 March 2027 (may remove from 1 April 2027)
**Integration target:** draft #251 only; no authorisation to merge to `main` or publish.

## Decision

The maintainer has never used the HDF5 processor-snapshot feature and is
removing it as a breaking change. Retain the existing `to_hdf()` and
`from_hdf()` call signatures, but make them raise `NotImplementedError`
before any file I/O. Retain the migration notices through **2027-03-31**.

The published PyPI fallback is [`ryan-tools==26.11.8.3`](https://pypi.org/project/ryan-tools/26.11.8.3/):

```powershell
python -m pip install "ryan-tools==26.11.8.3"
```

This version contains the historical HDF methods; its older HDF5 code has
known categorical/nullable round-trip limitations. The more recent
Arrow-in-HDF5 implementation on draft #251 has **never been published**
on PyPI and is not a valid installable version reference.

Remove the unneeded HDF serialization, manifest, checksum, HDF reader/writer
and PyTables cache extra and CI coverage. Preserve the independent
`ryan-tools` TUFLOW processors; **do not use PyTuflow readers** in place
of them. Parquet remains the standard for processed tabular output and
partitioned datasets. This does not create a replacement automatic
`ProcessorCollection` object restoration API.

Retain the accepted ccA/SQLite/WAL protections and existing non-HDF Parquet
result/cache functionality. Existing historical WP05 acceptance evidence is
superseded only for the removed HDF implementation.

## Validation and integration

- Check fail-fast HDF stubs with absent and existing files; no cache writes.
- Check no `tables` dependency, optional HDF extra, or HDF CI import remains.
- Check scope regression treats dependency changes as broad static validation.
- Run Ruff, strict Pyright, focused tests and all exact-head hosted CI checks;
  request Codex review before feature-branch integration.
- Do **not** merge into `main`, publish a release or close #226 merely
  because this implementation branch exists. Main integration requires
  separate maintainer authorisation.

## Separate native Parquet-directory collection follow-up

Draft [#293](https://github.com/Chain-Frost/ryan-tools/pull/293) proposes
`ProcessorCollection.to_parquet(directory)` /
`ProcessorCollection.from_parquet(directory)` using ordinary Parquet tables
plus `manifest.json`. This supersedes the unpublished single-file
`.rtparquet` ZIP proposal; it does not restore HDF5 or modify the last
published version retaining HDF5 methods. The sunset remains 2027-03-31.
Integration waits on WP04 #284 and independent acceptance.
