# Shared dependency type stubs

These declarations support the repository's normal strict Pyright configuration
in `pyproject.toml`. They apply equally to reusable helpers, processors,
orchestrators, maintained scripts and tests, including the TUFLOW initialiser.

Keep public dependency names and keyword parameters unchanged. Add declarations
for actual dependency APIs exercised by the repository; do not add catch-all
attributes or weaken strictness to conceal a diagnostic. Dynamic metadata and
NumPy dtype parameters may need explicit `Any` where the external API itself
permits arbitrary values.

Rasterio uses the official typeshed distribution
[`types-rasterio`](https://pypi.org/project/types-rasterio/) from the development
extra. Its affine dependency (3.0+) publishes its own typing. Do not restore local
`rasterio` or `affine` stubs: they would shadow those maintained declarations.
Pillow-HEIF uses its native typing (the development extra requires 1.8.0+, which
also fixes public typing exports). PyShp's official `pyshp[stubs]` extra was also
tested against the installed 3.x environment. Its current 0.0.2 package contains
only an empty marker; Pyright 1.1.414 still reports missing stubs and types writer
context managers as their base class. Retain the narrow `shapefile` fallback
until that upstream integration passes. Its runtime requirement is unchanged;
do not upgrade a 2.x deployment solely to obtain typing without checking API
compatibility. The other local fallbacks cover Fiona records and scaled LAS
point views. Pillow-HEIF's optional registration kwargs remain untyped upstream;
the import has a single diagnostic exception rather than a shadowing stub.
Scientific, Arrow and process dependencies use maintained
stub distributions where available. Pyright also reports unnecessary ignore
comments so retired exceptions do not accumulate.

The Rasterio declarations currently type a dataset CRS as always present and do
not distinguish `read(masked=True)` from an ordinary ndarray. Call sites retain
their native missing-CRS checks and use narrow casts for these specific upstream
gaps. Do not remove runtime validation or replace masked arrays to satisfy a
stub. Parameter dictionaries at the driver boundary can have arbitrary metadata
values; retain typed construction where possible and bound dynamic casts to the
external call.

The former `typings-initialiser` directory and `pyright-initialiser.json` were
retired on 2026-11-08. GIS dependencies still require their normal runtime
environment for native acceptance; stubs do not establish runtime correctness.
