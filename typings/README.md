# Shared dependency type stubs

These declarations support the repository's normal strict Pyright configuration
in `pyproject.toml`. They apply equally to reusable helpers, processors,
orchestrators, maintained scripts and tests, including the TUFLOW initialiser.

Keep public dependency names and keyword parameters unchanged. Add declarations
for actual dependency APIs exercised by the repository; do not add catch-all
attributes or weaken strictness to conceal a diagnostic. Dynamic metadata and
NumPy dtype parameters may need explicit `Any` where the external API itself
permits arbitrary values.

The former `typings-initialiser` directory and `pyright-initialiser.json` were
retired on 2026-11-08. GIS dependencies still require their normal runtime
environment for native acceptance; stubs do not establish runtime correctness.
