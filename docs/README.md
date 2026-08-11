# Project documentation

This directory contains repository-wide development, design and maintenance documents. User-facing setup and navigation
remain in the root [`README.md`](../README.md).

## Start here

- [Development guide](DEVELOPMENT_GUIDE.md): canonical architecture, code categories, execution environments, lifecycle
  terms, validation expectations, safety constraints and sources of truth.
- [Development and execution environments](ENVIRONMENTS.md): Python setup, VS Code tasks, installed-wheel behaviour,
  QGIS/OSGeo4W execution and headless use.
- [Logging guide](LOGGING.md): console levels, multiprocessing, notebooks, concise AI/MCP output and message policy.
- [Ryan Scripts guide](../ryan-scripts/README.md): how to choose and safely run wrappers and standalone utilities.
- [Maintained wrapper standard](../ryan-scripts/WRAPPER_STANDARD.md): required structure and behaviour for library-backed
  wrappers.
- [TUFLOW processor development notes](../ryan_library/processors/tuflow/README.md): processor-specific implementation
  and validation guidance.
- [Examples](../examples/README.md): direct API demonstrations retained outside the concise project README.

## Integrations

- [MCP server setup](MCP_SETUP.md): configure the repository's filename, log, raster-health and GDAL-discovery tools
  in supported AI clients.

## File-format references

- [Observed binary Surpac STR format](SURPAC_BINARY_STR_FORMAT.md): empirically derived record layout, validation rules,
  known limits, converter behavior and guidance for documenting additional variants.
- [Observed binary Surpac DTM format](SURPAC_BINARY_DTM_FORMAT.md): empirically derived mesh-block and triangle layout,
  neighbour topology, DXF validation evidence and known limits.

## Maintenance documents

- [Compatibility policy & inventory](COMPATIBILITY_POLICY.md)
- [Repository improvement roadmap](REPOSITORY_IMPROVEMENT_ROADMAP.md)

Maintenance documents describe planned or unfinished work. They are not sources of current architectural policy unless
the development guide explicitly adopts their decisions.

## Dated audits and implementation plans

- [Logging pipeline implementation plan (8 August 2026)](audits/2026-08-08-logging-pipeline-implementation-plan.md)
- [`ryan_library` lifecycle audit and implementation plan](audits/2026-08-08-ryan-library-lifecycle-plan.md)
