# Parquet-directory snapshots for native TUFLOW processor collections

**Decision:** 8 November 2026. **Status:** Windows source/installed-wheel repairs validated; final pushed-head CI and independent review pending. **Next review:** 15 November 2026.

**Scope:** [PR #293](https://github.com/Chain-Frost/ryan-tools/pull/293) stacked on draft [#251](https://github.com/Chain-Frost/ryan-tools/pull/251). [WP04 #284](https://github.com/Chain-Frost/ryan-tools/pull/284) remains a separate prerequisite; do not modify or merge it from this work front.

## Revised architectural decision

The previous single-file `.rtparquet` ZIP proposal is **superseded** before release.
The public `ProcessorCollection.to_parquet(directory)` and
`ProcessorCollection.from_parquet(directory)` APIs now use a **directory** of
ordinary ZSTD-compressed PyArrow Parquet tables with a versioned
`manifest.json`. This does not replace standard DataFrame `.parquet` exports,
adopt PyTuflow readers, add PyTables/HDF5 or introduce Dask.

For a fresh dataset the directory holds:
- `manifest.json`
- `processors/000000.parquet`, `processors/000001.parquet`, etc.
- optional `basic_info_lookup.parquet`.

Each member is independently loadable with pandas/PyArrow. Manifest entries
include SHA-256, byte size, pandas column/index/dtype contract, source
fingerprint, factory type/class/module, canonical run code, filters and input
health. Raw PO/POMM source locations are recorded independently from processed
rows to distinguish absent locations from locations with wholly rejected
observations. The only permitted processor instantiation is the current
installed `BaseProcessor.from_file` factory, checked against the manifest;
there is no arbitrary dynamic module loading based on serialized data.

## Source provenance and semantics

`BaseProcessor` records the bounded source signature before processing
(path, byte size, nanosecond modification time, plus GeoPackage WAL state).
The signature travels with the processed data through imports and subsequent
exports. A default export checks this original identity both before writing
and just before publication. The default import also verifies the current
source. `verify_sources=False` is an explicit historical/offline choice
that does not change the recorded original identity.

For strict snapshot compatibility, a registry digest and a conservative
processing-source digest (relevant native processor/source files) must match.
Data payloads are rehydrated without reopening the source result CSV. This
bounded fingerprint is not equivalent to content-hashing a huge source:
a deliberate or accidental same-size/same-mtime replacement can evade it.

## Safe publication and selection

For a new directory, an owned sibling staging directory is written, validated,
and moved into place by an atomic no-clobber directory rename. Unsupported
POSIX rename semantics fail closed. Existing targets are never overwritten
implicitly. On `overwrite=True` the writer publishes an immutable generation
under `.versions/<uuid>/` and atomically replaces only `manifest.json`,
preserving the prior generation for readers who have already opened it.
Linux pins directory descriptors for the entire publication window and uses
atomic RENAME_NOREPLACE into a pinned versions directory and descriptor-relative
manifest replacement. Windows holds non-reparse-point CreateFileW GENERIC_READ handles with
FILE_SHARE_DELETE denied on the destination, its ancestors and versions folder
through cleanup, preventing path swaps. If these protections are unavailable
the overwrite fails closed. Overwrite consent is rechecked after staging, so a
concurrent initial writer cannot be replaced without explicit consent.
Readers resolve member paths relative to the generation recorded in the
manifest. Old generations are intentionally **not automatically deleted**.
The writer cleans only its owned temporary files/directories on failure,
never existing snapshots or source files. Windows NTFS and SMB mapped/UNC
rename/replace behaviour is a required native acceptance gate, not inferred
from POSIX validation.

The manifest is inspected before loading Parquet frames. Data-type selection
and independent raw-location presence evidence can skip unrelated members
without materialising their DataFrames. Per-frame row filtering and big
multi-run aggregation are deliberately not introduced: restoring an entire
300-run time-series collection may still exhaust RAM. Use independent
partitioned Parquet/reduction for those workloads.

The normal collection retains only nonempty successfully processed processor
objects. `parquet_selection_diagnostics` records `SELECTED`, `ABSENT`
or `INVALID` outcomes for location-filtered restores, so a source-present
but wholly rejected location is not silently rebranded as no-match.

## Acceptance and dependencies

- Native real factory Q/PO/POMM and heterogeneous DataFrame round-trip,
  categories/nullable/index and compacted lookup parity.
- Type/location selection before unrelated frame reads; present-invalid vs
  absent location evidence, with the final WP04 #284 behaviour integrated.
- Source modification after processing and immediately before publication,
  explicit historical/offline signatures, missing/moved sources, semantic
  incompatibility, corrupt/hash-mismatched and duplicate/traversal paths.
- Directory no-clobber, interruption/overwrite preservation and owned-temp
  cleanup, including native Windows NTFS, mapped-drive, UNC and installed wheel.
- Ruff format/lint, Python 3.14 strict Pyright on modified files, focused
  pytest, all six hosted CI jobs and an exact-head independent Codex review.
- No merge into draft #251 until WP04 #284 is incorporated and approval
  is explicit. No merge into main, release or issue closure.

The two original Codex P1 findings at the superseded ZIP implementation are
addressed by writing ordinary on-disk Parquet and capturing source provenance
before processing. The prior stream-incompatibility and export-time signature
bugs must not be carried forward.

## Delivery checkpoint

Changes are staged as a single coherent GitHub branch batch rather than
pushing incomplete file-by-file commits. The current execution environment
does not provide Windows Python 3.14, native SMB, a checked-out repository
worktree, or a locally installed PyArrow wheel. Do not infer local, native,
installed-wheel or hosted success without separate evidence. Log exact-head
CI and Codex results on PR #293 once available, without merging.


## 2026-11-09 Windows repair checkpoint

Continued the existing #293 branch from `b67c270f` in an isolated worktree;
the parent checkout and WP04 remain untouched. Fixed all six strict-Pyright
errors reproduced from the hosted Policy job. Windows execution additionally
exposed three failures in the existing Parquet tests: integer column-axis names
became strings, object lookup `pd.NA` values became `None`, and attribute-only
Windows directory handles did not prevent POSIX-style renames.

The manifest now preserves typed column-axis names and the positions of object
`pd.NA` values. Restoration validates those positions and refuses to replace
non-null values. Windows publication handles request GENERIC_READ and deny
FILE_SHARE_DELETE; the existing native directory-swap regression now passes.
No new MCP surface is needed: this repairs the existing Python snapshot API.

Validation on Windows Python 3.14.6 / PyArrow 24.0.0:

- `python -m pytest tests/processors/tuflow/test_processor_collection_parquet_bundle.py -q`:
  21 passed, two Linux-only safety tests skipped.
- Related collection/info/coverage/channel-selection/persistence tests: 57 passed.
- Ruff lint/format and strict Pyright on the PR's four Python implementation
  files and Parquet test module: passed with the declared development typing dependencies.
- `python repo-scripts/build_library.py --skip-pip`: built and verified local
  wheel `26.11.9.1`. Initial build refused missing QML resources; initialised only
  the required `qgis-resources` and `tests/test_data` submodules, without LFS downloads.
- Installed that wheel into an isolated temporary package directory, verified
  the imported module path, and ran a copied Parquet test module against it:
  21 passed, two Linux-only skips. No environment or installed package replacement.
- Documentation checker and diff whitespace checks: passed.

Delivery: code, version and this record are batched for the existing PR branch;
the ignored wheel remains local. Hosted results will be recorded on the PR at
the final pushed SHA. NTFS acceptance above does not establish mapped-drive/UNC
or native SMB acceptance. Independent final-head review and WP04 integration
remain separate gates. No merge, release or issue closure is authorised.
Next review remains 2026-11-15.
