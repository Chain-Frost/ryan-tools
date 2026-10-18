# TUFLOW cleanup: shared filesystem verification (#110)

| Field | Value |
| --- | --- |
| Status | Implemented; awaiting CI and review |
| Created | 2026-10-15 |
| Parent | PR #109, safe GeoTIFF recompression |
| Follow-up | Issue #110; branch `feature/shared-tuflow-verification-110` |

## Reuse assessment

| Helper / behaviour | Decision | Reason |
| --- | --- | --- |
| `FileSnapshot`, `snapshot_file()`, `snapshot_is_unchanged()` | Move to `ryan_library.functions.file_verification`; use in both scripts and GDAL | Size and nanosecond mtime are generic file metadata, not raster metadata |
| `has_linked_component()` | Move into the same filesystem module | Checks lexical path components (including Windows junctions); resolving paths *before* this check would erase evidence of links |
| `hash_file()` | Move into the same filesystem module with optional `on_chunk` callback | Binary SHA-256 for GDAL; script uses `.hex()` for existing report format; original 1 MiB per-chunk Rich accounting remains |
| `copy_file_with_digest()` | Keep in GDAL module for this follow-up | Used for staging and deployment, not duplicate deletion; its verified-copy contract should not be substituted for single-pass duplicate grouping |
| Window sizing | Keep local to each caller | Tiny deterministic arithmetic; reusing this alone would not meaningfully reduce duplicate code |
| `verify_rasters_exact()` | Not suitable for FLT/ASC conversion checks | Enforces decoded bytes, NoData metadata, dtype and additional metadata; the verifier permits different NoData sentinels if masks and all valid-cell values match |
| Streaming Float32 copy and staging | Not suitable for duplicate cleanup or legacy converter validation | Explicitly GeoTIFF-specific, and not an equality predicate for two different raster formats |
| RasterIO comparison loops | Keep separate | Recompression requires exact-bytes verification; FLT/ASC cleanup compares *valid values* using masked arrays and retains per-row progress |

## Behavioural guarantees

- Duplicate detection still prefilters candidates by extension and size, SHA-256 hashes candidates **once**, and preserves the Rich byte count, hexadecimal hash strings, manifest fields and `DELETE` confirmation.
- Both the retained and deleted files are checked for unchanged size/mtime immediately before unlink; linked components are now explicitly rejected by the common filesystem helper.
- The FLT/ASC utility retains NoData-sentinel tolerance, exact valid-pixel comparisons, strict CRS and grid-transform checks, TIFF-newer requirements, `.hdr` handling, reports, `DELETE` opt-in and anti-link checks before deletions. CRS equality is deliberately not axis-order agnostic: OGC:CRS84 and EPSG:4326 do **not** match merely because both describe WGS84.
- All workflows normalise file paths using `Path.absolute()` without dereferencing symlinks or junctions. The generic snapshot helper no longer accepts `resolve=True`; conversion verification and raster recompression both preserve lexical paths for link checks.
- Metadata snapshots do not lock files or prevent adversarial same-size/same-mtime changes. Do not run deletion against active simulation outputs.

## Validation

Focused coverage includes `tests/functions/test_file_verification.py`, `tests/scripts/test_delete_duplicate_check_files.py`,
`tests/scripts/test_verify_converted_tuflow_rasters.py`, and
`tests/orchestrators/gdal/test_raster_recompression.py`.

PR #109 has been squash-merged. PR #116 has been rebuilt on its merge commit to eliminate duplicated parent history and keep only the shared-verification changes. Hosted Ruff, Pyright, documentation, retained wheel and test outcomes must be checked on the follow-up PR head before merging.

### 2026-10-15 local validation and wheel refresh

Baseline: `feature/shared-tuflow-verification-110` at `e075b98`, initially clean parent worktree.
Python 3.14.6. Fixed two regression assertions to use the source CRS read from disk: GDAL serializes
FLT/ASC WGS84 PRJ data as OGC:CRS84. The strict CRS rejection behaviour remains correct.

- Hosted-boundary command from `.github/workflows/ci.yml`, with `-q -ra`: initially 1013 passed,
  2 failed, 9 skipped and 5 deselected. Both failures were the corrected assertions.
- `python -m pytest tests/scripts/test_verify_converted_tuflow_rasters.py -q -ra` after the fix:
  53 passed, 6 skipped. The full boundary was not repeated after this test-only correction.
- Additional tests: `python -m pytest tests/functions/gdal tests/orchestrators/gdal
  tests/scripts/gdal/test_gdal_flood_extent.py tests/scripts/test_promoted_utilities.py
  tests/scripts/test_archive_compress.py -q -ra`: 72 passed, 2 skipped.
  All skips across these runs concern unavailable Windows symlink privileges.
- Ruff lint/format and strict Pyright passed for all 24 first-party Python files changed against local `main`;
  repeated successfully for the corrected test file.
- Documentation and Loguru policy checks passed. Python 3.13 is unavailable locally;
  its compatibility/MCP CI job was not reproduced. Hosted CI was not inspected in this session.
- `python repo-scripts/build_library.py --skip-pip` produced version `26.10.15.4`.
  `--no-bump --skip-pip` rebuild preserved the version. `python repo-scripts/verify_wheel.py`
  passed and the final retained wheel passed an isolated `pip --target` installation and
  `smoke_test_installed_wheel.py --expected-root` check outside the checkout.
  Final wheel SHA-256: `1801cdfbd4c206fc4436238d37b7c82b547d1c86ed5eebd7c869ced1c33320b8`.

Delivery: metadata, replacement wheel, corrected test and this handoff are uncommitted and unstaged.
Next action: review and commit the refresh, then confirm hosted checks on the resulting PR head.
Next review: 2026-10-22. No commit, push or merge was requested.
