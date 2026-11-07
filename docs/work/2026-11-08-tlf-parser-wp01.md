# WP01 — TLF parser metadata reliability and diagnostics

| Field | Value |
| --- | --- |
| Status | Implemented in three stacked draft PRs; hosted validation and native acceptance pending |
| Owner | ChatGPT (WP01), for maintainer review |
| Created / updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Starting baseline | `main@5ad632cc9a17b302ad3f4f2b55315a1e2e8d1d51` |
| Allocation | [#240 WP01](https://github.com/Chain-Frost/ryan-tools/issues/240) |
| Requirements | [#239](https://github.com/Chain-Frost/ryan-tools/issues/239), [#224](https://github.com/Chain-Frost/ryan-tools/issues/224), [#231](https://github.com/Chain-Frost/ryan-tools/issues/231) |
| Delivery | [#251](https://github.com/Chain-Frost/ryan-tools/pull/251) (#239 and WP02 integration), [#259](https://github.com/Chain-Frost/ryan-tools/pull/259) (#224), [#263](https://github.com/Chain-Frost/ryan-tools/pull/263) (#231) |

## Scope and implementation

Keep shared TLF metadata parsing in
`ryan_library/functions/parse_tlf.py`; TLFProcessor, log summary and
activity reporting must share existing parsers. Do not duplicate PyTuflow/TPC
or existing mass-balance product readers; do not modify model naming,
unique-run semantics or the failure-state contract in #195/#223.

### Stage 1 — #239 (PR #251)

- Parse hyphen-separated names such as `IL-PEBBLE` while preserving ordinary
  and underscore variable names and excluding event/scenario placeholder keys.
- Keep malformed assignment warnings.
- Assert parser, TLFProcessor and public log-summary paths.
- The combined #251 parent now also includes independently reviewed WP02
  documentation from #234/#98. Its current documented integration intention
  is **one eventual squash merge into main** after other stacked work is
  reviewed and incorporated. Do not re-introduce duplicate WP02 changes.

### Stage 2 — #224 (PR #259, stacked on #251)

- Validate complete signed/exponent tokens for model times, runtime/CPU and
  final mass error; reject malformed and non-finite values without accepting
  prefixes. Do not equate absence of mass error with solver failure.
- Preserve control filenames containing multiple dots, spaces, forward or
  back slashes, UNC/relative/bare paths, and explicit redirect suffixes.
  ECF/TEF are stems without final extension; TGC/TBC/BC_dbase are basenames
  with extension, as in the pre-existing dictionary.
- Accept abbreviated/numeric month timestamps and optional seconds.
- Decode UTF-8/Windows-1252 per complete line, either side of the 10 MiB
  boundary. Large-log headers stream; the tail reads at most 30 KB/100 lines
  and discards an arbitrary partial line at seek boundaries.
- The first full Python CI run found three regressions: two synthetic
  large-log footer-path expectations and an ECF redirect suffix in an
  archived real TLF. All three have been corrected in the #259 branch;
  rerun exact-head tests to validate, do not accept the earlier failing run.
- Merge of concurrent #251 WP02 documentation was recorded as a normal
  two-parent commit in #259; no unrelated history discarded.

### Stage 3 — #231 (PR #263, stacked on #259)

- Parse typed integer main-TLF final-summary WARNING, CHECK,
  1D-negative-depth, HPC-repeat/NaN and quadtree ZC/ZU/ZV/ZH metrics;
  unknown remains missing, not zero.
- Add optional same-folder, same-run-code `*.hpc.tlf` companion scan:
  explicit default **off** in the maintained log-summary wrapper; Python
  callers can opt in per call. Reads complete bounded lines without
  exclusive locks or a competing result/TPC reader.
- Record explicitly labelled active-2D/sealed-wall/groundwater counts
  independently. Do not sum duplicate/multiple-domain counts.
- Label `HPC_nWet_Observed_Max` as the maximum **among sampled rows**,
  alongside sample count; it does not establish a full-solver timestep
  maximum. Include companion status and expected path for provenance,
  incomplete/truncated or changed inputs. Companion absence never discards
  the original completed-run row.
- Regression tests exercise the pinned two Module 11 HPC logs with their
  exact active-cell/sampled-nWet counts, EG15/EG01 main-TLF counters, missing
  companions, zero observations, truncation and multi-domain ambiguity.
  New fields and units are in `column_definitions.json`, with guidance in
  `ryan-scripts/TUFLOW-python/README.md`.
- The initial Ruff failures (complexity and redundant prefix tests) were
  addressed by splitting the companion reader into smaller helpers.
  Exact-head hosted CI still determines acceptance.

## Merge and acceptance gates

1. All three PRs remain **draft**; do not merge or auto-merge until reviewed.
   #263 targets #259, which targets the consolidated #251; integrate
   independently reviewed child changes into the combined parent for
   the intended **single eventual squash merge to main**, not additional
   standalone merges to main.
2. Confirm **each exact-head** Fast PR checks and Python CI, including
   Ruff/format/strict Pyright, hosted Python 3.14 tests, package and
   installed/script smoke. No inference from older green commits.
3. Validate installed/copied-wrapper behavior headlessly and on native
   Windows for real-data parser changes; benchmark optional HPC streaming on
   an SMB/UNC share and with an active writer before enabling it broadly.
4. Existing archived-output fixtures are not licensed solver re-runs;
   sampled HPC maxima and model-size counts are evidence, not automatic
   engineering acceptance thresholds.
5. No release, new solver run, production file changes, dependency repin,
   unrelated refactor or main merge are authorised by this record.

## Next action

Inspect exact-head #259/#263 hosted CI, repair failures, and coordinate
integration of reviewed WP01 children with the existing #251 WP02/WP06
stack. Keep the optional HPC companion setting off until native share
performance/active-writer acceptance is recorded.
