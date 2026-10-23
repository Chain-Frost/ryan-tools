# Native TUFLOW benchmark: explicit graph comparison

| Field | Value |
| --- | --- |
| Status | Active; initial hosted Windows acceptance passed, review fix undergoing final validation |
| Owner | ChatGPT |
| Created / Updated | 2026-10-23 |
| Next review | 2026-10-30 |
| Parser baseline | `e162c04330d24c37bee3ade94479cc4602651060` — PyTUFLOW PR #12 merged into `master` |
| Consumer baseline | `bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2` — PR #143 merged into `main` |
| Next action | Complete clean-source acceptance fix and final Windows archive/review gates, then give the owner the frozen native command |

Both merged trees match the validated source pair exactly: parser
`126ec64e221908f5327ee9dc9d372c8084afaf12` and consumer
`fa6ae1ef4a524336277e974a59220fd513c35c75`. This draft branches directly from the
consumer merge. Record the actual Git revisions used for each native trial.

## Outcome and current boundary

The supported repository regression harness now measures graph-off versus
graph-on at the **same parser source**, alongside its existing correctness
baselines. `run_local_checks --batch` still compares eager, individual
deferred and default batch collection. That is useful baseline evidence but
does not isolate the graph's contribution. The new explicit
`--batch --graph-compare` enables that comparison. Native timing remains pending;
this record contains no native performance claim.

PyTUFLOW's public `TCF.collect_batch_dependencies()` accepts `compile_tree` and
`cache_scopes`. The consumer's public `check_simulation_inputs()` accepts
`context_mode="individual"` or `"batch"`, but exposes neither a graph switch
nor a model-factory argument. Respect this distinction:

| Implemented diagnostic mode | Public parser call |
| --- | --- |
| Eager | `model.context(flags, file_resolution="eager")` |
| Individual deferred | `model.context(flags, file_resolution="deferred")` |
| Batch graph-off | `model.collect_batch_dependencies(flags, cache_scopes=True, compile_tree=False)` |
| Batch graph-on | Same call with `compile_tree=True` |

Graph-off retains the other batch caches. It is not an uncached baseline.
Keep the separate, untimed public individual/default-batch preflight gate,
including raster-header validation, matching checked paths and matching issues.
Label its batch graph policy as the parser default; **do not claim graph-off/on
public-preflight timing or validation**. Both diagnostic graph modes still run
the shared materialisation and physical inventory stages. Per-graph control of
the full public preflight would require a separately reviewed public consumer
API; private parser patching or replacement of imported `TCF` is not acceptable.

## Self-contained agent prompt

Implement this draft in `ryan-tools` on the recorded consumer baseline. Read
[AGENTS.md](../../AGENTS.md), the [development guide](../DEVELOPMENT_GUIDE.md),
[work tracking](../WORK_TRACKING.md) and the
[harness contract](../../tests/fixtures/tuflow_example_repro/README.md).
Use the normal Windows Python 3.14 environment. Classify this as repository
regression tooling, excluded from the package wheel and installed wrapper
catalogue; no library migration or production default/pin change is requested.

Primary files are `tests/fixtures/tuflow_example_repro/run_local_checks.py`,
`run_checks.py`, their nearby tests and README. Add a focused
`test_run_local_checks.py` if needed. Read
`ryan_library/orchestrators/tuflow/batch_preflight_adapter.py` and
`input_preflight.py` as consumer contracts; preserve their current production
behaviour. Register this record in `docs/work/README.md` and `docs/README.md`.

1. Add an explicit diagnostic opt-in, proposed `--batch --graph-compare`, which
   selects the four modes above. Preserve existing two/three-mode invocations.
   A diagnostic facade may forward the adapter's one ordered batch call to
   the public parser method with the selected keyword. Do not call parser
   private modules, reconstruct parser proofs or silently omit an unsupported
   keyword. Unknown modes and unsupported APIs must fail clearly. Keep the
   exact event/scenario flags, ordering, duplicate runs and expected-run-count
   checks; changing graph policy must not rewrite parser flags.
2. Use the existing strict `BatchModelView` and complete downstream collection,
   wildcard, database/GIS attribute/table-link materialisation and physical
   file/layer inventory. Retain authoritative reference counts, ordered
   command provenance, database activation ownership and per-run failures.
   Validate UUID links within each projection; across fresh parses compare
   stable ownership/command order/line provenance, not newly generated UUID
   bytes. Compare missing and extra identities for every mode against eager,
   and graph-off against graph-on. Equal counts or partial file sets cannot pass.
3. Preserve original path spellings for I/O, display and SQLite access.
   `.absolute()` remains the ordinary conversion; intentional physical identity,
   containment and imported-module checks still follow links with `.resolve()`.
   Keep one successful physical resolution and GeoPackage layer enumeration
   per exact spelling per `collect()` invocation, independent selector filtering,
   immediate connection closure and failure propagation. Reset caches for every
   model/mode call; do not share them across trials or convert mapped-drive I/O
   into a UNC SQLite URI.
4. Retain first/middle/last gating, full-run coverage and public preflight.
   Missing modes, exceptions, incomplete lineage, collection/inventory findings,
   parity differences or failed public preflight must prevent exit 0. Preserve
   nonzero argument-error behaviour and exit 1 for diagnostic findings.
   `--continue-on-findings` may extend coverage but never relax acceptance;
   `--representative-only` must be labelled incomplete full-workload validation.
5. Time the actual batch call, not just replay of its result. Load a fresh TCF
   per mode/trial, report parse separately, then context/batch and remaining
   materialisation/inventory costs. Keep startup, report writing and the separate
   public-preflight gate outside those collection timings and disclose that
   boundary. Run modes sequentially, rotate order through a complete four-mode
   cycle, and collect at least four trials for a balanced comparison. Record
   storage/cache conditions; do not flush caches or run competing trials.

Retain public parser counters and bounded fallback reasons with explicit mode
labels: contexts constructed/reused, compiled candidates, group and whole-run
reuse, actual constructor attempts/elisions and failed-run work. Eligibility or
cache hits alone do not establish saved construction. Key, clone, certification
and constructor timing fields overlap other phases; never sum them into an
end-to-end total or subtract them as exclusive phases. Preserve audit-event
counts and 20 ms sampled RSS, explaining that they omit native reads and exact
memory peaks. Empty audit counts are not proof of zero I/O.

Do not implement parser append/setting reuse, source mutation invalidation,
consumer production optimisations, dependency repins or launcher changes here.
Do not execute or import launchers, execute TUFLOW, upload model sources, or
change model inputs. `unsorted` and unrelated submodules remain outside scope.

## Validation and acceptance

Add focused executable coverage for explicit True/False forwarding with caches
enabled, one batch call and unchanged flags; unknown/unsupported modes; rotated
orders and labels; timing that includes the call; failed/missing mode detection;
per-run errors and corrupted reference counts/provenance; continued findings
still returning failure; and the accurately labelled public-preflight boundary.
Retain existing alias/link/cache-refresh/SQLite-failure tests in
`tests/fixtures/tuflow_example_repro/test_run_checks.py` and relevant ordered
adapter/preflight tests. Do not broaden the unrelated full suite.

Run Ruff format/check, strict Pyright on modified Python files, compilation,
CLI help and focused tests on Windows Python 3.14. Verify documentation paths
and links. Then require the pinned public archive's complete **161 TCFs / 174
runs** to pass all selected materialised comparisons and the unchanged public
preflight gate, with zero missing/extra identities and no collection/inventory
issues. A selected-case smoke does not replace that gate. Record exact source
heads and terminal CI evidence; documentation alone is not implementation proof.

After public validation, the authorised owner may run the **new SMB model** with
this private manifest, filled locally:

```text
anonymous_label: NEW_SMB
model_path: <PRIVATE_TCF_PATH>
flags_file: <PRIVATE_COMMAND_EXPORT_OR_RUNS_JSON>
expected_runs: <ACTUAL_RUN_COUNT>
distinct_complete_flags: <ACTUAL_DISTINCT_FLAG_COUNT>
parser_sha: e162c04330d24c37bee3ade94479cc4602651060
consumer_parent_sha: bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2
diagnostic_consumer_sha: <IMPLEMENTED_FOLLOWUP_HEAD_SHA>
storage_and_cache_conditions: <ANONYMISED_SMB_DESCRIPTION>
output_directory: <PRIVATE_DIRECTORY_OUTSIDE_REPOSITORIES>
```

Use one exact parser/diagnostic-consumer pair throughout each comparison; later
parser optimisation revisions need separately identified comparisons. Verify
physical import origin and record Git heads, dirty state, Python/version and
relevant source hashes. Both checkouts must be clean for benchmark acceptance.
Preserve exact flags and detailed paths privately;
publish only anonymised counts, timings, counters and source identifiers.
The command export is read as text only. Older M480/N42 results are historical,
not acceptance evidence for this new model or source pair.

## Delivery state

Implementation is on `test/native-graph-benchmark-controls` for PR #158. Changes
are limited to repository regression tooling, its focused tests, one Windows
workflow and documentation. No package code/default/dependency changed and no
package build or release is required. The draft remains open for hosted Windows
acceptance, Codex review and the owner's new SMB workload.

### 2026-10-23 implementation milestone

- Added four explicit public-API diagnostic modes. Graph-off and graph-on both
  use `cache_scopes=True`; unsupported APIs fail without fallback or monkeypatching.
- Kept `BatchModelView` ownership/reference-count validation and complete physical
  materialisation. Added per-run hashes of full public ordered-command/source-line
  provenance and deferred-reference fields; hashes exclude random input UUID bytes.
  Database activation ownership is checked against its ordered command.
- Shared fail-closed comparisons detect missing/error modes, equal-count identity
  differences, incomplete lineage and collection/inventory findings. Failed
  contexts retain their requested run indices. Continued diagnostics still exit 1.
- Added balanced full-trial rotation (default four, multiples of four required),
  explicit storage/cache notes, actual Git revisions/dirty states, bounded fallback
  counters and failed-mode timing. Overlapping parser counters remain separate.
- Kept public individual/default-batch preflight untimed and labelled as the
  parser default. Representative-only success remains incomplete full validation.
  Reused output directories start with non-accepted summary/preflight status.
- Updated the Windows ordered-batch workflow to pin merged parser `e162c04`, run
  the complete 161-TCF/174-run four-mode archive gate, and check changed Python
  with Ruff, strict Pyright, compilation and focused tests.

Local validation uses isolated Python **3.14.8** on Linux for focused logic only;
Windows remains the acceptance environment. At this milestone:

- 58 new diagnostic tests pass; the combined harness/ordered/batch selection
  passes **83 tests**, including the opt-in real-parser integration test.
- Ruff format/check, strict Pyright (0 errors/warnings) and compilation pass;
  both CLI controls are available in `--help`.
- A real-parser synthetic nested-TGC/scenario workload with duplicate run flags
  passes the representative gate and four full rotated trials, all materialised
  and ordered-provenance comparisons, and the separate public-preflight gate.
  This is diagnostic validation, not native performance evidence.

Focused commands (from the repository root):

```text
python -m pytest -q tests/fixtures/tuflow_example_repro/test_run_checks.py tests/fixtures/tuflow_example_repro/test_run_local_checks.py tests/orchestrators/tuflow/test_ordered_batch_adapter.py tests/orchestrators/tuflow/test_batch_preflight_adapter.py
python -m ruff check tests/fixtures/tuflow_example_repro/run_checks.py tests/fixtures/tuflow_example_repro/run_local_checks.py tests/fixtures/tuflow_example_repro/test_run_local_checks.py
python -m pyright tests/fixtures/tuflow_example_repro/run_checks.py tests/fixtures/tuflow_example_repro/run_local_checks.py tests/fixtures/tuflow_example_repro/test_run_local_checks.py
python -m tests.fixtures.tuflow_example_repro.run_checks --batch --graph-compare --pytuflow-checkout <frozen-parser-checkout> --output <public-report-directory>
```

The complete Windows archive and exact-head review results must be recorded
before recommending the native run. Use the implemented command in the
[harness README](../../tests/fixtures/tuflow_example_repro/README.md), with
`--batch --graph-compare --repeats 4 --storage-and-cache-conditions <description>`.
Compare each parser optimisation independently against the frozen baseline,
using the same final diagnostic commit. No merge or publication is authorised.

### 2026-10-23 review follow-up

Initial implementation `9cd839247c430dbd9a2ec4a7e2187f1e80cea91d` passed all five
hosted workflows, including the complete Windows **161-TCF / 174-run**
[four-mode ordered-batch gate](https://github.com/Chain-Frost/ryan-tools/actions/runs/39908449707).
Codex identified one reproducibility issue: recording a dirty status did not
identify every modified source byte, yet a run could be marked accepted.

The follow-up requires clean parser and consumer checkouts for benchmark
acceptance. Graph comparison stops before measured collection on a dirty source;
explicit `--continue-on-findings` still completes diagnostic phases but exits 1.
Legacy two/three-mode validation retains its exit behaviour while also refusing
benchmark acceptance for dirty source. `sources_clean` and `source_findings` are
recorded in provenance and summaries, including early failures. The final head
requires fresh Windows validation and review before the native command is frozen.
The local Python 3.14.8 combined selection passes **93 tests**, including ten
new source-acceptance regression cases (68 diagnostic tests in total). Ruff,
strict Pyright and whitespace checks pass. The new tests cover both dirty
repositories, early report replacement, continued full diagnostics and retained
legacy exit behaviour.
