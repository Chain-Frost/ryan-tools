# Native TUFLOW benchmark: explicit graph comparison

| Field | Value |
| --- | --- |
| Status | Complete; frozen diagnostic and 480-run native four-mode baseline accepted; PR delivery remains open |
| Owner | ChatGPT |
| Created / Updated | 2026-10-23 / 2026-10-25 |
| Next review | 2026-11-01 for outstanding PR delivery only |
| Parser baseline | `e162c04330d24c37bee3ade94479cc4602651060` — PyTUFLOW PR #12 merged into `master` |
| Consumer baseline | `bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2` — PR #143 merged into `main` |
| Frozen benchmark diagnostic | `f1f7958b79f3ffe84ba8ac0b04658f27c311460a` — all five Windows workflows passed, Codex review clean |
| Next action | Review PR #158 for delivery using the accepted baseline; do not repeat it. Keep parser instrumentation, optimisation candidates and runner #159 as separate follow-ups. |

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
`--batch --graph-compare` enables that comparison. The owner's
[accepted four-mode native baseline](https://github.com/Chain-Frost/ryan-tools/pull/158#issuecomment-6367217192)
now completes this scope at the frozen diagnostic/parser pair. All correctness
gates passed; the graph produced no demonstrated total-time benefit on this
workload. Reuse this evidence rather than requesting the same baseline again.
The older three-mode result remains historical context.

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

## Original implementation prompt (completed)

The implementation and native baseline requested below are complete. Retain
this contract for maintenance and candidate comparisons; it is not an instruction
to reimplement the harness or repeat the accepted baseline.

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
Do not execute or import launchers, execute TUFLOW, upload additional model sources, or
change model inputs. The owner's later publication approval is limited to the reduced
context bundle recorded below. `unsorted` and unrelated submodules remain outside scope.

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

The following private manifest records the accepted **new SMB model** protocol.
The baseline has completed; retaining this template does not request another run:

```text
anonymous_label: NEW_SMB
model_path: <PRIVATE_TCF_PATH>
flags_file: <PRIVATE_COMMAND_EXPORT_OR_RUNS_JSON>
expected_runs: 480
distinct_complete_flags: <ACTUAL_DISTINCT_FLAG_COUNT>
parser_sha: e162c04330d24c37bee3ade94479cc4602651060
consumer_parent_sha: bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2
diagnostic_consumer_sha: f1f7958b79f3ffe84ba8ac0b04658f27c311460a
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
package build or release is required. The executable diagnostic is pushed and
validated at `f1f7958b79f3ffe84ba8ac0b04658f27c311460a`; this later documentation
update records readiness and the owner's baseline without changing executable
code. Keep that frozen consumer revision for every native comparison. The native
four-mode baseline is now accepted. PR #158 remains open for delivery; this
documentation reconciliation does not merge or publish a package. Parser
optimisation results and runner acceptance are separate from this PR's completed
diagnostic scope.

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
using the same frozen diagnostic commit. No merge or package publication is authorised.

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
recorded in provenance and summaries, including early failures.
The local Python 3.14.8 combined selection passes **93 tests**, including ten
new source-acceptance regression cases (68 diagnostic tests in total). Ruff,
strict Pyright and whitespace checks pass. The new tests cover both dirty
repositories, early report replacement, continued full diagnostics and retained
legacy exit behaviour.

The final executable implementation at
`f1f7958b79f3ffe84ba8ac0b04658f27c311460a` passed all five hosted workflows:

- [Ordered contracts and full Windows 161-TCF / 174-run four-mode parity](https://github.com/Chain-Frost/ryan-tools/actions/runs/39909552834).
- [Established API / graph candidate batch matrix](https://github.com/Chain-Frost/ryan-tools/actions/runs/39909552823).
- [Deferred consumer contracts](https://github.com/Chain-Frost/ryan-tools/actions/runs/39909552802).
- [Python CI](https://github.com/Chain-Frost/ryan-tools/actions/runs/39909552829).
- [Fast PR checks](https://github.com/Chain-Frost/ryan-tools/actions/runs/39909552839).

[Refreshed Codex review](https://github.com/Chain-Frost/ryan-tools/pull/158#issuecomment-6358476440)
completed with no findings at that revision. A fresh synthetic exported-command
smoke also passed all four full trials and public preflight with clean sources
and exact parser/consumer revisions in its report. **The baseline native graph
comparison is ready at this frozen executable revision.**

### Owner's fixed SMB workload baseline

The owner posted the anonymised
[three-mode baseline](https://github.com/Chain-Frost/ryan-tools/pull/158#issuecomment-6358663146)
on 2026-10-23. The new model and **480-row run list are now fixed** for subsequent
comparisons. This uses clean merged parser `e162c04330d24c37bee3ade94479cc4602651060`
and consumer `bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2` on Windows Python 3.14.5.
Representative checks and three complete eager/deferred/default-batch trials
passed: **8,160 identities, 158 unique files**, zero missing/extra identities,
collection/inventory issues, and matching successful public preflight.

Median totals were eager **571.425 s**, individual deferred **40.766 s**, and
default batch **32.097 s**. Retain the complete timings in the linked comment:
batch varied from **31.743 to 65.424 s**. Initial client/server cache state and
server competing activity were unknown; later trials followed the gate and
earlier reads. These observations are not cold-cache measurements and do not
identify the cause of the variability.

The default batch reported 23,041 constructed input states and 479 elisions,
with one reusable group, no whole-projection reuse and no failed-run construction.
That is bounded component evidence, not a measurement of graph contribution.
Every subsequent full comparison must use this same model/run list with **four**
balanced full trials and explicit graph-off/on modes. The runner records the
distinct complete flag count automatically. Keep original project paths and
detailed reports private. The follow-up below updates the next-action sequence.

### 2026-10-23 context bundle and optimisation-scope review

The owner authorised publication of the reduced control-file bundle, excluding
GIS/raster/RF data. The SMB agent uploaded it in
[`5cb16b9e6cb7bc66a9c3dd1599cf96ed9c3759f3`](https://github.com/Chain-Frost/ryan-tools/commit/5cb16b9e6cb7bc66a9c3dd1599cf96ed9c3759f3)
and posted the [upload receipt](https://github.com/Chain-Frost/ryan-tools/pull/158#issuecomment-6359851720).
That commit adds only the ZIP; the executable diagnostic remains frozen at
`f1f7958b79f3ffe84ba8ac0b04658f27c311460a`.

The orchestrator fetched the uploaded commit, verified the **8,203-byte** archive
against SHA-256 `a361b7715224b3453942cf8d96623a420b819a7859c0caf5f8f5eb374a83c70f`,
and inspected all **nine text files** without executing the launcher. They are
the launcher, four control files, BC database, two material CSVs and reference
manifest. The manifest lists seven GeoPackage/layer references and two raster
references; their data files are absent. No RF inflows, results or logs are
included. This approval does not extend to other private inputs or reports.

Static inspection and independent parser-source reviews established:

| Item | Verified result / implication |
| --- | --- |
| Run grid | 480 distinct complete scenario/event flag sets: 6 AEPs x 8 durations x 10 temporal patterns. Model, cell-size and climate selections are fixed. |
| TGC | Seven active commands; only `VarCellSize` is substituted, and its selected value is `30` throughout this launcher. |
| TBC | Two literal GIS commands with no variable substitutions in the supplied text. |
| TEF | Event substitutions and duration-dependent `End Time` vary; it is not an invariant child. |
| BC Source | Three nonblank file rows depend on AEP and duration: 48 ordered row-set signatures, each used ten times, producing 144 distinct candidate paths. |
| BC Column 2 | Temporal-pattern and climate tokens are separate dependencies; the complete database row is not equivalent across all runs. |
| Inventory arithmetic | The text is consistent with 17 references per run, 8,160 run/reference pairs and 158 distinct syntactic paths. Physical inventory and GIS row selection still require the original native inputs. |

The frozen parser reuses parsed child build states but invokes child `context()`
again for each run. Existing certified input-group reuse does not cover complete
TGC/TBC subtrees. The proposed **958** avoided child evaluations is an upper bound
of two times 479 successful reuses, not an elapsed-time estimate. A subtree key
containing every active event or complete flag set would create 480 distinct
keys here and defeat that reuse. Admission must prove the transitive dependency
set, then key only the relevant scopes, substitutions and winning declaration
provenance, along with source/configuration identity and resolution policy.

Database Source projection is separate from the existing primary-reference
cache. The proposed **432** reuses count repeated three-row projections; they
correspond to up to **1,296** repeated individual row projections. A Source cache
must preserve selected database identity, its relative-path root, ordered row
identity, configuration and current activation/command ownership. It must not
reuse complete database rows or infer that identical paths share provenance.
This phase projects cached rows without opening RF contents, so the opportunity
is CPU projection work, not eliminated RF data reads.

The owner's [profile](https://github.com/Chain-Frost/ryan-tools/pull/158#issuecomment-6359626851)
reports 23,041 input-state constructions, 196,346 command initialisations and
221,788 configuration copies. The broader profiler counts include work outside
the public batch command-factory counter; they are not interchangeable metrics.
The public 21,125 constructed plus 1,916 cloned commands describe 23,041 factory
outcomes, while temporary commands from `parts()` and reference/classification
work can initialise additional commands. Base/subclass profiler rows also need
careful attribution before treating calls as distinct allocations.
Cumulative timings nest and overlap, and the profile itself adds overhead.
They locate investigation targets but do not establish exclusive phase costs
or predict the benefit of either cache. No raw profile is included in the ZIP.
First inspect any existing profile's self-time and caller tables; request only
sanitised extracts, with its invocation boundary and source revisions. Do not
replay the full workload just to obtain that existing evidence.

Keep three follow-up scopes separately measurable:

1. **Instrumentation only:** opt-in exclusive timing and call counts for scope
   evaluation, translation, command construction, configuration copying,
   reference projection and recursion overhead, attributed to TCF/TGC/TBC/TEF.
   Keep inclusive subtree totals separate; child time must not also count as
   exclusive parent time. Operation and control-file views describe the same
   elapsed work and must not be added together. Include context bootstrap/event
   loading, instrument generator consumption rather than creation, and accumulate
   failures through `finally`. Measure diagnostic overhead and retain uninstrumented
   end-to-end comparisons. If a replay is needed, six existing launcher rows can
   cover a same-AEP/duration TP pair and the five actual `End Time` values
   (8, 12, 20, 40, 60), with graph explicitly off/on and caches enabled. This is a
   diagnostic subset, not a replacement for full native acceptance.
2. **Invariant subtree reuse:** prove admission and relevant dependency keys,
   use immutable or fresh projections, and measure construction avoided against
   key/certification/projection cost. Preserve mutable-state isolation, command
   order, UUID ownership, parent provenance and successful-run certification.
3. **Database Source reuse:** measure the Source row pass separately from
   activation and provenance wrapping, then evaluate a narrow independent cache.
   Preserve active-database selection, failure isolation and conservative fallback.

All candidates still require complete eager/deferred/graph-off/graph-on parity,
public-preflight equality, mutation/failure coverage and clean source provenance.
The reduced ZIP is suitable for parser/context design; missing spatial and RF
files deliberately prevent it from replacing native materialisation/preflight
acceptance. Do not add dummy files or relax those gates to make this bundle pass.

The current review introduced no parser or runner implementation. Existing
append-accounting #14 and literal-setting #15 remain independent candidates;
the ZIP upload does not invalidate accepted results. Preserve any already-running
or completed measurements. Hold additional costly full launches while preparing
the instrumentation scope; do not combine the two new cache proposals initially.
The readiness watch now tracks #14's outstanding code review without issuing
an automatic native rerun prompt. This record and the work-register update are
documentation-only; no new native benchmark result is claimed.

### 2026-10-23 accepted native four-mode baseline

The owner published the
[sanitised aggregate result](https://github.com/Chain-Frost/ryan-tools/pull/158#issuecomment-6367217192)
and subsequently revalidated the retained local reports without launching a
duplicate benchmark. This is owner-run evidence; the orchestrator reviewed the
published aggregates and source counters, not the private reports themselves.

- Diagnostic: `f1f7958b79f3ffe84ba8ac0b04658f27c311460a`.
- Parser: `e162c04330d24c37bee3ade94479cc4602651060`.
- Windows CPython 3.14.5, 64-bit AMD64; process exit 0; both detached source
  checkouts clean before and after execution.
- 480 runs and 480 distinct complete flag sets; 8,160 materialised identities
  and 158 unique files.
- Representative gate and all four rotated full trials passed, with zero
  differences, collection/inventory issues and complete command/reference
  provenance parity.
- The separate untimed public individual/default-batch preflight passed with
  equal checked paths/issues. It remains labelled as the parser default and
  does not expose graph-off/on preflight or background-runner measurements.
- Final summary reports completed, passed, full-workload validation, clean
  sources and benchmark acceptance all true. Raw reports remain private.

| Mode | Full-trial median | Minimum–maximum |
| --- | ---: | ---: |
| Eager | 117.715532 s | 116.590263–120.534715 s |
| Individual deferred | 37.744556 s | 37.692232–38.058664 s |
| Batch graph-off | 30.619692 s | 30.460371–30.898996 s |
| Batch graph-on | 30.724459 s | 30.512430–30.850581 s |

Graph-on's median is 0.104767 s (0.342%) higher. Per-trial changes have both
signs, ranging from 0.570% slower to 0.157% faster. These four trials demonstrate
no practical elapsed-time benefit from the incremental graph feature on this
workload. They do not establish a universal regression or predict another model.
The published comment retains every full-trial time and mode order.

The graph is built and used: 110 nodes, 1,921 command-plan graph lookups and one
certified reusable input group. Graph-on avoids 479 ordinary input-state
initialisations out of 23,520 (2.037%), leaving 23,041. Both modes still report
21,125 command constructions and 480 constructed contexts; whole-projection reuse
is zero. Graph-off already retains the other batch caches.

The command-clone counter falls from 2,395 to 1,916 because group reuse bypasses
the instrumented command-factory path while independently copying the command
inside the group. It does not prove 479 fewer actual command copies. See the
pinned [batch counters](https://github.com/phantom-branch/PyTuflow/blob/e162c04330d24c37bee3ade94479cc4602651060/pytuflow/_tmf/batch_dependencies.py)
and [group-copy implementation](https://github.com/phantom-branch/PyTuflow/blob/e162c04330d24c37bee3ade94479cc4602651060/pytuflow/_tmf/group_input_reuse.py).
Overlapping internal timer fields must remain separate.

Initial SMB cache state and server load were unknown; earlier reads had occurred
and caches were not flushed. Do not treat these as cold-cache measurements or
attribute the changed eager timings relative to the older three-mode capture to
one optimisation. Use comparisons within each controlled measurement set.

#### Handoff after acceptance

1. Complete PR #158's delivery independently of parser speed improvements. Its
   diagnostic is successful when it measures parity and performance accurately;
   a graph speedup is not an acceptance requirement. Preserve the frozen
   executable revision and accepted baseline; no repeat baseline is requested.
2. Extract any needed bypass reasons, existing phase timings and cache counters
   from the retained reports before requesting another workload execution.
   Prepare exclusive cost attribution in a separate PyTUFLOW work front, following
   the instrumentation boundaries above. Keep invariant subtree reuse and
   database Source reuse as independent optimisation scopes.
3. Keep parser [#14](https://github.com/phantom-branch/PyTuflow/pull/14) and
   [#15](https://github.com/phantom-branch/PyTuflow/pull/15) independent. Their native
   benefits are still unmeasured by this baseline; verify each candidate's own
   current review/checks before a separately requested comparison. Do not merge
   the candidate branches together to obtain one ambiguous timing result.
4. Runner [#159](https://github.com/Chain-Frost/ryan-tools/pull/159) retains its own
   advisory scheduling, lifecycle, warning-delivery and GPU acceptance criteria.
   This benchmark does not validate those behaviours or complete that scope.

This reconciliation changes documentation only. The diagnostic, parser,
production consumer and runner are unchanged; no additional private artifacts,
benchmark run, solver launch, package build or merge is part of it.


## 2026-10-25 - Final review and exact-head delivery checkpoint

Live PR #158 is draft/open on `test/native-graph-benchmark-controls` at
`742b1d9481e25a9ed306bab524ce4da0fde2ab3b`, based on main
`bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2`. Review covered the current diff,
public graph forwarding, complete materialisation/provenance, failure acceptance,
source cleanliness and report lifecycle. No unresolved consequential code finding;
the one inline review thread is resolved. This is a local review, not a posted approval.
All executable diagnostic Python files are byte-identical to frozen `f1f7958`.

All five latest workflows and all **nine jobs** are terminal/success at this head:

| Workflow | Run | Successful jobs |
| --- | --- | --- |
| Python CI | [39974614943](https://github.com/Chain-Frost/ryan-tools/actions/runs/39974614943) | Policy; Hosted tests; Package; Python 3.13 compatibility and MCP |
| Fast PR checks | [39974615165](https://github.com/Chain-Frost/ryan-tools/actions/runs/39974615165) | Ruff and whitespace |
| Deferred consumer | [39974614869](https://github.com/Chain-Frost/ryan-tools/actions/runs/39974614869) | fork-consumer-smoke |
| Batch matrix | [39974614399](https://github.com/Chain-Frost/ryan-tools/actions/runs/39974614399) | established-api; graph-pr-12 |
| Ordered/four-mode archive | [39974614492](https://github.com/Chain-Frost/ryan-tools/actions/runs/39974614492) | windows-ordered-batch, including all 161 TCFs / 174 runs |

Live main rules require Policy, Hosted tests, Package, Python 3.13 compatibility
and MCP, and Ruff and whitespace, with strict current-base checks and resolved
review threads. All five pass. Hosted tests log: **1,465 passed, 6 skipped,
7 deselected**. Package includes fresh no-bump build, verification and isolated
installed-wheel smoke. These results cover the named remote head only.

Normal Windows Python 3.14.6 local verification:

- `python -m pytest -q tests/fixtures/tuflow_example_repro/test_run_local_checks.py tests/fixtures/tuflow_example_repro/test_run_checks.py tests/orchestrators/tuflow/test_batch_preflight_adapter.py`:
  **80 passed, 2 skipped**. Skips require opt-in fork integration; installed parser is PyTUFLOW 1.2.6.
- Ruff check/format and strict Pyright on `run_checks.py`, `run_local_checks.py`
  and `test_run_local_checks.py`: passed, zero typing errors/warnings.
- Documentation/index and whitespace validation: recorded below after reconciliation.

Preserve accepted 480-run / 8,160-identity / 158-file four-trial evidence and
frozen diagnostic/parser identities. The separate SMB_PC2 480-run /
12,480-identity / 49-file result retains three completed trials and an incomplete,
unaccepted four-trial summary. No benchmark or solver was launched here.

Independent delivery: [#160](https://github.com/Chain-Frost/ryan-tools/pull/160)
is not a runtime dependency on this diagnostic. Shared register conflicts must
retain its own row and the already-merged #143/#156 closure. Runner
[#159](https://github.com/Chain-Frost/ryan-tools/pull/159) stays a scaffold until
#158 lands; then carry only its four own documentation changes onto current main
before implementing its advisory lifecycle. Do not copy benchmark commits through
a squash rebase. Experimental [#161](https://github.com/Chain-Frost/ryan-tools/pull/161)
and unrelated [#145](https://github.com/Chain-Frost/ryan-tools/pull/145) retain
independent validation and acceptance gates and do not block these deliveries.

Delivery: documentation-only edits are unstaged/uncommitted in detached review
worktree `E:/Github/ryan-tools-review-158` at the exact head above. Original checkout
`E:/Github/ryan-tools` remains on its independent #161 branch and is unchanged.
No stage, commit, push, merge, publish or GitHub comment was performed. No wheel
rebuild is needed for these documentation-only edits. Next action: review and
authorize delivery of the documentation patch, require checks on the resulting
head, then make the explicit #158 merge/release decision. Next review: 2026-11-01.

Unrelated due reviews reported without starting them: vendor #139 (2026-10-21),
installed scripts/distribution rename and release automation #135 (2026-10-19),
floodway exclusions, safe GeoTIFF and shared filesystem #116 (2026-10-22), floodway
design (2026-10-13), and culvert workflow (no substantive update for over 14 days).
Their records/dates are preserved; reading the register is not a fresh review.


### Verified squash-merge and release boundary - 2026-10-25

Read current-main `.github/workflows/auto-publish.yml`,
`.github/workflows/pypi-publish.yml` and `repo-scripts/release_skip_policy.py`;
these files are unchanged by this PR. A successful post-merge main push Python CI
can initiate the automatic tag/release workflow, then trusted PyPI publication.
Green PR checks alone neither publish nor authorize a release.

Live repository allows squash merges only; ordinary merge and rebase merge are
disabled. Both #158 and #160 have CLEAN remote merge state against current main
`bcc8e5c2fee78c706e2b89feb051cded5cf8d2c2`; local `git merge-tree --write-tree
origin/main HEAD` also exits 0 without touching the working tree or index.
Both remain drafts, so an authorized delivery must also mark each ready separately.

Exact publication opt-out: retain/add the case-insensitive `skip-release` PR label
before the merge and retain it through post-merge release processing; include the
literal `[skip release]` in the final squash commit title/body as the immutable
backstop. Both automatic release selection and the final publisher honor either
condition. The repository uses COMMIT_MESSAGES for the default squash body, so
inspect/edit the actual final squash message rather than assuming PR prose is
included. Do not change global automation. Recheck label, exact head, main tip,
required checks, draft/merge state and unresolved threads immediately before any
separately authorized squash merge.

Live #158 already has `skip-release` (and `docs`); #160 has no labels. Therefore
#158 currently opts out, while a #160 main merge with no opt-out could publish.
Recommend explicit suppression for these independent deliveries unless the owner
separately chooses publication. No labels, PR titles, commit messages or release
settings were changed. Local policy checks confirm label/marker each return skip,
and absence of both returns publish.


### Documentation validation and delivery state

Normal Windows Python 3.14.6: `python repo-scripts/check_documentation.py`
passes all seven default documents and central-index coverage. Explicit
`--links-only` checks pass for every locally changed Markdown file in this
review worktree; `git diff --check` passes. All local changes are documentation
only, unstaged and uncommitted. No new remote head or hosted run exists for
these corrections. Revalidate the exact delivered head after authorized commit/push.


Automated-review revision precision: the latest clean Codex receipt is for frozen
`f1f7958b79f3ffe84ba8ac0b04658f27c311460a`, not current documentation/fixture
head `742b1d9`. Executable diagnostic files are unchanged, and current-head local
review found no consequential issue; do not describe that earlier automated
receipt as a fresh current-head Codex review. A fresh automated review request
is an optional separate authorized GitHub action, not a required approving-review
rule (the repository requires zero approvals and resolved conversations).


### Authorized local commit delivery

The owner subsequently authorized local commits. This work is retained separately
on local branch `review/pr158-final-review` in the review worktree above.
Local validation is complete; the completion report identifies the resulting commit.
No push or new hosted run exists. Earlier uncommitted/scaffold-only delivery
statements are historical checkpoints, superseded by this authorized local delivery.
Next action: obtain authorization to push this existing PR's changes, then require
CI/review on its exact delivered head. No merge, publication or GitHub discussion
mutation is authorized. Original `E:/Github/ryan-tools` remains unchanged.


### Current validation priority

The owner explicitly deferred hosted CI until local development has progressed.
Hosted checks are not on this work's current critical path. Continue local review,
implementation and proportionate tests; record exact local source and validation.
An authorized future push may run CI for later checking. Earlier statements making
new-head hosted checks an immediate development blocker are superseded by this
instruction. Push, merge and publication remain separate authorization boundaries.
Generated `.validation/` evidence is excluded locally and is not part of the commit.


### Authorized PR-branch push

The owner authorized pushing completed work. Reviewed local change commit
`3cac7c193a15f7891049f527e0afde33ae91f94f` and this delivery note are delivered independently to existing PR
branch `test/native-graph-benchmark-controls`. The remote was fetched immediately before delivery and
matched the expected parent; use an ordinary fast-forward push, never force.
Earlier unpushed/push-authorization-pending statements are historical checkpoints.
Hosted CI may run after the push and will be checked later, per owner direction;
no earlier-head hosted checks are claimed to validate the new head. Original
checkout source/index remain unchanged. No merge, release, GitHub comment or
native benchmark/solver execution is authorized by this delivery.
