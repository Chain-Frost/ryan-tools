# Repository Python lint, typing and import-isolation cleanup

| Field | Value |
| --- | --- |
| Status | Needs review; upstream typing adoption and clean-CI dependency repair pass local acceptance |
| Owner | ChatGPT / maintainer |
| Created / updated | 2026-11-08 |
| Next review | 2026-11-15 |
| Baseline | #251 at da08c477ddb52af2837d1b7f4cbe69dad42e5611; main checkout explicitly selected by user |

## Scope and current state

User requested full Windows testing, repository-wide Ruff/Pyright checks, then correction including older utilities/tests. The initialiser must follow the same maintained-code standard and shared typing configuration; GIS dependency stubs do not justify lower strictness. No submodule/vendor/unsorted inspection is authorized.

Baseline: 675 tracked first-party Python/stub files. Root-config strict audit: 3,175 errors in 111 files, including initialiser errors caused by using the narrow shared rasterio stub rather than the initialiser's richer GIS surface. The separate initialiser profile passed 38 files, but will be consolidated rather than reported as a whole-repository pass. Also correct script-directory import resolution in the audit configuration before classifying missing imports as code defects.

Ruff baseline: 13 diagnostics in four files and one format failure. Current repairs align the target to Python 3.14, preserve public third-party stub names through exact rule exceptions, order point-cloud imports correctly, and format the 31 files affected by Python 3.14 formatting. All 31 formatting changes are AST-equivalent. A single quoted annotation was also corrected.

Full native tests at 741803bc: 3,074 pass / 41 skip / one failure. The source-import test hid required user-site Loguru when disabling user-site. It now retains required dependencies while the probe still denies every forbidden optional import and asserts physical source origins; focused test passes. Wheel and copied-wrapper smoke passed after completing disposable dependency targets. Current-head 57 CI-scope tests and pandas 2.3.3 compatibility 64 tests passed. #251 exact-head hosted CI was six green jobs before these local repairs.

## Next action

Verify all required checks at the latest pushed #251 commit and record subsequent hosted outcomes in that PR's description, without extra source commits solely to record CI. Keep #284's independent no-data regression contract on its existing child branch; no merge into main is authorized.

## Evidence and delivery

Detailed baseline logs, complete diagnostics, AST checks and native reports are retained privately at E:/validation/pr251-full-20261108. Shared-profile checkpoint 169484c8 is pushed to #251. The following cleanup is local until the delivery entry below records its commit and push. No main merge, release or issue closure.


## 2026-11-08 shared typing and Ruff checkpoint

Removed pyright-initialiser.json and its CI split. Promoted the tested affine,
Fiona and rasterio declarations to shared typings, preserving the existing
fixture APIs and adding actual masked-band/window signatures used by profiling.
Retired duplicate initialiser stubs; the installed PyTuflow API remains authoritative.
The 38-file initialiser selection now passes through the normal root strict config.
Checkpoint Python sources/stubs pass strict Pyright, and **114 focused native
initialiser/profiling/import tests pass**. A native optional-HDF diagnostic emitted
a C-stack trace during the focused run; pytest completed successfully, so this is
retained in the log rather than relabeled a failed test.

Built/verified 26.11.8.5 (SHA256
7a01dbc48fb1b82e0f6fbb30e950958f7df9ee3be9adf10ddb503b1db1351339).
Whole-repository diagnostics fell from 3,175 to 1,316 after shared stubs,
correct script import paths and 214 concrete fixture/mock/literal annotations.
Further fixture typing is local and not part of this first checkpoint. Full
Windows tests are running; no whole-repository clean claim is made. The linked
#284 repair is pushed at 0e1be2bb, with 33 focused passes and hosted CI pending.

## 2026-11-08 whole-repository cleanup checkpoint

The explicit inventory now contains 674 first-party Python/stub files, excluding
independent vendor/resource/test-data content and the forbidden `unsorted`
submodule. Whole-inventory Ruff lint/format and strict Pyright pass. Modified
files also pass the normal root Pyright CLI; this is not an initialiser-specific
configuration. The profile now reports redundant ignore comments, and 111
confirmed unnecessary ignore rules/comments have been removed. Concrete test
fixtures, NumPy arrays, CAD geometry and synchronous Dask results are annotated;
untyped native GDAL calls have bounded local protocols. The remaining exceptions
are individual external calls with incomplete optional-keyword annotations and
private-helper access in unit tests, rather than lower typing strictness for an
area of the repository.

The declared development environment includes the tested minimum Pyright
1.1.414 and pandas-stubs 3.1.0.261029, SciPy/process stubs, and dependencies used
by the existing Dask/HEIF utilities. Matplotlib uses its own published typing.
Shared stubs cover actual Rasterio/Fiona APIs and scaled LAS array views.

The Ubuntu preflight failures at 169484c8 were ten shebang-bearing scripts
missing executable Git modes. All ten modes are corrected; the complete
first-party shebang/index-mode audit reports no remaining mismatches.

Runtime corrections are limited to missing regional parameters in ARF
interpolation and an unbound completion counter for empty CAD drawings.
Existing hydrology coverage adds three missing-parameter cases. Disposable
native checks pass: 48 exact before/after ARF comparisons, three exact equal-area
slope profiles, four empty-DXF entry points, three real LWPOLYLINE extractions,
GDAL raster/OGR vector CRS auditing, and LAS chunk clipping with reopened output.
These checks do not claim acceptance against proprietary drawings or models.
No new MCP entry is warranted for annotation changes or these narrow legacy
edge cases; existing discovery/CLI boundaries remain intact.

The first broad rerun reported 3,073 passes / 41 skips / five failures. One
test still expected a string after its input fixture was corrected to `Path`;
four publication hooks exposed unnecessary positional-to-keyword changes in the
typing cleanup. Restored positional calls preserve those hooks; all 30 affected
tests pass. The final complete suite is being rerun, with no exclusions,
deselections or early-stop option.

Rebuilt and verified package 26.11.8.6, SHA256
e6f5038b368e4d53694be5490d56c508d3c59dbea0e263ea4443dfa669710d5e.
The wheel is retained locally under ignored `dist`; it is not a tracked artifact
and is not published. Installed core/MCP and copied dashboard/result-wrapper
smokes are rerun against this exact rebuild. Final local/hosted results follow.

## Final local acceptance and delivery

- Source checkpoint: a0502210, `Complete shared strict typing and fix CI executable modes`.
- Complete Windows Python 3.14 suite: **3,078 passed / 41 skipped / six warnings**, 326.94 seconds. All 3,119 collected tests were considered; no ignores, deselections, marker filters or early stopping. Existing native/privilege skip reasons are retained in the XML/log; no new skip was introduced.
- Whole first-party inventory: **674 files**, zero Ruff lint findings, all formatted; strict Pyright reports **zero errors / zero warnings**, including unnecessary-ignore checking. The normal root CLI also passes the modified-file selection.
- Documentation policy/links, Loguru formatting and patch whitespace pass. All first-party shebang files have executable Git modes.
- Final 26.11.8.6 rebuild succeeds. The isolated installation resolves that exact version; core/MCP, copied log-dashboard and copied result-dashboard/wrapper smokes all exit zero. Final wheel SHA256 remains e6f5038b368e4d53694be5490d56c508d3c59dbea0e263ea4443dfa669710d5e.
- Runtime parity and native synthetic checks above were repeated successfully after the final edits. Installed application/model-specific acceptance is not inferred from typing success.
- The validation-record commit accompanies a0502210 in one normal push to `fix/tlf-hyphenated-variables-239` for [#251](https://github.com/Chain-Frost/ryan-tools/pull/251). The PR stays draft; use its exact-head checks and latest validation section for hosted acceptance. No force push, parent/main merge or publication.
- The pre-existing `tests/test_data` submodule change is preserved and excluded from staging. No unrelated submodule work is included.

Private evidence: `full-suite-accepted.xml`, `full-suite-accepted.log`,
`pyright-delivery.json`, `pyright-root-delivery.log`, `ruff-delivery.log`,
`build-delivery.log`, `wheel-final-results.json` and native runtime receipts under
the campaign directory above. Historical failed runs remain retained, not
relabeled as passing evidence.

## Official Rasterio typing and clean-CI environment correction

At ab9bad42, [run 42480983322](https://github.com/Chain-Frost/ryan-tools/actions/runs/42480983322)
passed Ruff, hosted tests, package, MCP and POSIX checks; Policy exposed missing
declared development dependencies. Local PyArrow 24 supplied a placeholder stub,
whereas CI's PyArrow 26 did not. The Word-document utility also requires
`python-docx`, present locally but absent in CI. Add maintained `pyarrow-stubs`
and `python-docx` to the shared development extra and verify against CI's newer
NumPy/pandas/Arrow/Matplotlib versions instead of accepting the earlier local
environment as clean-CI proof.

Following the user's correction, Rasterio now uses official typeshed
`types-rasterio>=1.5.2.20261029`. Affine 3.0+ publishes its own typing. Remove all
twelve local Rasterio/affine stub files so they cannot shadow these declarations;
the first-party inventory is consequently **662 files**, not a narrowed source
scope. That complete inventory passes strict Pyright with redundant-ignore
checking and Ruff lint/format. Add explicit Rasterio submodule imports, preserve
real missing-CRS validation through narrow casts, and state masked-array return
types at the calls where upstream declarations are incomplete. No initialiser
profile or area-specific reduction in strictness is introduced.

The native Rasterio metadata calls pass their existing default band index
explicitly; Parquet buffers enumerate the same ordered schema names using the
typed public property. Formulae, grids, masks, raw inputs, publication ownership
and overwrite rules are unchanged. Existing focused/full coverage and installed
wheel smokes are being rerun for this checkpoint.

Rebuilt/verified 26.11.8.7, SHA256
e99fbf94e22954da7742f7021e6f1a7be68d917e8ff3ce1d493443836cb7f1f8.
This source/metadata correction requires a new checked push; the failing Policy
job is not a transient rerun candidate. Final local and hosted outcomes follow.

### Upstream support verification and final acceptance

Pillow-HEIF 1.8.1 includes native typing; remove its local stub and require the
development extra's 1.8.0+ public-export fix. Its registration function still
has untyped optional kwargs, handled by one import diagnostic exception rather
than a replacement stub. A native synthetic HEIC-to-JPEG conversion passes
with unchanged source bytes and verified JPEG pixels/dimensions.

PyShp is already 3.x (normal installation 3.1.0, CI 3.1.7). Tested the official
`pyshp[stubs]` extra: pyshp-stubs 0.0.2 installs only an empty
`shapefile-stubs/py.typed`. Pyright 1.1.414 still reports missing stubs and the
writer context-manager base type lacks writer methods. Restore/retain the
existing narrow shapefile stub until that upstream integration works. No PyShp
major upgrade or runtime dependency constraint change is made. Fiona and LAS
also retain verified bounded fallbacks; no `types-fiona` or `types-laspy`
distribution was available from the package index.

Final first-party inventory: **661 Python/stub files**, zero Ruff/format and
strict Pyright findings, including redundant-ignore checking. The thirteen-file
reduction from 674 is removal of twelve Rasterio/affine files and one HEIF stub;
no source scope is excluded. Existing legacy dependency exceptions remain;
this change does not introduce broad unknown-type suppression.

Complete Windows suite with NumPy 2.5.4, pandas 3.1.1, Arrow 26.0.1,
Matplotlib 3.11.2, affine 3.0.1 and Rasterio 1.5.2: **3,078 passed / 41 skipped /
176 warnings**, 307.98 seconds. All 3,119 tests considered, no exclusions or
deselections. The larger warning count includes affine 3's deprecation of
matrix `*`; numerical results remain accepted. The final PyShp 3.1.7/DBF and LAS
selection adds **30 focused passes**. The Parquet buffer's ordered schema-name
iteration preserves exact output bytes and rows against the original code.

The final declared rebuild is **26.11.8.8**, SHA256
39828617c3df4f0e0613eea47d42847a70d046ca6253aa2cd750cc3925d19554.
Installed-wheel core/MCP and copied log/result-wrapper smokes all pass against
that exact artifact before the checked push. The previous 26.11.8.7 receipt is
historical. This correction is delivered on the existing #251 branch; latest
hosted results are recorded in its description, with no main merge/publication.

Evidence adds `full-suite-official.xml/log`, `pyright-upstream-final-results.json`,
`ruff-final-upstream.log`, `build-upstream-final.log`, `native-heif.log`, the
26.11.8.8 installed smoke logs, and updated native parity receipts in the same
private campaign directory. No raw model data is published.
