# Repository Python lint, typing and import-isolation cleanup

| Field | Value |
| --- | --- |
| Status | Active; whole-repository static cleanup passes, final Windows acceptance and delivery underway |
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

Finish final Windows suite and rebuilt-wheel acceptance, commit this coherent checkpoint and push the existing #251 branch once. Verify every required hosted job at the pushed commit. Keep #284's independent no-data regression contract on its existing child branch; no merge into main is authorized.

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
