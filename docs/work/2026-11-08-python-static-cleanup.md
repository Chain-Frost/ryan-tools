# Repository Python lint, typing and import-isolation cleanup

| Field | Value |
| --- | --- |
| Status | Active; Ruff and source-import fixes implemented, broad typing remediation underway |
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

Consolidate GIS typing support, correct genuine maintained/legacy/test typing findings without broad suppressions, rerun full first-party Ruff and strict checks and full Windows tests, build and verify wheel, then batch delivery to the existing branch. Preserve concurrent remote work. Linked #284's two-file formatting failure is a separate small fix on its existing child branch/worktree.

## Evidence and delivery

Detailed baseline logs, complete diagnostics, AST checks and native reports are retained privately at E:/validation/pr251-full-20261108. Current changes are local/uncommitted; do not imply they are pushed or hosted-accepted. No main merge, release or issue closure.


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
