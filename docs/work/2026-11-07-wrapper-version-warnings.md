# Offline wrapper revision warnings

- Status: Needs review; implementation and local validation complete.
- Owner: ChatGPT / maintainer.
- Updated: 2026-11-07.
- Next review: 2026-11-14.
- Delivery: implementation on existing PR #183 branch `feat/batch-dashboard-foundation-180`; included with this commit.
- Next action: review [current-head checks](https://github.com/Chain-Frost/ryan-tools/pull/183/checks) and implementation. No merge authorized.

## Contract and scope

The user requested an older-wrapper warning in `print_wrapper_banner`, comparing
against the installed package without network access, applying across maintained
scripts and documented in the wrapper standard.

The shared helper reads the original installed script's literal `WRAPPER_VERSION`
or `SCRIPT_VERSION` without executing it, and compares date/revision numerically.
It warns only for an older revision. Unknown versions, missing references,
ambiguous filenames or unreadable source skip the optional comparison.
It never rewrites a user script or settings. Both opening and closing banners
can show the warning. Display paths preserve configured spelling; the input
checker retains its relative banner display.

All 65 versioned installed entry points now pass their original package-relative
script identity, including 60 existing banner users, the input checker and four
standalone entry points. Copied scripts can be renamed without losing identity.
The standalone tools retain an optional import and a package-free banner fallback.
They also fall back when an older installed helper lacks the new keyword; four
regressions simulate that older API without calling it.
Legacy unversioned/experimental files are not promoted by this work; a script
without an installed counterpart has no installed reference to compare.

The wrapper standard and shared maintained-script principles document the
contract. The GDAL catalogue's embedded revisions match the modified wrappers.
MCP assessment: no new workflow or inputs are introduced, so existing catalogue
entries remain sufficient; the updated catalogue metadata and installed smoke
cover the shared behavior without exposing a duplicate MCP tool.

## Validation

- Ruff formatting/lint and strict Python 3.14 Pyright pass for the changed scripts.
- Focused shared-banner, CSV continuation, standalone, input-checker, wrapper,
  catalogue and inventory checks: 110 passed, two Windows symlink-privilege skips.
- Copied-wrapper help/behavior, result dashboards, culvert wrappers, file inventory
  and editable-default checks: 153 passed, two existing skips.
- All 68 changed Python files compile and pass Ruff formatting/lint and strict
  Pyright. The 60 existing banner scripts have unchanged processing/settings ASTs
  after normalizing only revisions and the new identity keyword.
- Static coverage requires every versioned installed script to supply its correct
  original identity to every shared banner call.
- Package `26.11.7.6` built and verified, and installed through the normal user
  Python 3.14 environment. The normal build produced the final wheel;
  SHA-256: `e33999fd19973614a898ac1e0cf32f1fa64e531e04a469f2d4fb9bd6c20ceb2d`.
- Final installed-wheel smoke passes outside the checkout, including older renamed
  copy warnings, unchanged copied bytes, help/CSV/listing/PO-label behavior and
  solver-free TUFLOW TXT export.
- DOCX and PO-label `--help` pass under `python -S` without the installed package.
- Default documentation/index checks, explicit status-record links, Loguru policy
  and `git diff --check` pass. The ignored local wheel remains in `dist`; CI builds
  its own artifact. No native GIS or licensed solver acceptance is claimed.
- Initial copied hydrograph check found the previous installed helper lacked the
  new keyword; rebuilding/installing the matching package resolved it. An encoding
  error introduced while editing the checker was corrected, and its existing
  presentation regression now passes.
- Hosted CI was green at prerequisite `b538fb87` (six checks); this is not evidence
  for the wrapper-warning changes. Use the PR checks link for this commit.
