# Publishing ryan_functions to PyPI

## Prerequisites

The PyPI Trusted Publisher must be configured for `Chain-Frost`, the
repository's `pypi-publish.yml` workflow and GitHub environment `pypi`.
The environment permits release tags matching `v*`.

Required PR review and CI checks on protected `main` remain enabled.
The automatic release reconciler requires permission to create release tags
and GitHub Releases and to dispatch the tag-based publishing workflow.

## New merges only

The release cutover is the merge that first introduces
`.github/release-cutover.md`. The cutover merge and **every earlier commit**
are deliberately ineligible for new automatic releases.

Afterwards, each successful `Python CI` run for the **latest merged PR**
on `main` can automatically allocate the next Perth-calendar version, create
its Git tag and stable GitHub Release, and dispatch `Publish to PyPI` on the
tag reference. A newer merge supersedes an older still-pending release; older
merges are not backfilled or assigned later versions.

The publisher checks the source SHA, project metadata, GitHub Release and
PyPI version history, builds and verifies a fresh wheel, and uploads it using
OIDC. The separate wheel-asset workflow attaches the byte-for-byte verified
PyPI wheel to GitHub Releases. No wheels should be committed into `dist/`.

A nightly and manually triggered reconciler checks **only the current
verified main tip**; it does not replay historical merge history or failed
releases predating this cutover. Existing PyPI versions and tags are used to
reserve strictly increasing versions but are not republished.

The historical source commit's `pyproject.toml` may still contain an older
baseline version. The release workflow stamps the version from the immutable
Git tag into its temporary builder checkout. The published wheel's distribution
metadata is authoritative for installation.

## Failures

If a current tagged publication fails, inspect its tag-based Actions run.
Do not retag, delete published releases, rewrite source history or overwrite
an existing PyPI version. Manual tag-scoped publication is supported only
for a valid stable GitHub Release pointing to verified main source.

Historical failed releases are not part of the new-merge automation. See
[automatic release policy](AUTO_PYPI_RELEASES.md).

## References

- [PyPI Trusted Publishers](https://docs.pypi.org/trusted-publishers/)
- [GitHub Actions release events](https://docs.github.com/en/actions/reference/events-that-trigger-workflows)
