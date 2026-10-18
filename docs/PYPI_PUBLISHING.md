# Publishing ryan_functions to PyPI

The repository is `Chain-Frost/ryan-tools`; the PyPI distribution is
`ryan-functions` and its import package remains `ryan_library`.

## Prerequisites

The PyPI Trusted Publisher must be configured for `Chain-Frost`, the
repository's `pypi-publish.yml` workflow and GitHub environment `pypi`.
The `pypi` environment must permit deployment from the protected `main`
branch (not solely from `v*` tags). Chained `workflow_run` jobs execute
on the default branch, even though the publisher independently validates
the immutable release tag and verified merged source SHA.

**One-time environment change when upgrading from tag-only publishing:**

```powershell
$repo = "Chain-Frost/ryan-tools"
gh api "repos/$repo/environments/pypi/deployment-branch-policies"
gh api -X POST "repos/$repo/environments/pypi/deployment-branch-policies" -f name=main -f type=branch
```

The POST is needed only if a `main` branch rule is absent. Preserve the
`pypi` environment name and the existing PyPI Trusted Publisher workflow
registration (`pypi-publish.yml`); no new publisher registration is required.

Required PR review and CI checks on protected `main` remain enabled.
The automatic release reconciler requires permission to create release tags
and GitHub Releases. The publisher follows completion through `workflow_run`.

## New merges only

The release cutover is the merge that first introduces
`.github/release-cutover.md`. The cutover merge and **every earlier commit**
are deliberately ineligible for new automatic releases.

Afterwards, each successful `Python CI` run for the **latest merged PR**
on `main` can automatically allocate the next Perth-calendar version, create
its Git tag and stable GitHub Release, and allow the stand-alone PyPI publisher
to run on successful release-decision workflow completion. A newer merge
supersedes an older still-pending release; older merges are not backfilled.

Add the exact PR label `skip-release` **before merging**, or place
`[skip release]` in the squash-merge commit message, to suppress GitHub
Release and PyPI publication. The keyword is case-insensitive and persists
in the immutable merge commit. Labels can be changed after merging, so
retain the label until completion. The opt-out is checked before allocating
a tag and independently by the publisher. CI and MkDocs Pages still run.

The publisher checks the source SHA, project metadata, GitHub Release and
PyPI version history, builds and verifies a fresh wheel, and uploads it using
OIDC. The separate wheel-asset workflow attaches the byte-for-byte verified
PyPI wheel to GitHub Releases. No wheels should be committed into `dist/`.

**No periodic or manually dispatched publication actions exist.**
The chain is a successful post-merge CI run, a release-decision workflow,
the separately trusted PyPI publisher and optional wheel asset attachment.
Each workflow follows its predecessor through GitHub `workflow_run`
events. Only the most recent merged `main` revision is eligible.
Historical releases are not replayed; existing versions and tags are used
solely to allocate new versions in order.

The historical source commit's `pyproject.toml` may still contain an older
baseline version. The release workflow stamps the version from the immutable
Git tag into its temporary builder checkout. The published wheel's distribution
metadata is authoritative for installation.

## Failures

If publication fails, inspect its merge-triggered GitHub Actions run.
There is no alternate scheduled, release-published, or manual
publication trigger. Fix the problem in another PR and merge it to
initiate a new eligible release. Do not retag, delete published
releases, rewrite source history or overwrite an existing PyPI version.

Historical failed releases are not part of the new-merge automation. See
[automatic release policy](AUTO_PYPI_RELEASES.md).

## References

- [PyPI Trusted Publishers](https://docs.pypi.org/trusted-publishers/)
- [GitHub Actions release events](https://docs.github.com/en/actions/reference/events-that-trigger-workflows)
