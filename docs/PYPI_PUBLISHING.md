# Publishing ryan_functions to PyPI

The repository is `Chain-Frost/ryan-tools`, but its Python distribution is
`ryan_functions` (normalised by PyPI to `ryan-functions`). Import names such
as `ryan_library` do not change.

## One-time account setup

1. Sign in to [PyPI](https://pypi.org/) with an account you control and enable
   two-factor authentication.
2. If this project has never been uploaded, open your account's **Publishing**
   page and add a **pending** Trusted Publisher. Otherwise, use the existing
   project's **Publishing** settings.
3. Configure the exact GitHub trusted-publisher identity:
   - PyPI project: `ryan_functions` (pending projects only)
   - GitHub owner: `Chain-Frost`
   - Repository: `ryan-tools`
   - Workflow filename: `pypi-publish.yml`
   - GitHub environment: `pypi`
4. In GitHub, create the repository environment named `pypi`
   (Settings → Environments). Configure required reviewers and deployment
   restrictions where available. For release-tag publishing, allow release
   tags matching `v*` if deployment restrictions are enabled.
5. Protect release tags and restrict who can create releases. Review changes
   to the publishing workflow before merging them. Create a tag ruleset for
   `v*` and a branch ruleset for `main` (required PRs/status checks where
   available). Environment deployment tag restrictions alone do not establish
   that a tag's commit came from `main`; the workflow separately verifies
   that the release SHA is an ancestor of `origin/main`.

No PyPI password, long-lived API token or GitHub secret is required for
Trusted Publishing.

The configured repository policy permits only squash merges, requires the
five CI checks on an up-to-date PR, and requires resolved review conversations.
The required contexts are `Policy`, `Hosted tests`, `Package`, `Python 3.13 compatibility and MCP`, and
`Ruff and whitespace`, all from GitHub Actions. The last check was added to the `main only` branch ruleset
on 17 October 2026; the `v*` tag ruleset is unchanged because Fast PR checks does not run on tag pushes.
Required approval count remains zero for the solo-maintainer workflow. The
maintainer's main-branch bypass is limited to PRs, and merged branches are
automatically deleted. Release-tag creation is allowed after required CI;
updates and deletions remain restricted. The `pypi` environment allows `v*`
tags without a required reviewer so publication proceeds automatically.

**Important:** Pending publisher configuration does **not** reserve a package
name. Confirm the name is available or that you control the existing project.
You must configure PyPI and GitHub yourself; the repository workflow cannot
register the publisher or create the GitHub environment on your behalf.

## Automatic release from main (default)

Every **successful push CI** on `main`, including an ordinary approved PR
merge, wakes `Reconcile automatic PyPI releases`, which dispatches the tag-scoped
`Publish to PyPI` workflow. There is no manual version
bump, separate release PR or manual workflow dispatch. The hourly reconciler revisits the entire durable backlog
instead of relying on GitHub's lossy pending-run concurrency.

The release job verifies the exact CI commit is on `main`, reads the
complete PyPI release history and repository tags, then chooses a strictly
greater Perth-calendar version. It reserves the corresponding lightweight
Git tag on that same tested commit. Multiple merges on the same day use
successive revision numbers; both release selection and final upload checks
reject an equal or older PyPI version. All unknown historical version formats
and registry errors fail closed.

The publisher stamps `pyproject.toml` **in its isolated publication checkout**
to match the release tag, builds/verifies the wheel, runs isolated installed
smoke checks and publishes with the existing OIDC Trusted Publisher. The source
commit itself may retain an older baseline version; the tag and generated
wheel metadata identify the published version. The tagged checkout must be
built through the publication workflow to reproduce its version correctly.
Protected `main` is never written to or bypassed by the publisher.

See [automatic PyPI release policy](AUTO_PYPI_RELEASES.md) for version
provenance, retry behaviour, concurrency limits and recovery.

### Recovery and manual release tags

The old `Release to PyPI` preparation and `Finalize PyPI release` workflows have been
retired to prevent duplicate release creation. The durable `Reconcile automatic PyPI
releases` workflow processes successful `main` revisions in first-parent order,
with an hourly reconciliation check in case of missed workflow notifications.
It reserves a tag, creates a stable GitHub Release, and explicitly dispatches the
existing tag-scoped `Publish to PyPI` workflow. A manual dispatch of the
publisher on an existing tag remains possible for recovery. Do not manually
move existing tags or reupload PyPI versions.

## Installation and scope

Once the first release succeeds:

```powershell
py -3.14 -m pip install ryan_functions
```

The wheel declares `run-hy8` and `ryan-culverts` as PyPI dependencies rather than bundling their source. The `ryan-culverts` dependency applies to Python 3.14+, until the planned 3.14-only baseline. TUFLOW QML resources are staged by `setup.py`, which still requires the `qgis-resources` submodule. The project currently declares
`Python >=3.13`, and its metadata includes substantial geospatial
dependencies. Confirm a fresh-environment installation on supported
platforms before announcing broad compatibility: PyPI upload success by
itself does not prove downstream dependency installation or functionality.

For build architecture, see [the root README](../README.md) and
[`repo-scripts/build_library.py`](../repo-scripts/build_library.py).

## CI checks and dependency scope

Policy runs Actionlint 1.7.12 (checksum-verified) across repository workflows
and checks documentation and logging rules. It installs the full development
and GIS dependencies only when changed Python files require Ruff and strict
Pyright. Hosted tests retain the full application development environment.
The Python 3.13 compatibility job remains a separate compatibility check.

Package and publication smoke checks install only their tooling plus pandas
and Loguru, which are needed by the public imports exercised by the smoke
script. These are focused packaging checks, not full runtime-dependency or
GIS/application validation. The package job still verifies the retained wheel
before rebuilding, and the builder verifies the newly built wheel before
promotion. Publication smoke-tests the newly built wheel that is uploaded;
it does not reuse the wheel from a separate CI run.

References:
- https://docs.pypi.org/trusted-publishers/creating-a-project-through-oidc/
- https://docs.pypi.org/trusted-publishers/using-a-publisher/

## Reconciliation retries

If a publisher fails, inspect its exact tag and run. The hourly reconciler can
recover a reserved tag with no GitHub Release, and does not allocate newer
versions past an unpublished earlier release. Rerun the failed tag-scoped
publisher after correcting the cause.
