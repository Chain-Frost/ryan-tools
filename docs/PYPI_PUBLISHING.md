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

## Create a release

### Automated release (recommended)

Run **Actions → Release to PyPI → Run workflow**, selecting `main`.
The workflow uses the existing transactional builder to bump the calendar
version and retain the verified wheel, then opens a `release/pypi-<version>`
pull request. Only one release PR may be open at a time.

The common `prepare_release.py` helper selects the date in Perth time
(UTC+08:00), updates metadata and invokes the existing builder without another
bump. It restores metadata if that builder fails. The two-digit year remains
unchanged here; run-hy8 preserves its existing four-digit year to avoid a downgrade.
All three repositories use the same helper and release lifecycle, with their
existing package-specific verified builders and smoke checks. The existing-PR
check runs before checkout/build to avoid wasting work on duplicate requests.

Review and squash merge that PR after the five required CI checks pass.
Successful **Python CI** on the merge commit triggers **Finalize PyPI release**,
which creates the matching tag and GitHub Release and explicitly dispatches
**Publish to PyPI** on that tag. Ordinary PR merges do not publish releases.
The PyPI publisher identity remains `pypi-publish.yml` / environment `pypi`.
Both release-event and manually dispatched publication require an existing,
published, non-draft, non-prerelease GitHub Release for the exact tag. The
publisher checks this through GitHub's API before checkout or building;
a bare tag, draft or prerelease cannot be published through manual dispatch.

Enable **Settings → Actions → General → Allow GitHub Actions to create and
approve pull requests** for the preparation workflow. It requests write
permissions only for its preparation job; it does not approve or merge PRs.
The bot-created PR triggers PR-event CI. If GitHub displays **Approve workflows
to run**, a maintainer must approve that run before the required checks can
complete. Preparation does not dispatch CI: manually dispatched workflow job
checks do not satisfy PR ruleset requirements. See GitHub's
[required-check event restrictions](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks#checks-from-some-workflow-jobs-are-not-evaluated).

Each push to `main` gets a separate CI concurrency group, so later merges
cannot cancel or replace the release merge's running or pending validation.
PR runs still cancel superseded runs. Finalization uses the successful run's
exact merge SHA even if `main` has advanced. Its concurrency group is also
specific to that SHA. If main CI fails or is manually cancelled, rerun that
exact push run; a successful later commit does not release the earlier SHA.
Finalization parses the merged version using Python's TOML parser rather than
assuming a particular TOML layout or quoting style.

If preparation fails after pushing a branch, inspect that branch and any PR
before retrying. If finalization fails, rerun its failed job. If publication
fails, rerun **Publish to PyPI** on the existing release tag after correcting
the cause. Never move an existing release tag or overwrite a PyPI version.

### Manual release

1. Merge approved code and ensure the package CI checks have passed.
2. Choose a new, unused calendar version and update the `[project]` version
   in `pyproject.toml` **before creating a tag**. The existing
   `repo-scripts/build_library.py --skip-artifacts` or normal build workflow
   can advance the version; commit this change to `main`.
3. Create and push the exact `v<version>` tag on that commit. The tagged
   commit must be reachable from the repository's current `main` branch.
4. In GitHub, create a **non-prerelease** release for that tag and publish it.
   Creating a release is the deliberate publishing trigger.
5. Monitor **Actions → Publish to PyPI**. The build job verifies the wheel
   against the existing repository packaging checks and `twine check`, then
   smoke-tests an isolated installation of that exact wheel before uploading it.
   The publish job then uses OIDC to upload the same wheel to PyPI.
6. Verify the resulting project on `https://pypi.org/project/ryan-functions/`.

The workflow rejects a release tag that does not exactly match the version
recorded in the tagged `pyproject.toml`. The publishing workflow does not bump versions,
publish on pull requests, or publish prereleases. PyPI does not permit
re-uploading a previously published version; advance the version for each
release, even when correcting packaging.

## Installation and scope

Once the first release succeeds:

```powershell
py -3.14 -m pip install ryan_functions
```

The wheel includes bundled `run_hy8` and `culvert_solver` packages from
the pinned git submodules and stages TUFLOW QML resources via `setup.py`.
Build jobs must initialise those submodules. The project currently declares
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

## Finalization retries

Finalization checks PyPI before dispatching publication. If the version already exists,
it skips dispatch; if publication for that SHA is active, it also skips duplicate work.
A missing version with no active run can be dispatched again after a failed attempt.
Unexpected PyPI responses or lookup timeouts fail visibly rather than guessing. Existing
tag/SHA and stable-release checks still apply before these recovery decisions.
