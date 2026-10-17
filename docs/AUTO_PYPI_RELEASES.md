# Automatic PyPI releases from successful main CI

The GitHub Actions workflow `.github/workflows/auto-publish.yml`
reconciles GitHub's durable `main` commit history after successful
`Python CI` push runs and hourly. No release PR or manual workflow
dispatch is required. `pypi-publish.yml` builds and publishes **on
a release-tag ref**, which is permitted by the `pypi` environment.

## Processing and provenance

1. Check out release tooling from a main commit verified by successful
   Python CI, never a newer unverified commit.
2. Enumerate first-parent main commits since the last tagged release.
   GitHub concurrency may replace a pending workflow run, so the
   reconciler scans the entire backlog every time. The hourly schedule
   recovers missed events. A CI run still in progress blocks later
   releases. Completed unsuccessful CI runs are skipped.
3. Process eligible source commits in ancestry order, preserving version
   and source ordering even if newer CI finishes first.
4. Select a Perth-calendar version strictly newer than the source
   version, every existing PyPI version and reserved Git tags.
5. Atomically reserve a lightweight tag on the exact verified source SHA.
   When a tag is already present for that SHA, reuse it. If a reserved
   tag lacks a GitHub Release, create that Release on retry.
6. Dispatch `pypi-publish.yml` on that `v<version>` tag. The tag
   identifies the verified source commit. Stamp version metadata into
   the temporary build checkout; do not push changes to protected main.
7. Build and inspect the wheel, test its installed copy, verify that
   no equal or newer PyPI version exists, then publish through OIDC
   Trusted Publishing. Do not advance to a later eligible commit until
   this version's publication is confirmed.

## Failure and recovery

- The former `Release to PyPI` and `Finalize PyPI release` workflows
  are retired. This eliminates racing duplicate tag creators.
- Existing published tags are no-ops on rerun, even if newer versions exist.
- A missing GitHub Release after atomic tag creation is recoverable.
- If publication fails, inspect its tag-scoped run. Fix the error and
  rerun `Publish to PyPI` on that same tag if still newer than the
  registry. Never retag or overwrite a published version.
- Unknown historical versions, registry failures, tag collisions,
  non-main source commits, and unresolved publication failures stop
  publication rather than silently breaking version monotonicity.
- GitHub can replace a pending reconciler wake-up, but cannot remove
  commits from main history. The next wake-up or hourly run rescans the
  backlog, so eligible revisions are not omitted by lossy concurrency.

## Rebuilding a historical release

The release tag points at the original verified source commit. Its
`pyproject.toml` can contain an older baseline version. The release
workflow stamps the tag version into the publication checkout before
building; a plain `python -m build` of an unstamped checkout is *not*
a byte-for-byte reconstruction of the published version.

No Git history rewriting or version commits to `main` are needed.
Local `dist/` artifacts are ignored by Git and CI builds them from
scratch.
