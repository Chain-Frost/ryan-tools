# Automatic PyPI publication after successful main CI

The `.github/workflows/pypi-publish.yml` workflow is triggered by completion of
successful **push** CI on `main`. Ordinary merges do **not** require a separate
release PR, manual Actions dispatch or a manual GitHub Release.

## Release stages

1. Require successful `Python CI` from this repository's `main`, then verify
   that its tested commit remains on `main` ancestry.
2. Serialize all publication workflows using a FIFO queue (up to 100 pending
   workflows) without canceling older releases.
3. Read the current `pyproject.toml` version at the verified commit, the
   complete PyPI version history and all repository Git tags. Reject unexpected
   historical version formats rather than guessing.
4. Select a strictly greater Perth-calendar version. Use the next revision
   when several merges occur on the same date. Preserve the repository's
   existing two- or four-digit calendar year width.
5. Create a **lightweight tag at the CI-verified main commit**, then publish
   the corresponding stable GitHub Release. The version/tag is the atomic
   reservation and reruns reuse it.
6. Check out the **exact verified source commit**. Stamp the version into
   local build metadata from the tag, verify source ancestry and release
   identity, build the wheel, check metadata and run isolated-install smoke.
7. Compare against PyPI again directly before the Trusted Publishing upload.
   Never upload a version equal to or older than an existing PyPI release.
8. Upload through the existing PyPI OIDC Trusted Publisher. No tokens or
   direct writes to protected `main` are needed.

## Version provenance

The **Git tag identifies the source code commit** and the final wheel version
is **generated at build time** from the release tag. The `pyproject.toml` in
that original Git commit may still show an older baseline version. This is a
deliberate alternative to modifying protected `main` or merging release PRs.
For reproducibility, rebuild a release through the tag-aware publishing
workflow, not a naive `python -m build` of a raw tagged checkout. The
published wheel's own distribution metadata is the authoritative version.

## Failure, duplicates and manual publishing

- A rerun checks for an existing tag pointing to the same CI commit and
  reuses its version; a successful PyPI publication is never uploaded twice.
- A failed upload is **not** reported as a successful release. Re-run the
  failed publication while its reserved version is still greater than all
  PyPI versions; otherwise reconcile the failed release and create a
  sufficiently newer version. GitHub Releases may exist before upload.
- Unrecognised PyPI versions, API errors, tag conflicts, CI failures,
  superseded versions and unsupported dependency metadata all fail closed.
- The former release-PR finalizer is still retained for backward compatibility;
  the new automatic publisher avoids duplicate publication for release PRs.
- Manual release dispatch must target an existing `v<version>` tag.
- The concurrency queue holds up to 100 pending workflow runs. A larger
  exceptional burst requires manual reconciliation.

Automatic publication creates immutable, permanent PyPI versions. Keep
`main` protected by PR review/CI, and use normal PyPI release safeguards.
