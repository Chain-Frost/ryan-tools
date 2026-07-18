# Repository Storage Migration Plan and Record

## Status

Completed on 18 July 2026. The test-data repository was extracted and published, `tests/test_data` was replaced
by a strict fixed-location submodule, remaining XLSX history was migrated to Git LFS, and all 34 ordinary
branches in `ryan-tools` were rewritten and published.

Executed on 18 July 2026 against:

- Main repository: `https://github.com/Chain-Frost/ryan-tools.git`
- New data repository: `https://github.com/Chain-Frost/ryan-tools-test-data.git`
- Main branch: `main`

The migration was performed from fresh WSL clones under
`$HOME/ryan-tools-storage-migration-20260718`. The pre-existing checkout on the Windows-mounted filesystem was
not used as a migration source and was not modified during the rewrite.

## Completion record

- Pre-rewrite `ryan-tools` transition commit: `e90ddd83d7a61788408e684cb00f17cf1cd96917`.
- Rewritten `ryan-tools` migration commit: `057c8416effa521343c700c8eda0753ff462d062`.
- Published `ryan-tools-test-data` commit: `b347e9b9668c50b1d0130301d45f7cd3dbdc7caa`.
- The test-data repository contains 7,432 current files (147,063,632 bytes). Its current tree was compared
  byte-for-byte with the source tree before publication.
- XLSX paths were removed from every published test-data ref. No XLSX path is reachable in that repository.
- Divergent extracted histories are preserved as `archive/work-on-qprocessor-that-could-break-stuff`,
  `archive/codex-outline-mean-value-calculation-in-workflow`, and `archive/pull-22-head`.
- Reachable `tests/test_data/...` object paths in the main repository changed from 3,243 to zero. The exact
  `tests/test_data` path remains as a mode-`160000` gitlink to the data commit above.
- Git LFS migrated 18 historical XLSX objects (about 50 MB) into 15 unique LFS objects. The current tree has
  11 XLSX files, all represented by LFS pointers and verified after hydration.
- The original mirror pack was 57.78 MiB. A fresh post-rewrite GitHub clone uses an 8.92 MiB ordinary Git pack;
  workbook content is downloaded separately through LFS.
- Fresh local-staging and GitHub clones passed `git fsck --full`, `git lfs fsck`, submodule sentinel checks,
  historical-path checks, and XLSX pointer/hydration checks.
- With the submodule absent, `pytest -s --collect-only` exits with the deliberate fixed-path usage error. With
  it present, pytest reaches the existing suite and collects 448 tests before three unrelated pre-existing
  collection errors (`run_hy8`, `tkinter`, and `tuflow_logsummary`).
- `git-filter-repo` 2.47.0 and Git LFS 3.7.1 were installed under `~/.local/bin` because WSL package installation
  required unavailable `sudo` credentials. GitHub HTTPS authentication used Windows Git Credential Manager.
- Verified recovery bundles and SHA-256 files were retained outside the active repositories. Their ref sets
  include the original ordinary branches and the pull-request refs advertised by GitHub at backup time.

The command sections below are retained as the audit runbook. Minor staging-path and installation differences
are recorded above; the filtering and publication commands were run as specified.

## Final decisions

1. `ryan-tools` remains the main repository and contains most code, all test code, packaging, and documentation.
2. `ryan-tools-test-data` stores the current and historical contents of `tests/test_data`.
3. `ryan-tools-test-data` is mounted directly at **one permissible location**:

   ```text
   ryan-tools/tests/test_data
   ```

4. There is no environment-variable override, cache location, download helper, fallback path, or skip behavior.
5. Every pytest invocation validates `tests/test_data` during startup. Missing or uninitialised data is a hard
   pytest error, not a skip or warning.
6. The existing test paths continue to work because the submodule replaces the directory at the same path.
7. The current `tests/test_data` tree contains no XLSX files, so the new data repository will not use Git LFS.
8. The old test-data history contains two deleted XLSX files totalling 48,966 bytes. Those paths and blobs will
   be deliberately pruned from every ref in `ryan-tools-test-data`; preservation of test-data history explicitly
   excludes XLSX content.
9. Remaining XLSX history in `ryan-tools` will be migrated to Git LFS after test-data history is removed. At the
   current tip this is 11 XLSX files totalling 49,394,540 bytes, primarily under `excel-tools` plus one regression
   snapshot.
10. All old `tests/test_data/...` blobs will be removed from every published branch and tag in `ryan-tools`.
11. Original hashes and every original ref remain available only in an offline bundle and mirror backup.

## Consequences

- Rewriting history changes every affected commit ID.
- Existing clones must be discarded and cloned again after publication.
- Open pull requests and unmerged branches based on old commit IDs must be completed, closed, or recreated.
- Branch protection must permit one coordinated force-push.
- Old-history branches or tags must not be retained in the active `ryan-tools` remote because they would keep the
  removed blobs reachable.
- The rewrite removes test-data descendants from all normal branches and tags and therefore from fresh ordinary
  clones. It cannot rewrite GitHub's read-only pull-request refs, other users' forks, existing clones, or cached
  commit views.
- GitHub currently advertises 74 `refs/pull/*/head` refs for this repository. Some can retain old commits even
  after every branch and tag is rewritten. Because this is storage cleanup rather than sensitive-data removal,
  GitHub Support should not be expected to delete those PR refs or perform a special server-side purge.
- Consequently, a smaller fresh clone is an achievable acceptance criterion; complete physical deletion from
  every GitHub backend object store is not.

## Target structure

```text
ryan-tools/
|-- ryan_library/
|-- ryan-scripts/
|-- tests/
|   |-- conftest.py
|   |-- test_data/               # gitlink to Chain-Frost/ryan-tools-test-data
|   `-- ...                      # test code remains in ryan-tools
|-- vendor/                      # existing third-party submodules
|-- .gitattributes               # XLSX is managed by Git LFS
|-- .gitmodules
`-- REPOSITORY_STORAGE_MIGRATION_PLAN.md
```

`ryan-tools` itself is the complete integration repository. There will be no `ryan-tools-workspace` repository.

## Pre-migration facts to retain in the migration record

The following read-only commands established the current baseline:

```bash
git rev-parse HEAD
git rev-list --count --all
git ls-files 'tests/test_data/**' | wc -l
git log --all --format='%H' -- tests/test_data | wc -l
git count-objects -vH
git ls-tree -r -l HEAD
git ls-remote https://github.com/Chain-Frost/ryan-tools-test-data.git
git ls-remote origin 'refs/pull/*/head' | wc -l
```

Observed values include:

- `ryan-tools` tip: `76b5f07` when reviewed.
- 7,432 tracked paths under `tests/test_data`.
- 12 test-data commits reachable from `main` and additional test-data history on old remote branches.
- No current XLSX paths under `tests/test_data`.
- No refs in `ryan-tools-test-data`.
- 74 GitHub pull-request head refs, which cannot be force-pushed by repository clients.

These values will be measured again immediately before execution. If `origin/main` has moved, the new values—not
the review-time hash—become the migration baseline.

## Tool and authentication prerequisites

The inspected WSL environment has Git 2.43 and Python 3.12, but currently lacks `gh`, `git-lfs`, and
`git-filter-repo`. I will not start the migration until the following succeeds.

The installation commands for this Ubuntu/WSL environment are:

```bash
sudo apt-get update
sudo apt-get install -y gh git-filter-repo git-lfs
git lfs install
git --version
git filter-repo --version
git lfs version
```

Git LFS must be version 3.3.0 or newer because that version fixed `--everything` to process all ordinary refs.
I will enforce that prerequisite with:

```bash
LFS_VERSION="$(git lfs version | sed -E 's#git-lfs/([^ ]+).*#\1#')"
dpkg --compare-versions "$LFS_VERSION" ge '3.3.0'
```

The shell is not currently authenticated for GitHub pushes. The repository owner must complete the interactive
authentication once:

```bash
gh auth login --hostname github.com --git-protocol https --web
gh auth setup-git
gh auth status
```

I will then verify both remotes without changing them:

```bash
git ls-remote https://github.com/Chain-Frost/ryan-tools.git refs/heads/main
git ls-remote https://github.com/Chain-Frost/ryan-tools-test-data.git
```

The second command must still print nothing before the first import.

## Execution environment

I will not perform destructive work in the existing `/mnt/e/Library/Automation/ryan-tools` checkout. It has
extensive working-tree changes and is not an acceptable migration source.

All migration work will use fresh clones under the Linux filesystem:

```bash
set -euo pipefail

export SOURCE_URL='https://github.com/Chain-Frost/ryan-tools.git'
export DATA_URL='https://github.com/Chain-Frost/ryan-tools-test-data.git'
export MIGRATION_ROOT="$HOME/ryan-tools-storage-migration-20260718"

test ! -e "$MIGRATION_ROOT"
mkdir -p "$MIGRATION_ROOT"
cd "$MIGRATION_ROOT"
```

The `test ! -e` guard deliberately aborts instead of deleting an earlier migration directory. The existing
repository checkout is not inside this location.

## Phase 1: freeze and create recoverable backups

Before these commands, merges and pushes to `ryan-tools` must be frozen.

```bash
cd "$MIGRATION_ROOT"
git clone --mirror "$SOURCE_URL" ryan-tools-original.git
git -C ryan-tools-original.git fsck --full
git -C ryan-tools-original.git show-ref > ryan-tools-original-refs.txt
git -C ryan-tools-original.git bundle create ../ryan-tools-original.bundle --all
git clone ryan-tools-original.bundle bundle-restore-check
git -C bundle-restore-check fsck --full
```

I will record baseline data:

```bash
git -C ryan-tools-original.git rev-parse refs/heads/main > original-main.txt
git -C ryan-tools-original.git count-objects -vH > original-object-count.txt
git -C ryan-tools-original.git for-each-ref \
    --format='%(refname) %(objectname)' \
    > original-all-refs.txt
```

The mirror, bundle, and reports will be copied to durable storage outside the active GitHub repositories. The
bundle will be retained for audit/recovery but will never be pushed as an archive branch or tag.

## Phase 2: extract all reachable test-data history

This starts from the mirror backup and retains the data path from all published branches and tags, not only
`main`. XLSX paths are then removed from that extracted history before anything is pushed.

```bash
cd "$MIGRATION_ROOT"
git clone --mirror ryan-tools-original.git ryan-tools-test-data-filtered.git
git -C ryan-tools-test-data-filtered.git filter-repo \
    --path 'tests/test_data/' \
    --path-rename 'tests/test_data/:' \
    --force
git -C ryan-tools-test-data-filtered.git filter-repo \
    --path-regex '(?i).*\.xlsx$' \
    --invert-paths \
    --force
git -C ryan-tools-test-data-filtered.git fsck --full
```

I will prove that no XLSX path remains reachable before configuring or pushing a remote:

```bash
if git -C ryan-tools-test-data-filtered.git rev-list --objects --all \
    | grep -Ei ' .*\.xlsx$'; then
    echo 'ERROR: XLSX history remains in test-data repository'
    exit 1
fi
```

After filtering, `refs/heads/main` is the canonical data branch. Some old `ryan-tools` branches contain divergent
test-data commits not reachable from `main`. To preserve those commits without reproducing dozens of redundant
code-branch names, I will push one `archive/...` branch for each unique divergent filtered tip.

First push `main` to initialise the empty repository:

```bash
cd "$MIGRATION_ROOT/ryan-tools-test-data-filtered.git"
git remote remove origin 2>/dev/null || true
git remote add origin "$DATA_URL"
git push origin refs/heads/main:refs/heads/main
```

Then preserve unique divergent filtered branch histories:

```bash
cd "$MIGRATION_ROOT/ryan-tools-test-data-filtered.git"
declare -A pushed_tips=()

while read -r ref tip; do
    [ "$ref" = 'refs/heads/main' ] && continue
    git merge-base --is-ancestor "$tip" refs/heads/main && continue
    [ -n "${pushed_tips[$tip]:-}" ] && continue

    source_name="${ref#refs/heads/}"
    archive_name="archive/${source_name}"
    git push origin "$ref:refs/heads/$archive_name"
    pushed_tips[$tip]="$archive_name"
done < <(git for-each-ref --format='%(refname) %(objectname)' refs/heads)
```

This preserves every filtered commit reachable from a published source branch. Source tags will be audited
separately; there are currently no tags. If tags exist at execution time, meaningful data tags will be pushed
under `refs/tags/source/...` rather than copying unrelated code-release tags verbatim.

## Phase 3: verify the extracted repository before touching `ryan-tools`

I will create current-tree archives from the original and filtered histories and compare them byte-for-byte:

```bash
cd "$MIGRATION_ROOT"
mkdir original-data-tip filtered-data-tip

git --git-dir=ryan-tools-original.git archive refs/heads/main:tests/test_data \
    | tar -x -C original-data-tip
git --git-dir=ryan-tools-test-data-filtered.git archive refs/heads/main \
    | tar -x -C filtered-data-tip

diff -qr original-data-tip filtered-data-tip
(cd original-data-tip && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum) \
    > original-data-tip.sha256
(cd filtered-data-tip && find . -type f -print0 | LC_ALL=C sort -z | xargs -0 sha256sum) \
    > filtered-data-tip.sha256
cmp original-data-tip.sha256 filtered-data-tip.sha256
```

The subshells ensure both checksum files contain identical relative paths, so `cmp` is an exact comparison.

I will also verify the remote through a fresh clone:

```bash
cd "$MIGRATION_ROOT"
git clone "$DATA_URL" test-data-remote-check
git -C test-data-remote-check fsck --full
test -f test-data-remote-check/expected_files.json
test -f test-data-remote-check/tlf_regression_snapshot.json
test -d test-data-remote-check/tuflow
find test-data-remote-check -type f -iname '*.xlsx' -print
if git -C test-data-remote-check rev-list --objects --all \
    | grep -Ei ' .*\.xlsx$'; then
    echo 'ERROR: XLSX history exists in remote test-data repository'
    exit 1
fi
```

The `find` command must print nothing, and the history check must find no XLSX path on any remote data ref.

I will add `README.md` and `DATASETS.md` to `ryan-tools-test-data` using `apply_patch`, then commit and push them:

```bash
cd "$MIGRATION_ROOT/test-data-remote-check"
git add README.md DATASETS.md
git commit -m 'Document extracted test datasets'
git push origin main
```

No Git LFS commands will be run in `ryan-tools-test-data`.

## Phase 4: replace the main-repository directory with the submodule

Only after Phase 3 passes will I prepare the normal source change in a fresh clone:

```bash
cd "$MIGRATION_ROOT"
git clone "$SOURCE_URL" ryan-tools-prep
cd ryan-tools-prep
git switch main
git pull --ff-only origin main
git rm -r tests/test_data
git submodule add "$DATA_URL" tests/test_data
```

I will add `tests/conftest.py` with `apply_patch`. It will have one repository-relative constant and a startup
hook equivalent to:

```python
from pathlib import Path

import pytest

TEST_DATA_ROOT = Path(__file__).resolve().parent / "test_data"
REQUIRED_TEST_DATA_PATHS = (
    TEST_DATA_ROOT / "expected_files.json",
    TEST_DATA_ROOT / "tlf_regression_snapshot.json",
    TEST_DATA_ROOT / "tuflow",
)


def pytest_sessionstart(session: pytest.Session) -> None:
    """Fail immediately when the required test-data submodule is unavailable."""
    missing = [path for path in REQUIRED_TEST_DATA_PATHS if not path.exists()]
    if missing:
        missing_text = "\n".join(f"- {path}" for path in missing)
        raise pytest.UsageError(
            "Required test data is missing or incomplete at tests/test_data.\n"
            "Run: git submodule update --init --recursive\n"
            f"Missing paths:\n{missing_text}"
        )
```

There will be no `RYAN_TOOLS_TEST_DATA`, fallback, cache, alternate path, or `pytest.skip` call.

```bash
cd "$MIGRATION_ROOT/ryan-tools-prep"
git add .gitmodules tests/conftest.py tests/test_data
git diff --cached --check
git diff --cached --stat
git commit -m 'Move test data to a submodule'
git push origin main
```

At this point the current `main` works, but old test-data blobs and old non-LFS XLSX blobs still exist in its
history. The next phase removes and migrates them in one coordinated rewrite. I will not run `git lfs track`
or publish `.gitattributes` before that rewrite; `git lfs migrate import` creates the correct historical and
current attributes entries while converting the blobs.

## Phase 5: prove the strict test-data failure behavior

These are deliberate pre-rewrite checks in the clean preparation clone. They require the repository test
dependencies to be installed.

First, prove that an uninitialised submodule fails pytest:

```bash
cd "$MIGRATION_ROOT/ryan-tools-prep"
git submodule deinit -f tests/test_data
python3 -m pytest --collect-only
```

That command must exit nonzero with the explicit missing-data error.

Then initialise the one supported location and prove collection can proceed:

```bash
git submodule update --init --recursive tests/test_data
python3 -m pytest --collect-only
```

The second command must get past the test-data startup check. Any unrelated collection failures will be recorded
separately rather than weakening the data requirement.

## Phase 6: purge old test-data blobs and migrate remaining XLSX history

After another push freeze, I will make a fresh mirror that includes the Phase 4 commit:

```bash
cd "$MIGRATION_ROOT"
git clone --mirror "$SOURCE_URL" ryan-tools-rewritten.git
git -C ryan-tools-rewritten.git bundle create ../ryan-tools-immediate-pre-rewrite.bundle --all
```

The old test data is stored as paths below `tests/test_data/`. The new submodule is a gitlink at the exact path
`tests/test_data`. The following regular expression removes the historical descendants while preserving that
exact gitlink and `.gitmodules`:

```bash
git -C ryan-tools-rewritten.git filter-repo \
    --path-regex '^tests/test_data/.+' \
    --invert-paths \
    --force
```

I will verify that the gitlink survived and descendant blobs did not:

```bash
git -C ryan-tools-rewritten.git ls-tree refs/heads/main tests/test_data
if git -C ryan-tools-rewritten.git rev-list --objects --all \
    | grep ' tests/test_data/'; then
    echo 'ERROR: old test-data paths remain'
    exit 1
fi
```

The `ls-tree` result must have mode `160000`, identifying a submodule.

Next I will migrate all remaining XLSX history on every rewritten ref:

```bash
cd "$MIGRATION_ROOT/ryan-tools-rewritten.git"
git lfs migrate info \
    --include='*.xlsx' \
    --everything \
    --pointers=ignore \
    > "$MIGRATION_ROOT/lfs-before-migration.txt"
git lfs migrate import \
    --include='*.xlsx' \
    --everything \
    --skip-fetch \
    --object-map="$MIGRATION_ROOT/lfs-object-map.csv"
git lfs migrate info \
    --include='*.xlsx' \
    --everything \
    --pointers=ignore \
    > "$MIGRATION_ROOT/lfs-after-migration.txt"
git lfs fsck
git fsck --full
```

This migration applies to the remaining XLSX files and their historical versions in `ryan-tools`; it does not
touch the already-separated `ryan-tools-test-data` repository.

## Phase 7: stage and verify the rewritten main repository

Before replacing GitHub history, I will push to a temporary local bare repository and clone it normally:

```bash
cd "$MIGRATION_ROOT"
git init --bare ryan-tools-staging.git

cd ryan-tools-rewritten.git
git remote remove origin 2>/dev/null || true
git remote add staging "$MIGRATION_ROOT/ryan-tools-staging.git"
git lfs push --all staging
git push --force staging 'refs/heads/*:refs/heads/*'
git push --force staging 'refs/tags/*:refs/tags/*'

cd "$MIGRATION_ROOT"
git clone --recurse-submodules ryan-tools-staging.git ryan-tools-staging-check
cd ryan-tools-staging-check
git lfs pull
git submodule update --init --recursive
```

The submodule URL points to GitHub, so the recursive clone still tests the real data remote.

I will verify:

```bash
test -f tests/test_data/expected_files.json
test -f tests/test_data/tlf_regression_snapshot.json
test -d tests/test_data/tuflow
test "$(git ls-tree HEAD tests/test_data | awk '{print $1}')" = '160000'
git lfs ls-files
git lfs fsck
git fsck --full
python3 -m pytest --collect-only
```

I will also record the rewritten size:

```bash
git --git-dir="$MIGRATION_ROOT/ryan-tools-rewritten.git" count-objects -vH \
    > "$MIGRATION_ROOT/rewritten-object-count.txt"
```

No GitHub force-push occurs unless all storage, gitlink, LFS hydration, and strict-path checks pass.

## Phase 8: publish rewritten `ryan-tools` history

Branch protection must be temporarily adjusted immediately before this phase. No one may push during it.

```bash
cd "$MIGRATION_ROOT/ryan-tools-rewritten.git"
git remote remove origin 2>/dev/null || true
git remote add origin "$SOURCE_URL"

git lfs push --all origin
git push --force origin 'refs/heads/*:refs/heads/*'
git push --force origin 'refs/tags/*:refs/tags/*'
```

I will not use `git push --mirror` against GitHub because it can include or delete refs outside the intended
branch/tag namespaces.

Branch protection will then be restored immediately.

## Phase 9: final remote verification

I will verify only from a completely fresh recursive clone:

```bash
cd "$MIGRATION_ROOT"
git clone --recurse-submodules "$SOURCE_URL" ryan-tools-final-check
cd ryan-tools-final-check
git lfs pull
git submodule update --init --recursive

test "$(git ls-tree HEAD tests/test_data | awk '{print $1}')" = '160000'
test -f tests/test_data/expected_files.json
test -d tests/test_data/tuflow

if git rev-list --objects --all | grep ' tests/test_data/'; then
    echo 'ERROR: old test-data paths remain'
    exit 1
fi

git lfs ls-files
git lfs fsck
git fsck --full
python3 -m pytest --collect-only
```

I will compare before/after object counts and fresh-clone sizes, then save those results with the migration
record.

## Contributor instructions after publication

Every existing clone is obsolete. Contributors must clone again:

```bash
git clone --recurse-submodules https://github.com/Chain-Frost/ryan-tools.git
cd ryan-tools
git lfs install
git lfs pull
```

Running tests without the populated `tests/test_data` submodule is intentionally unsupported and fails.

## Rollback

Before Phase 8, rollback means discarding the disposable migration clones and returning to the untouched GitHub
remote.

After Phase 8, rollback requires another coordinated force-push from
`ryan-tools-immediate-pre-rewrite.bundle` or `ryan-tools-original.bundle`, followed by another mandatory re-clone
for every contributor. The offline bundles are therefore retained until the rewritten repository has been used
and verified for an agreed period.

## Work explicitly deferred

- Moving `excel-tools` into a future resources submodule.
- Moving optional QGIS resources.
- Adding GPKG or other formats to Git LFS.
- Renaming `ryan_library` or introducing a `src/` package layout.
- General test cleanup unrelated to the fixed test-data path.

Those changes must not be combined with this destructive migration.

## Command references

- [`git-filter-repo` path filtering and renaming](https://github.com/newren/git-filter-repo/blob/main/Documentation/git-filter-repo.txt)
- [`git lfs migrate` manual](https://github.com/git-lfs/git-lfs/blob/main/docs/man/git-lfs-migrate.adoc)
- [GitHub history-rewrite limitations and pull-request refs](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/removing-sensitive-data-from-a-repository)
