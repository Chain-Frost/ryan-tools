# Automatic release cutover

This file marks the merge at which automatic publication switches to **new merges only**.
The workflow deliberately skips this cutover commit and all earlier `main` commits.
Future successful PR merges on the current `main` tip are eligible for publication.
No historical Git tags, releases or PyPI distributions are deleted or replayed.

Do not move, remove or recreate this file; its original addition commit is the cutover anchor.
