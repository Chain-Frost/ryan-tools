# Tracking ongoing work

This is the canonical status convention adopted by [AGENTS.md](../AGENTS.md) and the
[development guide](DEVELOPMENT_GUIDE.md). Use it for substantial work spanning sessions, phases or dependencies,
or leaving follow-up work. Small completed fixes need no separate project file.

## Where information belongs

| Location | Responsibility |
| --- | --- |
| [Work register](work/README.md) | One row per work front: status, owner, dates, next action and link to its authoritative status record. |
| `docs/work/YYYY-MM-DD-short-name.md` | Default living status record for a new project. Use its creation date and keep updating the same file when resuming. |
| Existing local plan or roadmap | Can remain the authoritative status record. Add the fields below and register it; do not duplicate progress history. |
| [Repository roadmap](REPOSITORY_IMPROVEMENT_ROADMAP.md) | Broad priorities, milestones and links to selected projects. Update for scope/milestone changes rather than each session. |
| Nearest relevant README | Short ongoing-work pointer when useful for local discovery; no copied status table or session log. |
| [Documentation index](README.md) | Direct links to every repository-owned Markdown status file, including completed records. |
| `docs/audits/` | Dated investigation evidence and implementation plans. Preserve historical baselines; use a separate living record if updates would obscure original evidence. |

Do not create additional root task lists or rely on chat, agent memory or unlinked files as the only handoff.
Git status is evidence of edits, not a list of authorized projects.

## Starting and resuming

1. Read the register at the start of repository work and follow the relevant record before deep exploration.
2. Reuse an existing work record where applicable. Independent projects get separate rows/files; link dependencies
   and state what must happen before dependent work can resume.
3. Compare recorded branch, commit, worktree and validation with the current checkout before following old next steps.
4. Register a new substantial project and add a direct documentation-index link in the same change. Use the known
   owner or `Unassigned`; do not imply someone accepted responsibility.

Reading the register does not authorize unrelated work. Mention due items briefly in the handoff and continue the
user's selected scope unless directed otherwise.

## Status and review dates

| Status | Meaning |
| --- | --- |
| Planned | Identified work, implementation not started or not selected. |
| Active | Work underway with a concrete next step; not a lock or a claim that an agent is currently running. |
| Blocked | Cannot progress without a named dependency, decision or external change; identify who or what can unblock it. |
| Deferred | Deliberately paused, with a reason and revisit date or trigger. |
| Needs review | Imported or stale status not yet reconciled with current implementation and validation. |
| Complete | Agreed scope and required validation complete, with evidence and separate follow-up links. |
| Cancelled | Deliberately stopped; explain why and link any replacement. |

Use ISO dates (`YYYY-MM-DD`, repository local time). Every open row needs `Updated` and `Next review`, including
blocked/deferred work. Default to a review within 14 days for planned/active/needs-review work; use a justified date
for external dependencies or scheduled removals. Complete/cancelled rows use `—` for next review.

At session start and handoff, check for due review dates or planned/active/needs-review work without a substantive
update for 14 days. Blocked/deferred work follows its explicit review date. Report due items; when actually reviewing
one, reconcile evidence and update status, next action and date. Do not advance dates merely because a file was read.
Silence does not mean completion, cancellation or permission to resume.

This is a session-driven convention, not an automated reminder service. Nothing runs if no agent or maintainer reads
the register. A maintainer can request a register review to examine all due work together.

## Saving a handoff

Update the authoritative record and register row before returning unfinished substantial work, at milestones, when
scope or blockers change, and on completion. Keep the top summary current and append a concise dated progress entry.
Record the outcome, remaining scope, decisions, blockers and one concrete next action. Include exact validation
commands, environment, date and results; distinguish passed, failed, not run and historical evidence.

Record paths, branch/commit and whether changes are untracked, unstaged, staged, committed or pushed. Separate parent
and submodule state and identify unrelated changes to preserve. Never stage or commit merely to save a handoff.
Keep completion criteria and a next review date while open. Avoid terminal transcripts, sensitive project data and
vague next steps such as "continue cleanup". After an interrupted session, reconstruct status from current evidence.

Each simultaneous work front owns its own file. Re-read shared register/index sections immediately before editing;
change only relevant rows/links and preserve concurrent updates. Resolve duplicate records by choosing one
authoritative record and leaving a pointer from the superseded one.

On completion, record validation and delivery separately: complete implementation may be uncommitted when no commit
was requested. If publishing is in scope, it must happen before completion. Move the row to the closed section and
retain the indexed record. Put lasting behavioral guidance in canonical/local documentation; replace or remove local
ongoing-work pointers. Separate optional future work into another row instead of keeping finished projects active.

## Status record template

Copy into a new dated file, replacing placeholders:

```markdown
# Short project name

| Field | Value |
| --- | --- |
| Status | Active |
| Owner | Unassigned |
| Created | YYYY-MM-DD |
| Updated | YYYY-MM-DD |
| Next review | YYYY-MM-DD |
| Baseline | branch / commit; relevant worktree and submodule state |

## Outcome and scope

Requested result, boundaries and links to plans/dependencies.

## Current state

Implemented work, remaining work, decisions and blockers (or None).

## Next action

One concrete action, followed by ordered follow-ups if needed.

## Completion criteria

- Observable results and required validation/delivery conditions.

## Validation and delivery

Commands, environment, date and results; checks not run; edited/committed/pushed state.

## Progress

### YYYY-MM-DD

What changed, why, evidence and remaining concerns.
```

After documentation edits run `python repo-scripts/check_documentation.py` and explicit `--links-only` checks for
touched status/policy files. Default index coverage uses tracked Markdown; explicitly check new files and their
index links without staging just for validation.
