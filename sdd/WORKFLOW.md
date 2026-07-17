# navigator-eventbus SDD Workflow for Claude Code

## Overview

This document defines the **Spec-Driven Development (SDD)** methodology for navigator-eventbus, optimized for Claude Code with multi-agent task distribution.

The key idea: specifications are the Single Source of Truth (SSOT). Claude Code agents
consume spec documents and produce **Task Artifacts** — discrete, self-contained files
in `tasks/active/` that can be independently picked up and executed by any Claude Code
agent in parallel.

---

## The SDD Lifecycle

```
                                 ┌─ /sdd-proposal → discuss → brainstorm ──────────┐
                                 │                                                 │
                                 ├─ /sdd-spec → scaffold spec ─────────────────────┤
[Human] ────────────────────────┤                                           Feature Spec → [Planner] Tasks → [Executors] Code → [Reviewer] Validation
                                 │                                                 ↑              ↑                                       |
                                 │                                                 │              └────────── Feedback Loop ──────────────┘
                                 │                                                                                                        |
                                 └────────── /sdd-task → decomposes spec into tasks ──────────────────────────────────────────────────────┘
```

### Phase 0 — Feature Proposal *(optional)*
Start here when the idea is not yet well-defined. Use `/sdd-proposal` to discuss
a feature in non-technical language. The agent walks through motivation, scope,
and impact with you, producing `sdd/proposals/<feature>.proposal.md`.

The proposal can then automatically scaffold a formal spec (Phase 1).

### Phase 1 — Feature Specification
Start here when you already know what you want to build. Use `/sdd-spec` to scaffold
`sdd/specs/<feature>.spec.md`, or accept one auto-generated from `/sdd-proposal`.

### Phase 2 — Task Generation (Claude Code Planner Agent)
Run `/sdd-task <spec-file>` to decompose the spec into Task Artifacts.

Each task is written to `tasks/active/TASK-<id>-<slug>.md`.
The **per-spec index** at `sdd/tasks/index/<feature-slug>.json` is created
or updated with task metadata.

Tasks are designed to be:
- **Atomic** — completable independently
- **Bounded** — clear scope, no ambiguity
- **Testable** — every task includes its own test criteria
- **Assignable** — formatted so any Claude Code agent can start immediately

### Phase 3 — Task Execution (Claude Code Executor Agents)
Each executor agent picks up a task file:
```bash
# In a new Claude Code session:
claude "Read tasks/active/TASK-003-redis-streams-backend.md and implement it"
```

Tasks declare their dependencies, so agents know what must be done first.

### Phase 4 — Validation (Claude Code Reviewer Agent)
After execution, tasks move to `tasks/completed/`.
A reviewer agent validates against the Test Specification.

---

## Task Artifact Format

Every task file (`tasks/active/TASK-<NNN>-<slug>.md`) follows this structure:

```markdown
# TASK-<NNN>: <Title>

**Feature**: <parent feature name>
**Spec**: sdd/specs/<feature>.spec.md
**Status**: [ ] pending | [ ] in-progress | [x] done
**Priority**: high | medium | low
**Depends-on**: TASK-<X>, TASK-<Y>   (or "none")
**Assigned-to**: (agent session ID or "unassigned")

## Context
Brief explanation of why this task exists and how it fits the feature.

## Scope
Exactly what this task must implement. Be precise.

## Files to Create/Modify
- `src/navigator_eventbus/path/to/file.py` — description
- `tests/path/to/test_file.py` — unit tests

## Implementation Notes
Technical guidance for the agent: patterns to follow, existing code to reference,
gotchas, constraints.

## Reference Code
Existing patterns in the codebase the agent should follow:
- See `src/navigator_eventbus/core.py` for BusCore pattern
- See `src/navigator_eventbus/evb.py` for EventBus facade pattern

## Acceptance Criteria
- [ ] Criterion 1
- [ ] Criterion 2
- [ ] All tests pass: `pytest tests/path/ -v`

## Test Specification
`` `python
# Minimal test scaffold the agent must make pass
def test_feature_does_x():
    ...

def test_feature_handles_edge_case():
    ...
`` `

## Output
When complete, the agent must:
1. Move this file to `tasks/completed/`
2. Update per-spec index status to "done"
3. Add a brief completion note below

### Completion Note
(Agent fills this in when done)
```

---

## Git Configuration

The project uses a simplified branch model:

- **`main`** — tagged releases and feature integration. Default base
  for `type: feature` flows.
- **`dev`** — optional integration branch (when the project grows).

**Flow types**:
- `feature` — base is `main` (default).
- `hotfix` — base is `main` (mandatory).

---

## Per-Spec Index Schema (`sdd/tasks/index/<feature-slug>.json`)

Each per-spec index file contains a header describing the feature plus
the `tasks[]` array for that feature only. Two parallel features touch
disjoint files and never collide on merge.

```json
{
  "feature": "feature-slug",
  "feature_id": "FEAT-NNN",
  "spec": "sdd/specs/feature-slug.spec.md",
  "type": "feature",
  "base_branch": "main",
  "created_at": "ISO-8601",
  "completed_at": null,
  "tasks": [
    {
      "id": "TASK-001",
      "slug": "bus-core-queues",
      "title": "Implement per-priority queue workers",
      "feature_id": "FEAT-NNN",
      "feature": "feature-slug",
      "status": "done",
      "priority": "high",
      "depends_on": [],
      "assigned_to": null,
      "started_at": null,
      "completed_at": "ISO-8601",
      "file": "sdd/tasks/completed/TASK-001-bus-core-queues.md"
    }
  ]
}
```

---

## Parallelism Rules

Claude Code agents can work in parallel when tasks have no shared dependencies:

```
TASK-001 (bus core interface)
    ├── TASK-002 (redis-pubsub backend)    ← parallel after 001
    ├── TASK-003 (redis-streams backend)   ← parallel after 001
    └── TASK-004 (dlq handler)             ← parallel after 001
            └── TASK-005 (integration tests) ← waits for 002, 003, 004
```

A Claude Code agent should **never start a task** if its `depends_on` tasks
are not in `tasks/completed/`.

---

## Commands Reference

These commands are available as Claude Code commands (`.claude/commands/`):

| Command | Description |
|---|---|
| `/sdd-proposal` | Propose and discuss a feature idea before building a spec |
| `/sdd-brainstorm` | Structured idea exploration with options and Q&A |
| `/sdd-spec` | Scaffold a new Feature Specification |
| `/sdd-task <spec.md>` | Decompose a spec into Task Artifacts |
| `/sdd-start` | Start implementing a specific task |
| `/sdd-status` | Show task index status summary |
| `/sdd-next` | Suggest next unblocked tasks to assign |
| `/sdd-done` | Verify, merge, and cleanup a completed feature |
| `/sdd-codereview` | Code review a completed SDD task |

---

## Quality Rules for Agents

1. **Never modify files outside the task scope** — respect boundaries
2. **Follow existing patterns** — reference code mentioned in the task
3. **Write tests first** — TDD approach per task
4. **Update the index** — always update per-spec index on completion
5. **Small commits** — one task = one logical commit
6. **Ask via the spec** — if unclear, note the ambiguity in the completion note
   and let the Planner agent refine the spec for the next iteration
