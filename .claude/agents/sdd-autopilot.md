# SDD Multi-Agent Orchestration — Design Document

## Overview

A two-phase orchestration pipeline for Claude Code that takes a feature
from inception to reviewed PR with minimal human intervention.

**Phase 1 — Planning (interactive):** `/sdd-brainstorm` or `/sdd-proposal` command
**Phase 2 — Execution (autonomous):** `sdd-autopilot` agent

The separation exists because planning REQUIRES human judgment (scope decisions,
architectural tradeoffs, priority calls) while execution can be pre-authorized
once the spec is approved.

---

## Architecture

### The Key Insight: Shell-Level Orchestration

Claude Code agents run as separate CLI processes. Orchestration happens at the
shell level via `claude -p "prompt"` (non-interactive) and `claude --agent <name>`
invocations. This mirrors a DAG execution pattern but with:

- **Agents** = Claude Code agent definitions (`.claude/agents/*.md`)
- **Tasks** = CLI invocations with structured prompts
- **Dependencies** = sequential execution with exit code checks
- **Context passing** = files on disk (reports, diffs, task index)
- **Gates** = file-based checkpoints that the orchestrator reads before proceeding

### Process Model

```
┌─────────────────────────────────────────────────────────────────┐
│ Phase 1: Planning (INTERACTIVE — human in the loop)            │
│                                                                 │
│   Idea ──► brainstorm Q&A ──► spec ──► tasks ──► approve       │
│            (2+ rounds)        (commit)  (commit)               │
└─────────────────────────────────┬───────────────────────────────┘
                                  │ human approves spec
                                  ▼
┌─────────────────────────────────────────────────────────────────┐
│ Phase 2: sdd-autopilot (AUTONOMOUS — pre-authorized)           │
│                                                                 │
│   ┌──────────┐   ┌───────────────┐   ┌──────────┐              │
│   │sdd-worker│──►│code-reviewer  │──►│qa-runner  │              │
│   │(worktree)│   │(same worktree)│   │(worktree) │              │
│   └──────────┘   └───────┬───────┘   └─────┬────┘              │
│                          │                  │                    │
│                   ┌──────▼──────┐    ┌──────▼──────┐            │
│                   │  Gate:      │    │  Gate:      │            │
│                   │  review OK? │    │  tests pass?│            │
│                   └──────┬──────┘    └──────┬──────┘            │
│                          │                  │                    │
│              ┌───────────▼──────────────────▼─────────┐         │
│              │ sdd-done (merge) + create PR            │         │
│              └───────────────────────────────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                      Human reviews PR, approves, merges
```

---

## Orchestration Engine

The autopilot doesn't implement features itself — it orchestrates other agents
via bash and reads their outputs to make decisions:

### Stage 1: IMPLEMENT (sdd-worker)
```bash
claude --agent sdd-worker --model sonnet --verbose \
  -p "Implement all tasks for $FEAT_ID"
```

### Stage 2: CODE REVIEW (code-reviewer)
Review loop with max 2 iterations. If `NEEDS_CHANGES`, feed review findings
back to `sdd-worker` for fixes.

### Stage 3: QA / TESTING (qa-runner)
Run test suite, linting, type checking. On failure, attempt one auto-fix
cycle via `sdd-worker`, then re-run QA.

### Stage 4: MERGE + CREATE PR (sdd-done)
Push feature branch and create PR against base branch.

### Gate System

Each stage produces a checkpoint file in `.autopilot/`:

```
.autopilot/
├── state.json              # Current pipeline state
├── worker-output.log       # sdd-worker stdout
├── review-report.md        # Code review findings
├── qa-report.md            # Test results
└── fix-output-*.log        # Fix iteration logs
```

---

## Error Recovery & Safety

### Retry Budget

| Stage | Max Retries | On Exhaustion |
|-------|-------------|---------------|
| Worker | 0 (tasks have internal retries) | STOP, report |
| Review | 2 iterations (review -> fix -> review) | STOP, mark PR draft |
| QA | 1 (fix -> rerun) | STOP, report |
| PR creation | 1 | STOP, manual PR |

### Blast Radius Control

The autopilot NEVER:
- Merges PRs (human does this)
- Modifies code on `main` directly
- Deletes branches or worktrees
- Pushes with `--force`
- Modifies files outside the feature scope

The autopilot CAN:
- Push the feature branch
- Create PRs
- Add PR comments and labels
- Convert PRs to draft
