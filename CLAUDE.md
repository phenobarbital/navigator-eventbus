# navigator-eventbus Development Guide for Claude

## Project

Standalone async event bus + generic hooks fabric for aiohttp-based servers.
See @CONTEXT.md for full architectural context.

**Main Branch**: `main`

## Development Environment

### Package Management & Virtual Environment

**CRITICAL RULES:**
1. **Package Manager**: Use **`uv`** exclusively for package management
   ```bash
   uv pip install <package>
   uv pip list
   uv add <package>
   ```

2. **Virtual Environment**: ALWAYS activate before Python operations
   ```bash
   source .venv/bin/activate
   ```
   **NEVER** run `uv`, `python`, or `pip` commands without activating first.

3. **Dependencies**: Manage all dependencies via `pyproject.toml`


## Event Bus Architecture

navigator-eventbus routes events through glob-matched topic strings with per-priority
`asyncio.Queue` workers. When working on the codebase:

1. **Core modules**: `src/navigator_eventbus/core.py` (BusCore), `src/navigator_eventbus/evb.py` (EventBus facade)
2. **Envelope model**: `src/navigator_eventbus/envelope.py` — event serialization
3. **Hooks system**: `src/navigator_eventbus/hooks/` — generic hooks fabric with `HookTypeRegistry`
4. **Backends**: `src/navigator_eventbus/backends/` — memory, redis-pubsub, redis-streams transports
5. **Ingress**: `src/navigator_eventbus/ingress/` — WebSocket/gRPC ingress
6. **DLQ**: `src/navigator_eventbus/dlq.py` — Dead Letter Queue handler

## Async-First Development

navigator-eventbus is built on async/await patterns — all I/O must be non-blocking.

## Non-Negotiable Rules

### Environment
- Package manager: `uv` exclusively (`uv add`, `uv pip install`)
- ALWAYS activate venv before any command: `source .venv/bin/activate`
- NEVER run python/uv/pip without activating first

### Code Standards
- All functions and classes: Google-style docstrings + strict type hints
- Pydantic models for all data structures
- async/await throughout — no blocking I/O in async contexts
- Logger (`self.logger`) instead of print statements

### Workflow: Think -> Act -> Reflect
1. For complex tasks: create plan in `artifacts/plan_[task_id].md` first
2. Implement incrementally
3. Run `pytest` after ANY logic change — no exceptions
4. Save evidence to `artifacts/logs/`

### Security
- Never commit API keys — use environment variables
- Never run `rm -rf` or system-level deletions
- No form submissions or logins without user approval

## Key References
- Architecture & patterns: @CONTEXT.md
- SDD workflow: @sdd/WORKFLOW.md
- Topic registry: @TOPICS.md

# SDD Workflow & Worktree Policy

---

## Git Configuration

The project uses a simplified branch model:

- **`main`** — tagged releases only. Feature work targets `main` via PR.
- **`dev`** — integration branch for feature work (when used). Default base
  for `type: feature` flows.

**Flow types**:
- `feature` — base is `main` (default) or `dev` (when the project grows).
- `hotfix` — base is `main` (mandatory).

## Worktree Creation

> **CRITICAL**: Do NOT use `claude --worktree`. It branches from the repo's default
> branch (`main`), which may not contain SDD artifacts.
>
> Always create worktrees manually from the current branch:

```bash
# Standard pattern: create worktree from current branch
git worktree add -b <branch-name> .claude/worktrees/<worktree-name> HEAD
```

### Quick reference

```bash
# From main (most common for this project)
git checkout main
git worktree add -b feat-014-redis-streams-backend \
  .claude/worktrees/feat-014-redis-streams-backend HEAD

# Then launch Claude inside the worktree
cd .claude/worktrees/feat-014-redis-streams-backend
claude   # interactive, manual /sdd-start
# or
claude --agent sdd-worker --model sonnet --verbose
```

### Cleanup

```bash
# After PR merge
git worktree remove .claude/worktrees/<name>
# or prune all dead worktrees
git worktree prune
```

### .gitignore

```gitignore
.claude/worktrees/
```

## SDD Auto-Commit Rule

> **CRITICAL**: Every SDD command that creates or modifies files MUST commit
> them on the appropriate branch before finishing. Uncommitted files are
> invisible to worktrees and other sessions.

| Command | What it commits | Where |
|---------|-----------------|-------|
| `/sdd-brainstorm` | `sdd/proposals/<n>.brainstorm.md` (with frontmatter) | `base_branch` |
| `/sdd-proposal`   | `sdd/proposals/<n>.proposal.md` (with frontmatter)  | `base_branch` |
| `/sdd-spec`       | `sdd/specs/<n>.spec.md` (with frontmatter)          | `base_branch` |
| `/sdd-task`       | `sdd/tasks/index/<feature>.json` + `sdd/tasks/active/TASK-*` | `base_branch` |
| `/sdd-start`      | Per-spec index status update + implementation code  | worktree (feature branch) |
| `/sdd-done`       | Per-spec index final state + task file moves; merges feature -> `base_branch` | `base_branch` (NEVER `main`) |

Commit message convention:
```
sdd: <action> for <feature-name>
```

## Isolation Model

Worktrees isolate **features** from each other. Tasks within a feature run
sequentially in the same worktree via `/sdd-start TASK-<NNN>`.

```
Terminal 1 (in .claude/worktrees/feat-007):     Terminal 2 (in .claude/worktrees/feat-008):
  /sdd-start TASK-001 -> commit                   /sdd-start TASK-010 -> commit
  /sdd-start TASK-002 -> commit (sees 001)         /sdd-start TASK-011 -> commit
  /sdd-start TASK-003 -> commit (sees 001+2)       /sdd-start TASK-012 -> commit
  push, PR against main                            push, PR against main
```

## Typical Workflow

```bash
# 1. Ensure you're on main with latest
git checkout main && git pull origin main

# 2. Create and approve a spec (committed automatically)
/sdd-spec redis-streams-backend -- ...
/sdd-task sdd/specs/redis-streams-backend.spec.md

# 3. Create worktree from main
git worktree add -b feat-014-redis-streams-backend \
  .claude/worktrees/feat-014 HEAD

# 4. Enter worktree and work
cd .claude/worktrees/feat-014

# Manual (task-by-task):
claude
/sdd-start TASK-069
/sdd-start TASK-070
/sdd-done FEAT-014

# Or autonomous:
claude --agent sdd-worker --dangerously-skip-permissions --model sonnet --verbose
/sdd-done FEAT-014

# 5. Push and PR
git push origin feat-014-redis-streams-backend
# Create PR against main

# 6. Cleanup after merge
cd ~/proyectos/navigator-eventbus   # back to main repo
git worktree remove .claude/worktrees/feat-014
```

## Autonomous Agent (`sdd-worker`)

The `sdd-worker` agent (`.claude/agents/sdd-worker.md`) implements all tasks for
a feature sequentially. Launch it **inside** a manually-created worktree:

```bash
cd .claude/worktrees/<feature-worktree>
claude --agent sdd-worker --model sonnet --verbose
```

Key properties: uses Sonnet, implements EXACTLY what tasks
specify (no redesigns), commits after each task.

For background execution:
```bash
cd .claude/worktrees/feat-014
tmux new -s feat-014 \
  "claude --agent sdd-worker --model sonnet --verbose"
# Ctrl+B, D to detach — tmux attach -t feat-014 to reconnect
```

## Task Index Schema (per-spec)

Each feature has its own per-spec index at `sdd/tasks/index/<feature-slug>.json`.
The header carries flow metadata cached from the spec frontmatter; the
`tasks[]` array is local to that feature only.

```json
{
  "feature": "<feature-slug>",
  "feature_id": "FEAT-<NNN>",
  "spec": "sdd/specs/<feature-slug>.spec.md",
  "type": "feature",
  "base_branch": "main",
  "created_at": "<ISO-8601>",
  "completed_at": null,
  "tasks": [
    {
      "id": "TASK-<NNN>",
      "feature_id": "FEAT-<NNN>",
      "feature": "<feature-slug>",
      "status": "pending",
      "depends_on": [],
      "...": "..."
    }
  ]
}
```

Both `feature_id` and `feature` must be present on every task entry.
Commands resolve features by matching either field (exact, numeric suffix,
or substring) against the per-spec index headers.

> **Heads-up**: `.gitignore` may have a global `templates/` rule.
> The `sdd/templates/*.md` files must remain tracked. If adding a NEW
> template file, use `git add -f` if needed.

### When NOT to Use Worktrees

- **Hotfixes on `main`**: Work directly on `main` or a short-lived `hotfix/*` branch.
- **Documentation-only changes**: No code conflicts possible, work on `main` directly.
- **Single-task features**: If a spec has only one task, a worktree adds overhead
  with no benefit. Work directly on a feature branch.
- **Exploratory brainstorming**: `/sdd-brainstorm` doesn't produce code — no worktree needed.
- **Quick bug fixes**: If the fix is a single commit, skip the worktree ceremony.
