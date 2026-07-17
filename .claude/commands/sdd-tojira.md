---
description: Export an SDD Specification to an issue tracker (and optionally subtasks). Creates or updates a ticket, updates the spec with the ticket key, and commits the change.
---

# /sdd-tojira -- Export Specification to Issue Tracker

Export the content of a formal specification file (`sdd/specs/*.spec.md`) to a new
or existing ticket. Optionally creates subtasks from decomposed SDD tasks.

```
/sdd-spec -> /sdd-task -> /sdd-tojira -> Ticket + Subtasks
```

## Usage
```
/sdd-tojira sdd/specs/eventbus-dlq.spec.md
/sdd-tojira sdd/specs/eventbus-dlq.spec.md --ticket FEAT-312   # link to existing ticket
/sdd-tojira sdd/specs/eventbus-dlq.spec.md --with-subtasks     # also create subtasks from tasks
/sdd-tojira FEAT-071                                           # resolve by Feature ID
/sdd-tojira FEAT-071 --ticket FEAT-312 --with-subtasks         # full combo
```

### Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `<spec_path or FEAT-ID>` | yes | Path to `.spec.md` or Feature ID to resolve |
| `--ticket <TICKET_KEY>` | no | Existing ticket to update instead of creating |
| `--with-subtasks` | no | Create sub-tasks from the feature's per-spec index `sdd/tasks/index/<feature-slug>.json` |

## Guardrails
- The input must be a valid path to an existing `.spec.md` file, or a Feature ID.
- Do NOT create duplicate tickets -- resolve existing ones first.
- **Always commit the spec update** (with ticket key) so worktrees can see it.
- Do NOT modify existing tickets unless the user explicitly requests an update
  OR the ticket was resolved via `--ticket` / spec metadata.

## Issue Access Strategy

Use **GitHub Issues/PRs** via `gh` CLI as the primary method:

```bash
# Search for existing issues
gh issue list --search "$TICKET_KEY" --json number,title,body 2>/dev/null

# Create a new issue
gh issue create --title "<title>" --body "<description>" 2>/dev/null
```

If the project uses an external tracker, adapt the access method accordingly.

## Steps

### 1. Resolve Spec File and Determine Mode

#### 1a. Resolve the spec

If the user passes a Feature ID instead of a path:
```bash
grep -rl "FEAT-071" sdd/specs/ | head -1
```

Read the spec file and extract all fields (see table in Step 2).

#### 1b. Resolve the ticket (tri-mode resolution)

Determine whether to CREATE a new ticket or UPDATE an existing one.
Evaluate these sources in priority order -- **first match wins**:

```
Priority 1: --ticket argument
   User passed --ticket FEAT-312
   -> MODE = UPDATE, TICKET_KEY = FEAT-312

Priority 2: Spec metadata
   Spec frontmatter contains `ticket: FEAT-312`
   OR spec body contains `**Ticket**: [FEAT-312](...)`
   -> MODE = UPDATE, TICKET_KEY = FEAT-312

Priority 3: Search by Feature ID
   gh issue list --search "FEAT-071" --json number,title
   -> If found:
       Existing ticket found: #42 -- "[FEAT-071] eventbus-dlq"
           Status: Open

           Options:
           1. Update -- sync description and AC from spec (recommended)
           2. Skip -- do nothing, just link spec to this ticket
           3. Create new -- create a separate ticket (not recommended)

       Wait for user choice. Default: update.
   -> MODE = UPDATE | SKIP | CREATE based on choice

Priority 4: No match
   -> MODE = CREATE
```

#### 1c. Announce mode

```
/sdd-tojira: FEAT-071 -- eventbus-dlq

   Spec: sdd/specs/eventbus-dlq.spec.md
   Mode: UPDATE existing FEAT-312  |  CREATE new ticket
```

### 2. Extract Spec Content

| Field | Source in Spec | Maps to Ticket |
|-------|---------------|----------------|
| Feature ID | Metadata header | Title prefix: `[FEAT-NNN]` |
| Feature Name | `# <title>` | Title |
| Section 1 | Motivation & Business Requirements | Description |
| Section 5 | Acceptance Criteria | AC section in description |
| Components | Module Breakdown / Impact | Labels |
| Effort | Worktree Strategy or task index | Estimate note |

**Description format:**
```markdown
## Motivation

<Section 1 content>

## Architectural Overview

<Section 2 summary -- first 2-3 paragraphs only, not full design>

## Acceptance Criteria

<Section 5 content>

---
_Exported from SDD spec: sdd/specs/<feature-name>.spec.md_
_Feature ID: FEAT-<ID>_
```

**Estimate** from the per-spec index (if `--with-subtasks`). The index file
is already feature-scoped, so read it directly -- no cross-feature filtering
needed:
```bash
INDEX="sdd/tasks/index/<feature-slug>.json"
TOTAL_HOURS=$(jq '[.tasks[] |
  if .effort=="S" then 4
  elif .effort=="M" then 8
  elif .effort=="L" then 16
  elif .effort=="XL" then 32
  else 8 end] | add' "$INDEX")
```
Default: `8` (1 day) if the index has no tasks.

### 3. Execute: CREATE or UPDATE

#### If MODE = CREATE

```bash
gh issue create \
  --title "[FEAT-071] eventbus-dlq -- DLQ Support for Event Bus" \
  --body "<formatted description>" \
  --label "sdd,feature"
```

Extract the created ticket number from the output.

#### If MODE = UPDATE

Update description on the existing ticket:

```bash
gh issue edit "$TICKET_NUMBER" --body "<formatted description>"
```

**Important**: In UPDATE mode, do NOT overwrite the title -- the user may have
customized it. Only update the description.

### 4. Create Subtasks (if --with-subtasks)

If tasks exist in the feature's per-spec index `sdd/tasks/index/<feature-slug>.json`:

For each task, create a sub-issue or checklist item:

```bash
gh issue create \
  --title "[TASK-001] Event handler implementation" \
  --body "<task description + scope>" \
  --label "sdd,task"
```

Map effort to hours: S=4h, M=8h, L=16h, XL=32h.

### 5. Update Spec with Ticket Key

Ensure the spec file has the ticket key. Skip if already present:

```bash
grep -q "^ticket:" sdd/specs/<feature>.spec.md && echo "Already linked"
grep -q "^\*\*Ticket\*\*:" sdd/specs/<feature>.spec.md && echo "Already linked"
```

If not present, add after the title:
```markdown
# FEAT-071 -- DLQ Support for Event Bus

**Ticket**: [FEAT-312](link-to-ticket)
**Status**: approved
```

Or in YAML frontmatter: add `ticket: FEAT-312`.

### 6. Update Task Index (if --with-subtasks)

Add ticket keys to each task entry in the per-spec index
`sdd/tasks/index/<feature-slug>.json`:

```json
{
  "id": "TASK-001",
  "feature_id": "FEAT-071",
  "ticket_key": "#43",
  "ticket_parent": "#42"
}
```

### 7. Commit Changes

```bash
# CRITICAL: Unstage everything first -- NEVER commit unrelated changes
git reset HEAD
# Stage ONLY the modified SDD files -- NEVER use "git add ." or "git add -A"
git add sdd/specs/<feature-name>.spec.md
# If subtasks were created:
git add sdd/tasks/index/<feature-slug>.json
# Verify ONLY the expected files are staged
git diff --cached --name-only
# If ANY unrelated files appear, run "git reset HEAD" and start over
git commit -m "sdd: export FEAT-<ID> to ticket <TICKET_KEY>"
```

Skip commit if nothing changed (spec already had ticket key, no new subtasks).

### 8. Output

#### CREATE mode
```
Spec exported to ticket: #42 (created)

   Type: Feature
   Estimate: 3d (24h across 4 tasks)
   AC: 3 criteria exported

   Subtasks created:
     #43 -- [TASK-001] Event handler implementation [S/4h]
     #44 -- [TASK-002] DLQ retry logic [M/8h]
     #45 -- [TASK-003] Ingress model validation [M/8h]
     #46 -- [TASK-004] Hook lifecycle management [S/4h]

   Spec updated and committed.

Next steps:
  1. Review the ticket.
  2. Assign and prioritize.
  3. To implement: /sdd-start or use sdd-autopilot.
```

#### UPDATE mode
```
Spec synced to ticket: #42 (updated)

   Updated: description, AC
   Subtasks: 2 existing + 2 created

   Spec already linked -- no commit needed.
```

## Reverse Linking

The `ticket:` metadata in the spec enables:
- `/pr-review` to auto-detect the ticket key from the spec
- `sdd-autopilot` to post completion comments back to the tracker
- `/sdd-done` to optionally transition the ticket to "Done"
- `/sdd-tojira` itself to detect UPDATE mode on re-runs (idempotent)

## Edge Cases

- **Spec not approved**: Warn and ask for confirmation.
- **No AC in spec**: Create ticket without AC. Warn.
- **gh CLI not authenticated**: Error with setup instructions (`gh auth login`).
- **Subtask type not available**: Fall back to linked issues.
- **Idempotent re-runs**: Second run detects existing key (Priority 2) -> UPDATE mode. No duplicates.

## Reference
- Spec template: `sdd/templates/spec.md`
- Per-spec task index: `sdd/tasks/index/<feature-slug>.json`
- SDD methodology: `sdd/WORKFLOW.md`
- Auto-commit rule: `CLAUDE.md` (section "SDD Auto-Commit Rule")
