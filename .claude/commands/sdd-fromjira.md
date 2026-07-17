---
description: Bootstrap an SDD Brainstorm from a ticket or issue tracker. Fetches requirements, conducts interactive Q&A, researches the codebase, and produces a worker-ready brainstorm document.
---

# /sdd-fromjira -- Bootstrap Brainstorm from Issue Tracker

Fetch requirements from a ticket or issue and scaffold a structured brainstorm document
in `sdd/proposals/`, following the full `/sdd-brainstorm` quality bar.

This command is the issue-seeded entry point to the SDD pipeline:
```
/sdd-fromjira <TICKET_KEY> -> (Q&A) -> brainstorm -> /sdd-spec -> /sdd-task -> implement
```

## Usage
```
/sdd-fromjira FEAT-312
/sdd-fromjira FEAT-312 --complexity=fix       # minimal Q&A, straight to brainstorm
/sdd-fromjira FEAT-312 --skip-qa              # use description as-is (rare)
```

## Guardrails
- Do NOT modify the source ticket -- only READ from it.
- Always use the template at `sdd/templates/brainstorm.md`.
- Output file: `sdd/proposals/<issue-key>-<slug>.brainstorm.md`.
- **Always commit the brainstorm file** so other commands and worktrees can see it.
- **No implementation code** -- this is about ideas, references, and tradeoffs.
- Include concrete library/package references with versions when possible.
- Reference existing codebase modules that would be reused or extended.

## Issue Access Strategy

Search the repository for existing specs, task files, or brainstorm docs that
reference the ticket key. Use `grep` and `find` to locate relevant context:

```bash
# Search for SDD artifacts referencing this ticket
grep -rl "$TICKET_KEY" sdd/ 2>/dev/null

# Check GitHub issues
gh issue list --search "$TICKET_KEY" --json number,title,body 2>/dev/null
```

## Steps

### 1. Fetch Ticket Information

Retrieve the ticket context using available tools. Extract:

| Field | Source | Purpose |
|-------|--------|---------|
| Summary | Issue title or spec title | Brainstorm title |
| Description | Issue body or spec motivation | Problem statement seed |
| Acceptance Criteria | Spec section 5 or issue description | Constraints & success criteria |
| Issue Type | Label or type field | Complexity hint |
| Priority | Priority label | Urgency context |
| Components | Labels or affected modules | Affected areas |

If the ticket is not found or inaccessible, notify the user with verification
guidance.

### 2. Parse Content

Convert the raw data into structured context:

**Extract structured context:**
- **Problem Statement**: What is the primary pain point described?
- **Constraints / Requirements**: Technical constraints or business rules from the description.
- **Acceptance Criteria**: Numbered list from the AC field (or extracted from description).
- **Context**: Existing systems, related tickets, background mentioned.
- **Scope Indicators**: Component names, labels, linked tickets.

### 3. Classify Complexity

Assess the ticket complexity to calibrate Q&A depth:

| Signal | Complexity | Q&A Rounds |
|--------|-----------|------------|
| Bug type, 1 component, clear AC, no subtasks | `fix` | 1 |
| Story type, 2-3 components, some AC | `simple` | 2 |
| Story/Epic, multiple components, vague or absent AC | `standard` | 2-3 |
| Epic, cross-cutting, architectural, many subtasks | `complex` | 3+ |

The `--complexity` flag overrides auto-detection.
The `--skip-qa` flag skips Q&A entirely (use only when description is exhaustive).

### 4. Present Context to User

Before asking questions, present the extracted context:

```
Ticket: FEAT-312

   Summary: Core Event Bus Extraction
   Type: Feature | Priority: High
   Components: eventbus, hooks

   Description (extracted):
   <parsed description, truncated to ~500 chars>

   Acceptance Criteria:
   1. Event bus core is extracted as standalone package
   2. Hooks system works independently
   3. DLQ support is included

   Complexity assessment: standard (2-3 Q&A rounds)
```

### 5. Interactive Discovery (Mandatory -- adapted to complexity)

**DO NOT skip this step** unless `--skip-qa` is explicitly passed.

The ticket provides initial context but it is never complete enough for
implementation. Use the ticket content to ask TARGETED questions -- not generic
ones. The questions should fill gaps that the ticket leaves open.

**For `fix` complexity (1 round):**
Ask 2-3 targeted questions:
- Root cause hypothesis -- does the user already know what's broken?
- Affected files -- which modules should be touched?
- Regression risk -- could this fix break something else?

**For `simple` / `standard` complexity (2+ rounds):**

**Round 1 -- Clarify intent beyond what the ticket says:**
Ask 3-5 questions about:
- Ambiguities in the description (what does "X" mean exactly?)
- Integration points not mentioned in the ticket
- Non-obvious constraints (performance, backwards compatibility, security)
- What success looks like beyond the AC

Wait for the user's answers before proceeding.

**Round 2 -- Drill into gaps and tradeoffs:**
Based on Round 1 answers, ask 3-5 follow-up questions about:
- Edge cases and failure scenarios
- Priority between competing concerns
- Assumptions you're making that need validation
- Relationships with linked/subtask tickets

Wait for the user's answers before proceeding.

**For `complex` complexity (3+ rounds):**
Additional rounds for:
- Architectural tradeoff discussion
- Scope negotiation (suggest breaking into multiple tickets if needed)
- Cross-team impact assessment

**Rule:** Only proceed to codebase research once you are confident that
all core questions have been answered. If a question is critical and unanswered,
ask -- do not assume.

### 6. Research the Codebase & Build Code Context

Scan the project for relevant existing components:
- Search for related modules, classes, and patterns.
- Identify reusable code that any solution should build on.
- Note existing dependencies that could be leveraged.
- Use the ticket's **components** and **labels** as search hints.

**CRITICAL -- Code Context Capture:**
This is where hallucinations are prevented. For every class, method, or module
you reference in the brainstorm, you MUST:

1. **Read the actual source file** and record exact signatures (class name, method names,
   parameter types, return types) with file path and line numbers.
2. **Verify imports** -- confirm `from navigator_eventbus.X import Y` actually works by checking
   `__init__.py` files and module structure.
3. **Capture user-provided code** -- if the user pasted code snippets during discovery
   (Steps 4-5), preserve them verbatim in the Code Context section.
4. **Note what does NOT exist** -- if you searched for something plausible that turned
   out not to exist, record it in the "Does NOT Exist" subsection. This is critical
   for preventing downstream agents from hallucinating these references.

### 7. Generate Options

Produce **at least 3** distinct approaches. For each option:
- Descriptive name and explanation (WHAT, not HOW).
- Pros and cons (be honest about tradeoffs).
- Effort estimate (Low / Medium / High).
- **Libraries / Tools**: table of packages with purpose and notes.
- **Existing Code to Reuse**: specific paths and descriptions.

Include at least one unconventional or less obvious approach.

**Enrichment**: Map each option against the acceptance criteria.
If an option doesn't cover all AC, note which ones it misses and why.

### 8. Recommend

Select one option and explain the reasoning:
- Reference specific tradeoffs from the options.
- Explain what you're trading off and why it's acceptable.
- Confirm that all AC are covered by the recommendation.

### 9. Describe the Feature

Write a detailed feature description based on the recommended option:
- **User-facing behavior**: what the user sees/experiences.
- **Internal behavior**: high-level flow and responsibilities (no code).
- **Edge cases & error handling**: boundary conditions, failure modes.

### 10. Map to SDD Structures

Fill in the remaining template sections:
- **Capabilities**: new and modified (kebab-case identifiers).
- **Impact & Integration**: affected components table.
- **Open Questions**: unresolved items with owners.

### 11. Parallelism Assessment

Evaluate the feature's decomposition potential for parallel development:
- **Internal parallelism**: Can this feature's tasks be split into independent worktrees?
- **Cross-feature independence**: Does this feature conflict with any in-flight specs?
- **Recommended isolation**: `per-spec` or `mixed`.
- **Rationale**: Brief explanation.

### 12. Save and Commit

1. Read the template at `sdd/templates/brainstorm.md`.
2. Create `sdd/proposals/<issue-key>-<slug>.brainstorm.md` with today's date.
3. Add metadata block at the top, **including the FEAT-145 flow-type
   fields**:
   ```markdown
   ---
   # FEAT-145 flow-type fields. Default to feature/dev.
   type: feature
   base_branch: dev
   ticket: FEAT-312
   ticket_summary: "Core Event Bus Extraction"
   ticket_type: Feature
   ticket_priority: High
   ticket_components: [eventbus, hooks]
   complexity: standard
   status: exploration
   ---
   ```
   Validation rule: `type: hotfix` REQUIRES `base_branch: main`. The user
   can adjust the values before running `/sdd-spec`.
4. Set `Status: exploration`.
5. **Commit:**
   ```bash
   # Unstage everything first -- NEVER commit unrelated changes
   git reset HEAD
   # Stage ONLY the brainstorm file -- NEVER use "git add ." or "git add -A"
   git add sdd/proposals/<issue-key>-<slug>.brainstorm.md
   # Verify ONLY the brainstorm file is staged
   git diff --cached --name-only
   # If ANY unrelated files appear, run "git reset HEAD" and start over
   git commit -m "sdd: add brainstorm from <issue-key> -- <slug>"
   ```

### 13. Output

```
Brainstorm bootstrapped from ticket and committed:
   sdd/proposals/<issue-key>-<slug>.brainstorm.md

   Ticket: <issue-key> -- <summary>
   Complexity: <fix|simple|standard|complex>
   Recommended: Option <X> -- <name>
   Effort: <Low|Medium|High>
   AC coverage: <met>/<total> criteria addressed
   Worktree isolation: <per-spec|mixed>
   Open questions: <count>

Next steps:
  1. Review the generated brainstorm options.
  2. Refine the recommendation.
  3. When ready: /sdd-spec <issue-key>-<slug> (uses this brainstorm as input)
```

## How sdd-spec Consumes This Document

When `/sdd-spec` is invoked with a feature name matching a `<issue-key>-*.brainstorm.md`:
- **Ticket metadata** -> Spec metadata (ticket key, components)
- **Problem Statement** -> Spec Section 1 (Motivation & Business Requirements)
- **Acceptance Criteria** -> Spec Section 5 (Acceptance Criteria) -- carried from ticket
- **Constraints** -> Spec Section 5 (additional criteria)
- **Recommended Option** -> Spec Section 2 (Architectural Design)
- **Libraries / Tools** -> Spec Section 7 (External Dependencies)
- **Feature Description** -> Spec Section 2 (Overview + Integration Points)
- **Capabilities** -> Spec Section 3 (Module Breakdown)
- **Impact & Integration** -> Spec Section 2 (Integration Points)
- **Code Context** -> Spec Section 6 (Codebase Contract) -- **carries forward verified code**
- **Parallelism Assessment** -> Spec Worktree Strategy section
- **Open Questions** -> Spec Section 8

## Differences from /sdd-brainstorm

| Aspect | /sdd-brainstorm | /sdd-fromjira |
|--------|-----------------|---------------|
| Input source | Free-form user notes | Ticket (structured) |
| Problem statement | User describes it | Extracted from ticket description |
| Acceptance criteria | Discovered during Q&A | Imported from ticket AC field |
| Q&A focus | Open exploration | Gap-filling (ticket provides baseline) |
| Filename | `<slug>.brainstorm.md` | `<issue-key>-<slug>.brainstorm.md` |
| Metadata | Minimal | Includes ticket key, type, priority, components |
| Complexity hint | None | Auto-detected from ticket signals |

## Reference
- Brainstorm template: `sdd/templates/brainstorm.md`
- Spec template: `sdd/templates/spec.md`
- SDD methodology: `sdd/WORKFLOW.md`
- Worktree policy: `CLAUDE.md` (section "Worktree Policy")
