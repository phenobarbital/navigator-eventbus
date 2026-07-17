# /pr-review — Review a PR Against Acceptance Criteria

Fetches a GitHub Pull Request and its associated ticket, then reviews
whether the code changes satisfy the ticket's description and acceptance criteria.
Optionally converts the PR to draft if criteria are not met.

## Usage

```
/pr-review <PR_URL> <TICKET_KEY> [--auto-draft]
```

### Examples

```
/pr-review https://github.com/Trocdigital/navigator-eventbus/pull/12 FEAT-312
/pr-review https://github.com/Trocdigital/navigator-eventbus/pull/12 FEAT-312 --auto-draft
```

### Arguments

| Argument       | Required | Description |
|----------------|----------|-------------|
| `<PR_URL>`     | yes      | Full GitHub PR URL (e.g., `https://github.com/org/repo/pull/123`) |
| `<TICKET_KEY>` | yes      | Ticket or issue key (e.g., `FEAT-312`) |
| `--auto-draft` | no       | If present AND criteria fail, convert PR to draft automatically |

## Prerequisites

- `gh` CLI installed and authenticated (`gh auth status`)
- `jq` installed (for JSON parsing)

## Steps

### 1. Parse Input & Validate Prerequisites

Extract org, repo, and PR number from the URL:

```bash
# Parse: https://github.com/Trocdigital/navigator-eventbus/pull/12
PR_URL="$1"
TICKET_KEY="$2"
AUTO_DRAFT=false
if [[ "$3" == "--auto-draft" ]]; then AUTO_DRAFT=true; fi

# Extract components
REPO=$(echo "$PR_URL" | sed -E 's|https://github.com/([^/]+/[^/]+)/pull/.*|\1|')
PR_NUMBER=$(echo "$PR_URL" | sed -E 's|.*/pull/([0-9]+).*|\1|')

# Validate
gh auth status 2>/dev/null || echo "gh CLI not authenticated. Run: gh auth login"
```

### 2. Fetch GitHub PR Data

Collect three things from the PR: metadata, description, and the diff.

```bash
# PR metadata (title, state, author, base/head branches, labels, reviewers)
gh pr view "$PR_NUMBER" --repo "$REPO" --json title,state,author,baseRefName,headRefName,labels,reviewRequests,isDraft

# PR description (body)
gh pr view "$PR_NUMBER" --repo "$REPO" --json body --jq '.body'

# Full diff (the actual code changes)
gh pr diff "$PR_NUMBER" --repo "$REPO"

# List of changed files (for summary context)
gh pr diff "$PR_NUMBER" --repo "$REPO" --name-only
```

**Diff size guard**: If the diff exceeds ~8000 lines, switch to a summarized
approach -- fetch only filenames + stats, then selectively read the most relevant
files based on the ticket context:

```bash
DIFF_LINES=$(gh pr diff "$PR_NUMBER" --repo "$REPO" | wc -l)
if [[ "$DIFF_LINES" -gt 8000 ]]; then
    echo "Large PR ($DIFF_LINES lines). Fetching file-level stats and sampling key files."
    gh pr diff "$PR_NUMBER" --repo "$REPO" --stat
    # Then selectively: gh api repos/$REPO/pulls/$PR_NUMBER/files --paginate
    # and read only files matching patterns from the ticket
fi
```

### 3. Fetch PR Comments & AI Bot Reviews

Collect PR comments, review comments, and reviews to surface findings from
automated reviewers (Gemini Code Assist, GitHub Copilot, CodeRabbit, etc.).

```bash
# PR issue-level comments (general discussion)
gh api repos/$REPO/issues/$PR_NUMBER/comments --paginate \
  --jq '.[] | {author: .user.login, authorType: .author_association, body: .body, created: .created_at}' \
  | head -200

# PR review comments (inline on specific lines of code)
gh api repos/$REPO/pulls/$PR_NUMBER/comments --paginate \
  --jq '.[] | {author: .user.login, path: .path, line: .line, body: .body, created: .created_at}' \
  | head -200

# PR reviews (approve/request-changes/comment with body)
gh api repos/$REPO/pulls/$PR_NUMBER/reviews --paginate \
  --jq '.[] | {author: .user.login, state: .state, body: .body, submitted: .submitted_at}' \
  | head -200
```

**AI bot detection**: Identify comments from known automated reviewers by
matching author login patterns:

| Bot | Login Pattern |
|-----|---------------|
| Gemini Code Assist | `gemini-code-assist[bot]` |
| GitHub Copilot | `copilot-pull-request-review[bot]`, `github-copilot[bot]` |
| CodeRabbit | `coderabbitai[bot]` |
| SonarCloud | `sonarcloud[bot]` |
| Codacy | `codacy-production[bot]` |
| Deepsource | `deepsource-autofix[bot]`, `deepsource-io[bot]` |

```bash
# Filter for known bot authors
KNOWN_BOTS="gemini-code-assist|copilot-pull-request-review|github-copilot|coderabbitai|sonarcloud|codacy-production|deepsource"

# Bot issue comments
gh api repos/$REPO/issues/$PR_NUMBER/comments --paginate \
  --jq "[.[] | select(.user.login | test(\"$KNOWN_BOTS\"))]"

# Bot review comments (inline)
gh api repos/$REPO/pulls/$PR_NUMBER/comments --paginate \
  --jq "[.[] | select(.user.login | test(\"$KNOWN_BOTS\"))]"

# Bot reviews
gh api repos/$REPO/pulls/$PR_NUMBER/reviews --paginate \
  --jq "[.[] | select(.user.login | test(\"$KNOWN_BOTS\"))]"
```

**How to use bot findings in the review:**

- **Agree/Disagree**: For each substantive bot finding, state whether you agree
  or disagree and why. Bot tools can produce false positives.
- **Incorporate valid findings**: If a bot found a real issue (bug, security
  concern, missing edge case), include it in the Code Observations section
  and credit the source (e.g., "Gemini Code Assist flagged...").
- **Dismiss false positives**: If a bot finding is incorrect or irrelevant,
  note it briefly with reasoning so the PR author can ignore it confidently.
- **Resolved vs unresolved**: Check if the bot's comment was addressed in a
  subsequent commit. If resolved, note it as such.

Add a dedicated section to the review report:

```markdown
## AI Bot Review Findings

### {Bot Name} ({N} comments)

| # | File | Finding | Our Assessment |
|---|------|---------|----------------|
| 1 | {path:line} | {summary of bot finding} | Agree / Disagree -- {reason} / Resolved |

{If no bot comments found: "No automated reviewer comments found on this PR."}
```

### 4. Fetch Ticket Data

Use the ticket identifier to gather context. Search the repository for specs,
brainstorm docs, or task files that reference the ticket key:

```bash
# Search for SDD artifacts referencing this ticket
grep -rl "$TICKET_KEY" sdd/ 2>/dev/null

# Check for a spec file
find sdd/specs/ -name "*${TICKET_KEY}*" 2>/dev/null

# Check for task files
find sdd/tasks/ -name "*${TICKET_KEY}*" 2>/dev/null
```

If the ticket key matches a FEAT-ID or TASK-ID pattern, load the corresponding
SDD spec and task files for acceptance criteria context.

### 5. Build the Review Context

Assemble all gathered data into a structured format so Claude can analyze it directly:

```markdown
## PR Review Context

### Ticket: {TICKET_KEY}
**Summary**: {ticket summary or spec title}
**Status**: {status}
**Type**: {type}

#### Description
{ticket description or spec motivation}

#### Acceptance Criteria
{acceptance_criteria -- numbered list}

---

### Pull Request: {REPO}#{PR_NUMBER}
**Title**: {pr.title}
**Author**: {pr.author}
**Branch**: {pr.headRefName} -> {pr.baseRefName}
**State**: {pr.state} | Draft: {pr.isDraft}

#### PR Description
{pr.body}

#### Changed Files
{file list with +/- stats}

#### Full Diff
{diff content -- or sampled files for large PRs}

#### AI Bot Comments
{bot_name: [{path, line, finding}...] -- or "None found"}

#### Human Review Comments
{reviewer: [{path, line, comment}...] -- or "None found"}
```

### 6. Perform the Review

Analyze the PR against the ticket using these review dimensions:

#### 5a. Acceptance Criteria Compliance

For EACH acceptance criterion, determine:
- **Met**: The diff clearly implements the criterion.
- **Partially Met**: Some aspects are present but incomplete or unclear.
- **Not Met**: No evidence in the diff that this criterion is addressed.
- **Unable to Verify**: Requires runtime testing, external system, or
  context not available in the diff (e.g., "works on mobile").

#### 5b. Description Alignment

Check if the PR's changes match the ticket description:
- Does the PR address the core problem/feature described?
- Are there changes in the PR that are NOT related to the ticket? (scope creep)
- Are there aspects of the description NOT addressed by the PR? (gaps)

#### 5c. Code Quality Observations (lightweight)

Not a full code review -- focus on red flags:
- Obviously missing error handling in new code
- Hardcoded values that should be configurable
- Missing tests for new functionality
- Potential breaking changes

### 7. Generate the Review Report

Output a structured report:

```markdown
# PR Review: {REPO}#{PR_NUMBER} <-> {TICKET_KEY}

**Date**: {today}
**Reviewer**: Claude Code (automated)
**Overall Verdict**: Approved | Needs Attention | Does Not Meet Criteria

---

## Acceptance Criteria Compliance

| # | Criterion | Status | Evidence |
|---|-----------|--------|----------|
| 1 | {criterion_1} | Met/Partial/Not Met | {file:line or explanation} |
| 2 | {criterion_2} | Met/Partial/Not Met | {file:line or explanation} |
| ... | | | |

**Score**: {met}/{total} criteria met ({percentage}%)

## Description Alignment

### Addressed
- {aspect 1 from description that IS covered}

### Gaps
- {aspect from description NOT covered by the PR}

### Out of Scope
- {changes in PR not related to the ticket}

## Code Observations
- {any red flags, brief}

## AI Bot Review Findings

{If bot comments were found, add a subsection per bot:}

### {Bot Name} ({N} comments)

| # | File | Finding | Our Assessment |
|---|------|---------|----------------|
| 1 | {path:line} | {summary of bot finding} | Agree / Disagree -- {reason} / Resolved |

{If no bot comments found: "No automated reviewer comments found on this PR."}

## Verdict & Recommendation

{Summary paragraph explaining the overall assessment}

**Action**: {APPROVE | REQUEST_CHANGES | CONVERT_TO_DRAFT}
```

### 8. Take Action (Optional)

Based on the verdict and flags:

#### If `--auto-draft` AND verdict is Not Met:

```bash
# Convert PR to draft using GitHub GraphQL API
PR_NODE_ID=$(gh pr view "$PR_NUMBER" --repo "$REPO" --json id --jq '.id')
gh api graphql -f query='
  mutation {
    convertPullRequestToDraft(input: {pullRequestId: "'"$PR_NODE_ID"'"}) {
      pullRequest { isDraft }
    }
  }
'
echo "PR #$PR_NUMBER converted to draft -- criteria not met."
```

#### Always: Post review as PR comment

```bash
# Post the review report as a PR comment
gh pr comment "$PR_NUMBER" --repo "$REPO" --body "$REVIEW_REPORT"
```

#### Optionally: Add labels

```bash
# Tag the PR with review status
if [[ "$VERDICT" == "approved" ]]; then
    gh pr edit "$PR_NUMBER" --repo "$REPO" --add-label "review:passed"
elif [[ "$VERDICT" == "needs-attention" ]]; then
    gh pr edit "$PR_NUMBER" --repo "$REPO" --add-label "review:needs-attention"
else
    gh pr edit "$PR_NUMBER" --repo "$REPO" --add-label "review:blocked"
fi
```

### 9. Save Report (Optional)

If the user confirms, persist the review:

```bash
mkdir -p artifacts/reviews/
REPORT_FILE="artifacts/reviews/PR-${PR_NUMBER}-${TICKET_KEY}-review.md"
# Save report to file
git add "$REPORT_FILE"
git commit -m "review: PR #${PR_NUMBER} against ${TICKET_KEY}"
```

### 10. Output Summary

```
PR Review Complete: {REPO}#{PR_NUMBER} <-> {TICKET_KEY}

   Verdict: {verdict_text}
   Criteria: {met}/{total} met ({percentage}%)
   
   {if auto-draft triggered}
   PR converted to draft -- criteria not met.
   {end if}
   
   Comment posted: {pr_comment_url}
   Report saved: artifacts/reviews/PR-{PR_NUMBER}-{TICKET_KEY}-review.md

   To approve the PR:
     gh pr review {PR_NUMBER} --repo {REPO} --approve
   
   To request changes:
     gh pr review {PR_NUMBER} --repo {REPO} --request-changes --body "..."
```

## Edge Cases

- **No acceptance criteria found**: Use the ticket description as the evaluation
  baseline. Warn that the review is based on description only.
- **PR already merged**: Warn and still produce the review (useful for auditing).
- **PR is already draft**: Skip the convert-to-draft step, note it in the report.
- **Private repo**: `gh` handles auth. No extra steps.
- **Ticket not found**: Error with clear message suggesting to verify the key.
- **Large PR (>8000 lines)**: Use file-level stats + selective file reading.
  Note in the report which files were fully reviewed vs. summarized.
- **Bot comments with outdated suggestions**: If a bot comment references code
  that was changed in a subsequent commit, mark the finding as "Resolved" rather
  than evaluating the stale suggestion.
- **Conflicting bot opinions**: If two bots disagree (e.g., Gemini says X is fine,
  Copilot flags X), evaluate the code independently and state which bot is correct.

## Reference

- Existing code review: `.claude/commands/sdd-codereview.md`
- Code reviewer agent: `.claude/agents/code-reviewer.md`
- GitHub CLI docs: `gh pr --help`
