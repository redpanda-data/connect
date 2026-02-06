---
name: rpcn:review
description: Code review a pull request for Redpanda Connect, checking Go patterns, tests, and component architecture
arguments:
  - name: pr
    description: "Pull request number or URL to review. Defaults to current branch PR."
    required: false
allowed-tools: Bash(gh issue view:*), Bash(gh search:*), Bash(gh issue list:*), Bash(gh pr comment:*), Bash(gh pr diff:*), Bash(gh pr view:*), Bash(gh pr list:*), Bash(git log:*), Bash(git blame:*), Bash(git diff:*), Bash(git show:*)
---

Code review pull request {{pr}} for Redpanda Connect. If no PR was specified, resolve the current branch's PR with `gh pr view --json number -q .number`.

This review orchestrates specialized agents for domain-specific analysis. Do not duplicate the expertise of these agents -- delegate to them and synthesize their findings.

## Workflow

1. **Gate check** - Run 1 Haiku agent to check if the PR (a) is closed, (b) is a draft, (c) does not need review (automated PR, trivial change), or (d) already has your review. Stop if any apply.

2. **Gather context** - Run 2 Haiku agents in parallel:
   - **Agent A**: Collect paths to relevant CLAUDE.md files (root `CLAUDE.md`, `config/CLAUDE.md`, and any in directories touched by the PR)
   - **Agent B**: Summarize the PR (files modified, change categories: component implementation, tests, configuration, CLI, etc.)

3. **Review** - Run 3 Sonnet agents in parallel. Each receives the PR diff, change summary, and relevant CLAUDE.md content. Each returns a list of issues with a reason and confidence score 0-100:
   - 0: False positive or pre-existing issue
   - 25: Possibly real, possibly false positive. Stylistic issues not in CLAUDE.md/agent files.
   - 50: Real but nitpick or rare. Not important relative to the rest of the PR.
   - 75: Verified, will be hit in practice. Directly impacts functionality or mentioned in CLAUDE.md/agent files.
   - 100: Confirmed, will happen frequently. Evidence directly confirms this.

   **Agent 1 - Go Patterns & Architecture** (`godev`): Component registration (single vs batch MustRegister*), ConfigSpec construction, field name constants, ParsedConfig extraction, Resources pattern, import organization, license headers, formatting/linting, error handling (wrapping with gerund form, %w), context propagation (no context.Background() in methods, no storing ctx on structs), concurrency patterns (mutex, goroutine lifecycle), shutdown/cleanup (idempotent Close, sync.Once), public wrappers, bundle registration, info.csv metadata, distribution classification.

   **Agent 2 - Tests** (`tester`): Unit: table-driven tests with errContains, assert vs require, config parsing with MockResources, enterprise InjectTestService, processor/input/output/bloblang lifecycle tests, config linting, NewStreamBuilder pipelines, HTTP mock servers. Integration: integration.CheckSkip(t), Given-When-Then with t.Log(), testcontainers-go (module helpers preferred, GenericContainer fallback), NewStreamBuilder with AddBatchConsumerFunc, side-effect imports, async stream.Run with context.Canceled handling, assert.Eventually polling (no require inside), parallel subtest safety, cleanup with context.Background(). Flag changed code lacking tests and new components without integration tests.

   **Agent 3 - Bugs and Security**: Logic errors, nil dereferences, race conditions, resource leaks, SQL/command injection, XSS, hardcoded secrets. Focus on real bugs, not nitpicks.

4. **Filter** - Drop issues scoring below 75. If none remain, skip commenting.

5. **Comment** - Post results via `gh pr comment`. Keep output brief, no emojis. Link and cite relevant code/files/URLs with full git SHA.

## False Positives to Filter (steps 3 and 4)

- Pre-existing issues not introduced in this PR
- Code that looks wrong but is intentional
- Pedantic nitpicks a senior engineer wouldn't flag
- Issues that linters, typecheckers, or compilers catch (imports, types, formatting)
- General quality issues unless explicitly required in CLAUDE.md or agent files
- Issues called out in CLAUDE.md but silenced in code via lint ignore comments
- Functionality changes that are clearly intentional
- Real issues on lines the user did not modify

## Comment Format

If issues found (score >= 75):

```
### Code review

Found N issues:

1. <brief description> (<source>: "<relevant guideline>")

<link to file and line with full SHA + line range>

2. ...

Generated with Claude Code
```

If no issues found:

```
### Code review

No issues found. Checked for bugs, Go patterns and component architecture (godev), test quality (tester), and CLAUDE.md compliance.

Generated with Claude Code
```

## Link Format

Links must follow this exact format for GitHub Markdown rendering:
```
https://github.com/redpanda-data/connect/blob/[full-sha]/path/file.ext#L[start]-L[end]
```
- Full git SHA required (not abbreviated)
- `#L` notation after filename
- Line range format: `L[start]-L[end]`
- Include at least 1 line of context before and after

## Notes

- Do not build, lint, or run tests. Those run separately in CI.
- Use `gh` for all GitHub interactions, not web fetch.
- Create a todo list first to track progress.
- Cite and link every issue (if referring to a CLAUDE.md or agent file, link it).
