---
name: review
description: Code review a pull request for Redpanda Connect, checking Go patterns, tests, component architecture, and commit policy
argument-hint: "[pr-number]"
disable-model-invocation: true
allowed-tools: Bash(gh pr view *), Bash(gh pr diff *), Bash(gh pr comment *), Bash(git log *), Bash(git show *), Bash(git diff *), Read, Glob, Grep, Task
---

Code review pull request $ARGUMENTS for Redpanda Connect. If no PR was specified, resolve the current branch's PR with `gh pr view --json number -q .number`.

This review orchestrates specialized agents for domain-specific analysis. Do not duplicate the expertise of these agents -- delegate to them and synthesize their findings.

## Workflow

1. **Gather context** - Collect the information needed for review. Prefer running these in parallel when possible:
   - Collect paths to relevant CLAUDE.md files (root `CLAUDE.md`, `config/CLAUDE.md`, and any in directories touched by the PR)
   - Summarize the PR (files modified, change categories: component implementation, tests, configuration, CLI, etc.)

2. **Review** - Launch review agents. Each receives the PR diff, change summary, and relevant CLAUDE.md content. Each returns a list of issues with a reason, confidence score 0-100, and (for scores 50-74) a brief explanation of why the reviewer is uncertain. Prefer running independent agents in parallel when possible.

   Confidence scale:
   - 0: False positive or pre-existing issue
   - 25: Possibly real, possibly false positive. Stylistic issues not in CLAUDE.md/skill files.
   - 50: Real but uncertain — reviewer lacks context to confirm severity or correctness. Include uncertainty reason (e.g., "unfamiliar domain pattern", "can't determine intent without runtime context", "possible edge case but depends on caller behavior").
   - 75: Verified, will be hit in practice. Directly impacts functionality or mentioned in CLAUDE.md/skill files.
   - 100: Confirmed, will happen frequently. Evidence directly confirms this.

   **Go Patterns & Architecture** (`godev` agent): Component registration (single vs batch MustRegister*), ConfigSpec construction, field name constants, ParsedConfig extraction, Resources pattern, import organization, license headers, formatting/linting, error handling (wrapping with gerund form, %w), context propagation (no context.Background() in methods, no storing ctx on structs), concurrency patterns (mutex, goroutine lifecycle), shutdown/cleanup (idempotent Close, sync.Once), public wrappers, bundle registration, info.csv metadata, distribution classification.

   **Tests** (`tester` agent): Unit: table-driven tests with errContains, assert vs require, config parsing with MockResources, enterprise InjectTestService, processor/input/output/bloblang lifecycle tests, config linting, NewStreamBuilder pipelines, HTTP mock servers. Integration: integration.CheckSkip(t), Given-When-Then with t.Log(), testcontainers-go (module helpers preferred, GenericContainer fallback), NewStreamBuilder with AddBatchConsumerFunc, side-effect imports, async stream.Run with context.Canceled handling, assert.Eventually polling (no require inside), parallel subtest safety, cleanup with context.Background(). Flag changed code lacking tests and new components without integration tests.

   **Bugs and Security** (general-purpose agent): Logic errors, nil dereferences, race conditions, resource leaks, SQL/command injection, XSS, hardcoded secrets. Focus on real bugs, not nitpicks.

   **Commit Policy** (general-purpose agent): Uses `git log` and `git show --stat` on the PR commits. Checks:
   - **Granularity**: Each commit is one small, self-contained, logical change. Flag commits mixing unrelated work.
   - **Message format** (enforced): Must match one of these patterns:
     - `system: message` (e.g., `otlp: add authz support`, `kafka: fix consumer group rebalance`)
     - `system(subsystem): message` (e.g., `gateway(authz): add http middleware`, `cli(mcp): handle shutdown`)
     - Uppercase plain message for repo-wide/global changes (e.g., `Bump to Go 1.26`, `Update CI workflows`)
   - **Message quality** (enforced): Flag messages that are vague ("fix stuff", "updates", "WIP"), misleading (title doesn't match the actual changes), or incomprehensible.
   - **Fixup/squash**: Flag unsquashed `fixup!`/`squash!` commits.
   - Ignore PR number suffixes `(#1234)`.

3. **Filter** - Drop issues scoring below 50. Separate remaining items into two buckets:
   - **Issues** (score >= 75): Confirmed problems to flag.
   - **Attention areas** (score 50-74): Areas where the reviewer is uncertain and a human should look. Each must include the uncertainty reason from step 2.

   If both buckets are empty, skip commenting.

4. **Comment** - Post results via `gh pr comment`. Keep output brief, no emojis. Link and cite relevant code/files/URLs with full git SHA.

## False Positives to Filter (steps 2 and 3)

- Pre-existing issues not introduced in this PR
- Code that looks wrong but is intentional
- Pedantic nitpicks a senior engineer wouldn't flag
- Issues that linters, typecheckers, or compilers catch (imports, types, formatting)
- General quality issues unless explicitly required in CLAUDE.md or skill files
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

If attention areas found (score 50-74), append after issues (or after the heading if no issues):

```
### Areas for human review

The following areas had observations where the automated review was not confident enough to flag as issues. A human reviewer should verify these.

1. <brief description> — <uncertainty reason>

<link to file and line with full SHA + line range>

2. ...
```

If neither issues nor attention areas found:

```
### Code review

No issues found. Checked for bugs, Go patterns and component architecture (godev), test quality (tester), commit policy, and CLAUDE.md compliance.

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
- Cite and link every issue (if referring to a CLAUDE.md or skill file, link it).
