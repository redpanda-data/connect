# Claude Review Policy

Shared judgment policy for the Redpanda Connect automated review. It is consumed by
both the interactive `/review` skill (`.claude/skills/review/SKILL.md`) and the CI
reviewer (`.github/workflows/claude-code-review.yml`) so the two never drift. Each
entry point wires up its own tools and orchestration separately; the rules below are
identical for both.

The rules a PR is judged against live in `CLAUDE.md` and `CONTRIBUTING.md` — this file
does not restate them. It defines only *how* to review: what clears the signal bar,
what to filter out, and the comment formats.

## Signal bar

We only want HIGH SIGNAL issues. Flag:

- Clear, unambiguous `CLAUDE.md` or `CONTRIBUTING.md` violations where you can quote the exact rule broken. Cite the clause (e.g. "§1.2.2", "§5.4.1"). For inherently subjective criteria (UX polish, documentation wording) flag only clear, material gaps backed by concrete evidence in the diff — never stylistic preferences. Connector selection criteria (§2) are NOTEs, never blocking.
- Project Go pattern or test pattern violations.
- Bugs and security issues: logic errors, nil dereferences, race conditions, resource leaks, injection, hardcoded secrets.
- Commit policy violations (`CONTRIBUTING.md` §3.3 change size, §3.4 commit conventions).

If you are not certain an issue is real, do not flag it. False positives erode trust and waste reviewer time.

## False positives to filter

- Pre-existing issues not introduced in this PR
- Code that looks wrong but is intentional
- Pedantic nitpicks a senior engineer wouldn't flag
- Issues that linters, typecheckers, or compilers catch (imports, types, formatting)
- General quality issues unless explicitly required in `CLAUDE.md` or a skill file
- Issues called out in `CLAUDE.md` but silenced in code via a lint ignore comment
- Functionality changes that are clearly intentional
- Real issues on lines the author did not modify

## Summary comment format

```
**Commits**
<either "LGTM" if no violations, or a numbered list of violations>

**Review**
<short summary>

<either "LGTM" if no code review issues, or a numbered list of issues with links>
```

## Link format

Links must follow this exact format for GitHub Markdown rendering:

```
https://github.com/redpanda-data/connect/blob/[full-sha]/path/file.ext#L[start]-L[end]
```

- Full git SHA required (not abbreviated, not a command like `$(git rev-parse HEAD)`)
- `#L` notation after the filename
- Line range format: `L[start]-L[end]`
- Include at least 1 line of context before and after
