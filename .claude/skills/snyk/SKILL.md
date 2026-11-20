---
name: snyk
description: Scan the repo's Go modules with Snyk, report which dependency upgrades fix which CVEs, and apply them with go get followed by lint and tests. Use when asked to run Snyk, check for or fix vulnerable dependencies, bump dependencies for CVEs, or work from a Snyk report.
argument-hint: "[report|fix|dry-run] [FILES=\"go.mod ...\"]"
allowed-tools: Bash(task snyk:*), Bash(task lint), Bash(task test*), Bash(jq *), Bash(git status*), Bash(git diff*), Read, Edit
---

# Snyk dependency scan and fix

Two tasks in `taskfiles/snyk.yml`:

| Task | What it does | Output |
|---|---|---|
| `task snyk:report` | Runs `snyk test --file=<go.mod>` once per project in `SNYK_FILES` | `target/snyk/report.json` |
| `task snyk:fix` | Runs `go get <module>@<version>` and `go mod tidy` for each upgrade in the report, then `task lint` and `task test` | `target/snyk/fix.json` |

## Which version `fix` upgrades to

Each module goes to exactly the **minimum version Snyk says fixes the CVE**. This keeps security bumps small, easy to review and backport, and avoids pulling in unrelated dependency bumps or a newer `go` directive. If the user wants a newer version than that, bump it by hand with `go get` after the fix.

`target/` is gitignored. Raw Snyk output for each project is in `target/snyk/raw/`, and the per-project `fix` plans and `go get` logs are in `target/snyk/fix/`.

## Prerequisites

- `snyk` CLI, authenticated: `brew install snyk-cli && snyk auth`. If `snyk:report` fails with an auth error, ask the user to run `! snyk auth`. It opens a browser, so you can't do it for them.
- `jq`

## Choosing projects

`SNYK_FILES` at the top of `taskfiles/snyk.yml` lists every Go module in the repo. Entries that are commented out are skipped by both tasks. If the user wants a module included or excluded permanently, edit that list.

For a one-off run, override it with a space-separated string:

```bash
task snyk:report SNYK_FILES="go.mod public/bundle/free/go.mod"
task snyk:fix FILES="public/bundle/free/go.mod"   # narrows fix further; must be in the report
```

`snyk:fix` only touches projects that are both in `report.json` and in `SNYK_FILES` (or `FILES`). Other projects in an old report are ignored. A listed project that isn't in the report is printed as `not in target/snyk/report.json, skipping`; rerun `snyk:report` to include it.

If someone adds a new `go.mod` to the repo, it won't be scanned until it's added to `SNYK_FILES`. This finds every module:

```bash
find . -name go.mod -not -path '*/vendor/*' | sed 's|^\./||' | sort
```

## Workflow

1. **Scan**: `task snyk:report`. This takes about 1–2 minutes for every module, most of it on `go.mod` and the bundles. It ends by printing a summary in the form `package  from -> to  CVEs`, where `NO FIX` means Snyk knows of no fixed version.
2. **Plan**: `task snyk:fix DRY_RUN=true`. This prints a `plan go get ...` or `skip ...: <reason>` line for each module and changes nothing. Show the plan to the user before applying it.
3. **Apply**: `task snyk:fix`. This runs `go get` for each module and `go mod tidy` for each project, then runs `task lint` and `task test` if anything was updated. Use `VERIFY=false` to skip lint and test, for example to batch several runs and test once at the end.
4. **Review**: run `git status` and `git diff -- '*go.mod'`, and check `fix.json` for anything `failed` or `skipped`.

Always rerun `snyk:report` after the dependencies change, including after a `fix` or a branch switch. `fix` works from whatever is in `report.json`, but it never downgrades: a module already at or above the target is skipped as `already at vX`.

## Reading the output

`report.json`:
- `summary[]` merges results across projects, with `package`, `from`, `to` (`null` when no fix exists), `fixedCves`, `unfixedCves` and `projects`.
- `projects[]` has one entry per project, with `file`, `exitCode`, `error` and `upgrades[]`. Each upgrade has full `vulnerabilities[]` detail: `id`, `title`, `severity`, `fixedIn`, `cves` and `ghsa`.
- **How `to` is chosen:** Snyk's `fixedIn` often lists one fixed version per maintained branch, for example `["25.0.6", "26.1.4", "27.1.1"]`. For each advisory, the report takes the lowest fixed version at or above `from` on the same line (major, or `0.minor` for v0), so a module on v26 gets `26.1.4`. Only when the branch has no fix does it move to the next line. The chosen version for each advisory is in `vulnerabilities[].fixVersion`.
- **Split entries:** if one package's advisories are fixed on different lines, the package appears once per line. For example, docker might have one entry `-> 26.1.4` and another `-> 27.3.0` for an advisory that's only fixed on 27. `snyk:fix` applies the same-major one and skips the other as a major bump.
- A non-null `error` means Snyk itself failed for that project (exit code 2 or 3, or output that isn't valid JSON). That isn't a vulnerability finding. The project still gets an entry, but `snyk:report` exits non-zero and lists the failed projects, so a failed scan can't pass for a clean one.

`fix.json`: `results[]` has `project`, `module`, `from`, `to` (the version applied, which is the minimum version Snyk says fixes it), `status` and `message`:

| status | meaning |
|---|---|
| `updated` | `go get` succeeded; `message` lists the CVEs fixed |
| `planned` | dry run only |
| `skipped` | not attempted; `message` has the reason |
| `failed` | `go get` failed; `message` is the last line of its output, and the full log is at `target/snyk/fix/<project path with / as _>.plan.get.log` |
| `error` | `go list -m all` or `go mod tidy` failed for the whole project |

`snyk:fix` exits non-zero if any result is `failed` or `error`, and prints each one. A `failed` `go get` still lets lint and test run on the modules that did update. An `error` skips lint and test, because the tree may not be tidy.

Quick queries:

```bash
jq -r '.summary[] | select(.to == null) | "\(.package) \(.from): \(.unfixedCves | join(","))"' target/snyk/report.json
jq -c '.results[] | select(.status == "failed" or .status == "skipped")' target/snyk/fix.json
```

## Skips that need a human

- **`fix is in major version N, needs a manual migration to the /vN module path`**: the fixed version is a new major version with a different module path (for example `go-jose/v3` → `v4`). `go get` can't do that. Fixing it means changing imports to the `/vN` path and adapting to API changes. Report it to the user rather than attempting it unasked.
- **`fix is a major version bump (vX -> vY) of a +incompatible module`**: the module is v2+ without a `/vN` path (for example `github.com/docker/docker v28.5.2+incompatible`). `go get` would work because the import path doesn't change, but it's a major API bump, so it isn't applied unattended. Upgrades within the same major version of a `+incompatible` module, such as `v28.5.2` → `v28.5.3`, are applied as normal.
- **Mixed targets in one module:** when packages in one module need different fix versions, `fix` upgrades to the highest one that doesn't need a manual major migration. Any higher targets appear as separate `skipped` entries, so those CVEs still show up.
- **`module is replaced in go.mod`**: a `replace` directive pins the module. Update the replacement target instead.
- **`to: null` / `NO FIX`**: no fixed version is published yet. The only options are to remove the dependency, or to wait for a fix and add a Snyk ignore.

## Gotchas

- **Package paths vs module paths:** Snyk reports Go *package* paths (`golang.org/x/net/http2`). `fix` maps each one to its module (`golang.org/x/net`) by picking the longest matching path in `go list -m all`, and combines several packages in one module into a single `go get` at the highest target.
- **`GOFLAGS=-mod=mod`:** both tasks set this because the bundles' gitignored `vendor/` directories are usually stale. As a side effect, the `go list` that Snyk runs may write to `go.mod`/`go.sum`. Both tasks back those files up and restore them, so a report or dry run leaves the tree clean. If they show as modified after a report, that's a bug in the task.
- **Vendor directories:** `fix` doesn't regenerate `vendor/` in the bundles. Run `go mod vendor` in a bundle only if the user needs vendored builds.
- **Lint and test scope:** both run from the repo root, so they check the root module's code. They don't build the bundle or `testdata` modules separately. For those, run `go build ./...` in the module directory.
- **Extra Snyk arguments:** anything after `--` is passed to `snyk test`, for example `task snyk:report -- --severity-threshold=high`.

## Changing the tasks

The version and upgrade logic is in jq programs under `taskfiles/snyk/`. The taskfile only runs them:
- `semver.jq`: shared helpers, loaded with `-L taskfiles/snyk` and `include "semver";`. These are the version sort key, release lines, picking the fix for a branch and major-version skips.
- `project.jq`: turns one project's raw Snyk output into a `report.json` project entry. It is run with `-R -s` so that output that isn't JSON still becomes an entry with `error` set.
- `summary.jq`: merges the project entries into `report.json`.
- `plan.jq`: resolves packages to modules and decides what `fix` applies or skips. It emits `fix.json` result rows, `skipped` or `pending`; the task records the skips as they are and turns `pending` into `planned`, `updated` or `failed`.

You can try a helper on its own, for example:

```bash
jq -n -L taskfiles/snyk 'include "semver"; ["27.1.1","28.5.3","29.0.1"] | pickfix("28.5.2+incompatible")'
```
