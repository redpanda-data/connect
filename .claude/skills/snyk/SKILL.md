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

By default (`UPGRADE=latest`), each module goes to the **newest release that isn't a breaking change**, as long as it still includes the version Snyk requires:

- **v1 and above:** the newest release with the same major version as the fix. For example, grpc needing `>= v1.83.2` goes to the newest `v1.x.y`.
- **v0:** the newest patch on the fix's `0.minor` line. Semver treats v0 minor bumps as breaking, so `golang.org/x/net` needing `v0.53.0` goes to the newest `v0.53.x`, not `v0.54.0`.
- **Not considered:** pre-releases (`-rc`, `-dev`, etc.) and retracted versions.
- **Fallback:** if `go list -m -versions` fails, for example because the module is private or you're offline, the version Snyk requires is used.

`UPGRADE=fix` goes to exactly the minimum version Snyk says fixes the CVE instead. The plan prints `fix needs >= vX` whenever the chosen version is higher than the one Snyk requires.

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

`snyk:fix` only touches projects that are both in `report.json` and in `SNYK_FILES` (or `FILES`). Any other project still in an old report is printed as `not in SNYK_FILES/FILES, skipping`.

If someone adds a new `go.mod` to the repo, it won't be scanned until it's added to `SNYK_FILES`. This finds every module:

```bash
find . -name go.mod -not -path '*/vendor/*' | sed 's|^\./||' | sort
```

## Workflow

1. **Scan**: `task snyk:report`. This takes about 1–2 minutes for every module, most of it on `go.mod` and the bundles. It ends by printing a summary in the form `package  from -> to  CVEs`, where `NO FIX` means Snyk knows of no fixed version.
2. **Plan**: `task snyk:fix DRY_RUN=true`. This prints a `plan go get ...` or `skip ...: <reason>` line for each module and changes nothing. Show the plan to the user before applying it.
3. **Apply**: `task snyk:fix` (or `task snyk:fix UPGRADE=fix` for the minimum versions only). This runs `go get` for each module and `go mod tidy` for each project, then runs `task lint` and `task test` if anything was updated. Use `VERIFY=false` to skip lint and test, for example to batch several runs and test once at the end.
4. **Review**: run `git status` and `git diff -- '*go.mod'`, and check `fix.json` for anything `failed` or `skipped`.

Always rerun `snyk:report` after the dependencies change, including after a `fix` or a branch switch. `fix` works from whatever is in `report.json`, but it never downgrades: a module already at or above the target is skipped as `already at vX`.

## Reading the output

`report.json`:
- `summary[]` merges results across projects, with `package`, `from`, `to` (`null` when no fix exists), `fixedCves`, `unfixedCves` and `projects`.
- `projects[]` has one entry per project, with `file`, `exitCode`, `error` and `upgrades[]`. Each upgrade has full `vulnerabilities[]` detail: `id`, `title`, `severity`, `fixedIn`, `cves` and `ghsa`.
- A non-null `error` means Snyk itself failed for that project (exit code 2 or 3). That isn't a vulnerability finding.

`fix.json`: `results[]` has `project`, `module`, `from`, `to` (the version applied), `required` (the minimum version Snyk says fixes it), `status` and `message`:

| status | meaning |
|---|---|
| `updated` | `go get` succeeded; `message` lists the CVEs fixed |
| `planned` | dry run only |
| `skipped` | not attempted; `message` has the reason |
| `failed` | `go get` failed; `message` is the last line of its output, and the full log is at `target/snyk/fix/<project path with / as _>.plan.json.get.log` |
| `error` | `go list -m all` or `go mod tidy` failed for the whole project |

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
