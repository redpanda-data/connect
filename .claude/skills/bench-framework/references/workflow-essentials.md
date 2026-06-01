# Bench workflow essentials

Non-negotiables when running an AWS bench. Each was discovered by failure during real runs.

## 1. Always wrap in `aws-vault exec`, never `AWS_PROFILE=`

Plain SSO sessions expire in under an hour. A bench takes ~90 min. The Go runner makes AWS SDK calls (S3 upload, SSM RunCommand) for the full duration; mid-run SSO refresh fails with `InvalidGrantException`. aws-vault mints a 12-hour STS session that the SDK uses throughout.

```bash
aws-vault exec <your-profile> -- task aws:bench scenario=<path>
```

`<your-profile>` is whatever you configured during one-time account setup — see [bootstrap.md](bootstrap.md). The skill examples use `bench-<initials>` as a convention but any name works.

See also: [traps.md#aws-vault](traps.md#aws-vault).

## 2. License file lives at the repo root

The Connect Enterprise license file must live at `<repo-root>/rpcn.license`. macOS TCC blocks file reads from `~/Downloads/`, `~/Documents/`, `~/Desktop/` for sandboxed processes. The Go runner (launched via `task aws:bench`) inherits the sandbox and `os.Open` fails with "operation not permitted" if the license is in those directories.

`.gitignore` already covers `*.license` — placing it at repo root does not risk committing.

See also: [traps.md#license-location](traps.md#license-location).

## 3. SIGINT, not SIGKILL

To kill a stuck bench: find the runner binary's pid and send default-signal `kill`. SIGKILL skips the deferred `terraform destroy` and leaves orphan VPC/EC2/IAM/S3.

```bash
pgrep -fl 'benchmarking/aws/runner.*bench'
# Pick the one without 'go run' in the line — that's the compiled binary
kill <pid>
```

`TaskStop` from the agent harness also kills without giving the defer time to run. Avoid for the bench process.

See also: [traps.md#sigint](traps.md#sigint).

## 4. `defer destroy` is registered BEFORE the first `terraform apply`

If you're modifying `runner/main.go::runBench`, search for the comment `Register destroy BEFORE any apply` — don't refactor without preserving the order. `terraform destroy` is idempotent against empty state, so registering it pre-apply is safe.

See also: [traps.md#defer-destroy-before-apply](traps.md#defer-destroy-before-apply).

## 5. `task aws:validate` is free

It runs locally (no AWS calls). Use it whenever a scenario YAML changes, before any `aws:bench`. Catches typos and shape errors that would otherwise burn a real bench.

```bash
task aws:validate scenario=benchmarking/aws/scenarios/<stack>/<scenario>.yaml
```

## 6. Use `--keep-on-fail` for live debug

When a bench fails and you want to SSH into the runner to investigate before infra is destroyed, pass `--keep-on-fail` to `task aws:bench`. Tears down on success; preserves on failure. Pair with explicit `task aws:down` when you're done.

When investigating mid-run failures with `--keep-on-fail`, remember the orphan-cleanup Lambda (4h TTL) — see [traps.md#orphan-ttl](traps.md#orphan-ttl).
