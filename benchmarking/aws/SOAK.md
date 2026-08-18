# Soak testing runbook (CON-179 R6)

Soak runs hold one connector configuration under sustained moderate load
for 90 minutes (nightly) so the bug classes that only manifest under
runtime — slow leaks, silent stalls, rotation windows, growing lag — show
up before customers find them. This is the operational guide; design
history lives in the Aha feature (CON-179) and the commit messages on the
files named below.

## The moving parts

| Piece | Where | Job |
|---|---|---|
| Soak scenario | `scenarios/<engine>/*-soak.yaml` (`soak: true`) | sustained-load profile; validated to 1 cpu_point, connect-only |
| Runner soak mode | `runner/` (main.go, matrix.go, cloudwatch.go) | scaled cadences, 10-min S3 checkpoints, per-minute CloudWatch emission, backlog series |
| Dashboards + alarms | `terraform/persistent/` (`main.tf` `soak_scenarios` var, `alarms.tf`) | one dashboard + three alarms (stall / rss-slope / backlog) per scenario → SNS `redpanda-connect-bench-soak-alerts` |
| Archive + baseline | `redpanda-connect-bench-soak-archive` bucket | result.json + raw artifacts per run; `soak-index/` feeds the rolling-baseline comparator (advisory < 3 runs, then fails the job on throughput < 85% / RSS > 130% of baseline) |
| Nightly workflow | `.github/workflows/soak_nightly.yml` | 08:10 UTC cron (arms only from the default branch) + manual dispatch; OIDC creds (4h), license from Secrets Manager, teardown verified against AWS |
| PR comparison | `.github/workflows/soak_pr.yml` | `/soak` comment (write-access gated) → base-vs-PR binaries, same infra, sticky comparison comment |

## Adding a connector to the rotation

1. **Know its ceiling first.** Run (or find) the standard bench sweep for
   the connector; the soak rate should be ~10–15% of measured ceiling —
   soak tests correctness-over-time, not throughput.
2. **Write the soak scenario**: copy `scenarios/postgres/orders-soak.yaml`
   and adjust connector/stack/pipeline/rate. Keep instances small (the
   postgres soak uses db.r6g.xlarge + c8g.xlarge). Mind engine-specific
   floors (e.g. RDS gp3 forbids provisioned iops/throughput under 400 GB).
   `task aws:validate scenario=<engine>/<name>` must pass.
3. **Register the dashboard + alarms**: add an entry to `soak_scenarios`
   in `terraform/persistent/variables.tf` (key → connector + scenario
   name), then `task aws:persistent`. Alarms and the dashboard are
   generated per entry.
4. **First runs**: dispatch the nightly workflow manually with the
   scenario input. The baseline comparator stays advisory until three
   soak-index entries exist.
5. **Optionally add a PR variant** (`*-soak-pr.yaml`): same scenario with
   30m duration and `arms: [{id: base, binary: base}, {id: pr, binary:
   pr}]`. The scenario NAME must differ from the nightly's so its metrics
   land outside the alarm dimensions.

## Operating it

- **One bench at a time, ever.** All sessions share (and destroy) the same
  shared Terraform stack. The runner's pre-flight refuses to start while
  any bench EC2 exists (`--preflight=off` is the loudly-warned override).
  This applies across machines: laptop benches and scheduled soaks
  collide.
- **One-time account setup** (already done in 605419575229, needed again
  only for a new account): `make -C cleanup-lambda zip && task
  aws:persistent`; create the license secret: `aws secretsmanager
  create-secret --name redpanda-connect-bench/license --secret-string
  file://<license> --region us-east-2`; confirm the SNS email
  subscription.
- **Laptop runs**: always from a git worktree (never a checkout you might
  branch-switch mid-run), always with credentials that outlive the run —
  `aws-vault exec` static creds die at ~1h; prefer the scheduled workflow.
  Never Ctrl-C more than once: the second/third interrupt kills the
  deferred teardown.
- **Teardown truth**: log lines are not teardown proof. The workflows
  verify by querying EC2/RDS; do the same after any manual run.
- **The orphan reaper** (persistent stack) destroys any
  `Project=redpanda-connect-bench` EC2/RDS/S3/IAM resource older than 4h,
  sweeping every 15 min. A bench legitimately running past 4h needs the
  rule disabled first (and re-enabled after — set a reminder). The
  persistent stack itself is exempt via its distinct Project tag.
- **Alerts** land at the `redpanda-connect-bench-soak-alerts` SNS topic
  (email today; swap the subscription for Chatbot/Slack without touching
  alarms). Alarm emails during a run are the acute channel; a red nightly
  workflow is the between-builds channel; the `/soak` comment is the
  before-merge channel.

## Known limitations

- postgres_cdc IAM auth cannot work against vanilla RDS (replication-
  protocol connections reject IAM tokens — verified live 2026-08-12), so
  the credential-rotation window is covered by a future mysql_cdc soak or
  Aurora, not the postgres soak.
- The nightly cron only arms once `soak_nightly.yml` is on the repo's
  default branch; until then, manual dispatch.
- The weekly 24h soak needs two prerequisites before it can exist: a
  reaper exemption tag for declared-long runs, and a conductor that
  outlives GitHub's 6h job cap (Fargate).
- GitHub disables cron workflows on public repos after 60 days without
  repo activity.

## Proving the pipeline (the increment-6 demo)

To demonstrate detection end-to-end, plant a marked leak in the
connector's hot path on a throwaway branch (see the `con-179-r6-leak-demo`
branch: ~1 in 150 message payloads retained ≈ 4.8 MB/min at soak rate),
then: `/soak` on its PR → the sticky comment reads REGRESSION on RSS; a
manual nightly dispatch of the same branch → the `rpcn-soak-*-rss-slope`
alarm fires mid-run. Delete the leak run's `soak-index/` entry from the
archive bucket afterward so it never pollutes the baseline.
