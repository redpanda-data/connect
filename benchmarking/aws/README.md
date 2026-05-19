# AWS Benchmarking Framework

Production-shaped benchmarks for Redpanda Connect connectors, run on real AWS infrastructure.

See [`docs/superpowers/specs/2026-05-19-aws-benchmarking-framework-design.md`](../../docs/superpowers/specs/2026-05-19-aws-benchmarking-framework-design.md) for the full design.

## Prerequisites

- Go 1.22+, Terraform 1.6+, AWS CLI v2, `task`, `jq`.
- AWS credentials with admin in a dedicated benchmarking account (set via `aws-vault exec` or env vars).
- S3 bucket and DynamoDB table for Terraform state — see `terraform/backend.hcl`.

## One-command usage

```bash
task aws:bench -- scenario=postgres/orders-cdc
```

Runs end-to-end: `terraform apply` → seed dataset → CPU sweep [1, 2, 4, 8] → write JSON + append markdown → `terraform destroy`. Expect ~90 min wall-clock and ~$5 in AWS costs.

Other tasks:

- `task aws:validate -- scenario=<…>` — schema validation + `terraform plan` + pipeline lint. No AWS spend.
- `task aws:down` — explicit teardown when running with `keep=true`.
- `task aws:cost-check` — estimated hourly cost of any currently-running stacks.

## Cost guardrails

Every resource carries a `bench-session-id` tag. An EventBridge-triggered Lambda destroys stacks older than 24 h — the "I closed my laptop" safety net.
