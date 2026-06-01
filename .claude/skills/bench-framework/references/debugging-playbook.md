# Debugging playbook

Symptom-keyed lookup. Each section is anchored so SKILL.md and other references can deep-link. The pattern: symptom → root cause → existing fix (commit SHA) → what you should do.

<a name="empty-workload"></a>
## Connect reports `Summary.MedianMBPerSec=0` with no `[load]` lines

**Symptom:** All 899+ samples report `msg/sec=0 bytes/sec=0`. Plan 1's smoke would produce 25 MB/s at the same shape.

**Likely cause:** The workload subcommand of the seeder (`benchmarking/aws/seeders/<seeder>/`) is either silent on success or never wrote rows. With `output: redpanda` instead of `output: drop`, runtime issues that drop's no-op masked are now visible.

**Where it bit:** Plan 2 smoke first run.

**Investigate:**

1. Look for `[load]` lines in the sweep log on S3. None? Workload generator silent.
2. Compare against Plan 1 smoke at same shape with `output: drop`.
3. SSH/SSM into runner, manually invoke the workload subcommand: `POSTGRES_DSN=<dsn> /opt/bench/seeders/cdc-rows workload --rate=20000 --duration=30s`. Look for stderr.

<a name="kc-http-500"></a>
## KC connector PUT returns HTTP 500 with no body

**Symptom:** Bench logs `curl -fsS -X PUT ... → HTTP 500`. No body printed.

**Likely cause:** `curl --fail (-f)` suppressed the response body.

**Fix:** Already addressed in `f59b98cf1` — KC bench script uses `curl -sS -o /tmp/resp -w '%{http_code}' ...` and cats the body on non-2xx. If you're seeing this now, the KC script in `runner/kcscript.go` may have been edited.

**Verify:** `git log -p benchmarking/aws/runner/kcscript.go | grep -A2 'HTTP_CODE'`

<a name="broker-series-empty"></a>
## `broker_series` empty even though KC log shows writes

**Symptom:** Result JSON's `BrokerSeries` is null/empty for KC despite `kc-<vcpu>.log` showing millions of records produced.

**Likely cause:** Either (a) the parser is reading the wrong label name (`topic` instead of `redpanda_topic`) — see [traps.md#redpanda-topic-label](traps.md#redpanda-topic-label); or (b) the per-broker metric exposition isn't being aggregated — see [traps.md#per-broker-metrics](traps.md#per-broker-metrics).

**Diagnosis:**

```bash
# Pull a sample frame from the live broker:
aws ssm start-session --target <broker-instance-id>
# inside the session:
curl -s http://localhost:9644/public_metrics | grep redpanda_kafka_request_bytes_total | head
```

If labels are `redpanda_topic=` and you see your topic, but `broker_series` is still empty, you're scraping only one broker (case b).

**Fixes:**
- Label name: `559559c53`
- Multi-broker scrape: `5b0d56306` — new TF output `metrics_endpoints`, bench script iterates all 3 brokers per scrape interval.

<a name="auto-create"></a>
## `UNKNOWN_TOPIC_OR_PARTITION` for every write

**Symptom:** Both engines silently fail; bench logs show repeated `UNKNOWN_TOPIC_OR_PARTITION`.

**Root cause:** Redpanda's default `auto_create_topics_enabled=false` (opposite of Apache Kafka).

**Fix:** `79794a84c` — enable `kafka_api.auto_create_topics_enabled=true` in the Redpanda module's `redpanda.yaml`. Or pre-create topics in the reset block.

<a name="offset-isolation"></a>
## KC at vCPU≥2 reports 0 MB/s; KC log says `redo log is no longer available`

**Symptom:** KC works at vCPU=1, then 0 MB/s for vCPU=2 and beyond.

**Root cause:** Debezium offsets persist in `_kc_offsets` across sweep points. Reusing the same connector name means sweep point N+1 tries to resume from the LSN/binlog position of sweep point N. The DB has aged that position out.

**Fix:** Per-vCPU connector name (`bench_<conn>_v<N>`). Each sweep point gets a fresh offset namespace. Commit `992370235`.

**Verify:** In `runner/kcscript.go`, look for the connector name being rendered with the vCPU count.

<a name="dns-flake"></a>
## RDS DNS timeout mid-bench

**Symptom:** A reset step (or any RDS call) fails with `could not translate host name "<rds-endpoint>" to address: Name or service not known`. First reset of the same bench worked.

**Root cause:** VPC DNS resolver flake. Same runner instance, same RDS endpoint — DNS state degrades across long sessions.

**Mitigation:** Retry the reset with `getent hosts` precondition, or add a `nslookup` to the reset script preface for diagnostics. Currently not patched in code; the bench will fail and need a fresh run.

<a name="orphan-ttl"></a>
## Bench hangs after several hours; SSM calls return "InstanceId not found"

**Symptom:** Bench wall-clock crosses 4h boundary, then SSM commands fail with the target instance unknown.

**Root cause:** EventBridge → orphan-cleanup Lambda fired at the TTL window. EC2 + S3 already destroyed. Bench process keeps trying to talk to ghosts.

**Mitigation:** Bump `orphan_ttl_hours` in scenario or shared TF stack for long benches. `5ccb10eb1` raised default from 3h → 4h.

<a name="tf-destroy-session-id"></a>
## `task aws:down` fails: `bench_session_id: required variable not set`

**Symptom:** Manual teardown after a hung bench fails.

**Root cause:** `runner/main.go::downCmd` passes only `region` and `runner_instance_type`. Shared stack declares `bench_session_id` as required (no default). Destroy doesn't need the tag value but TF can't pass var validation.

**Workaround:**

```bash
cd benchmarking/aws/terraform/shared && \
  aws-vault exec bench -- terraform init -reconfigure \
    -backend-config=../backend.hcl \
    -backend-config=key=shared/terraform.tfstate && \
  terraform destroy -auto-approve \
    -var=bench_session_id=teardown-$(date +%Y%m%d) \
    -var=runner_instance_type=c8g.xlarge
```

**Follow-up:** Give `bench_session_id` a `""` default OR have `downCmd` read it from state. Documented in `bench-debugging-history` #30.

<a name="terraform-init-backend-changed"></a>
## `terraform init` fails: "Backend configuration changed"

**Symptom:** Bench fails immediately at first apply with `terraform init shared: exit status 1`.

**Root cause:** Earlier `terraform init -backend=false` during development left cached `.terraform/` directories with `backend=local` config. The runner's real-S3 init detects the mismatch.

**Fix:**

```bash
find benchmarking/aws/terraform -type d -name '.terraform' -exec rm -rf {} +
```

Then retry. They're gitignored, so it's safe.

**Pattern:** If you ran `terraform init -backend=false` for validation during dev, blow away `.terraform/` before the next live bench.

<a name="rds-dynamodb-lock"></a>
## `ConditionalCheckFailedException: Error acquiring the state lock`

**Symptom:** Bench fails at `terraform apply shared` immediately after a prior bench's destroy.

**Root cause:** Stale DynamoDB lock entry from the prior run's destroy phase, OR DDB lock table missing entirely.

**Fix (stale lock):**

```bash
aws-vault exec bench -- terraform force-unlock -force <lock-id>
```

**Fix (table missing — bench-debugging-history #27):**

```bash
aws-vault exec bench -- aws dynamodb create-table \
  --table-name redpanda-connect-bench-tflocks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-2
```

**Rule:** Wait ~30s between back-to-back benches.
