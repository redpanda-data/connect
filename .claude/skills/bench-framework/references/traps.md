# Bench framework traps

Each entry: what it looks like, why it happens, where the fix lives. Anchors are stable — SKILL.md and other references link in.

<a name="aws-vault"></a>
## 1. Wrap in aws-vault, not AWS_PROFILE

**Symptom:** Mid-bench AWS calls fail with `InvalidGrantException: failed to refresh cached SSO token`.

**Cause:** Plain SSO sessions expire in under an hour; benches take ~90 min. aws-vault mints a 12h STS session that survives the run.

**Where it bit:** Multiple T20 smoke runs (`bench-debugging-history` #13).

**Fix:** Always wrap `task aws:bench` invocations in `aws-vault exec bench --`. No code commit; operational rule.

<a name="license-location"></a>
## 2. License at repo root, NOT in ~/Downloads/

**Symptom:** `stageArtefacts` errors with `operation not permitted` ~8 min into terraform apply.

**Cause:** macOS TCC blocks file reads from Downloads/Documents/Desktop for sandboxed processes.

**Where it bit:** `bench-debugging-history` #9, #14.

**Fix:** Place license at repo root (`/path/to/connect/rpcn.license`). `.gitignore` covers `*.license`. Fail-fast `os.Open` at validate-time landed in `458100ff4`.

<a name="sigint"></a>
## 3. SIGINT (not SIGKILL) for clean teardown

**Symptom:** Orphaned VPC/EC2/IAM/S3 resources after killing a stuck bench.

**Cause:** SIGKILL skips the deferred `terraform destroy`. The runner's signal handler cancels ctx, `MatrixRunner.Run` returns, then `defer destroy` fires.

**Where it bit:** `bench-workflow-essentials` #3.

**Fix:** `pgrep -fl 'benchmarking/aws/runner.*bench'` then `kill <pid>` (default is SIGTERM/SIGINT). NEVER `kill -9`.

<a name="defer-destroy-before-apply"></a>
## 4. defer destroy registered BEFORE first apply

**Symptom:** Failed shared apply orphans VPC/EC2/IAM/S3.

**Cause:** Defer registered after apply; destroy never queued.

**Where it bit:** Early framework versions; fixed in `3e571c03a`.

**Fix:** In `main.go::runBench`, the comment `Register destroy BEFORE any apply` marks the location. Don't refactor without preserving order. `terraform destroy` is idempotent against empty state.

<a name="rds-logical-replication"></a>
## 5. RDS Postgres: rds.logical_replication (not wal_level)

**Symptom:** RDS parameter group apply fails: `Could not find parameter with name: wal_level`.

**Cause:** `wal_level` is not user-settable on RDS Postgres. RDS exposes it as `rds.logical_replication = "1"`.

**Fix:** `terraform/modules/rds-postgres/main.tf` parameter group uses `rds.logical_replication = "1"`. Commit `a1ba55127`. See [rds-quirks.md](rds-quirks.md).

<a name="postgres-cdc-tls"></a>
## 6. postgres_cdc tls field: no `enabled:` toggle

**Symptom:** Config lint error `field enabled not recognised`.

**Cause:** `postgres_cdc` uses `service.NewTLSField` (not `NewTLSToggledField`); the field has no `enabled` sub-key. Setting it explicitly is also required because RDS rejects unencrypted replication connections.

**Fix:** In scenario YAML: `tls: { skip_cert_verify: true }` with no `enabled:` key. Commits `8b47cd202`, `f1f9e271a`.

<a name="workload-rate-per-table"></a>
## 7. workload --rate is per-table-per-sec, not total

**Symptom:** Bench reports throughput far below target.

**Cause:** `scenarios/<x>/<scenario>.yaml` `write_rate_per_sec` is interpreted by the seeder as per-table-per-second. With N tables, total = N × rate.

**Where it bit:** `bench-debugging-history` #17.

**Fix:** Size the workload knowing rate × tables = total. See [scenario-sizing.md](scenario-sizing.md#workload-rate-shape).

<a name="truncate-between-points"></a>
## 8. TRUNCATE between sweep points required

**Symptom:** Later sweep points stall — vCPU 8 returns ~0 MB/s despite vCPU 1/2/4 reporting real numbers.

**Cause:** Table grows unboundedly across sweep points; per-insert latency on the b-tree stretches until the producer stalls.

**Where it bit:** `bench-debugging-history` #18.

**Fix:** Scenario YAML `reset:` block must include `TRUNCATE TABLE <table>`. Initial seed becomes `initial_rows: 0` (the seeder still ensures the table exists).

<a name="redpanda-auto-create-topics"></a>
## 9. Redpanda auto_create_topics_enabled defaults to FALSE

**Symptom:** Both engines silently fail; bench logs `UNKNOWN_TOPIC_OR_PARTITION`.

**Cause:** Redpanda's default is opposite to Apache Kafka. Topics must be created explicitly OR `redpanda.yaml` config must enable auto-create.

**Fix:** Either pre-create topics in reset SQL or enable `redpanda.kafka_api.auto_create_topics_enabled = true` in the Redpanda module. Commit `79794a84c`.

<a name="redpanda-topic-label"></a>
## 10. Redpanda metric label is `redpanda_topic`, not `topic`

**Symptom:** `broker_series` always empty in result JSON despite obvious produce traffic.

**Cause:** Redpanda's `/public_metrics` labels topics as `redpanda_topic`. Code reading `topic` returns nothing.

**Fix:** `extractTopicProduceBytes` in `runner/brokermetrics.go` reads `redpanda_topic`. Commit `559559c53`.

<a name="per-broker-metrics"></a>
## 11. Per-topic byte metrics are per-broker — scrape ALL brokers

**Symptom:** `broker_series` empty for KC even though KC log shows millions of records written.

**Cause:** Redpanda emits `redpanda_kafka_request_bytes_total{redpanda_topic=X}` PER-BROKER, only for partitions that broker leads. Scraping only `broker_ips[0]:9644` misses topics led elsewhere.

**Fix:** TF output `metrics_endpoints` (plural, comma-separated). Bench script iterates all brokers per scrape interval; `extractTopicProduceBytes` sums `+=` across brokers per frame. Commit `5b0d56306`.

<a name="kc-connector-offset-isolation"></a>
## 12. Per-vCPU KC connector name (Debezium offset isolation)

**Symptom:** KC produces 0 MB/s at vCPU ≥ 2 despite vCPU 1 working. KC log: `redo log is no longer available`.

**Cause:** KC stores Debezium offsets in `_kc_offsets` keyed by connector name. Reusing the same name across sweep points means later points try to resume from stale LSN/binlog positions the database has aged out.

**Fix:** Per-vCPU connector name (e.g. `bench_<conn>_v<N>`). Each sweep point gets a fresh offset namespace. Commit `992370235`.

<a name="mysql-debezium-snapshot"></a>
## 13. MySQL Debezium needs snapshot.mode=no_data

**Symptom:** MySQL KC connector fails before warmup; Debezium error references missing offset.

**Cause:** Debezium MySQL with `snapshot.mode=never` requires an existing committed offset. Per-vCPU connector naming creates fresh connectors with no offset. Postgres `pgoutput` is forgiving and can stream from current WAL position; MySQL is stricter.

**Fix:** Use `snapshot.mode=no_data`: snapshot the schema (table is TRUNCATEd → no rows) then stream from current binlog. Commit `47ee0d481`. Encoded in `kcConnectorSpecs["mysql_cdc"]`.

<a name="chrt-fifo-deadlock"></a>
## 14. chrt --fifo deadlocks JVM on single-core taskset

**Symptom:** KC JVM under `taskset -c N-N chrt --fifo 50 ...` stays alive but writes ZERO bytes for 180+ s; never binds port 8083.

**Cause:** SCHED_FIFO is cooperative within a priority class. JVM has many threads (GC, finalizer, JMX, signal dispatcher) — when all bound to one core under FIFO, they deadlock waiting on each other. Go's preemptive runtime is unaffected, so Connect tolerates `chrt --fifo`.

**Fix:** Drop `chrt --fifo 50` from BOTH Connect and KC bench scripts (Plan 3 dropped it from Connect too for scheduler parity). Commit `a22183533`.

<a name="cloud-init-runner-loadgen"></a>
## 15. Cloud-init can't share between runner + load-gen (KC split-brain)

**Symptom:** KC connector PUT returns HTTP 500 / `SocketTimeoutException`.

**Cause:** Plan 1 used one cloud-init template for runner AND load-gen. Both installed kafka-connect and joined the `kc-bench-workers` group. Runner's KC saw load-gen as leader, forwarded PUT to it, runner→load-gen SG blocked 8083.

**Fix:** Parameterize cloud-init with `install_kc`. Runner gets `true`, load-gen gets `false`. Commit `0a318a043`.

<a name="orphan-ttl"></a>
## 16. Orphan-cleanup Lambda 4h TTL trips long benches

**Symptom:** Bench hangs after several hours; infra has been wiped from under it.

**Cause:** EventBridge → Lambda → terraform destroy fires at `BENCH_ORPHAN_TTL_HOURS` (currently 4h). `--keep` flag preserves infra against the runner's `defer destroy` but NOT against the orphan Lambda.

**Fix:** For long benches (8 vCPU × 2 engines), bump `orphan_ttl_hours` in the scenario or in the shared TF stack vars. Commit `5ccb10eb1` bumped 3h→4h.

<a name="bench-session-id-not-output"></a>
## 17. bench_session_id is NOT a TF output — inject in Go

**Symptom:** All Connect topics collapse to `bench__<connector>_connect`; KC cleanup silently skips.

**Cause:** Shared TF stack declares `bench_session_id` as input (for `default_tags`) but doesn't expose it as an output. Downstream code reading `sharedOuts["bench_session_id"]` finds nothing.

**Fix:** Runner injects `bench_session_id` into `sharedOuts` after apply, before passing the map further. Commit `655f36783`.
