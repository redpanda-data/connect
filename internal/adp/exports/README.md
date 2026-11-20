# Audit-log export pipeline

`audit-log-export.yaml` exports an OCSF audit log from a Redpanda topic to
an S3 bucket. Records on the topic are schema-registry-framed protobuf
(`ocsf.v1.AuditEvent`); objects in the bucket are gzipped OCSF 1.8 NDJSON.
The file is submitted unchanged to the Redpanda Cloud pipeline API, and the
tests here run it against the real wire format.

Flow: `redpanda` input (SASL/SCRAM, TLS) → `schema_registry_decode`
(protobuf, proto names, integer enums) → Bloblang (`time` to a number, OCSF
class fix-ups) → batches of up to 2,000 events / 16 MiB / 60 s → NDJSON →
gzip → `aws_s3`.

## Inputs

One file for every deployment. Anything deployment-specific arrives through
`${VAR}` interpolation when the pipeline starts.

Variables Redpanda Cloud injects into every pipeline pod:

| Variable | Used for |
|---|---|
| `REDPANDA_BROKERS` | `seed_brokers` |
| `REDPANDA_SCHEMA_REGISTRY_URL` | `schema_registry_decode.url` |
| `REDPANDA_TLS_ENABLED` | `tls.enabled` |
| `REDPANDA_PIPELINE_ID` | consumer group `audit-log-export-<id>` |

The consumer group follows the pipeline id, so recreating a pipeline (rather
than updating it) starts a new group and replays the retained topic. That is
the intended behaviour when an export is deleted and created again; a
controller that wants to resume must update the pipeline in place.

Dataplane secrets created on the cluster before the pipeline
(`${secrets.<ID>}`, ids match `^[A-Z][A-Z0-9_]*$`; one export per cluster,
so the names are fixed):

| Secret ID | Required | Default | Notes |
|---|---|---|---|
| `AUDIT_LOG_EXPORT_TOPIC` | yes | | schema-registry-framed protobuf `AuditEvent` records |
| `AUDIT_LOG_EXPORT_KAFKA_USERNAME` | yes | | SCRAM user that reads the topic |
| `AUDIT_LOG_EXPORT_KAFKA_PASSWORD` | yes | | |
| `AUDIT_LOG_EXPORT_S3_BUCKET` | yes | | |
| `AUDIT_LOG_EXPORT_S3_REGION` | yes | | |
| `AUDIT_LOG_EXPORT_AWS_ACCESS_KEY_ID` | static or role | empty | |
| `AUDIT_LOG_EXPORT_AWS_SECRET_ACCESS_KEY` | with the key id | empty | |
| `AUDIT_LOG_EXPORT_AWS_ROLE_ARN` | static or role | empty | role to assume instead of static keys |
| `AUDIT_LOG_EXPORT_AWS_ROLE_EXTERNAL_ID` | no | empty | with the role |
| `AUDIT_LOG_EXPORT_S3_PREFIX` | no | `audit-log` | no trailing slash; one is added |
| `AUDIT_LOG_EXPORT_S3_SSE` | no | empty | e.g. `aws:kms` |
| `AUDIT_LOG_EXPORT_S3_KMS_KEY_ID` | no | empty | with `aws:kms` |
| `AUDIT_LOG_EXPORT_S3_OBJECT_ACL` | no | `bucket-owner-full-control` | empty sends no ACL |
| `AUDIT_LOG_EXPORT_BATCH_COUNT` | no | `2000` | events per object, upper bound; memory grows with it |
| `AUDIT_LOG_EXPORT_BATCH_PERIOD` | no | `60s` | flush interval |
| `AUDIT_LOG_EXPORT_S3_ENDPOINT` | no | empty | S3-compatible emulators only |
| `AUDIT_LOG_EXPORT_S3_FORCE_PATH_STYLE` | no | `false` | with a custom endpoint |

Non-secret settings live in the secret store because it is the only
per-pipeline input the Cloud API has; the alternative was templating the
YAML per deployment. Optional values use `${X:default}`, so a missing secret
is fine, and Connect treats an empty credential, role, endpoint, SSE or KMS
field as unset. Pipeline creation only checks that secrets referenced
without a default exist. Confirm that with a real `CreatePipeline` before
relying on it.

## OCSF conformance

The source schema is one protobuf message for two OCSF classes, API
Activity (6003) and Entity Management (3004), and OCSF 1.8 rejects
attributes the event's class doesn't define. The mapping moves those under
`unmapped` rather than dropping them, and declares the `trace` profile on
API Activity events that carry a trace.

| Class | Moved to `unmapped` | Why |
|---|---|---|
| 6003 API Activity | `entity`, `entity_result` | only on Entity Management |
| 3004 Entity Management | `api`, `resources`, `trace` | `api` needs the `cloud` profile, which the events don't declare; the others aren't on the class |

Nothing else needs changing: OCSF enum fields are integers, and the string
siblings (`activity_name`, `severity`, ...) are optional and absent from
the source schema.

Every record, golden and live, is validated against a vendored copy of the
compiled OCSF 1.8.0 schema (`testdata/ocsf-1.8.0.json`): unknown
attributes (with profile gating), required attributes, types, enum values
and object constraints, the same checks the official validator applies.

## Throughput and memory

The export is sized for up to 30M events/day per topic, under 3,000
events/s at peak.

```sh
go test ./internal/adp/exports -run '^$' -bench '^BenchmarkPipelineProcessors' -benchtime 200000x -count 3
go test ./internal/adp/exports -bench '^BenchmarkIntegration' -benchtime 300000x
```

`BenchmarkPipelineProcessors` runs the config's processors in-process
against an in-memory schema registry (no Docker), one batch at a time
(`serial`) or one per core (`parallel`). `BenchmarkIntegrationAuditLogExport`
pre-loads a real Redpanda with the requested number of events and times the
pipeline until the last one is in localstack S3, once per batch count. Use
a multiple of the batch counts so no partial batch waits out the period.

Apple M3 Pro, Docker in colima with 4 cores, 2026-11-13, ~780-byte events:

| Benchmark | events/s | peak live heap |
|---|---|---|
| processors, serial | ~28k | ~50 MB |
| processors, parallel | ~115k | ~440 MB |
| end to end, 300k events, batch count 2000 | ~37k | ~125 MB |
| end to end, 300k events, batch count 10000 | ~21k | ~310 MB |

End-to-end times include start-up and the final flush. Localstack is not
S3, so read these as "the pipeline is not the bottleneck". Heap figures are
for the whole test binary; `peak_heap_MB` in the output also counts garbage
not yet collected and is nearer what the process needs.

### Sizing for a 400 MB pod

A Redpanda Cloud compute unit is 0.1 CPU and 400 MB, and a pipeline that
hits its memory limit is killed and restarts. Memory is set by three
things, not by the event rate:

- **The batch.** The output batcher holds a whole batch decoded until it
  flushes, about 20 KB of heap per event. At 0.1 CPU a 2,000-event batch
  fills in a few seconds when there is a backlog.
- **Fetch buffers.** Fetched records sit decompressed until processed.
  `fetch_max_bytes` and `fetch_max_partition_bytes` are lowered from the
  50 MiB / 1 MiB defaults for this.
- **Garbage.** Go collects when the heap doubles unless `GOMEMLIMIT` is set,
  so the process needs about twice what it holds. Setting `GOMEMLIMIT` on
  the pipeline pod, around 75% of the memory limit, makes the collector work
  harder near the limit instead of the pod being killed. It is an
  environment variable on the pod, so it is set by whatever creates the
  pipeline, not in this file.

`TestSaturateAuditLogExport` measures this: it runs the cloud binary in a
container with the pod's limits against a large backlog (`start_from_oldest`
reads it as fast as the CPU allows) and records memory, heap, events in
flight, export progress and OOM kills. Every knob is an environment
variable; the doc comment in `saturate_test.go` lists them.

```sh
SATURATE=1 go test ./internal/adp/exports -run '^TestSaturateAuditLogExport$' -v -timeout 2h
```

400k events of ~1.6 KB (634 MB of protobuf), 400 MB, 0.1 CPU, same machine
as above:

| batch count | fetch sizes | `GOMEMLIMIT` | result | peak memory |
|---|---|---|---|---|
| 10000 | 50 MiB / 1 MiB | unset | OOM killed after ~1 min, every restart, nothing exported | 381 MiB |
| 10000 | 8 MiB / 512 KiB | 300MiB | OOM killed after ~2 min, every restart | 379 MiB |
| 2000 | 8 MiB / 512 KiB | 300MiB | no kills, ~470 events/s | 290 MiB |
| 2000 | 8 MiB / 512 KiB | unset | no kills, ~500 events/s | 311 MiB |

At 0.1 CPU the pipeline is CPU-bound at about 500 events/s, above the 30M
events/day average (~350 events/s) but below the peak; a backlog drains at
that rate. Raising the batch count needs more memory than one compute unit
has.

### Why `unordered_processing` is on

In the `redpanda` input's default ordered mode, each partition has one
yielded batch (`max_yield_batch_bytes`, 32 KB, about 40 of these events) in
flight until the output signals it has been dispatched. Only the `redpanda`
output sends that signal early; any other output effectively sends it on
ack, which for a batching `aws_s3` is the batch flush. That caps the export
at about 40 events per partition per batch period: ~115 events/s measured
on three partitions with a 1 s period, ~2 events/s with the 60 s default.
`unordered_processing` removes the gate and allows `checkpoint_limit`
records per partition in flight; the limit is above the batch count so one
busy partition can still fill a batch. Raise it with the batch count. Offsets still commit in order.

The cost is ordering: with pipeline threads and `max_in_flight` above one,
a partition's records can land out of offset order within and across
objects. A Connect change is planned so batchers fire the dispatch signal
when a message is merged into a batch; once it lands, drop
`unordered_processing` here and ordered mode keeps full throughput.

## Behaviour

- A record that fails to decode is logged at ERROR with
  topic/partition/offset and rejected by `reject_errored`. The input retries
  it indefinitely and never commits past it. Records around it still export
  (and are written again after a restart, since the batch is never acked).
  Later records on that partition flow until `checkpoint_limit` (5k) are in
  flight behind it; then the partition pauses. The same ERROR repeats about
  once a second while the record is there; clearing it means moving the
  group past the record. `catch` is not used because it clears the error
  flag and would export the bad record.
- An event without `time` is exported without it, under the 1970-01-01
  date, rather than stalling the pipeline.
- Object keys contain no wall-clock time:
  `<prefix>/<yyyy/mm/dd of first event>/<topic>-p<partition>-o<offset>-n<count>-<xxhash64 of content>.ndjson.gz`,
  with the first record's partition and offset. Objects can hold records
  from several partitions, so treat the key as an opaque stable id, not a
  range. Re-writing the same batch lands on the same key, but a restart
  re-batches, so overlapping objects after a restart are possible. The key
  scheme is a default, not a settled design.
- `bucket-owner-full-control` is sent by default so the bucket owner owns
  objects written from another AWS account. S3 accepts it with ACLs
  disabled.
- Credentials are static keys or `role_arn` (plus optional external id).
  Role-based auth still needs confirming for wherever the pipeline pod
  actually runs.

## Tests

Unit (`task test`):

- `TestConfigBuildsWithRequiredInputsOnly`: the config lints and builds with
  only the required inputs, so every default is exercised, and it contains
  no template expressions.
- `TestConfigUsesOnlyCloudComponents`: every component is in the cloud
  distribution per `internal/plugins/info.csv`.
- `TestExpectedOutputIsValidOCSF`: the golden output is valid OCSF 1.8.
- `TestOCSFValidatorCatchesKnownProblems`: the validator rejects the shapes
  the mapping fixes, so it can't silently stop checking.

Integration (`task test:integration -- exports`): Redpanda with SASL and
schema-registry basic auth plus localstack, the `AuditEvent` schema
registered, the fixtures in `testdata/events.jsonl` produced in the real
wire format, and the config run in-process with the inputs above.

- `export`: every record lands, deep-equal to `testdata/expected.ndjson`
  and valid OCSF 1.8; keys follow the scheme; nothing logged at ERROR.
- `poison`: one undecodable record, then a good one on the same partition,
  run as a new pipeline id. All good records land, the poison record is
  logged with its offset, the group never commits past it on that partition
  and commits to the end on the others.

Benchmarks and the saturation harness: see above. `testdata/README.md`
covers the fixtures' origin.

## Not covered

Real AWS S3 (localstack ignores credentials, ACLs and KMS), role-based
credentials, TLS to the broker, and the control plane around the pipeline:
`CreatePipeline`, secret creation, the topic-reader identity.
