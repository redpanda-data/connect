# MySQL CDC Benchmark Results

**Environment:** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.87.2), x86_64

See [`internal/impl/mysql/bench/`](../../internal/impl/mysql/bench/) for configs and run instructions.

---

## CDC / Snapshot — Small Rows (cart table)

Full snapshot of `cart`: 10,000,000 rows × ~600 B. Varying `GOMAXPROCS` and `batching.count`.

```bash
task bench:load:cart COUNT=10000000
task bench:run CORES=1 BATCH=1000
task bench:run CORES=2 BATCH=1000
# ...
```

### msg/sec

| GOMAXPROCS | batch=1000 | batch=5000 | batch=10000 |
|------------|------------|------------|-------------|
| 1          |     99,977 |    103,433 |     104,630 |
| 2          |    163,592 |    173,022 |     173,045 |
| 4          |    187,419 |    187,439 |     187,462 |
| 8          |    191,439 |    187,464 |     187,464 |

### MB/sec

| GOMAXPROCS | batch=1000 | batch=5000 | batch=10000 |
|------------|------------|------------|-------------|
| 1          |         60 |         62 |          63 |
| 2          |         98 |        104 |         104 |
| 4          |        113 |        113 |         113 |
| 8          |        115 |        113 |         113 |

**Observations:**
- Core scaling is strong from 1→2 cores (~1.67×) then rapidly plateaus: 2→4 is ~1.09×, 4→8 is ~1.02×.
- Throughput saturates at ~187K msg/sec beyond 4 cores — additional cores provide no benefit on this machine.
- Batch size has negligible effect at all core counts. At 1 core the range is only 99K→105K; at 4+ cores all batch sizes converge to the same value.
- Peak throughput: **191,439 msg/sec, 115 MB/sec** at CORES=8 BATCH=1000.

---

## Kafka Connect JDBC Sink Comparison

Same 10,000,000 rows written from Kafka to MySQL via Confluent JDBC Sink connector.
Schema/payload JSON envelope, 16 partitions.

See [`internal/impl/mysql/bench/kafka-connector/`](../../internal/impl/mysql/bench/kafka-connector/) for configs and run instructions.

```bash
task bench:load COUNT=10000000
task bench:run TASKS=16
```

### msg/sec

#### batch.size = 3000

| tasks.max | msg/sec |
|-----------|---------|
| 4         |  18,518 |
| 8         |  31,250 |
| 16        |  42,553 |

#### batch.size = 10000

| tasks.max | msg/sec |
|-----------|---------|
| 16        |  43,859 |

**Observations:**
- Peak throughput at batch=3000: **42,553 msg/sec** at 16 tasks. Increasing to batch=10000 yields marginal improvement (**43,859 msg/sec**) — batch size is not the bottleneck.
- Task scaling diminishes quickly: 4→8 tasks ~1.7×, 8→16 tasks ~1.4×.
- MySQL CDC is roughly **4.5× faster** than Kafka Connect JDBC Sink at peak (191K vs 43K msg/sec), with a fraction of the infrastructure.

---

## Debezium Kafka Source Connector Comparison

Debezium MySQL source connector reading the same 10,000,000-row `cart` snapshot into a Kafka topic.
Varying `max.batch.size`, `max.queue.size`, and `max.poll.records`.

See [`internal/impl/mysql/bench/kafka-source/`](../../internal/impl/mysql/bench/kafka-source/) for configs and run instructions.

### msg/sec

| fetch.size | batch.size | queue.size | elapsed | msg/sec |
|------------|------------|------------|---------|---------|
| 1,000      | 1,000      | 4,000      | 841s    |  11,890 |
| 5,000      | 5,000      | 20,000     | 747s    |  13,386 |
| 10,000     | 10,000     | 40,000     | 781s    |  12,804 |

**Observations:**
- Peak throughput: **13,386 msg/sec** at fetch=5000/batch=5000/queue=20000.
- Increasing to fetch=10000 gives no further gain (12,804 msg/sec) — throughput plateaus around 13K msg/sec regardless of fetch/batch size.

---

## Redpanda Connect — Kafka → MySQL

Redpanda Connect consuming from a Kafka topic (`kafka_franz` input) and writing to MySQL (`sql_insert` output).
Same Kafka broker as above (`cpus: 3`), 16 partitions. Varying `GOMAXPROCS` and `batching.count`.

See [`internal/impl/mysql/bench/redpanda-connector/`](../../internal/impl/mysql/bench/redpanda-connector/) for configs and run instructions.

```bash
task bench:load COUNT=10000000
task bench:run CORES=1 BATCH=10000
task bench:run CORES=4 BATCH=10000
# ...
```

### msg/sec (writes to MySQL)

| GOMAXPROCS | batch=10000 |
|------------|-------------|
| 4          |      64,102 |
| 8          |      60,975 |

**Observations:**
- Throughput plateaus at 4→8 cores (~60-64K msg/sec) — MySQL write throughput is the bottleneck, not CPU.
- Redpanda Connect is **~1.5× faster** than Kafka Connect JDBC Sink at peak (64K vs 43K msg/sec) using fewer resources (4 cores vs 16 tasks).
- Compared to MySQL CDC (191K msg/sec), the Kafka→MySQL write path is ~3× slower — the extra hop through Kafka and MySQL insert overhead both contribute.
