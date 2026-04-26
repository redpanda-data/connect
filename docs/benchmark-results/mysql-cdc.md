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

See [`internal/impl/mysql/bench/mysql-write/jdbc-sink/`](../../internal/impl/mysql/bench/mysql-write/jdbc-sink/) for configs and run instructions.

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

See [`internal/impl/mysql/bench/mysql-read/debezium/`](../../internal/impl/mysql/bench/mysql-read/debezium/) for configs and run instructions.

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

## Redpanda Connect — MySQL → Kafka

Redpanda Connect `mysql_cdc` input reading the same 10,000,000-row `cart` snapshot into a Kafka topic (`kafka_franz` output).
Varying `GOMAXPROCS` and `batching.count`.

See [`internal/impl/mysql/bench/mysql-read/rpcn/`](../../internal/impl/mysql/bench/mysql-read/rpcn/) for configs and run instructions.

```bash
task bench:build
task bench:load COUNT=10000000
task bench:all OUT=results.txt
```

### msg/sec

| GOMAXPROCS | batch=1,000 | batch=5,000 | batch=10,000 |
|------------|-------------|-------------|--------------|
| 1          |      15,085 |      27,849 |       46,137 |
| 2          |      39,253 |      38,760 |       41,322 |
| 4          |      29,412 |      45,455 |       45,455 |
| 8          |      29,412 |      45,455 |       45,872 |
| unbounded  |      28,592 |      41,908 |       50,440 |

**Observations:**
- Peak throughput: **50,440 msg/sec** (unbounded cores, batch=10000) — roughly **4× faster** than Debezium Kafka Source (13,386 msg/sec).
- Core scaling is strong from 1→2 cores (~2.5× at batch=1000) but plateaus at 2→4→8 cores — Kafka write throughput is the bottleneck beyond 2 cores.
- Batch size matters most at 1 core (15K→46K, ~3×); at 2+ cores the gain narrows significantly as the bottleneck shifts to Kafka.
- Beyond batch=5000 there is no meaningful gain at any core count.

---

## Redpanda Connect — Kafka → MySQL

Redpanda Connect consuming from a Kafka topic (`kafka_franz` input) and writing to MySQL (`sql_insert` output).
Same Kafka broker as above (`cpus: 3`), 16 partitions. Varying `GOMAXPROCS` and `batching.count`.

See [`internal/impl/mysql/bench/mysql-write/rpcn/`](../../internal/impl/mysql/bench/mysql-write/rpcn/) for configs and run instructions.

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

---

## Kafka Connect CDC (Debezium Source — Change Events)

Debezium MySQL source connector streaming CDC change events (inserts) for 10,000,000 rows into a Kafka topic.
Varying `max.batch.size` and `max.queue.size`.

See [`internal/impl/mysql/bench/mysql-read/debezium/`](../../internal/impl/mysql/bench/mysql-read/debezium/) for configs and run instructions.

### msg/sec

| batch.size | queue.size | elapsed | msg/sec |
|------------|------------|---------|---------|
|  1,000     |  4,000     | ~549s   |  18,227 |
|  5,000     | 20,000     | 392s    |  25,510 |
| 10,000     | 40,000     | 427s    |  23,419 |

**Observations:**
- Peak throughput: **25,510 msg/sec** at batch=5000/queue=20000.
- Increasing to batch=10000 gives no further gain (23,419 msg/sec) — throughput plateaus around 25K msg/sec regardless of batch size.
- Batch size has diminishing returns beyond 5000; the bottleneck is Debezium's internal processing, not fetch/queue sizing.

---

## Redpanda Connect CDC — MySQL → Kafka

Redpanda Connect `mysql_cdc` input streaming CDC change events (inserts) for 10,000,000 rows into a Kafka topic (`kafka_franz` output).
Varying `GOMAXPROCS` and `batching.count`.

See [`internal/impl/mysql/bench/mysql-read/rpcn/`](../../internal/impl/mysql/bench/mysql-read/rpcn/) for configs and run instructions.

```bash
task bench:build
task bench:load:cdc
task bench:all:cdc COUNT=10000000 OUT=cdc_results.txt
```

### msg/sec

| GOMAXPROCS | batch=1,000 | batch=5,000 | batch=10,000 |
|------------|-------------|-------------|--------------|
| 1          |      17,361 |      19,920 |       19,920 |
| 2          |      18,939 |      15,873 |       15,974 |
| 4          |      15,873 |      15,873 |       15,823 |
| 8          |      16,077 |      15,773 |       16,287 |

**Observations:**
- Peak throughput: **19,920 msg/sec** at 1 core, batch=5000/10000.
- Adding more cores provides no benefit — throughput is flat across 1→8 cores (~15–19K msg/sec). The bottleneck is the single-threaded CDC reader, not CPU or Kafka write parallelism.
- Batch size has minimal impact beyond 5000; the gain from 1000→5000 at 1 core (~15%) disappears entirely at 2+ cores.
- **Debezium CDC is ~1.3× faster** (~25K vs ~20K msg/sec) — the only benchmark where Redpanda Connect does not win. All other workloads (snapshot read, Kafka→MySQL write) favor Redpanda Connect.
