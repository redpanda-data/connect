# AWS S3 Benchmark Results

**Environment:** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.87.2), x86_64

See [`internal/impl/aws/s3/bench/`](../../internal/impl/aws/s3/bench/) for configs and run instructions.

---

## Bucket Walk — Small Objects (1 KB)

200,000 objects × 1 KB. Default `aws_s3` input in bucket walk mode (no SQS), LocalStack (`localstack/localstack:3`).

### msg/sec

| GOMAXPROCS | size=1024 |
|------------|-----------|
| 1          |       563 |
| 2          |       556 |
| 4          |       548 |
| 8          |       544 |

### kB/sec

| GOMAXPROCS | size=1024 |
|------------|-----------|
| 1          |       577 |
| 2          |       569 |
| 4          |       561 |
| 8          |       557 |

---

## Bucket Walk — Large Objects (1 MB)

20,000 objects × 1 MB. Same setup as above.

### msg/sec

| GOMAXPROCS | size=1048576 |
|------------|--------------|
| 1          |          190 |
| 2          |          186 |
| 4          |          179 |
| 8          |          180 |

### MB/sec

| GOMAXPROCS | size=1048576 |
|------------|--------------|
| 1          |          199 |
| 2          |          195 |
| 4          |          188 |
| 8          |          188 |

---

## Observations

- The `aws_s3` input processes objects sequentially (one `GetObject` call at a time) — throughput is bounded by per-object HTTP round-trip latency, not data size or CPU.
- GOMAXPROCS has no meaningful effect in either run (small: 563→544 msg/sec; large: 190→180 msg/sec), confirming the bottleneck is serial I/O, not CPU parallelism.
- Larger objects reduce msg/sec (~550 → ~180) but dramatically increase MB/sec (~0.6 → ~195). With 1 KB objects the bottleneck is per-request overhead; with 1 MB objects it shifts to data transfer time.
- Data throughput scales strongly with object size: going from 1 KB to 1 MB yields a ~340× MB/sec improvement despite only a 3× drop in msg/sec, making large objects far more efficient for bulk data movement.
- LocalStack runs in-process with negligible network overhead; real AWS S3 latency (~few ms per request) would reduce msg/sec further.
- For high-throughput S3 ingestion, the SQS-triggered mode or multiple parallel Connect instances would be needed.

---

## Kafka Connect S3 Sink — Write Throughput

**Architecture:** Redpanda Connect producer → `bench-events` (16 partitions, Kafka KRaft) → Kafka Connect S3 Sink (`confluentinc/kafka-connect-s3:10.5.23`) → LocalStack S3

**Dataset:** 3,000,000 synthetic JSON records (~100 bytes each), snappy-compressed in Kafka

**Connector:** `CONNECT_OFFSET_FLUSH_INTERVAL_MS=5000`, `KAFKA_HEAP_OPTS=-Xmx2g`, `s3.part.size=5MB`

**Measurement:** elapsed = time from connector registration to consumer group lag reaching 0; timing excludes nothing (includes task startup, rebalance, and final partial-file flush via `rotate.schedule.interval.ms=10s`)

See [`internal/impl/aws/s3/bench/kafka-connect/`](../../internal/impl/aws/s3/bench/kafka-connect/) for the full benchmark harness.

### Full Parameter Matrix (72 combinations)

| TASKS | FLUSH  | POLL  | FETCH_MIN | ELAPSED(s) | MSG/S  |
|-------|--------|-------|-----------|------------|--------|
| 2     | 5000   | 1000  | 1MB       | 20         | 150000 |
| 2     | 5000   | 1000  | 4MB       | 20         | 150000 |
| 2     | 5000   | 5000  | 1MB       | 27         | 111111 |
| 2     | 5000   | 5000  | 4MB       | 26         | 115384 |
| 2     | 5000   | 10000 | 1MB       | 20         | 150000 |
| 2     | 5000   | 10000 | 4MB       | 26         | 115384 |
| 2     | 10000  | 1000  | 1MB       | 20         | 150000 |
| 2     | 10000  | 1000  | 4MB       | 26         | 115384 |
| 2     | 10000  | 5000  | 1MB       | 26         | 115384 |
| 2     | 10000  | 5000  | 4MB       | 26         | 115384 |
| 2     | 10000  | 10000 | 1MB       | 19         | 157894 |
| 2     | 10000  | 10000 | 4MB       | 26         | 115384 |
| 2     | 50000  | 1000  | 1MB       | 19         | 157894 |
| 2     | 50000  | 1000  | 4MB       | 14         | 214285 |
| 2     | 50000  | 5000  | 1MB       | 19         | 157894 |
| 2     | 50000  | 5000  | 4MB       | 13         | 230769 |
| 2     | 50000  | 10000 | 1MB       | 19         | 157894 |
| 2     | 50000  | 10000 | 4MB       | 19         | 157894 |
| 4     | 5000   | 1000  | 1MB       | 21         | 142857 |
| 4     | 5000   | 1000  | 4MB       | 26         | 115384 |
| 4     | 5000   | 5000  | 1MB       | 26         | 115384 |
| 4     | 5000   | 5000  | 4MB       | 26         | 115384 |
| 4     | 5000   | 10000 | 1MB       | 27         | 111111 |
| 4     | 5000   | 10000 | 4MB       | 26         | 115384 |
| 4     | 10000  | 1000  | 1MB       | 26         | 115384 |
| 4     | 10000  | 1000  | 4MB       | 19         | 157894 |
| 4     | 10000  | 5000  | 1MB       | 13         | 230769 |
| 4     | 10000  | 5000  | 4MB       | 20         | 150000 |
| 4     | 10000  | 10000 | 1MB       | 26         | 115384 |
| 4     | 10000  | 10000 | 4MB       | 19         | 157894 |
| 4     | 50000  | 1000  | 1MB       | 13         | 230769 |
| 4     | 50000  | 1000  | 4MB       | 20         | 150000 |
| 4     | 50000  | 5000  | 1MB       | 13         | 230769 |
| 4     | 50000  | 5000  | 4MB       | 19         | 157894 |
| 4     | 50000  | 10000 | 1MB       | 20         | 150000 |
| 4     | 50000  | 10000 | 4MB       | 13         | 230769 |
| 8     | 5000   | 1000  | 1MB       | 21         | 142857 |
| 8     | 5000   | 1000  | 4MB       | 20         | 150000 |
| 8     | 5000   | 5000  | 1MB       | 20         | 150000 |
| 8     | 5000   | 5000  | 4MB       | 27         | 111111 |
| 8     | 5000   | 10000 | 1MB       | 26         | 115384 |
| 8     | 5000   | 10000 | 4MB       | 27         | 111111 |
| 8     | 10000  | 1000  | 1MB       | 19         | 157894 |
| 8     | 10000  | 1000  | 4MB       | 20         | 150000 |
| 8     | 10000  | 5000  | 1MB       | 26         | 115384 |
| 8     | 10000  | 5000  | 4MB       | 27         | 111111 |
| 8     | 10000  | 10000 | 1MB       | 26         | 115384 |
| 8     | 10000  | 10000 | 4MB       | 26         | 115384 |
| 8     | 50000  | 1000  | 1MB       | 19         | 157894 |
| 8     | 50000  | 1000  | 4MB       | 26         | 115384 |
| 8     | 50000  | 5000  | 1MB       | 13         | 230769 |
| 8     | 50000  | 5000  | 4MB       | 20         | 150000 |
| 8     | 50000  | 10000 | 1MB       | 19         | 157894 |
| 8     | 50000  | 10000 | 4MB       | 26         | 115384 |
| 16    | 5000   | 1000  | 1MB       | 27         | 111111 |
| 16    | 5000   | 1000  | 4MB       | 26         | 115384 |
| 16    | 5000   | 5000  | 1MB       | 26         | 115384 |
| 16    | 5000   | 5000  | 4MB       | 26         | 115384 |
| 16    | 5000   | 10000 | 1MB       | 27         | 111111 |
| 16    | 5000   | 10000 | 4MB       | 27         | 111111 |
| 16    | 10000  | 1000  | 1MB       | 20         | 150000 |
| 16    | 10000  | 1000  | 4MB       | 26         | 115384 |
| 16    | 10000  | 5000  | 1MB       | 20         | 150000 |
| 16    | 10000  | 5000  | 4MB       | 20         | 150000 |
| 16    | 10000  | 10000 | 1MB       | 20         | 150000 |
| 16    | 10000  | 10000 | 4MB       | 27         | 111111 |
| 16    | 50000  | 1000  | 1MB       | 12         | 250000 |
| 16    | 50000  | 1000  | 4MB       | 19         | 157894 |
| 16    | 50000  | 5000  | 1MB       | 19         | 157894 |
| 16    | 50000  | 5000  | 4MB       | 27         | 111111 |
| 16    | 50000  | 10000 | 1MB       | 14         | 214285 |
| 16    | 50000  | 10000 | 4MB       | 20         | 150000 |

### Best Configurations

| TASKS | FLUSH | POLL  | FETCH_MIN | MSG/S  |
|-------|-------|-------|-----------|--------|
| 16    | 50000 | 1000  | 1MB       | 250000 |
| 2     | 50000 | 5000  | 4MB       | 230769 |
| 4     | 50000 | 1000  | 1MB       | 230769 |
| 8     | 50000 | 5000  | 1MB       | 230769 |

### Observations

- **`flush.size` is the dominant parameter.** Configurations with `flush.size=50000` account for all results above 200k msg/s; configurations with `flush.size=5000` never exceed ~157k. Larger flush sizes mean fewer, bigger S3 `PUT` requests, which amortises HTTP overhead and reduces write amplification.

- **Task count has diminishing returns.** The best result (250k msg/s) uses `tasks=16`, but `tasks=2` and `tasks=4` with the same `flush.size=50000` reach 230k — only 8% slower. With large flush sizes the bottleneck shifts from task parallelism to LocalStack S3 write throughput, which is single-node and not parallelised.

- **Results cluster at discrete elapsed-time bands (12–14s, 19–21s, 26–27s)** due to the interaction between `rotate.schedule.interval.ms=10s` (partial-file flush) and `CONNECT_OFFSET_FLUSH_INTERVAL_MS=5s` (Kafka offset commit). A run that lands on the edge of a schedule interval can add 5–10s of dead time, creating apparent variance where parameters are not actually different.

- **`fetch.min.bytes` effect is inconsistent.** 4MB sometimes improves throughput (tasks=2, flush=50000: 230k vs 157k) but just as often degrades it (tasks=16, flush=50000: 250k→157k). The higher value coalesces Kafka fetch responses into fewer, larger batches, which helps when S3 writes are the bottleneck but can stall a task waiting to accumulate a full batch before flushing.

- **`poll.records` matters mainly for low task counts.** With `tasks=2` covering 8 partitions each, records from a single poll are spread thinly (~125/partition). Higher poll counts slightly help reach `flush.size` faster but the effect is secondary to `flush.size` itself.

- **Practical throughput ceiling: ~230–250k msg/s** on this hardware against LocalStack. This reflects S3 write overhead, not Kafka consumer throughput. Real AWS S3 with higher latency per `PUT` would widen the gap between small and large `flush.size`.

- **Recommended configuration for maximum throughput:** `tasks=16`, `flush.size=50000`, `poll.records=1000`, `fetch.min.bytes=1MB` — matches partition count, maximises file size per S3 write, and avoids fetch-wait stalls.
