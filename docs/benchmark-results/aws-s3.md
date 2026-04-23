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
