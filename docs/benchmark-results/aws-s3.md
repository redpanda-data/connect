
# AWS S3 Benchmark Results

**Environment:** Intel Core i7-10850H @ 2.70GHz, 32 GB RAM, WSL2 (Linux 6.6.87.2), x86\_64

See `../../internal/impl/aws/s3/bench/` for configs and run instructions.  
Read benchmarks are under `bench/read/`, write benchmarks under `bench/write/`.

***


# 📊 PERFORMANCE REPORT

## Executive Summary

This benchmark evaluates S3 read and write throughput across three approaches:

*   Sequential S3 access (`aws_s3` bucket walk)
*   Kafka Connect S3 Sink/Source
*   Redpanda Connect S3 pipelines (single and multi-instance)

### Key Findings

*   **S3 access is latency-bound unless parallelized.**  
    Sequential reads and writes are dominated by per-request overhead.

*   **Kafka Connect achieves the highest throughput (\~250k msg/s)**  
    due to parallel task execution and large-batch S3 writes.

*   **Redpanda Connect is throughput-capped (\~60k–73k msg/s)**  
    due to shared output constraints limiting S3 write concurrency.

*   **Batching is the dominant factor for write performance.**  
    Larger batches significantly reduce S3 request overhead.

*   **LocalStack introduces artificial ceilings.**  
    Results reflect LocalStack’s single-node S3 implementation rather than real AWS scalability.

***

## Performance Summary

| Workload                  | Best Throughput          | Limiting Factor       |
| ------------------------- | ------------------------ | --------------------- |
| Bucket walk (1KB)         | \~563 msg/s              | Request latency       |
| Bucket walk (1MB)         | \~190 msg/s (\~195 MB/s) | Transfer bandwidth    |
| Kafka Connect S3 Source   | \~73k msg/s              | S3 read throughput    |
| Kafka Connect S3 Sink     | \~250k msg/s             | S3 write throughput   |
| Redpanda Connect (single) | \~61k msg/s              | Shared S3 writer      |
| Redpanda Connect (multi)  | \~73k msg/s              | S3 backend saturation |

***

## Key Conclusions

*   **Parallelism is the primary driver of performance.**  
    Systems that issue multiple concurrent S3 requests achieve significantly higher throughput.

*   **Batch size is critical for write-heavy workloads.**  
    Larger batches reduce request overhead and improve efficiency.

*   **Architectural differences dominate tuning effects.**  
    Kafka Connect scales via independent tasks; Redpanda Connect is constrained by shared output.

*   **Measured ceilings are environment-dependent.**  
    LocalStack limits concurrency; real AWS S3 would likely increase absolute throughput and widen scaling differences.


***

# READ BENCHMARKS

## Bucket Walk — Small Objects (1 KB)

200,000 objects × 1 KB. Default `aws_s3` input in bucket walk mode (no SQS), LocalStack.

### msg/sec

| GOMAXPROCS | size=1024 |
| ---------- | --------- |
| 1          | 563       |
| 2          | 556       |
| 4          | 548       |
| 8          | 544       |

### kB/sec

| GOMAXPROCS | size=1024 |
| ---------- | --------- |
| 1          | 577       |
| 2          | 569       |
| 4          | 561       |
| 8          | 557       |

***

## Bucket Walk — Large Objects (1 MB)

20,000 objects × 1 MB. Same setup.

### msg/sec

| GOMAXPROCS | size=1048576 |
| ---------- | ------------ |
| 1          | 190          |
| 2          | 186          |
| 4          | 179          |
| 8          | 180          |

### MB/sec

| GOMAXPROCS | size=1048576 |
| ---------- | ------------ |
| 1          | 199          |
| 2          | 195          |
| 4          | 188          |
| 8          | 188          |

***

## Kafka Connect S3 Source — Read Throughput

### Results

| TASKS | FLUSH | ELAPSED(s) | MSG/S |
| ----- | ----- | ---------- | ----- |
| 1     | 5000  | 49         | 61224 |
| 1     | 10000 | 50         | 60000 |
| 1     | 50000 | 66         | 45454 |
| 2     | 5000  | 41         | 73170 |
| 2     | 10000 | 42         | 71428 |
| 2     | 50000 | 51         | 58823 |
| 4     | 5000  | 42         | 71428 |
| 4     | 10000 | 42         | 71428 |
| 4     | 50000 | 50         | 60000 |
| 8     | 5000  | 42         | 71428 |
| 8     | 10000 | 41         | 73170 |
| 8     | 50000 | 57         | 52631 |

***

## Read Observations

*   **Throughput is latency-bound for bucket walk.**  
    Sequential `GetObject` calls make HTTP round-trip time the dominant factor.

*   **CPU parallelism has no impact.**  
    Increasing `GOMAXPROCS` does not improve performance, confirming serialized I/O.

*   **Object size determines efficiency.**  
    Small objects (\~1 KB) are dominated by request overhead; large objects (\~1 MB) achieve high throughput due to efficient data transfer.

*   **Small-object workloads are inefficient.**  
    A 1000× size increase yields \~340× better throughput (MB/sec), showing request overhead dominates.

*   **Kafka Connect source follows the same S3 limits.**  
    Single-task throughput (\~60k msg/s) matches Redpanda write ceilings, indicating S3 request cost dominates.

*   **Parallelism improves read throughput up to saturation (\~73k msg/s).**  
    Beyond that, S3 becomes the bottleneck.

*   **LocalStack underestimates real latency impact.**  
    Real S3 deployments will show lower msg/sec due to network RTT.

***

# WRITE BENCHMARKS

## Kafka Connect S3 Sink — Write Throughput

### Best Configurations

| TASKS | FLUSH | POLL | FETCH MIN | MSG/S  |
| ----- | ----- | ---- | ---------- | ------ |
| 16    | 50000 | 1000 | 1MB        | 250000 |
| 2     | 50000 | 5000 | 4MB        | 230769 |
| 4     | 50000 | 1000 | 1MB        | 230769 |
| 8     | 50000 | 5000 | 1MB        | 230769 |

***

## Redpanda Connect S3 Sink — Single Process

### Best Configurations

| THREADS | FLUSH | FETCH MIN | MSG/S |
| ------- | ----- | ---------- | ----- |
| 2       | 5000  | 1MB        | 61224 |
| 2       | 10000 | 1MB        | 61224 |
| 4       | 10000 | 4MB        | 61224 |
| 8       | 10000 | 4MB        | 61224 |

***

## Redpanda Connect S3 Sink — Multi-Instance

### Best Configurations

| INSTANCES | FLUSH | FETCH MIN | MSG/S |
| --------- | ----- | ---------- | ----- |
| 2         | 5000  | 1MB        | 73170 |
| 8         | 10000 | 1MB        | 73170 |

***

## Write Observations

### Kafka Connect

*   **`flush.size` is the dominant factor.**  
    Larger batches significantly improve throughput by reducing the number of S3 `PUT` operations.

*   **Parallelism helps but saturates quickly.**  
    Increasing tasks improves throughput until S3 becomes the limiting factor.

*   **Timing effects create discrete result bands.**  
    Flush interval and commit timing introduce measurable latency variance.

*   **Practical ceiling: \~230k–250k msg/s.**  
    This reflects LocalStack S3 limits rather than Kafka itself.

***

### Redpanda Connect

*   **Single-process throughput is capped (\~60k msg/s).**  
    Performance is invariant across thread count and configuration.

*   **Processing parallelism does not translate to S3 parallelism.**  
    A shared output path limits scalability.

*   **Multiple instances improve throughput (\~73k msg/s).**  
    Parallel S3 writers across processes unlock limited scaling.

*   **Scaling saturates quickly.**  
    Beyond 2 instances, gains disappear due to S3 bottlenecks.

*   **Smaller flush sizes perform better.**  
    They avoid delays caused by timer-based flushes.

***

#  FINAL COMPARISON

| Metric                | Redpanda Connect                   | Kafka Connect  |
| --------------------- | ---------------------------------- | -------------- |
| Peak throughput       | **61k (single)** / **73k (multi)** | **250k msg/s** |
| Typical throughput    | 51k–61k                            | 111k–230k      |
| Parameter sensitivity | Low                                | High           |
| Scaling model         | Process-level                      | Task-level     |
| Output concurrency    | Limited                            | High           |
| Resource footprint    | \~200 MB RSS                       | \~2 GB JVM     |

***

## Summary

*   **Kafka Connect achieves \~4× higher peak throughput**, driven by multiple independent S3 writers and strong batching efficiency.

*   **Redpanda Connect is limited by shared output constraints.**  
    Internal concurrency improves processing but not S3 write parallelism.

*   **Batching is critical for Kafka Connect but largely ineffective for Redpanda Connect.**

