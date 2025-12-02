# S3 Sink - Basic

**Pattern**: Cloud Storage - S3 Write
**Difficulty**: Intermediate
**Components**: aws_s3, kafka_franz
**Use Case**: Write Kafka messages to S3 with batching

## Overview

Batch and write Kafka messages to S3 for archival, analytics, or data lake use cases. Includes automatic path generation and batching.

## Configuration

See [`s3-sink-basic.yaml`](./s3-sink-basic.yaml)

## Key Concepts

### Batching
- count: Messages per file
- period: Max time between writes

### Path Generation
Dynamic S3 paths with date partitioning.

## Related

- [S3 Polling](s3-polling.md)
- [S3 Sink Time-Based](s3-sink-time-based.md)
