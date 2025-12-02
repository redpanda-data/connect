# S3 Polling with Bookmarking

**Pattern**: Cloud Storage - S3 Polling
**Difficulty**: Intermediate
**Components**: aws_s3 input, kafka_franz
**Use Case**: Poll S3 for new files and stream to Kafka

## Overview

Continuously poll S3 for new files and stream contents to Kafka. Tracks processed files to avoid re-processing.

## Configuration

See [`s3-polling.yaml`](./s3-polling.yaml)

## Key Concepts

### Scanner
Tracks which files have been processed.

### Polling Interval
Balance between latency and S3 API costs.

## Related

- [S3 Sink Basic](s3-sink-basic.md)
