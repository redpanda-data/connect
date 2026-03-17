# S3 Sink - Time-Based Partitioning

**Pattern**: Cloud Storage - Time-Based Partitioning
**Difficulty**: Advanced
**Components**: aws_s3, kafka_franz, timestamp processing
**Use Case**: Partition S3 data by event time for time-series queries

## Overview

Write messages to S3 with time-based partitioning (year/month/day/hour) based on event timestamps. Optimized for time-range queries in analytics systems.

## Configuration

See [`s3-sink-time-based.yaml`](./s3-sink-time-based.yaml)

## Key Concepts

### Time-Based Paths
Extract event time and format into S3 path hierarchy.

### Batching Strategy
Balance file size with query performance.

## Related

- [S3 Sink Basic](s3-sink-basic.md)
