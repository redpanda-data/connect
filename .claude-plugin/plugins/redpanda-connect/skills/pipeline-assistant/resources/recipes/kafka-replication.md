# Kafka Topic Replication

**Pattern**: Replication - Kafka to Kafka
**Difficulty**: Intermediate
**Components**: kafka_franz, fallback, retry, file
**Use Case**: Replicate Kafka topics between clusters while preserving order, timestamps, and headers

## Overview

Replicate data between Kafka clusters with full fidelity - preserving partitions, keys, timestamps, and headers. Includes retry logic and DLQ for poison messages. Essential for cross-datacenter replication, disaster recovery, and data migration.

## Configuration

See [`kafka-replication.yaml`](./kafka-replication.yaml) for the complete configuration.

## Key Concepts

### 1. Metadata Preservation

Preserve all source characteristics:
- Partition assignment (manual partitioner)
- Message key (ordering guarantee)
- Timestamp (event time preservation)
- All custom headers

### 2. Fallback with Retry

```yaml
fallback:
  - retry:
      max_retries: 3
      output:
        kafka_franz: {}
  - file: {}  # DLQ
```

Try writing with retries, fall back to DLQ on failure.

### 3. Poison Message Handling

Messages that fail after retries go to DLQ with full context for manual recovery.

## Important Details

- **Security**: SASL/TLS for both source and destination
- **Performance**: Idempotent writes prevent duplicates during retries
- **Error handling**: DLQ prevents pipeline blocking on bad messages
- **Monitoring**: Log all DLQ writes for alerting

## Testing

```bash
# Set environment variables
export SOURCE_BROKER=source:9092
export DEST_BROKER=dest:9092
export SOURCE_TOPIC=events
export DEST_TOPIC_PREFIX=replicated_
export CONSUMER_GROUP=replication_cg
export DLQ_PATH=./dlq

# Run replication
rpk connect run kafka-replication.yaml
```

## Related Recipes

- [Multicast](multicast.md) - Fan-out to multiple destinations
- [DLQ Basic](dlq-basic.md) - Dead letter queue pattern

## References

- [Fallback Output](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/outputs/fallback.adoc)
- [Retry Output](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/outputs/retry.adoc)
