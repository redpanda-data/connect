# Content-Based Router for Kafka

**Pattern**: Kafka Patterns - Content-Based Routing
**Difficulty**: Basic
**Components**: kafka_franz (input/output), mapping
**Use Case**: Route Kafka messages to different topics based on message content fields

## Overview

The Content-Based Router pattern dynamically routes messages to various destinations based on message content. This recipe shows how to filter Kafka messages by examining payload fields and routing only matching messages to the output topic, while preserving partition keys, timestamps, and headers for ordering guarantees.

## Configuration

See [`content-based-router.yaml`](./content-based-router.yaml) for the complete configuration.

## Key Concepts

### 1. Content Inspection

Messages are examined using Bloblang to check specific fields:
```bloblang
if (this.marketid == "nyse") {
  root = this
} else {
  root = deleted()  # Filter out non-matching messages
}
```

Only messages matching the condition are forwarded; others are silently dropped.

### 2. Metadata Preservation

Kafka-specific metadata is preserved through the pipeline:
- Partition key - Maintains message ordering
- Partition number - Preserves partitioning strategy
- Timestamp - Keeps original event time
- Headers - Retains all custom metadata

This is critical for maintaining ordering guarantees in distributed systems.

### 3. Manual Partitioning

The output uses `partitioner: "manual"` to explicitly control which partition messages go to:
```yaml
partitioner: "manual"
partition: "${!metadata(\"kafka_partition\")}"
```

This ensures messages maintain their source partition assignment.

## Important Details

- **Security**: Uses environment variables for broker addresses (`${KAFKA_BROKER}`)
- **Performance**:
  - `max_in_flight: 256` - High parallelism for throughput
  - `idempotent_write: true` - Prevents duplicates
  - `broker_write_max_bytes: 100MiB` - Handles large messages
- **Error handling**: `auto_replay_nacks: true` retries failed messages
- **Ordering**: Manual partitioning preserves source partition order

## Testing

```bash
# Set environment variables
export KAFKA_BROKER=localhost:9092
export SOURCE_TOPIC=test_in
export DEST_TOPIC=topic_a
export CONSUMER_GROUP=test_cg

# Run the pipeline
rpk connect run content-based-router.yaml

# Produce test messages
echo '{"marketid":"nyse","symbol":"AAPL","price":150}' | rpk topic produce $SOURCE_TOPIC
echo '{"marketid":"nasdaq","symbol":"MSFT","price":300}' | rpk topic produce $SOURCE_TOPIC
echo '{"marketid":"nyse","symbol":"GOOGL","price":2800}' | rpk topic produce $SOURCE_TOPIC

# Check output topic (only NYSE messages should appear)
rpk topic consume $DEST_TOPIC
```

## Variations

**Multiple Destinations:**
Replace the filter processor with a `switch` output to route to different topics:
```yaml
output:
  switch:
    cases:
      - check: 'json("marketid") == "nyse"'
        output:
          kafka_franz:
            topic: topic_nyse
      - check: 'json("marketid") == "nasdaq"'
        output:
          kafka_franz:
            topic: topic_nasdaq
```

## Related Recipes

- [DLQ Basic](../error-handling/dlq-basic.md) - Handle messages that fail routing
- [CDC Replication](./cdc-replication.md) - Advanced switch-based routing

## References

- [Kafka Franz Input Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/inputs/kafka_franz.adoc)
- [Manual Partitioner](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/outputs/kafka_franz.adoc#partitioner)
