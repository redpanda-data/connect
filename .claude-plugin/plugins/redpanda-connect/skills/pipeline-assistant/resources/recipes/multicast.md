# Message Multicast (Fan-Out)

**Pattern**: Routing - Multicast / Fan-Out
**Difficulty**: Basic
**Components**: kafka_franz, broker output, mapping
**Use Case**: Send the same message to multiple destinations simultaneously

## Overview

The multicast pattern delivers a single message to multiple recipients. This recipe shows how to fan out Kafka messages to multiple topics based on message content, enabling parallel processing by different consumers. Essential for building event-driven architectures where multiple services need the same data.

## Configuration

See [`multicast.yaml`](./multicast.yaml) for the complete configuration.

## Key Concepts

### 1. Dynamic Destination List

Build a list of target topics based on message content:

```bloblang
let target_topics = []

if (this.type.contains("A")) {
  let target_topics = $target_topics.append("topic_a")
}
if (this.type.contains("B")) {
  let target_topics = $target_topics.append("topic_b")
}

meta target_topics = $target_topics
```

The list determines which outputs receive the message.

### 2. Broker Output Pattern

The `broker` output with `fan_out` pattern sends to all targets:

```yaml
output:
  broker:
    pattern: fan_out
    outputs:
      - kafka_franz:
          topic: topic_a
      - kafka_franz:
          topic: topic_b
```

All outputs receive the message simultaneously.

### 3. Metadata Preservation

Preserve source Kafka metadata for each destination:
- Original partition key
- Original timestamp
- Custom headers

This maintains message ordering and traceability.

## Important Details

- **Security**: Use environment variables for broker addresses
- **Performance**:
  - Messages sent in parallel to all destinations
  - `fan_out` pattern waits for all outputs to succeed
  - Use `fan_out_sequential` for ordered delivery
- **Error handling**: If any destination fails, entire message fails (can be changed with `drop_on`)
- **Ordering**: Preserved per-destination via partition key

## Testing

```bash
# Set environment variables
export KAFKA_BROKER=localhost:9092
export SOURCE_TOPIC=multicast_in
export CONSUMER_GROUP=multicast_cg

# Run the pipeline
rpk connect run multicast.yaml

# Send test messages
echo '{"data":"hello","type":"A"}' | rpk topic produce $SOURCE_TOPIC
echo '{"data":"world","type":"AB"}' | rpk topic produce $SOURCE_TOPIC
echo '{"data":"test","type":"ABC"}' | rpk topic produce $SOURCE_TOPIC

# Check destinations
rpk topic consume topic_a  # Should see all messages with "A"
rpk topic consume topic_b  # Should see messages with "B"
rpk topic consume topic_c  # Should see messages with "C"
```

## Variations

### Static Fan-Out (All Messages to All Topics)

```yaml
output:
  broker:
    pattern: fan_out
    outputs:
      - kafka_franz:
          topic: topic_a
      - kafka_franz:
          topic: topic_b
      - kafka_franz:
          topic: topic_c
```

All messages go to all three topics.

### Conditional with Drop on Error

```yaml
output:
  broker:
    pattern: fan_out
    outputs:
      - kafka_franz:
          topic: topic_a
        drop_on:
          error: true  # Don't fail entire message if topic_a fails
```

Continue on partial failures.

### Cross-System Multicast

```yaml
output:
  broker:
    pattern: fan_out
    outputs:
      - kafka_franz:
          topic: kafka_destination
      - aws_s3:
          bucket: s3_destination
      - http_client:
          url: http://webhook
```

Fan out to different systems simultaneously.

## Related Recipes

- [Content-Based Router](content-based-router.md) - Single destination routing
- [Kafka Replication](kafka-replication.md) - Cross-cluster replication

## References

- [Broker Output Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/outputs/broker.adoc)
- [Fan-Out Pattern](https://www.enterpriseintegrationpatterns.com/patterns/messaging/Broadcast.html)
