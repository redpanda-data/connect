# Dead Letter Queue - Basic Pattern

**Pattern**: Error Handling - Dead Letter Queue (DLQ)
**Difficulty**: Basic
**Components**: stdin, file, switch, mapping, log
**Use Case**: Route invalid or malformed messages to a dead letter queue for later analysis

## Overview

This recipe demonstrates the fundamental Dead Letter Queue (DLQ) pattern for handling invalid messages. Messages are validated for JSON format, and those that fail validation are written to a separate file (the DLQ) instead of causing pipeline failures. This pattern is essential for building resilient data pipelines that can handle malformed data gracefully.

## Configuration

See [`dlq-basic.yaml`](./dlq-basic.yaml) for the complete configuration.

## Key Concepts

### 1. Validation with Metadata Flags

The pipeline validates each message and sets metadata flags to track validation status:
- `@json_error = true` - Message failed validation
- `@json_error = false` - Message passed validation
- Original content and error details are preserved in metadata

### 2. Conditional Routing with Switch Output

The `switch` output component routes messages based on the `@json_error` metadata:
- Valid messages → stdout (or your primary destination)
- Invalid messages → DLQ file

### 3. DLQ File Storage

Invalid messages are written to a file (`json_error_dlq.txt`) for later processing:
- Each message written as a separate line
- Error details and original content preserved
- Can be processed manually or automatically later

### 4. Error Tracking

The pipeline maintains a counter of invalid messages in an in-memory cache:
- Tracks how many errors have occurred
- Can be used for alerting or circuit breaking
- Counter persists for the pipeline's lifetime

## Important Details

- **Security**: No credentials needed for this example (uses stdin/file)
- **Performance**: Minimal overhead from JSON parsing and metadata operations
- **Error handling**: Invalid messages don't block the pipeline - they're routed to DLQ
- **Extensibility**: Easy to replace file DLQ with Kafka topic, S3, or database

## Testing

```bash
# Run the pipeline
rpk connect run dlq-basic.yaml

# Test with valid JSON
echo '{"name":"John","age":30}' | rpk connect run dlq-basic.yaml

# Test with invalid JSON (will go to DLQ)
echo 'not valid json' | rpk connect run dlq-basic.yaml
echo '{"incomplete":' | rpk connect run dlq-basic.yaml

# Check DLQ file
cat json_error_dlq.txt
```

## Variations

### AVRO Encoding Errors

Handle AVRO schema validation and encoding errors:

```yaml
pipeline:
  processors:
    - mapping: |
        # Try AVRO encoding with schema
        let result = this.encode("avro", schema_id: "${SCHEMA_ID}").catch(null)

        if $result == null {
          meta avro_error = true
          meta error_text = "AVRO encoding failed: " + error()
          meta origin_value = content().string()
        } else {
          root = $result
          meta avro_error = false
        }

output:
  switch:
    cases:
      - check: "@avro_error"
        output:
          file:
            path: ./avro_error_dlq.txt
```

### Processor Error Handling

Catch errors from any processor and route to DLQ:

```yaml
pipeline:
  processors:
    - try:
        - http:
            url: https://api.example.com
            verb: POST
      catch:
        - mapping: |
            meta processor_error = true
            meta error_text = "HTTP request failed: " + error()
            meta origin_value = content().string()
```

All processor errors are automatically routed to DLQ.

### Error Tolerance Threshold

Add configurable error limits with tolerance:

```yaml
cache_resources:
  - label: error_cache
    memory:
      init_values:
        error_count: 0
        error_threshold: 100  # Stop after 100 errors
        error_tolerance_percent: 5  # Or 5% error rate

pipeline:
  processors:
    - switch:
        - check: 'json("error_count") > json("error_threshold")'
          processors:
            - log:
                level: ERROR
                message: "Error threshold exceeded, stopping pipeline"
            - crash: 'Too many errors'
```

This implements both absolute and percentage-based error tolerance.

## Related Recipes

- [Stateful Counter](stateful-counter.md) - Advanced error counting with cache
- [Content-Based Router](content-based-router.md) - Routing based on message content

## References

- [Switch Output Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/outputs/switch.adoc)
- [File Output Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/outputs/file.adoc)
- [Bloblang parse_json Method](https://github.com/redpanda-data/connect/blob/main/docs/modules/guides/pages/bloblang/methods.adoc#parse_json)
