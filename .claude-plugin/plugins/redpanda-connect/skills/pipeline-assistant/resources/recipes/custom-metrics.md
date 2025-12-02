# Custom Prometheus Metrics

**Pattern**: Monitoring - Custom Metrics
**Difficulty**: Basic
**Components**: stdin, metric processor, prometheus
**Use Case**: Emit custom application metrics to Prometheus for monitoring and alerting

## Overview

This recipe demonstrates how to add custom Prometheus metrics to your Redpanda Connect pipelines. The example tracks JSON validation errors as a counter metric, which can be scraped by Prometheus and used for alerting. This pattern is essential for building observable data pipelines.

## Configuration

See [`custom-metrics.yaml`](./custom-metrics.yaml) for the complete configuration.

## Key Concepts

### 1. Metric Processor

The `metric` processor emits metrics during message processing:

```yaml
- metric:
    type: counter_by
    name: json_error_count
    value: 1
    labels:
      pipeline: "json_validation"
      error_type: "invalid_json"
```

- **type**: `counter_by` increments by the specified value
- **name**: Metric name (appears in Prometheus)
- **value**: Amount to increment (can use Bloblang expressions)
- **labels**: Key-value pairs for filtering/grouping

### 2. Prometheus Endpoint

The `metrics` section configures how metrics are exposed:

```yaml
metrics:
  prometheus: {}  # Default HTTP endpoint on :4195/stats
  mapping: |
    # Filter which metrics to expose
    if this != "json_error_count" { deleted() }
```

The mapping filters internal metrics, exposing only custom ones.

### 3. Metric Types

Redpanda Connect supports multiple metric types:
- `counter` - Monotonically increasing (e.g., total messages)
- `counter_by` - Increment by value
- `gauge` - Current value (e.g., queue depth)
- `timing` - Duration tracking

## Important Details

- **Security**: Metrics endpoint is HTTP by default, consider adding auth for production
- **Performance**: Minimal overhead - metrics are asynchronous
- **Error handling**: Metrics don't block pipeline - failures are logged
- **Cardinality**: Be careful with label values - high cardinality can cause issues

## Testing

```bash
# Run the pipeline
rpk connect run custom-metrics.yaml

# In another terminal, send test data
echo '{"valid":"json"}' | nc localhost 8080
echo 'invalid json' | nc localhost 8080
echo '{"more":"data"}' | nc localhost 8080

# Check metrics endpoint
curl -s http://localhost:4195/stats | grep json_error_count

# Expected output (after one error):
# json_error_count{error_type="invalid_json",label="emit_error_metric",path="root.pipeline.processors.1",pipeline="json_validation"} 1
```

## Variations

**Gauge Metric (Current Value):**
```yaml
- metric:
    type: gauge
    name: queue_depth
    value: ${!json("queue_size")}
```

**Timing Metric (Duration):**
```yaml
- metric:
    type: timing
    name: processing_duration_ms
    value: ${!json("duration")}
```

**Dynamic Labels:**
```yaml
- metric:
    type: counter_by
    name: messages_by_topic
    value: 1
    labels:
      topic: ${!metadata("kafka_topic")}
```

### Multi-Instance Monitoring (Streams Mode)

For distributed deployments with multiple pipeline instances:

```yaml
- metric:
    type: counter_by
    name: messages_processed
    value: 1
    labels:
      instance_id: "${HOSTNAME}"
      stream_id: "${STREAM_ID}"
      pipeline: "production"

metrics:
  prometheus:
    push_url: "http://pushgateway:9091"
    push_interval: "10s"
    push_job_name: "redpanda_connect"
```

This enables:
- Per-instance metrics tracking
- Aggregation across distributed deployments
- Pushgateway integration for ephemeral jobs
- Stream-specific monitoring in streams mode

### Pipeline Health Metrics

Track pipeline health with multiple metric types:

```yaml
pipeline:
  processors:
    # Track throughput
    - metric:
        type: counter_by
        name: messages_total
        value: 1

    # Track processing time
    - metric:
        type: timing
        name: processing_latency_ms
        value: ${!timestamp_unix_milli() - json("timestamp")}

    # Track queue depth
    - metric:
        type: gauge
        name: backlog_size
        value: ${!json("queue_size")}

    # Track error rate
    - switch:
        - check: meta("error")
          processors:
            - metric:
                type: counter_by
                name: errors_total
                value: 1
                labels:
                  error_type: ${!meta("error_type")}
```

Combine multiple metrics for comprehensive observability.

## Related Recipes

- [DLQ Basic](../error-handling/dlq-basic.md) - Combine with DLQ for comprehensive error tracking
- [Stateful Counter](../stateful/stateful-counter.md) - In-memory counters vs Prometheus metrics

## References

- [Metric Processor Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/processors/metric.adoc)
- [Prometheus Metrics Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/metrics/prometheus.adoc)
- [Prometheus Best Practices](https://prometheus.io/docs/practices/naming/)
