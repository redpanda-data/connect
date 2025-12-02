# Stateful Counter with Circuit Breaker

**Pattern**: Stateful Processing - Counter with Threshold
**Difficulty**: Intermediate
**Components**: stdin, cache, mapping, switch
**Use Case**: Track error counts in memory and implement circuit breaker pattern to stop pipeline when threshold is exceeded

## Overview

This recipe demonstrates stateful counting using an in-memory cache. The pattern tracks JSON validation errors and implements a circuit breaker that stops the pipeline when errors exceed a threshold. This is useful for building resilient pipelines that fail-fast when data quality degrades.

## Configuration

See [`stateful-counter.yaml`](./stateful-counter.yaml) for the complete configuration.

## Key Concepts

### 1. In-Memory State with Cache

The cache resource maintains state across messages:

```yaml
cache_resources:
  - label: error_cache
    memory:
      compaction_interval: ''  # Never expire
      init_values:
        error_count: 0  # Initialize counter
```

State persists for the pipeline's lifetime but is lost on restart.

### 2. Atomic Counter Operations

The counter is updated using three cache operations:
1. **GET** - Retrieve current count
2. **INCREMENT** - Add 1 to count (via Bloblang mapping)
3. **SET** - Store new count

Using the `branch` processor ensures these operations are atomic within the branch.

### 3. Circuit Breaker Pattern

After updating the counter, check if threshold is exceeded:

```yaml
- check: json("error_count") > 3
  processors:
    - crash: 'Pipeline failed due to error threshold'
```

This implements fail-fast behavior when data quality is poor.

### 4. Branch Processor for Side Effects

The `branch` processor runs operations without affecting the main message:
- Cache operations happen in the branch
- Main message continues unmodified
- Results can be read from metadata if needed

## Important Details

- **Security**: No credentials required (in-memory cache)
- **Performance**: In-memory cache is very fast but not persistent
- **Error handling**: Circuit breaker prevents endless bad data processing
- **State loss**: Counter resets on pipeline restart

## Testing

```bash
# Run the pipeline
rpk connect run stateful-counter.yaml

# Send valid JSON (should pass)
echo '{"test":"valid"}' | rpk connect run stateful-counter.yaml

# Send invalid JSON (increments counter)
echo 'invalid' | rpk connect run stateful-counter.yaml
echo '{broken' | rpk connect run stateful-counter.yaml
echo 'nope' | rpk connect run stateful-counter.yaml

# Fourth error should trigger circuit breaker and crash pipeline
echo 'error4' | rpk connect run stateful-counter.yaml
# Pipeline stops with: "Pipeline failed due to error threshold"
```

## Variations

**Persistent Counter with Redis:**
```yaml
cache_resources:
  - label: error_cache
    redis:
      url: ${REDIS_URL}
      default_ttl: "24h"
```

**Per-Topic Counters:**
```yaml
- cache:
    resource: error_cache
    operator: get
    key: ${!metadata("kafka_topic")}_error_count
```

**Windowed Counters:**
```yaml
cache_resources:
  - label: error_cache
    memory:
      compaction_interval: "1h"  # Reset hourly
```

## Related Recipes

- [DLQ Basic](../error-handling/dlq-basic.md) - Combines counter with DLQ
- [Custom Metrics](../monitoring/custom-metrics.md) - Alternative using Prometheus metrics

## References

- [Cache Processor Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/processors/cache.adoc)
- [Memory Cache Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/caches/memory.adoc)
- [Branch Processor Documentation](https://github.com/redpanda-data/connect/blob/main/docs/modules/components/pages/processors/branch.adoc)
