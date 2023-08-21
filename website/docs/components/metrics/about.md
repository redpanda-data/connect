---
title: Metrics
sidebar_label: About
---

Benthos emits lots of metrics in order to expose how components configured within your pipeline are behaving. You can configure exactly where these metrics end up with the config field `metrics`, which describes a metrics format and destination. For example, if you wished to push them via the StatsD protocol you could use this configuration:

```yaml
metrics:
  statsd:
    address: localhost:8125
    flush_period: 100ms
```

The default metrics configuration is to expose Prometheus metrics on the [service-wide HTTP endpoint][http.about] at the endpoints `/metrics` and `/stats`.

### Timings

It's worth noting that timing metrics within Benthos are measured in nanoseconds and are therefore named with a `_ns` suffix. However, some exporters do not support this level of precision and are downgraded, or have the unit converted for convenience. In these cases the exporter documentation outlines the conversion and why it is made.

## Metric Names

Each major Benthos component type emits one or more metrics with the name prefixed by the type. These metrics are intended to provide an overview of behaviour, performance and health. Some specific component implementations may provide their own unique metrics on top of these standardised ones, these extra metrics can be found listed on their respective documentation pages.

### Inputs

- `input_received`: A count of the number of messages received by the input.
- `input_latency_ns`: Measures the roundtrip latency in nanoseconds from the point at which a message is read up to the moment the message has either been acknowledged by an output, has been stored within a buffer, or has been rejected (nacked).
- `batch_created`: A count of each time an input-level batch has been created using a batching policy. Includes a label `mechanism` describing the particular mechanism that triggered it, one of; `count`, `size`, `period`, `check`.
- `input_connection_up`: For continuous stream based inputs represents a count of the number of the times the input has successfully established a connection to the target source. For poll based inputs that do not retain an active connection this value will increment once.
- `input_connection_failed`: For continuous stream based inputs represents a count of the number of times the input has failed to establish a connection to the target source.
- `input_connection_lost`: For continuous stream based inputs represents a count of the number of times the input has lost a previously established connection to the target source.

:::caution
The behaviour of connection metrics may differ based on input type due to certain libraries and protocols obfuscating the concept of a single connection.
:::

### Buffers

- `buffer_received`: A count of the number of messages written to the buffer.
- `buffer_batch_received`: A count of the number of message batches written to the buffer.
- `buffer_sent`: A count of the number of messages read from the buffer.
- `buffer_batch_sent`: A count of the number of message batches read from the buffer.
- `buffer_latency_ns`: Measures the roundtrip latency in nanoseconds from the point at which a message is read from the buffer up to the moment it has been acknowledged by the output.
- `batch_created`: A count of each time a buffer-level batch has been created using a batching policy. Includes a label `mechanism` describing the particular mechanism that triggered it, one of; `count`, `size`, `period`, `check`.

### Processors

- `processor_received`: A count of the number of messages the processor has been executed upon.
- `processor_batch_received`: A count of the number of message batches the processor has been executed upon.
- `processor_sent`: A count of the number of messages the processor has returned.
- `processor_batch_sent`: A count of the number of message batches the processor has returned.
- `processor_error`: A count of the number of times the processor has errored. In cases where an error is batch-wide the count is incremented by one, and therefore would not match the number of messages.
- `processor_latency_ns`: Latency of message processing in nanoseconds. When a processor acts upon a batch of messages this latency measures the time taken to process all messages of the batch.

### Outputs

- `output_sent`: A count of the number of messages sent by the output.
- `output_batch_sent`: A count of the number of message batches sent by the output.
- `output_error`: A count of the number of send attempts that have failed. On failed batched sends this count is incremented once only.
- `output_latency_ns`: Latency of writes in nanoseconds. This metric may not be populated by outputs that are pull-based such as the `http_server`.
- `batch_created`: A count of each time an output-level batch has been created using a batching policy. Includes a label `mechanism` describing the particular mechanism that triggered it, one of; `count`, `size`, `period`, `check`.
- `output_connection_up`: For continuous stream based outputs represents a count of the number of the times the output has successfully established a connection to the target sink. For poll based outputs that do not retain an active connection this value will increment once.
- `output_connection_failed`: For continuous stream based outputs represents a count of the number of times the output has failed to establish a connection to the target sink.
- `output_connection_lost`: For continuous stream based outputs represents a count of the number of times the output has lost a previously established connection to the target sink.

:::caution
The behaviour of connection metrics may differ based on output type due to certain libraries and protocols obfuscating the concept of a single connection.
:::

### Caches

All cache metrics have a label `operation` denoting the operation that triggered the metric series, one of; `add`, `get`, `set` or `delete`.

- `cache_success`: A count of the number of successful cache operations.
- `cache_error`: A count of the number of cache operations that resulted in an error.
- `cache_latency_ns`: Latency of operations in nanoseconds.
- `cache_not_found`: A count of the number of get operations that yielded no value due to the item not being found. This count is separate from `cache_error`.
- `cache_duplicate`: A count of the number of add operations that were aborted due to the key already existing. This count is separate from `cache_error`.

### Rate Limits

- `rate_limit_checked`: A count of the number of times the rate limit has been probed.
- `rate_limit_triggered`: A count of the number of times the rate limit has been triggered by a probe.
- `rate_limit_error`: A count of the number of times the rate limit has errored when probed.

## Metric Labels

The standard metric names are unique to the component type, but a benthos config may consist of any number of component instantiations. In order to provide a metrics series that is unique for each instantiation Benthos adds labels (or tags) that uniquely identify the instantiation. These labels are as follows:

### `path`

The `path` label contains a string representation of the position of a component instantiation within a config in a format that would locate it within a Bloblang mapping, beginning at `root`. This path is a best attempt and may not exactly represent the source component position in all cases and is intended to be used for assisting observability only.

This is the highest cardinality label since paths will change as configs are updated and expanded. It is therefore worth removing this label with a [mapping](#metric-mapping) in cases where you wish to restrict the number of unique metric series.

### `label`

The `label` label contains the unique label configured for a component emitting the metric series, or is empty for components that do not have a configured label. This is the most useful label for uniquely identifying a series for a component.

### `stream`

The `stream` label is present in a metric series emitted from a stream config executed when Benthos is running in [streams mode][streams.about], and is populated with the stream name.

## Example

The following Benthos configuration:

```yaml
input:
  label: foo
  http_server: {}

pipeline:
  processors:
    - mapping: |
        root.message = this
        root.meta.link_count = this.links.length()
        root.user.age = this.user.age.number()

output:
  label: bar
  stdout: {}

metrics:
  prometheus: {}
```

Would produce the following metrics series:

```text
input_latency_ns{label="foo",path="root.input"}
input_received{endpoint="post",label="foo",path="root.input"}
input_received{endpoint="websocket",label="foo",path="root.input"}

processor_batch_received{label="",path="root.pipeline.processors.0"}
processor_batch_sent{label="",path="root.pipeline.processors.0"}
processor_error{label="",path="root.pipeline.processors.0"}
processor_latency_ns{label="",path="root.pipeline.processors.0"}
processor_received{label="",path="root.pipeline.processors.0"}
processor_sent{label="",path="root.pipeline.processors.0"}

output_batch_sent{label="bar",path="root.output"}
output_connection_failed{label="bar",path="root.output"}
output_connection_lost{label="bar",path="root.output"}
output_connection_up{label="bar",path="root.output"}
output_error{label="bar",path="root.output"}
output_latency_ns{label="bar",path="root.output"}
output_sent{label="bar",path="root.output"}
```

## Metric Mapping

Since Benthos emits a large variety of metrics it is often useful to restrict or modify the metrics that are emitted. This can be done using the [Bloblang mapping language][bloblang.about] in the field `metrics.mapping`. This is a mapping executed for each metric that is registered within the Benthos service and allows you to delete an entire series, modify the series name and delete or modify individual labels.

Within the mapping the input document (referenced by the keyword `this`) is a string value containing the metric name, and the resulting document (referenced by the keyword `root`) must be a string value containing the resulting name. As is standard in Bloblang mappings, if the value of `root` is not assigned within the mapping then the metric name remains unchanged. If the value of `root` is `deleted()` then the metric series is dropped.

Labels can be referenced as metadata values with the function `meta`, where if the label does not exist in the series being mapped the value `null` is returned. Labels can be changed by using meta assignments, and can be assigned `deleted()` in order to remove them.

For example, the following mapping removes all but the `label` label entirely, which reduces the cardinality of each series. It also renames the `label` (for some reason) so that labels containing meows now contain woofs. Finally, the mapping restricts the metrics emitted to only three series; one for the input count, one for processor errors, and one for the output count, it does this by looking up metric names in a static array of allowed names, and if not present the `root` is assigned `deleted()`:

```yaml
metrics:
  mapping: |
    # Delete all pre-existing labels
    meta = deleted()

    # Re-add the `label` label with meows replaced with woofs
    meta label = meta("label").replace("meow", "woof")

    # Delete all metric series that aren't in our list
    root = if ![
      "input_received",
      "processor_error",
      "output_sent",
    ].contains(this) { deleted() }

  prometheus:
    use_histogram_timing: false
```

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="metrics" singular="metrics target"></ComponentSelect>

[bloblang.about]: /docs/guides/bloblang/about
[http.about]: /docs/components/http/about
[streams.about]: /docs/guides/streams_mode/about