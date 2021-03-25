---
title: Metrics
sidebar_label: About
---

Benthos emits lots of metrics in order to expose how components configured within your pipeline are behaving. You can configure exactly where these metrics end up with the config field `metrics`, which describes a metrics format and destination. For example, if you wished to push them via the StatsD protocol you could use this configuration:

```yaml
metrics:
  statsd:
    prefix: foo
    address: localhost:8125
    flush_period: 100ms
```

## Metric Names

The metric names that are emitted depend on your configured pipeline, as each individual component will emit one or more metrics, with each name prefixed with a label that uniquely identifies the component within your config (specified with the field `label`). If a component is configured without a label (or an empty one) then a label is generated based on where the component appears within your config.

The following is a list of standard metric names that are emitted for components of different types. This list does not cover _all_ metric names as it's possible that certain component implementations might have more granular metrics for other events.

Names are listed here in dot notation, which is how they will appear if sent to StatsD. Metrics exported in Prometheus format will display these metrics with dots replaced with underscores (and underscores replaced with double underscores).

### Inputs

- `<label>.count`: The number of times the input has attempted to read messages.
- `<label>.received`: The number of messages received by the input.
- `<label>.batch.received`: The number of message batches received by the input.
- `<label>.connection.up`
- `<label>.connection.failed`
- `<label>.connection.lost`
- `<label>.latency`: Measures the roundtrip latency from the point at which a message is read up to the moment the message has either been acknowledged by an output or has been stored within an external buffer.

### Buffers

Buffers do not have a label field, and instead will always emit metric names with the prefix `buffer`:

- `buffer.backlog`: The (sometimes estimated) size of the buffer backlog in bytes.
- `buffer.write.count`
- `buffer.write.error`
- `buffer.read.count`
- `buffer.read.error`
- `buffer.latency`: Measures the roundtrip latency from the point at which a message is read from the buffer up to the moment it has been acknowledged by the output.

### Processors

- `<label>.count`, the number of times the processor has been invoked (once per batch).
- `<label>.sent`, the number of messages returned by the processor.
- `<label>.batch.sent`, the number of message batches returned by the processor.
- `<label>.error`, the number of errors encountered during processing.

### Outputs

- `<label>.count`: The number of times the output has attempted to send messages.
- `<label>.sent`: The number of messages sent.
- `<label>.batch.sent`: The number of message batches sent.
- `<label>.batch.bytes`: The total number of bytes sent.
- `<label>.batch.latency`: Latency of message batch write in nanoseconds. Includes only successful attempts.
- `<label>.connection.up`
- `<label>.connection.failed`
- `<label>.connection.lost`

## Changing or Dropping Metric Names

Each metrics output type has a field `path_mapping` that allows you to change or remove metric names by applying a [Bloblang mapping][bloblang.about]. For example, the following mapping reduces the metrics exposed by Benthos to an explicit list by deleting names that aren't in that list:

```yaml
metrics:
  prometheus:
    prefix: benthos
    path_mapping: |
      if ![
        "foo_received",
        "foo_latency",
        "bar_sent"
      ].contains(this) { deleted() }
```

The value of `this` in the context of the mapping is the full name of the metric. Metrics are registered and renamed when Benthos first starts up, and when trace level logging is enabled you will see a log entry for each metric that outlines the effect of your mapping, which can help diagnose them.

[bloblang.about]: /docs/guides/bloblang/about

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="metrics" singular="metrics target"></ComponentSelect>