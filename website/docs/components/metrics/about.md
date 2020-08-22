---
title: Metrics
sidebar_label: About
---

Benthos exposes lots of metrics in order to expose how components configured within your pipeline are behaving. In order to aggregate these metrics you must configure a destination under the field `metrics`. For example, if you wished to push them via the StatsD protocol you could use this configuration:

```yaml
metrics:
  statsd:
    prefix: foo
    address: localhost:8125
    flush_period: 100ms
```

The metrics paths that you will see depend on your configured pipeline. However, there are some critical metrics that will always be present that are [outlined below](#paths).

Each metrics output type has a field `path_mapping` that allows you to change or remove metric paths before they are registered by applying a [Bloblang mapping][bloblang.about]. For example, the following mapping reduces the metrics exposed by Benthos to an explicit list by deleting names that aren't in the desired list:

```yaml
metrics:
  prometheus:
    prefix: benthos
    path_mapping: |
      if ![
        "input_received",
        "input_latency",
        "output_sent"
      ].contains(this) { deleted() }
```

The value of `this` in the context of the mapping is the full path of the metric (excluding the prefix).

## Paths

This document lists some of the most useful metrics exposed by Benthos, there are lots of more granular metrics available that may not appear here which will depend on your pipeline configuration.

Paths are listed here in dot notation, which is how they will appear if sent to StatsD. Other metrics destinations such as Prometheus will display these metrics with other notations (underscores instead of dots.)

### Input

- `input.count`: The number of times the input has attempted to read messages.
- `input.received`: The number of messages received by the input.
- `input.batch.received`: The number of message batches received by the input.
- `input.connection.up`
- `input.connection.failed`
- `input.connection.lost`
- `input.latency`: Measures the roundtrip latency from the point at which a message is read up to the moment the message has either been acknowledged by an output or has been stored within an external buffer.

### Buffer

- `buffer.backlog`: The (sometimes estimated) size of the buffer backlog in bytes.
- `buffer.write.count`
- `buffer.write.error`
- `buffer.read.count`
- `buffer.read.error`
- `buffer.latency`: Measures the roundtrip latency from the point at which a message is read from the buffer up to the moment it has been acknowledged by the output.

### Processors

Processor metrics are prefixed by the area of the Benthos pipeline they reside in and their index. For example, processors in the `pipeline` section will be prefixed with `pipeline.processor.N`, where N is the index.

- `pipeline.processor.0.count`
- `pipeline.processor.0.sent`
- `pipeline.processor.0.batch.sent`
- `pipeline.processor.0.error`

Processors that are children of other processors have paths that describe the full hierarchy, e.g. `pipeline.processor.0.if.0.1.count`. This can become a burden when the hierarchies are large and the paths are long. As an alternative it's possible to place processors within the [resources section](#resources) which results in flattened named paths.

### Conditions

Conditions provide `count`, `true` and `false` counters. They are either configured as direct children of other components and their path respects that, e.g.:

`pipeline.processors.0.condition.count`

Or they are a configured as a [resource](#resources), where their path is flattened using their name.

### Output

- `output.count`: The number of times the output has attempted to send messages.
- `output.sent`: The number of messages sent.
- `output.batch.sent`: The number of message batches sent.
- `output.batch.bytes`: The total number of bytes sent.
- `output.batch.latency`: Latency of message batch write in nanoseconds. Includes only successful attempts.
- `output.connection.up`
- `output.connection.failed`
- `output.connection.lost`

### Resources

Components within the resources section have a metrics path containing their name:

- `resource.cache.foo.latency`
- `resource.condition.bar.count`
- `resource.processor.baz.count`
- `resource.rate_limit.quz.count`

[bloblang.about]: /docs/guides/bloblang/about

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="metrics" singular="metrics target"></ComponentSelect>