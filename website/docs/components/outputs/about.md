---
title: Outputs
sidebar_label: About
---

An output is a sink where we wish to send our consumed data after applying an optional array of [processors][processors]. Only one output is configured at the root of a Benthos config. However, the output can be a [broker][output.broker] which combines multiple outputs under a chosen brokering pattern.

An output config section looks like this:

```yaml
output:
  s3:
    bucket: TODO
    path: "${!metadata:kafka_topic}/${!json_field:message.id}.json"

  # Optional list of processing steps
  processors:
   - jmespath:
       query: '{ message: @, meta: { link_count: length(links) } }'
```

### Back Pressure

Benthos outputs apply back pressure to components upstream. This means if your output target starts blocking traffic Benthos will gracefully stop consuming until the issue is resolved.

### Retries

When a Benthos output fails to send a message the error is propagated back up to the input, where depending on the protocol it will either be pushed back to the source as a Noack (e.g. AMQP) or will be reattempted indefinitely with the commit withheld until success (e.g. Kafka).

It's possible to instead have Benthos indefinitely retry an output until success with a [`retry`][output.retry] output. Some other outputs, such as the [`broker`][output.broker], might also retry indefinitely depending on their configuration.

### Multiplexing Outputs

It is possible to perform content based multiplexing of messages to specific outputs either by using the [`switch`][output.switch] output, or a [`broker`][output.broker] with the `fan_out` pattern and a [filter processor][processor.filter_parts] on each output, which is a processor that drops messages if the condition does not pass. Conditions are content aware logical operators that can be combined using boolean logic.

For more information regarding conditions, including a full list of available conditions please [read the docs here][conditions].

### Dead Letter Queues

It's possible to create fallback outputs for when an output target fails using a [`try`][output.try] output.

[processors]: /docs/components/processors/about
[processor.filter_parts]: /docs/components/processors/filter_parts
[conditions]: /docs/components/conditions/about
[output.broker]: /docs/components/outputs/broker
[output.retry]: /docs/components/outputs/retry
[output.try]: /docs/components/outputs/try