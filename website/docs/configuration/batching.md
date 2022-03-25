---
title: Message Batching
---

Benthos is able to join sources and sinks with sometimes conflicting batching behaviours without sacrificing its strong delivery guarantees. It's also able to perform powerful [processing functions][windowing] across batches of messages such as grouping, archiving and reduction. Therefore, batching within Benthos is a mechanism that serves multiple purposes:

1. [Performance (throughput)](#performance)
2. [Grouped message processing](#grouped-message-processing)
3. [Compatibility (mixing multi and single part message protocols)](#compatibility)

## Performance

For most users the only benefit of batching messages is improving throughput over your output protocol. For some protocols this can happen in the background and requires no configuration from you. However, if an output has a `batching` configuration block this means it benefits from batching and requires you to specify how you'd like your batches to be formed by configuring a [batching policy](#batch-policy):

```yaml
output:
  kafka:
    addresses: [ todo:9092 ]
    topic: benthos_stream

    # Either send batches when they reach 10 messages or when 100ms has passed
    # since the last batch.
    batching:
      count: 10
      period: 100ms
```

However, a small number of inputs such as [`kafka`][input_kafka] must be consumed sequentially (in this case by partition) and therefore benefit from specifying your batch policy at the input level instead:

```yaml
input:
  kafka:
    addresses: [ todo:9092 ]
    topics: [ benthos_input_stream ]
    batching:
      count: 10
      period: 100ms

output:
  kafka:
    addresses: [ todo:9092 ]
    topic: benthos_stream
```

Inputs that behave this way are documented as such and have a `batching` configuration block.

Sometimes you may prefer to create your batches before processing in order to benefit from [batch wide processing](#grouped-message-processing), in which case if your input doesn't already support [a batch policy](#batch-policy) you can instead use a [`broker`][input_broker], which also allows you to combine inputs with a single batch policy:

```yaml
input:
  broker:
    inputs:
      - resource: foo
      - resource: bar
    batching:
      count: 50
      period: 500ms
```

This also works the same with [output brokers][output_broker].

## Grouped Message Processing

And some processors such as [`while`][processor.while] are executed once across a whole batch, you can avoid this behaviour with the [`for_each` processor][proc_for_each]:

```yaml
pipeline:
  processors:
    - for_each:
      - while:
          at_least_once: true
          max_loops: 0
          check: errored()
          processors:
            - catch: [] # Wipe any previous error
            - resource: foo # Attempt this processor until success
```

There's a vast number of processors that specialise in operations across batches such as [grouping][proc_group_by] and [archiving][proc_archive]. For example, the following processors group a batch of messages according to a metadata field and compresses them into separate `.tar.gz` archives:

```yaml
pipeline:
  processors:
    - group_by_value:
        value: ${! meta("kafka_partition") }
    - archive:
        format: tar
    - compress:
        algorithm: gzip

output:
  aws_s3:
    bucket: TODO
    path: docs/${! meta("kafka_partition") }/${! count("files") }-${! timestamp_unix_nano() }.tar.gz
```

For more examples of batched (or windowed) processing check out [this document][windowing].

## Compatibility

Benthos is able to read and write over protocols that support multiple part messages, and all payloads travelling through Benthos are represented as a multiple part message. Therefore, all components within Benthos are able to work with multiple parts in a message as standard.

When messages reach an output that _doesn't_ support multiple parts the message is broken down into an individual message per part, and then one of two behaviours happen depending on the output. If the output supports batch sending messages then the collection of messages are sent as a single batch. Otherwise, Benthos falls back to sending the messages sequentially in multiple, individual requests.

This behaviour means that not only can multiple part message protocols be easily matched with single part protocols, but also the concept of multiple part messages and message batches are interchangeable within Benthos.

### Shrinking Batches

A message batch (or multiple part message) can be broken down into smaller batches using the [`split`][split] processor:

```yaml
input:
  # Consume messages that arrive in three parts.
  resource: foo
  processors:
    # Drop the third part
    - select_parts:
        parts: [ 0, 1 ]
    # Then break our message parts into individual messages
    - split:
        size: 1
```

This is also useful when your input source creates batches that are too large for your output protocol:

```yaml
input:
  aws_s3:
    bucket: todo

pipeline:
  processors:
    - decompress:
        algorithm: gzip
    - unarchive:
        format: tar
    # Limit batch sizes to 5MB
    - split:
        byte_size: 5_000_000
```

## Batch Policy

When an input or output component has a config field `batching` that means it supports a batch policy. This is a mechanism that allows you to configure exactly how your batching should work on messages before they are routed to the input or output it's associated with. Batches are considered complete and will be flushed downstream when either of the following conditions are met:


- The `byte_size` field is non-zero and the total size of the batch in bytes matches or exceeds it (disregarding metadata.)
- The `count` field is non-zero and the total number of messages in the batch matches or exceeds it.
- A message added to the batch causes the [`check`][bloblang] to return to `true`.
- The `period` field is non-empty and the time since the last batch exceeds its value.

This allows you to combine conditions:

```yaml
output:
  kafka:
    addresses: [ todo:9092 ]
    topic: benthos_stream

    # Either send batches when they reach 10 messages or when 100ms has passed
    # since the last batch.
    batching:
      count: 10
      period: 100ms
```

:::caution
A batch policy has the capability to _create_ batches, but not to break them down.
:::

If your configured pipeline is processing messages that are batched _before_ they reach the batch policy then they may circumvent the conditions you've specified here, resulting in sizes you aren't expecting.

If you are affected by this limitation then consider breaking the batches down with a [`split` processor][split] before they reach the batch policy.

### Post-Batch Processing

A batch policy also has a field `processors` which allows you to define an optional list of [processors][processors] to apply to each batch before it is flushed. This is a good place to aggregate or archive the batch into a compatible format for an output:

```yaml
output:
  http_client:
    url: http://localhost:4195/post
    batching:
      count: 10
      processors:
        - archive:
            format: lines
```

The above config will batch up messages and then merge them into a line delimited format before sending it over HTTP. This is an easier format to parse than the default which would have been [rfc1342](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html).

During shutdown any remaining messages waiting for a batch to complete will be flushed down the pipeline.

[processors]: /docs/components/processors/about
[processor.while]: /docs/components/processors/while
[split]: /docs/components/processors/split
[archive]: /docs/components/processors/archive
[unarchive]: /docs/components/processors/unarchive
[proc_for_each]: /docs/components/processors/for_each
[proc_group_by]: /docs/components/processors/group_by
[proc_archive]: /docs/components/processors/archive
[input_broker]: /docs/components/inputs/broker
[output_broker]: /docs/components/outputs/broker
[input_kafka]: /docs/components/inputs/kafka
[function_interpolation]: /docs/configuration/interpolation#bloblang-queries
[bloblang]: /docs/guides/bloblang/about
[windowing]: /docs/configuration/windowed_processing