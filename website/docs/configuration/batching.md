---
title: Message Batching
---

Benthos is able to join sources and sinks with sometimes conflicting batching behaviours without sacrificing its strong delivery guarantees. It's also able to perform powerful processing functions across batches of messages such as grouping, archiving and reduction. Therefore, batching within Benthos is a mechanism that serves multiple purposes:

1. [Performance (throughput)](#performance)
2. [Grouped message processing](#grouped-message-processing)
3. [Compatibility (mixing multi and single part message protocols)](#compatibility)

## Performance

For most users the only benefit of batching messages is improving throughput over your output protocol. For some protocols this can happen in the background and requires no configuration from you. However, if an output has a `batching` configuration block this means it benefits from batching and requires you to specify how you'd like your batches to be formed by configuring a [batching policy](#batch-policy):

```yaml
output:
  foo:
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
    topic: baz
    batching:
      count: 10
      period: 100ms

output:
  type: foo
```

Inputs that behave this way are documented as such and have a `batching` configuration block.

Sometimes you may prefer to create your batches before processing in order to benefit from [batch wide processing](#grouped-message-processing), in which case if your input doesn't already support [a batch policy](#batch-policy) you can instead use a [`broker`][input_broker], which also allows you to combine inputs with a single batch policy:

```yaml
input:
  broker:
    inputs:
      - type: foo
      - type: bar
    batching:
      count: 50
      period: 500ms
```

This also works the same with [output brokers][output_broker].

## Grouped Message Processing

One of the more powerful features of Benthos is that all processors are "batch aware", which means processors that operate on single messages can be configured using the `parts` field to only operate on select messages of a batch:

```yaml
pipeline:
  processors:
    # This processor only acts on the first message of a batch
    - jmespath:
        parts: [ 0 ]
        query: "{ nested: @, links: join(', ', data.urls) }"
```

And some [function interpolation][function_interpolation] operations are evaluated batch wide:

```yaml
pipeline:
  processors:
    # Set the field `common_foo` of every message of the batch to the value of
    # `body.source_id` of the last message of the batch.
    - json:
        operator: set
        path: common_foo
        value: "${!json_field:body.source_id,-1}"
```

You can also avoid this behaviour with the [`for_each` processor][proc_for_each].

There's a vast number of processors that specialise in operations across batches such as [grouping][proc_group_by], [archiving][proc_archive], [joining][proc_merge_json] and more. For example, the following processors group a batch of messages according to a metadata field and compresses them into separate `.tar.gz` archives:

```yaml
pipeline:
  processors:
  - group_by_value:
      value: ${!metadata:kafka_partition}
  - archive:
      format: tar
  - compress:
      algorithm: gzip

output:
  s3:
    bucket: TODO
    path: docs/${!metadata:kafka_partition}/${!count:files}-${!timestamp_unix_nano}.tar.gz
```

## Compatibility

Benthos is able to read and write over protocols that support multiple part messages, and all payloads travelling through Benthos are represented as a multiple part message. Therefore, all components within Benthos are able to work with multiple parts in a message as standard.

When messages reach an output that _doesn't_ support multiple parts the message is broken down into an individual message per part, and then one of two behaviours happen depending on the output. If the output supports batch sending messages then the collection of messages are sent as a single batch. Otherwise, Benthos falls back to sending the messages sequentially in multiple, individual requests.

This behaviour means that not only can multiple part message protocols be easily matched with single part protocols, but also the concept of multiple part messages and message batches are interchangeable within Benthos.

### Shrinking Batches

A message batch (or multiple part message) can be broken down into smaller batches using the [`split`][split] processor:

```yaml
input:
  # Consume messages that arrive in three parts.
  type: foo
  processors:
    # Drop the third part
    - select_parts:
        parts: [ 0, 1 ]
    # Then break our message parts into individual messages
    - split:
        count: 1
```

This is also useful when your input source creates batches that are too large for your output protocol:

```yaml
input:
  s3:
    bucket: todo

pipeline:
  processors:
    - decompress:
        algorith: gzip
    - unarchive:
        format: tar
    # Limit batch sizes to 5MB
    - split:
        byte_size: 5_000_000
```

## Batch Policy

When an input component has a config field `batching` that means it supports a batch policy. This is a mechanism that allows you to configure exactly how your batching should work.

Batches are considered complete and will be flushed downstream when either of the following conditions are met:

- The `byte_size` field is non-zero and the total size of the batch in bytes matches or exceeds it (disregarding metadata.)
- The `count` field is non-zero and the total number of messages in the batch matches or exceeds it.
- A message added to the batch causes the [`condition`][conditions] to resolve to `true`.
- The `period` field is non-empty and the time since the last batch exceeds its value.

This allows you to combine conditions:

```yaml
output:
  foo:
    # Either send batches when they reach 10 messages or when 100ms has passed
    # since the last batch.
    batching:
      count: 10
      period: 100ms
```

During shutdown any remaining messages waiting for a batch to complete will be flushed down the pipeline.

[processors]: /docs/components/processors/about
[conditions]: /docs/components/conditions/about
[split]: /docs/components/processors/split
[archive]: /docs/components/processors/archive
[unarchive]: /docs/components/processors/unarchive
[proc_for_each]: /docs/components/processors/for_each
[proc_group_by]: /docs/components/processors/group_by
[proc_archive]: /docs/components/processors/archive
[proc_merge_json]: /docs/components/processors/merge_json
[input_broker]: /docs/components/inputs/broker
[output_broker]: /docs/components/outputs/broker
[input_kafka]: /docs/components/inputs/kafka
[function_interpolation]: interpolation#functions