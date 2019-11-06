Message Batching
================

Benthos is able to read and write over protocols that support multiple part messages, and all payloads travelling through Benthos are represented as a multiple part message. Therefore, all components within Benthos are able to work with multiple parts in a message as standard.

When messages reach an output that _doesn't_ support multiple parts the message is broken down into an individual message per part, and then one of two behaviours happen depending on the output. If the output supports batch sending messages then the collection of messages are sent as a single batch. Otherwise, Benthos falls back to sending the messages sequentially in multiple, individual requests.

This behaviour means that not only can multiple part message protocols be easily matched with single part protocols, but also the concept of multiple part messages and message batches are interchangeable within Benthos.

## Creating Batches

Most input types have some way of creating batches of messages from their feeds. These input specific methods are usually the most efficient and should therefore be preferred.

### Batch Policy

When an input component has a config field `batching` that means it supports a batch policy. This is a mechanism that allows you to configure exactly how your batching should work.

Batches are considered complete and will be flushed downstream when either of the following conditions are met:

- The `byte_size` field is non-zero and the total size of the batch in bytes matches or exceeds it (disregarding metadata.)
- The `count` field is non-zero and the total number of messages in the batch matches or exceeds it.
- A message added to the batch causes the [`condition`][conditions] to resolve to `true`.
- The `period` field is non-empty and the time since the last batch exceeds its value.

This allows you to combine conditions:

```yaml
input:
  foo:
    # Either send batches when they reach 10 messages or when 100ms has passed
    # since the last batch.
    batching:
      count: 10
      period: 100ms
```

During shutdown any remaining messages waiting for a batch to complete will be flushed down the pipeline.

### Combined Batching

Inputs that do not support their own batch policy can instead be batched using an [input `broker`][input_broker], which also allows you to combine input feeds into shared batching mechanisms:

```yaml
input:
  broker:
    inputs:
      - foo:
          bar: baz
      - bep:
          qux: quz 
    batching:
      count: 50
      period: 500ms
```

Some inputs do not support broker based batching and will specify this in their documentation.

### Batching After Processing

When using inputs that support broker based batching you can choose to batch at the end of your processing with an [output `broker`][output_broker]:

```yaml
input:
  type: foo

pipeline:
  processors:
    - filter:
        text:
          operator: contains
          value: bar

output:
  broker:
    pattern: fan_out
    outputs:
      - foo:
          bar: baz
    batching:
      count: 50
      period: 500ms
```


## Shrinking Batches

A message batch can be broken down into smaller batches using the [`split`][split] processor:

```yaml
# Batch messages into one second bursts
input:
  foo:
    batching:
      period: 1s
  processors:
    # Then break them down into smaller batches of max size 10
    - split:
        count: 10
```

## Archiving Batches

Batches of messages can be condensed into a single message part using a selected scheme such as `tar` with the [`archive`][archive] processor, the opposite is also possible with the [`unarchive`][unarchive] processor.

This allows you to send and receive message batches over protocols that do not support batching or multiple part messages.

## Processing Batches

Most processors will perform the same action on each message part of a batch, and therefore they behave the same regardless of whether they see a single message or a batch of plenty.

However, some processors execute across an entire batch and this might be undesirable. For example, imagine we batch our messages for the purpose of throughput optimisation but also need to perform deduplication on the payloads. The `dedupe` processor works batch wide, so in this case we need to force the processor to execute on each message individually. We can do this with the [`for_each`][for_each] processor:

``` yaml
input:
  foo:
    batching:
      count: 10
      period: 100ms
  processors:
  - for_each:
    - dedupe:
        cache: foocache
        key: ${!json_field:foo.bar}
```

[processors]: ./processors/README.md
[conditions]: ./conditions/README.md
[batch]: ./processors/README.md#batch
[split]: ./processors/README.md#split
[archive]: ./processors/README.md#archive
[unarchive]: ./processors/README.md#unarchive
[for_each]: ./processors/README.md#for_each
[input_broker]: ./inputs/README.md#broker
[output_broker]: ./outputs/README.md#broker
