Message Batching
================

Benthos is able to read and write over protocols that support multiple part
messages, and all payloads travelling through Benthos are represented as a
multiple part message. Therefore, all components within Benthos are able to work
with multiple parts in a message as standard.

When messages reach an output that _doesn't_ support multiple parts the message
is broken down into an individual message per part, and then one of two
behaviours happen depending on the output. If the output supports batch sending
messages then the collection of messages are sent as a single batch. Otherwise,
Benthos falls back to sending the messages sequentially in multiple, individual
requests.

This behaviour means that not only can multiple part message protocols be easily
matched with single part protocols, but also the concept of multiple part
messages and message batches are interchangeable within Benthos.

## Creating Batches

Most input types have some way of creating batches of messages from their feeds.
These input specific methods are usually the most efficient and should therefore
be preferred.

### Batch Policy

When an input component has a config field `batching` that means it supports
a batch policy. This is a mechanism that allows you to configure exactly how
your batching should work.

Batches are considered complete and will be flushed downstream when either of
the following conditions are met:

- The `byte_size` field is non-zero and the total size of the batch in
  bytes matches or exceeds it (disregarding metadata.)
- The `count` field is non-zero and the total number of messages in
  the batch matches or exceeds it.
- A message added to the batch causes the [`condition`][conditions] to resolve
  to `true`.
- The `period` field is non-empty and the time since the last batch exceeds its
  value.

### Other Fields

Sometimes an input doesn't support batch policies but has other less powerful
ways of aggregating batches. For example, the [`kafka`][kafka] input has the
field `max_batch_count`, which specifies the maximum count of prefetched
messages each batch should contain (defaults at 1).

### Batch Processors

Alternatively, there are also [processors][processors] within Benthos that can
expand and contract batches, these are the [`batch`][batch] and [`split`][split]
processors.

The `batch` processor follows the same rules as any other
[batch policy](#batch-policy). However, the rules are only checked when a new
message is added, meaning a pending batch can last beyond the specified period
if no messages are added since the period was reached.

As messages are read and stored within a batch processor the input they
originated from is told to grab the next message but defer from acknowledging
the current one, this allows the entire batch of messages to be acknowledged at
the same time and only when they have reached their final destination.

It is good practice to always expand or contract batches within the `input`
section. Like this, for example:

``` yaml
input:
  broker:
    inputs:
    - type: foo
    - type: bar
  processors:
  - batch:
      byte_size: 20_000_000
```

You should *never* configure processors that remove payloads (`filter`,
`dedupe`, etc) before a batch processor as this can potentially break
acknowledgement propagation. Instead, always batch first and then configure your
processors [to work with the batch](#processing-batches).

## Archiving Batches

Batches of messages can be condensed into a single message part using a selected
scheme such as `tar` with the [`archive`][archive] processor, the opposite is
also possible with the [`unarchive`][unarchive] processor.

This allows you to send and receive message batches over protocols that do not
support batching or multiple part messages.

## Processing Batches

Most processors will perform the same action on each message part of a batch,
and therefore they behave the same regardless of whether they see a single
message or a batch of plenty.

However, some processors act on an entire batch and this might be undesirable.
For example, imagine we batch our messages for the purpose of throughput
optimisation but also need to perform deduplication on the payloads. The
`dedupe` processor works batch wide, so in this case we need to force the
processor to work as though each batched message is its own batch. We can do
this with the [`for_each`][for_each] processor:

``` yaml
input:
  type: foo
  processors:
  - batch:
      byte_size: 20_000_000
  - for_each:
    - dedupe:
        cache: foocache
        key: ${!json_field:foo.bar}
```

The `dedupe` processor will now treat all items of the batch as if it were a
batch of one message. Whatever messages result from the child processors will
continue as their own batch.

[processors]: ./processors/README.md
[conditions]: ./conditions/README.md
[batch]: ./processors/README.md#batch
[split]: ./processors/README.md#split
[archive]: ./processors/README.md#archive
[unarchive]: ./processors/README.md#unarchive
[for_each]: ./processors/README.md#for_each
[kafka]: ./inputs/README.md#kafka
[kafka_balanced]: ./inputs/README.md#kafka_balanced
