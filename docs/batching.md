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
messages and message batches are interchangable within Benthos.

## Creating Batches

There are [processors][processors] within Benthos that can expand and contract
batches, these are the [`batch`][batch] and [`split`][split] processors.

The `batch` processor continously reads messages until a target size has been
reached, then the batch continues through the pipeline.

As messages are read and stored in a batch the input they originated from is
told to grab the next message but defer from acknowledging the current one, this
allows the entire batch of messages to be acknowledged at the same time and only
when they have reached their final destination.

It is good practice to always expand or contract batches within the `input`
section. Like this, for example:

``` yaml
input:
  type: broker
  broker:
    inputs:
    - type: foo
    - type: bar
  processors:
  - type: batch
    batch:
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
this with the [`process_batch`][process_batch] processor:

``` yaml
input:
  type: foo
  processors:
  - type: batch
    batch:
      byte_size: 20_000_000
  - type: process_batch
    process_batch:
    - type: dedupe
      dedupe:
        cache: foocache
        key: ${!json_field:foo.bar}
```

The `dedupe` processor will now treat all items of the batch as if it were a
batch of one message. Whatever messages result from the child processors will
continue as their own batch.

[processors]: ./processors/README.md
[batch]: ./processors/README.md#batch
[split]: ./processors/README.md#split
[archive]: ./processors/README.md#archive
[unarchive]: ./processors/README.md#unarchive
[process_batch]: ./processors/README.md#process_batch
