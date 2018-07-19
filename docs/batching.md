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
batches, these are the [`combine`][combine], [`batch`][batch] and
[`split`][split] processors.

When creating batches with `combine` or `batch` the processor continously reads
messages until a target size has been reached, then the batch continues through
the pipeline.

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
    - type: combine
      combine:
          parts: 100
```

## Archiving Batches

Batches of messages can be condensed into a single message part using a selected
scheme such as `tar` with the [`archive`][archive] processor, the opposite is
also possible with the [`unarchive`][unarchive] processor.

This allows you to send and receive message batches over protocols that do not
support batching or multiple part messages.

[processors]: ./processors/README.md
[combine]: ./processors/README.md#combine
[batch]: ./processors/README.md#batch
[split]: ./processors/README.md#split
[archive]: ./processors/README.md#archive
[unarchive]: ./processors/README.md#unarchive
