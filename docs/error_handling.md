Error Handling
==============

## Processor Errors

Sometimes things can go wrong. Benthos supports a range of
[processors][processors] such as `http` and `lambda` that have the potential to
fail if their retry attempts are exhausted. When this happens the data is not
dropped but instead continues through the pipeline mostly unchanged. The content
remains the same but a metadata flag is added to the message that can be
referred to later in the pipeline using the
[`processor_failed`][processor_failed] condition.

This behaviour allows you to define in your config whether you would like the
failed messages to be dropped, recovered with more processing, or routed to a
dead-letter queue, or any combination thereof. 

### Drop Failed Messages

In order to filter out any failed messages from your pipeline you can simply use
a [`filter_parts`][filter_parts] processor:

``` yaml
  - type: filter_parts
    filter_parts:
      type: processor_failed
```

This will remove any failed messages from a batch.

### Recover Failed Messages

In order to apply special recovery processing steps to only payloads that have
failed you can use a [`process_batch`][process_batch] and
[`conditional`][conditional] processor combination:

``` yaml
  - type: process_batch
    process_batch:
    - type: conditional
      conditional:
        condition:
          type: processor_failed
        processors:
        - type: foo # Recover here
```

Note that the `process_batch` processor is only required when your messages are
batched, as it allows you to conditionally process individual messages of the
batch.

### Route to a Dead-Letter Queue

Similar to both of the previous examples it is possible to send messages to
different destinations using either a [`group_by`][group_by] processor with a
[`switch`][switch] output, or a [`broker`][broker] output with
[`filter_parts`][filter_parts] processors.

``` yaml
pipeline:
  processors:
  - type: group_by
    group_by:
    - condition:
        type: processor_failed
output:
  type: switch
  switch:
    outputs:
    - output:
        type: foo # Dead letter queue
      condition:
        type: processor_failed
    - output:
        type: bar # Everything else
```

Note that the [`group_by`][group_by] processor is only necessary when messages
are batched.

Alternatively, using a `broker` output looks like this:

``` yaml
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: foo # Dead letter queue
      processors:
      - type: filter_parts
        filter_parts:
          type: processor_failed
    - type: bar # Everything else
      processors:
      - type: filter_parts
        filter_parts:
          type: not
          not:
            type: processor_failed
```

[processors]: ./processors/README.md
[processor_failed]: ./conditions/README.md#processor_failed
[filter_parts]: ./processors/README.md#filter_parts
[process_batch]: ./processors/README.md#process_batch
[conditional]: ./processors/README.md#conditional
[group_by]: ./processors/README.md#group_by
[switch]: ./outputs/README.md#switch
[broker]: ./outputs/README.md#broker