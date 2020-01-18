---
title: Error Handling
---

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

## Abandon on Failure

It's possible to define a list of processors which should be skipped for
messages that failed a previous stage using the [`try`][try] processor:

```yaml
  - try:
    - type: foo
    - type: bar # Skipped if foo failed
    - type: baz # Skipped if foo or bar failed
```

## Recover Failed Messages

Failed messages can be fed into their own processor steps with a
[`catch`][catch] processor:

```yaml
  - catch:
    - type: foo # Recover here
```

Once messages finish the catch block they will have their failure flags removed
and are treated like regular messages. If this behaviour is not desired then it
is possible to simulate a catch block with a [`conditional`][conditional]
processor placed within a [`for_each`][for_each] processor:

```yaml
  - for_each:
    - conditional:
        condition:
          type: processor_failed
        processors:
        - type: foo # Recover here
```

## Logging Errors

When an error occurs there will occasionally be useful information stored within
the error flag that can be exposed with the interpolation function
[`error`](interpolation#error). This allows you to expose the information with
processors.

For example, when catching failed processors you can [`log`][log] the messages:

```yaml
  - catch:
    - log:
        message: "Processing failed due to: ${!error}"
```

Or perhaps augment the message payload with the error message:

```yaml
  - catch:
    - json:
        operator: set
        path: meta.error
        value: ${!error}
```

## Attempt Until Success

It's possible to reattempt a processor for a particular message until it is
successful with a [`while`][while] processor:

```yaml
  - for_each:
    - while:
        at_least_once: true
        max_loops: 0 # Set this greater than zero to cap the number of attempts
        condition:
          type: processor_failed
        processors:
        - type: catch # Wipe any previous error
        - type: foo # Attempt this processor until success
```

This loop will block the pipeline and prevent the blocking message from being
acknowledged. It is therefore usually a good idea in practice to build your
condition with an exit strategy after N failed attempts so that the pipeline can
unblock itself without intervention.

## Drop Failed Messages

In order to filter out any failed messages from your pipeline you can simply use
a [`filter_parts`][filter_parts] processor:

```yaml
  - filter_parts:
      not:
        type: processor_failed
```

This will remove any failed messages from a batch.

## Route to a Dead-Letter Queue

It is possible to send failed messages to different destinations using either a
[`group_by`][group_by] processor with a [`switch`][switch] output, or a
[`broker`][broker] output with [`filter_parts`][filter_parts] processors.

```yaml
pipeline:
  processors:
  - group_by:
    - condition:
        type: processor_failed
output:
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

```yaml
output:
  broker:
    pattern: fan_out
    outputs:
    - type: foo # Dead letter queue
      processors:
      - filter_parts:
          type: processor_failed
    - type: bar # Everything else
      processors:
      - filter_parts:
          not:
            type: processor_failed
```

[processors]: /docs/components/processors/about
[processor_failed]: /docs/components/conditions/processor_failed
[filter_parts]: /docs/components/processors/filter_parts
[while]: /docs/components/processors/while
[for_each]: /docs/components/processors/for_each
[conditional]: /docs/components/processors/conditional
[catch]: /docs/components/processors/catch
[try]: /docs/components/processors/try
[log]: /docs/components/processors/log
[group_by]: /docs/components/processors/group_by
[switch]: /docs/components/outputs/switch
[broker]: /docs/components/outputs/broker
