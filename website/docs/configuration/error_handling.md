---
title: Error Handling
---

It's always possible for things to go wrong, be a good captain and plan ahead.

<div style={{textAlign: 'center'}}><img style={{maxWidth: '300px', marginBottom: '40px'}} src="/img/Blobpirate.svg" /></div>

Benthos supports a range of [processors][processors] such as `http` and `aws_lambda` that have the potential to fail if their retry attempts are exhausted. When this happens the data is not dropped but instead continues through the pipeline mostly unchanged, but a metadata flag is added allowing you to handle the errors in a way that suits your needs.

This document outlines common patterns for dealing with errors, such as dropping them, recovering them with more processing, routing them to a dead-letter queue, or any combination thereof.

## Abandon on Failure

It's possible to define a list of processors which should be skipped for messages that failed a previous stage using the [`try` processor][processor.try]:

```yaml
pipeline:
  processors:
    - try:
      - resource: foo
      - resource: bar # Skipped if foo failed
      - resource: baz # Skipped if foo or bar failed
```

## Recover Failed Messages

Failed messages can be fed into their own processor steps with a [`catch` processor][processor.catch]:

```yaml
pipeline:
  processors:
    - resource: foo # Processor that might fail
    - catch:
      - resource: bar # Recover here
```

Once messages finish the catch block they will have their failure flags removed and are treated like regular messages. If this behaviour is not desired then it is possible to simulate a catch block with a [`switch` processor][processor.switch]:

```yaml
pipeline:
  processors:
    - resource: foo # Processor that might fail
    - switch:
      - check: errored()
        processors:
          - resource: bar # Recover here
```

## Logging Errors

When an error occurs there will occasionally be useful information stored within the error flag that can be exposed with the interpolation function [`error`][configuration.interpolation]. This allows you to expose the information with processors.

For example, when catching failed processors you can [`log`][processor.log] the messages:

```yaml
pipeline:
  processors:
    - resource: foo # Processor that might fail
    - catch:
      - log:
          message: "Processing failed due to: ${!error()}"
```

Or perhaps augment the message payload with the error message:

```yaml
pipeline:
  processors:
    - resource: foo # Processor that might fail
    - catch:
      - mapping: |
          root = this
          root.meta.error = error()
```

## Attempt Until Success

It's possible to reattempt a processor for a particular message until it is successful with a [`retry`][processor.retry] processor:

```yaml
pipeline:
  processors:
    - retry:
        backoff:
          initial_interval: 1s
          max_interval: 5s
          max_elapsed_time: 30s
        processors:
          # Attempt this processor until success, or the maximum elapsed time is reached.
          - resource: foo
```

## Drop Failed Messages

In order to filter out any failed messages from your pipeline you can use a [`mapping` processor][processor.mapping]:

```yaml
pipeline:
  processors:
    - mapping: root = if errored() { deleted() }
```

This will remove any failed messages from a batch. Furthermore, dropping a message will propagate an acknowledgement (also known as "ack") upstream to the pipeline's input.

## Reject Messages

Some inputs such as NATS, GCP Pub/Sub and AMQP support nacking (rejecting) messages. We can perform a nack (or rejection) on data that has failed to process rather than delivering it to our output with a [`reject_errored` output][output.reject_errored]:

```yaml
output:
  reject_errored:
    resource: foo # Only non-errored messages go here
```

## Route to a Dead-Letter Queue

And by placing the above within a [`fallback` output][output.fallback] we can instead route the failed messages to a different output:

```yaml
output:
  fallback:
    - reject_errored:
        resource: foo # Only non-errored messages go here

    - resource: bar # Only errored messages, or those that failed to be delivered to foo, go here
```

And, finally, in cases where we wish to route data differently depending on the error message itself we can use a [`switch` output][output.switch]:

```yaml
output:
  switch:
    cases:
      # Capture specifically cat related errors
      - check: errored() && error().contains("meow")
        output:
          resource: foo

      # Capture all other errors
      - check: errored()
        output:
          resource: bar

      # Finally, route messages that haven't errored
      - output:
          resource: baz
```

[processors]: /docs/components/processors/about
[processor.mapping]: /docs/components/processors/mapping
[processor.switch]: /docs/components/processors/switch
[processor.retry]: /docs/components/processors/retry
[processor.for_each]: /docs/components/processors/for_each
[processor.catch]: /docs/components/processors/catch
[processor.try]: /docs/components/processors/try
[processor.log]: /docs/components/processors/log
[output.switch]: /docs/components/outputs/switch
[output.fallback]: /docs/components/outputs/fallback
[output.reject_errored]: /docs/components/outputs/reject_errored
[configuration.interpolation]: /docs/configuration/interpolation#bloblang-queries
