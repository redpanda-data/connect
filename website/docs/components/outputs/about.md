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
    path: '${! meta("kafka_topic") }/${! json("message.id") }.json'

  # Optional list of processing steps
  processors:
   - jmespath:
       query: '{ message: @, meta: { link_count: length(links) } }'
```

import ComponentsByCategory from '@theme/ComponentsByCategory';

## Categories

<ComponentsByCategory type="outputs"></ComponentsByCategory>

## Back Pressure

Benthos outputs apply back pressure to components upstream. This means if your output target starts blocking traffic Benthos will gracefully stop consuming until the issue is resolved.

## Retries

When a Benthos output fails to send a message the error is propagated back up to the input, where depending on the protocol it will either be pushed back to the source as a Noack (e.g. AMQP) or will be reattempted indefinitely with the commit withheld until success (e.g. Kafka).

It's possible to instead have Benthos indefinitely retry an output until success with a [`retry`][output.retry] output. Some other outputs, such as the [`broker`][output.broker], might also retry indefinitely depending on their configuration.

## Dead Letter Queues

It's possible to create fallback outputs for when an output target fails using a [`try`][output.try] output:

```yaml
output:
  try:
  - sqs:
      url: https://sqs.us-west-2.amazonaws.com/TODO/TODO
      max_in_flight: 20

  - http_client:
      url: http://backup:1234/dlq
      verb: POST
```

## Multiplexing Outputs

There are a few different ways of multiplexing in Benthos, here's a quick run through:

### Interpolation Multiplexing

The easiest form of multiplexing is by using [field interpolation][interpolation]:

```yaml
output:
  kafka:
    addresses: [ TODO:6379 ]
    topic: ${! meta("target_topic") }
```

Although this form of multiplexing is limited as it doesn't support different output types, and only some output fields support interpolation.

### Switch Multiplexing

It is possible to perform content based multiplexing of messages to specific outputs by using the [`switch`][output.switch] output:

```yaml
output:
  switch:
    outputs:
    - condition:
        bloblang: urls.contains("http://benthos.dev")
      output:
        cache:
          target: foo
          key: ${! json("id") }
      fallthrough: false
    - output:
        s3:
          bucket: bar
          path: ${! json("id") }
```

For each output case you are able to specify a [condition][conditions] to determine whether a message should be routed to it.

### Broker Multiplexing

An alternative way to multiplex is to use a [`broker`][output.broker] with the `fan_out` pattern and a [`bloblang` processor][processor.bloblang] on each output that selectively drops messages:

```yaml
output:
  broker:
    pattern: fan_out
    outputs:
    - cache:
        target: foo
        key: ${! json("id") }
      processors:
      - bloblang: |
          root = match {
            !urls.contains("http://benthos.dev") => deleted()
          }
    - s3:
        bucket: bar
        path: ${! json("id") }
      processors:
      - bloblang: |
          root = match {
            urls.contains("http://benthos.dev") => deleted()
          }
```

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="outputs"></ComponentSelect>

[processors]: /docs/components/processors/about
[processor.bloblang]: /docs/components/processors/bloblang
[conditions]: /docs/components/conditions/about
[output.broker]: /docs/components/outputs/broker
[output.switch]: /docs/components/outputs/switch
[output.retry]: /docs/components/outputs/retry
[output.try]: /docs/components/outputs/try
[interpolation]: /docs/configuration/interpolation