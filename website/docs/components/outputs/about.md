---
title: Outputs
sidebar_label: About
---

An output is a sink where we wish to send our consumed data after applying an optional array of [processors][processors]. Only one output is configured at the root of a Benthos config. However, the output can be a [broker][output.broker] which combines multiple outputs under a chosen brokering pattern, or a [switch][output.switch] which is used to multiplex against different outputs.

An output config section looks like this:

```yaml
output:
  s3:
    bucket: TODO
    path: '${! meta("kafka_topic") }/${! json("message.id") }.json'

  # Optional list of processing steps
  processors:
   - bloblang: '{"message":this,"meta":{"link_count":this.links.length()}}'
```

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

Some output fields support [field interpolation][interpolation], which is a super easy way to multiplex messages based on their contents in situations where you are multiplexing to the same service.

For example, multiplexing against Kafka topics is a common pattern:

```yaml
output:
  kafka:
    addresses: [ TODO:6379 ]
    topic: ${! meta("target_topic") }
```

Refer to the field documentation for a given output to see if it support interpolation.

### Switch Multiplexing

A more advanced form of multiplexing is to route messages to different output configurations based on a query. This is easy with the [`switch` output][output.switch]:

```yaml
output:
  switch:
    cases:
      - check: this.type == "foo"
        output:
          amqp_1:
            url: amqps://guest:guest@localhost:5672/
            target_address: queue:/the_foos

      - check: this.type == "bar"
        output:
          gcp_pubsub:
            project: dealing_with_mike
            topic: mikes_bars

      - output:
          redis_streams:
            url: tcp://localhost:6379
            stream: everything_else
          processors:
            - bloblang: |
                root = this
                root.type = this.type.not_null() | "unknown"
```

import ComponentsByCategory from '@theme/ComponentsByCategory';

## Categories

<ComponentsByCategory type="outputs"></ComponentsByCategory>

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="outputs"></ComponentSelect>

[processors]: /docs/components/processors/about
[processor.bloblang]: /docs/components/processors/bloblang
[output.broker]: /docs/components/outputs/broker
[output.switch]: /docs/components/outputs/switch
[output.retry]: /docs/components/outputs/retry
[output.try]: /docs/components/outputs/try
[interpolation]: /docs/configuration/interpolation