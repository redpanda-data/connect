---
title: Inputs
sidebar_label: About
---

An input is a source of data piped through an array of optional [processors][processors]:

```yaml
input:
  label: my_redis_input

  redis_streams:
    url: tcp://localhost:6379
    streams:
      - benthos_stream
    body_key: body
    consumer_group: benthos_group

  # Optional list of processing steps
  processors:
   - bloblang: |
       root.document = this.without("links")
       root.link_count = this.links.length()
```

Some inputs have a logical end, for example a [`csv` input][input.csv] ends once the last row is consumed, when this happens the input gracefully terminates and Benthos will shut itself down once all messages have been processed fully.

It's also possible to specify a logical end for an input that otherwise doesn't have one with the [`read_until` input][input.read_until], which checks a condition against each consumed message in order to determine whether it should be the last.

## Brokering

Only one input is configured at the root of a Benthos config. However, the root input can be a [broker][input.broker] which combines multiple inputs and merges the streams:

```yaml
input:
  broker:
    inputs:
      - kafka:
          addresses: [ TODO ]
          topics: [ foo, bar ]
          consumer_group: foogroup

      - redis_streams:
          url: tcp://localhost:6379
          streams:
            - benthos_stream
          body_key: body
          consumer_group: benthos_group
```

## Labels

Inputs have an optional field `label` that can uniquely identify them in observability data such as metrics and logs. This can be useful when running configs with multiple inputs, otherwise their metrics labels will be generated based on their composition. For more information check out the [metrics documentation][metrics.about].

### Sequential Reads

Sometimes it's useful to consume a sequence of inputs, where an input is only consumed once its predecessor is drained fully, you can achieve this with the [`sequence` input][input.sequence].

## Generating Messages

It's possible to generate data with Benthos using the [`generate` input][input.generate], which is also a convenient way to trigger scheduled pipelines.

import ComponentsByCategory from '@theme/ComponentsByCategory';

## Categories

<ComponentsByCategory type="inputs"></ComponentsByCategory>

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="inputs"></ComponentSelect>

[processors]: /docs/components/processors/about
[input.broker]: /docs/components/inputs/broker
[input.generate]: /docs/components/inputs/generate
[input.csv]: /docs/components/inputs/csv
[input.sequence]: /docs/components/inputs/sequence
[input.read_until]: /docs/components/inputs/read_until
[metrics.about]: /docs/components/metrics/about