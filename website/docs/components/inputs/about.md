---
title: Inputs
sidebar_label: About
---

An input is a source of data piped through an array of optional [processors][processors]. Only one input is configured at the root of a Benthos config. However, the root input can be a [broker][input.broker] which combines multiple inputs.

```yaml
input:
  redis_streams:
    url: tcp://localhost:6379
    streams:
      - benthos_stream
    body_key: body
    consumer_group: benthos_group

  # Optional list of processing steps
  processors:
   - jmespath:
       query: '{ message: @, meta: { link_count: length(links) } }'
```

Sometimes it's useful to generate data, in which case the most convenient option is the [`bloblang` input][input.bloblang].

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="inputs"></ComponentSelect>

[processors]: /docs/components/processors/about
[input.broker]: /docs/components/inputs/broker
[input.bloblang]: /docs/components/inputs/bloblang