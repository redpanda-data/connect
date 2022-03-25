---
title: Components
sidebar_label: About
description: Learn about Benthos components
---

A good ninja gets clued up on its gear.

<div style={{textAlign: 'center'}}><img style={{maxWidth: '300px'}} src="/img/Blobninja.svg" /></div>

## Core Components

Every Benthos pipeline has at least one [input][inputs], an optional [buffer][buffers], an [output][outputs] and any number of [processors][processors]:

```yaml
input:
  kafka:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

buffer:
  type: none

pipeline:
  processors:
  - bloblang: |
      message = this
      meta.link_count = links.length()

output:
  aws_s3:
    bucket: TODO
    path: '${! meta("kafka_topic") }/${! json("message.id") }.json'
```

These are the main components within Benthos and they provide the majority of useful behaviour.

## Observability Components

There are also the observability components [http][http], [logger][logger], [metrics][metrics], and [tracing][tracers], which allow you to specify how Benthos exposes observability data:

```yaml
http:
  address: 0.0.0.0:4195
  enabled: true
  debug_endpoints: false

logger:
  format: json
  level: WARN

metrics:
  statsd:
    address: localhost:8125
    flush_period: 100ms

tracer:
  jaeger:
    agent_address: localhost:6831
```

## Resource Components

Finally, there are [caches][caches] and [rate limits][rate_limits]. These are components that are referenced by core components and can be shared.

```yaml
input:
  http_client: # This is an input
    url: TODO
    rate_limit: foo_ratelimit # This is a reference to a rate limit

pipeline:
  processors:
    - cache: # This is a processor
        resource: baz_cache # This is a reference to a cache
        operator: add
        key: '${! json("id") }'
        value: "x"
    - bloblang: root = if errored() { deleted() }

rate_limit_resources:
  - label: foo_ratelimit
    local:
      count: 500
      interval: 1s

cache_resources:
  - label: baz_cache
    memcached:
      addresses: [ localhost:11211 ]
```

It's also possible to configure inputs, outputs and processors as resources which allows them to be reused throughout a configuration with the [`resource` input][inputs.resource], [`resource` output][outputs.resource] and [`resource` processor][processors.resource] respectively.

For more information about any of these component types check out their sections:

- [inputs][inputs]
- [processors][processors]
- [outputs][outputs]
- [buffers][buffers]
- [metrics][metrics]
- [tracers][tracers]
- [logger][logger]
- [caches][caches]
- [rate limits][rate_limits]

[inputs]: /docs/components/inputs/about
[inputs.resource]: /docs/components/inputs/resource
[processors]: /docs/components/processors/about
[processors.resource]: /docs/components/processors/resource
[outputs]: /docs/components/outputs/about
[outputs.resource]: /docs/components/outputs/resource
[buffers]: /docs/components/buffers/about
[metrics]: /docs/components/metrics/about
[tracers]: /docs/components/tracers/about
[logger]: /docs/components/logger/about
[http]: /docs/components/http/about
[caches]: /docs/components/caches/about
[rate_limits]: /docs/components/rate_limits/about