---
title: Components
sidebar_label: About
description: Learn about Benthos components
---

A good ninja gets clued up on its gear.

<div style={{textAlign: 'center'}}><img style={{maxWidth: '300px'}} src="/img/Blobninja.svg" /></div>

## Core Components

Every Benthos pipeline has at least one [input][inputs], an optional
[buffer][buffers], an [output][outputs] and any number of
[processors][processors]:

```yaml
input:
  kafka_balanced:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

buffer:
  type: none

pipeline:
  processors:
  - jmespath:
      query: '{ message: @, meta: { link_count: length(links) } }'

output:
  s3:
    bucket: TODO
    path: '${! meta("kafka_topic") }/${! json("message.id") }.json'
```

These are the main components within Benthos and they provide the majority of
useful behaviour.

## Observability Components

There are also the observability components [logger][logger], [metrics][metrics],
and [tracing][tracers], which allow you to specify how Benthos exposes
observability data:

```yaml
logger:
  prefix: benthos
  level: WARN
  json_format: true

metrics:
  statsd:
    prefix: foo
    address: localhost:8125
    flush_period: 100ms

tracer:
  jaeger:
    agent_address: localhost:6831
    service_name: benthos
```

## Resource Components

Finally, there are [conditions][conditions], [caches][caches] and
[rate limits][rate_limits]. These are components that are useful when used by
core components, and they are either configured as a field within that component:

```yaml
pipeline:
  processors:
    - filter_parts: # This is a processor
        text: # This is a child condition
          operator: contains
          arg: foo
```

Or as a resource where they are referenced by one or more core components:

```yaml
input:
  http_client: # This is an input
    url: TODO
    rate_limit: foo_ratelimit # This is a reference to a rate limit

pipeline:
  processors:
    - filter_parts: # This is a processor
        resource: bar_condition # This is a reference to a condition
    - dedupe: # This is another processor
        cache: baz_cache # This is a reference to a cache
        hash: xxhash
        key: ${! json("id") }

resources:
  rate_limits:
    foo_ratelimit:
      local:
        count: 500
        interval: 1s
  conditions:
    bar_condition:
      text:
        operator: contains
        arg: foo
  caches:
    baz_cache:
      memcached:
        addresses: [ localhost:11211 ]
        ttl: 60
```

For more information about any of these components check out their sections:

- [inputs][inputs]
- [processors][processors]
- [outputs][outputs]
- [buffers][buffers]
- [metrics][metrics]
- [tracers][tracers]
- [logger][logger]
- [conditions][conditions]
- [caches][caches]
- [rate limits][rate_limits]

[inputs]: /docs/components/inputs/about
[processors]: /docs/components/processors/about
[outputs]: /docs/components/inputs/about
[buffers]: /docs/components/buffers/about
[metrics]: /docs/components/metrics/about
[tracers]: /docs/components/tracers/about
[logger]: /docs/components/logger/about
[conditions]: /docs/components/conditions/about
[caches]: /docs/components/caches/about
[rate_limits]: /docs/components/rate_limits/about