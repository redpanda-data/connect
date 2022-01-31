---
title: Caches
sidebar_label: About
---

A cache is a key/value store which can be used by certain components for applications such as deduplication or data joins. Caches are configured as a named resource:

```yaml
cache_resources:
  - label: foobar
    memcached:
      addresses:
        - localhost:11211
      default_ttl: 60s
```

> It's possible to layer caches with read-through and write-through behaviour using the [`multilevel` cache][cache.multilevel].

And then any components that use caches have a field `resource` that specifies the cache resource:

```yaml
pipeline:
  processors:
    - cache:
        resource: foobar
        operator: add
        key: '${! json("message.id") }'
        value: "storeme"
    - bloblang: root = if errored() { deleted() }
```

For the simple case where you wish to store messages in a cache as an output destination for your pipeline check out the [`cache` output][output.cache]. To see examples of more advanced uses of caches such as hydration and deduplication check out the [`cache` processor][processor.cache]. 

You can find out more about resources [in this document.][config.resources]

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="caches"></ComponentSelect>

[cache.multilevel]: /docs/components/caches/multilevel
[processor.cache]: /docs/components/processors/cache
[output.cache]: /docs/components/outputs/cache
[config.resources]: /docs/configuration/resources