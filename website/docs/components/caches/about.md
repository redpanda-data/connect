---
title: Caches
sidebar_label: About
---

A cache is a key/value store which can be used by certain processors for applications such as deduplication or data joins. Caches are listed with unique labels which are referred to by processors that may share them.

Caches are configured as resources:

```yaml
resources:
  caches:
    foobar:
      memcached:
        addresses:
          - localhost:11211
        ttl: 60
```

And any components that use caches have a field used to refer to a cache resource:

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

It's possible to layer caches with read-through and write-through behaviour
using the [`multilevel` cache][multilevel-cache].

import ComponentSelect from '@theme/ComponentSelect';

<ComponentSelect type="caches"></ComponentSelect>

[multilevel-cache]: /docs/components/caches/multilevel