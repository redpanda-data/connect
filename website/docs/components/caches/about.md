---
title: Caches
sidebar_label: About
---

A cache is a key/value store which can be used by certain processors for applications such as deduplication. Caches are listed with unique labels which are referred to by processors that may share them.

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
    - dedupe:
        cache: foobar
        hash: xxhash
```