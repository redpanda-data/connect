---
title: memory
type: cache
---

```yaml
memory:
  compaction_interval: 60s
  init_values: {}
  ttl: 300
```

The memory cache simply stores key/value pairs in a map held in memory. This
cache is therefore reset every time the service restarts. Each item in the cache
has a TTL set from the moment it was last edited, after which it will be removed
during the next compaction.

A compaction only occurs during a write where the time since the last compaction
is above the compaction interval. It is therefore possible to obtain values of
keys that have expired between compactions.

The field `init_values` can be used to prepopulate the memory cache
with any number of key/value pairs which are exempt from TTLs:

```yaml
type: memory
memory:
  ttl: 60
  init_values:
    foo: bar
```

These values can be overridden during execution, at which point the configured
TTL is respected as usual.


