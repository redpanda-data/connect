Caches
======

This document was generated with `benthos --list-caches`

A cache is a key/value store which can be used by certain processors for
applications such as deduplication. Caches are listed with unique labels which
are referred to by processors that may share them. For example, if we were to
deduplicate hypothetical 'foo' and 'bar' inputs, but not 'baz', we could arrange
our config as follows:

``` yaml
input:
  type: broker
  broker:
    inputs:
    - type: foo
      processors:
      - type: dedupe
        dedupe:
          cache: foobar
          hash: none
          parts: [0]
    - type: bar
      processors:
      - type: dedupe
        dedupe:
          cache: foobar
          hash: none
          parts: [0]
    - type: baz
resources:
  caches:
    foobar:
      type: memcached
      memcached:
        addresses:
        - localhost:11211
        ttl: 60
```

In that example we have a single memcached based cache 'foobar', which is used
by the dedupe processors of both the 'foo' and 'bar' inputs. A message received
from both 'foo' and 'bar' would therefore be detected and removed since the
cache is the same for both inputs.

### Contents

1. [`memcached`](#memcached)
2. [`memory`](#memory)

## `memcached`

Connects to a cluster of memcached services, a prefix can be specified to allow
multiple cache types to share a memcached cluster under different namespaces.

## `memory`

The memory cache simply stores key/value pairs in a map held in memory. This
cache is therefore reset every time the service restarts. Each item in the cache
has a TTL set from the moment it was last edited, after which it will be removed
during the next compaction.

A compaction only occurs during a write where the time since the last compaction
is above the compaction interval. It is therefore possible to obtain values of
keys that have expired between compactions.
