---
title: redis
type: processor
---

```yaml
redis:
  key: ""
  operator: scard
  parts: []
  retries: 3
  retry_period: 500ms
  url: tcp://localhost:6379
```

Performs actions against Redis that aren't possible using a
[`cache`](cache) processor. Actions are performed for each message of
a batch, where the contents are replaced with the result.

The field `key` supports
[interpolation functions](/docs/configuration/interpolation#functions) resolved
individually for each message of the batch.

For example, given payloads with a metadata field `set_key`, you could
add a JSON field to your payload with the cardinality of their target sets with:

```yaml
- process_field:
    path: meta.cardinality
    result_type: int
    processors:
      - redis:
          url: TODO
          operator: scard
          key: ${!metadata:set_key}
 ```


### Operators

#### `scard`

Returns the cardinality of a set, or 0 if the key does not exist.

#### `sadd`

Adds a new member to a set. Returns `1` if the member was added.


