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
          hash: xxhash
    - type: bar
      processors:
      - type: dedupe
        dedupe:
          cache: foobar
          hash: xxhash
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

1. [`dynamodb`](#dynamodb)
2. [`file`](#file)
3. [`memcached`](#memcached)
4. [`memory`](#memory)
5. [`redis`](#redis)
6. [`s3`](#s3)

## `dynamodb`

``` yaml
type: dynamodb
dynamodb:
  backoff:
    initial_interval: 1s
    max_elapsed_time: 30s
    max_interval: 5s
  consistent_read: false
  credentials:
    id: ""
    profile: ""
    role: ""
    role_external_id: ""
    secret: ""
    token: ""
  data_key: ""
  endpoint: ""
  hash_key: ""
  max_retries: 3
  region: eu-west-1
  table: ""
  ttl: ""
  ttl_key: ""
```

The dynamodb cache stores key/value pairs as a single document in a DynamoDB
table. The key is stored as a string value and used as the table hash key. The
value is stored as a binary value using the `data_key` field name.

A prefix can be specified to allow multiple cache types to share a single
DynamoDB table. An optional TTL duration (`ttl`) and field
(`ttl_key`) can be specified if the backing table has TTL enabled.

Strong read consistency can be enabled using the `consistent_read`
configuration field.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).

## `file`

``` yaml
type: file
file:
  directory: ""
```

The file cache stores each item in a directory as a file, where an item ID is
the path relative to the configured directory.

This type currently offers no form of item expiry or garbage collection, and is
intended to be used for development and debugging purposes only.

## `memcached`

``` yaml
type: memcached
memcached:
  addresses:
  - localhost:11211
  prefix: ""
  retries: 3
  retry_period: 500ms
  ttl: 300
```

Connects to a cluster of memcached services, a prefix can be specified to allow
multiple cache types to share a memcached cluster under different namespaces.

## `memory`

``` yaml
type: memory
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

## `redis`

``` yaml
type: redis
redis:
  expiration: 24h
  prefix: ""
  retries: 3
  retry_period: 500ms
  url: tcp://localhost:6379
```

Use a Redis instance as a cache. The expiration can be set to zero or an empty
string in order to set no expiration.

## `s3`

``` yaml
type: s3
s3:
  bucket: ""
  content_type: application/octet-stream
  credentials:
    id: ""
    profile: ""
    role: ""
    role_external_id: ""
    secret: ""
    token: ""
  endpoint: ""
  force_path_style_urls: false
  region: eu-west-1
  retries: 3
  timeout: 5s
```

The s3 cache stores each item in an S3 bucket as a file, where an item ID is
the path of the item within the bucket.

It is not possible to atomically upload S3 objects exclusively when the target
does not already exist, therefore this cache is not suitable for deduplication.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).

