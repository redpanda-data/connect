Duplicate Monitoring
====================

Benthos has a [deduplication processor][dedupe-proc] which makes removing
duplicates from a stream easy. However, sometimes we might only be interested in
detecting duplicates without mutating the stream. This cookbook demonstrates how
to passively count duplicates and expose them via metrics aggregators.

We can do this by creating a stream passthrough that increments a metric counter
each time a duplicate is found in a stream using the [`cache`][cache-proc] and
[`metric`][metric-proc] processors.

This method is extremely flexible and therefore it's possible to detect
duplicates from a combination of any number of uniquely identifying fields
within the documents, or from a hash of the entire document contents. We'll
cover both.

### By ID Field

Here our chosen metrics aggregator is Prometheus, but this works with any
available Benthos [metrics target][metrics]. We're also going to assume that the
messages are JSON documents, and that we're detecting duplicates with a
combination of string fields at the paths `document.id` and `document.user.id`.

Here's the config (omitting our input and output sections for brevity):

``` yaml
pipeline:
  processors:
  - type: cache
    cache:
      cache: dupes
      key: "${!json_field:document.id}_${!json_field:document.user.id}"
      operator: add
      value: "x"
  - type: catch
    catch:
    - type: metric
      metric:
        type: counter
        path: duplicate_id

resources:
  caches:
    dupes:
      type: memory
      memory:
        ttl: 300

metrics:
  type: prometheus
```

For each message in a stream this pipeline begins by attempting to `add` a new
item to a cache, where the key is an
[interpolated][interp-funcs] combination of our identifying fields.

The cache action fails if the key already exists in the cache, and therefore
only messages that are duplicates will be caught within the following
[`catch`][catch-proc] block. Within the catch block we then increment a counter
which tracks the number of duplicates found.

The cache processor requires a target cache, which in this case we've labelled
`dupes`, and the configuration for that can be found within the following
`resources.caches` section. We've chosen a memory based cache here with a TTL of
5 minutes for simplicity, but there [are many options][caches] which would allow
us to share the cache across multiple instances of this pipeline.

#### But there's more!

Remember that within the metric processor it's also possible to label these
counts with extra information. For example, we could label our counters with the
source Kafka topic and partition:

``` yaml
      - type: metric
        metric:
          type: counter
          path: duplicate_id
          labels:
            topic: ${!metadata:kafka_topic}
            partition: ${!metadata:kafka_partition}
```

This would let us expose duplicate levels per topic per partition in our
dashboards!

### By Document Hash

In order to detect duplicates by a hash of the entire document we can modify the
previous configuration by adding a [`hash` processor][hash-proc] to convert
documents into their hashes. We then dedupe by the new full contents of the
message (which is now a hash).

However, this would mutate the contents of the stream, which we need to avoid.
Therefore, we wrap this action within a [`process_map` processor][procmap-proc]
and set a `postmap_optional` target to a path that will never be found, which
prevents the hash result from being added into the original message contents.

We then `catch` any cache errors like in the previous example. The config
(omitting sections shared with the previous example for brevity) would look like
this:

``` yaml
pipeline:
  processors:
  - type: process_map
    process_map:
      processors:
      - type: hash
        hash:
          algorithm: xxhash64
      - type: cache
        cache:
          cache: dupes
          key: "${!content}"
          operator: add
          value: "x"
      postmap_optional:
        will: never.exist
  - type: catch
    catch:
    - type: metric
      metric:
        type: counter
        path: duplicate_hash
```

[dedupe-proc]: ../processors/README.md#dedupe
[procmap-proc]: ../processors/README.md#process_map
[hash-proc]: ../processors/README.md#hash
[cache-proc]: ../processors/README.md#cache
[metric-proc]: ../processors/README.md#metric
[catch-proc]: ../processors/README.md#catch
[metrics]: ../metrics/README.md
[interp-funcs]: ../config_interpolation.md#functions
[caches]: ../caches/README.md