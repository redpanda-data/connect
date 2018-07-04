Twitter Firehose
================

This example demonstrates how Benthos can be used to stream the Twitter firehose
into a Kafka topic. The output section could be changed to target any of the
[supported output types][outputs]. This example includes deduplication, which
means multiple instances can be run for redundancy without swamping the data
sink with duplicates. Deduplication is performed via a shared Memcached cluster.

[As of the time of writing this example][stream-docs] there are three streaming
APIs for Twitter: PowerTrack, Firehose and Replay. All three provide an HTTP
stream connection, where tweets are delivered as line-delimited (`\r\n`) JSON
blobs. Occasionally the stream will deliver blank lines in order to keep the
connection alive. The stream is never ending, and therefore if the connection
closes it should be reopened. The example provided could be used to consume any
of the stream types.

The full config for this [example can be found here][example].

## Input

The input of this example is fairly standard. We initiate an HTTP stream which
is automatically recovered if a disconnection occurs. The only processor
attached to the input is a `bounds_check` filter that removes any empty lines.

``` yaml
input:
  type: http_client
  http_client:
    url: https://gnip-stream.twitter.com/stream/firehose/accounts/foo/publishers/twitter/prod.json?partition=1
    verb: GET
    content_type: application/json
    basic_auth:
      enabled: true
      password: "" # TODO
      username: "" # TODO
    stream:
      enabled: true
      max_buffer: 10_000_000 # 10MB - The max supported length of a single line
  processors:
  - type: bounds_check # Filter out keep alives (empty message)
    bounds_check:
      min_part_size: 2
```

It's worth noting that you can add the `backfillMinutes` URL parameter if you
have the feature enabled. This means any connection recovery will always gain a
small window of automatic backfill.

## Buffer

``` yaml
buffer:
  type: memory
  memory:
    limit: 500_000_000
```

We add a memory based buffer in this config which will help us keep up with the
stream during sudden traffic spikes. It also allows us to parallelise the next
layer of deduplication processors.

## Pipeline

``` yaml
pipeline:
  threads: 16 # Determines the max number of concurrent calls to dedupe cache
  processors:
  - type: filter # Filter out non-json objects and error messages
    filter:
      type: jmespath
      jmespath:
        query: "keys(@) | length(@) > `0` && !contains(@, 'error')"
  - type: dedupe
    dedupe:
      cache: dedupe
      parts: [0]
      drop_on_err: false # Prefer occasional duplicates over lost messages
      json_paths:
      - "id_str" # Dedupe based on the 'id_str' field of tweets
      hash: none
```

The pipeline section contains two processors.

The first processor is a [JMESPath][jmespath] query which checks whether the
message object is an invalid JSON object or system error message from Twitter.
We chose to remove these messages since client disconnects are handled
automatically and it's possible to observe the reasons for a disconnection from
the API dashboard.

The second processor is a deduplication step which checks the `id_str` field of
the tweet against a shared Memcached cluster (the cache details are configured
later on in the resources section). This is likely to be the bottleneck of the
system (mostly idle on network IO), therefore the `threads` field should be
tweaked in order to tune the optimum number of concurrent Memcached requests.

## Output

The output section is a standard Kafka connection.

``` yaml
output:
  type: kafka
  kafka:
    addresses:
    - localhost:9092 # TODO
    client_id: benthos_firehose_bridge
    topic: twitter_firehose
    max_msg_bytes: 10_000_000 # 10MB - The max supported message size
```

This can be changed to any other output type without impacting the rest of the
pipeline.

## Resources

``` yaml
resources:
  caches:
    dedupe:
      type: memcached
      memcached:
        addresses:
        - localhost:11211 # TODO
        ttl: 604_800 # Keep Twitter IDs cached for a week
```

The resources section contains the configuration of our deduplication cache. We
are using Memcached which allows us share the dedupe cache across multiple
redundant Benthos instances. If you aren't using redundant instances or wish to
deduplicate elsewhere then you can simply remove this section as well as the
`dedupe` processor in the pipeline section, this should also improve throughput.

[stream-docs]: http://support.gnip.com/apis/consuming_streaming_data.html
[example]: ./twitter-firehose.yaml
[outputs]: ../outputs/README.md
[jmespath]: http://jmespath.org/
