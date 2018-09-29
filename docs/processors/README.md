Processors
==========

This document was generated with `benthos --list-processors`.

Benthos processors are functions that will be applied to each message passing
through a pipeline. The function signature allows a processor to mutate or drop
messages depending on the content of the message.

Processors are set via config, and depending on where in the config they are
placed they will be run either immediately after a specific input (set in the
input section), on all messages (set in the pipeline section) or before a
specific output (set in the output section).

By organising processors you can configure complex behaviours in your pipeline.
You can [find some examples here][0].

### Batching and Multiple Part Messages

All Benthos processors support multiple part messages, which are synonymous with
batches. Some processors such as [batch](#batch) and [split](#split) are able to
create, expand and break down batches.

Many processors are able to perform their behaviours on specific parts of a
message batch, or on all parts, and have a field `parts` for
specifying an array of part indexes they should apply to. If the list of target
parts is empty these processors will be applied to all message parts.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the last
element will be selected, and so on.

Some processors such as `filter` and `dedupe` act across an entire
batch, when instead we'd like to perform them on individual messages of a batch.
In this case the [`process_batch`](#process_batch) processor can be
used.

### Contents

1. [`archive`](#archive)
2. [`batch`](#batch)
3. [`bounds_check`](#bounds_check)
4. [`combine`](#combine)
5. [`compress`](#compress)
6. [`conditional`](#conditional)
7. [`decode`](#decode)
8. [`decompress`](#decompress)
9. [`dedupe`](#dedupe)
10. [`encode`](#encode)
11. [`filter`](#filter)
12. [`filter_parts`](#filter_parts)
13. [`grok`](#grok)
14. [`hash`](#hash)
15. [`hash_sample`](#hash_sample)
16. [`http`](#http)
17. [`insert_part`](#insert_part)
18. [`jmespath`](#jmespath)
19. [`json`](#json)
20. [`lambda`](#lambda)
21. [`log`](#log)
22. [`merge_json`](#merge_json)
23. [`metadata`](#metadata)
24. [`metric`](#metric)
25. [`noop`](#noop)
26. [`process_batch`](#process_batch)
27. [`process_dag`](#process_dag)
28. [`process_field`](#process_field)
29. [`process_map`](#process_map)
30. [`sample`](#sample)
31. [`select_parts`](#select_parts)
32. [`split`](#split)
33. [`text`](#text)
34. [`throttle`](#throttle)
35. [`unarchive`](#unarchive)

## `archive`

``` yaml
type: archive
archive:
  format: binary
  path: ${!count:files}-${!timestamp_unix_nano}.txt
```

Archives all the parts of a message into a single part according to the selected
archive type. Supported archive types are: tar, zip, binary, lines.

Some archive types (such as tar, zip) treat each archive item (message part) as a
file with a path. Since message parts only contain raw data a unique path must
be generated for each part. This can be done by using function interpolations on
the 'path' field as described [here](../config_interpolation.md#functions). For
types that aren't file based (such as binary) the file field is ignored.

The resulting archived message adopts the metadata of the _first_ message part
of the batch.

## `batch`

``` yaml
type: batch
batch:
  byte_size: 0
  condition:
    type: static
    static: false
  count: 0
  period_ms: 0
```

Reads a number of discrete messages, buffering (but not acknowledging) the
message parts until either:

- The `byte_size` field is non-zero and the total size of the batch in
  bytes matches or exceeds it.
- The `count field is non-zero and the total number of messages in the
  batch matches or exceeds it.
- A message added to the batch causes the condition to resolve `true`.
- The `period_ms` field is non-zero and the time since the last batch
  exceeds its value.

Once one of these events trigger the parts are combined into a single batch of
messages and sent through the pipeline. After reaching a destination the
acknowledgment is sent out for all messages inside the batch at the same time,
preserving at-least-once delivery guarantees.

The `period_ms` field - when greater than zero - defines a period in
milliseconds whereby a batch is sent even if the `byte_size` has not
yet been reached. Batch parameters are only triggered when a message is added,
meaning a pending batch can last beyond this period if no messages are added
since the period was reached.

When a batch is sent to an output the behaviour will differ depending on the
protocol. If the output type supports multipart messages then the batch is sent
as a single message with multiple parts. If the output only supports single part
messages then the parts will be sent as a batch of single part messages. If the
output supports neither multipart or batches of messages then Benthos falls back
to sending them individually.

If a Benthos stream contains multiple brokered inputs or outputs then the batch
operator should *always* be applied directly after an input in order to avoid
unexpected behaviour and message ordering.

## `bounds_check`

``` yaml
type: bounds_check
bounds_check:
  max_part_size: 1.073741824e+09
  max_parts: 100
  min_part_size: 1
  min_parts: 1
```

Checks whether each message fits within certain boundaries, and drops messages
that do not. A metric is incremented for each dropped message and debug logs
are also provided if enabled.

## `combine`

``` yaml
type: combine
combine:
  parts: 2
```

DEPRECATED: Use the [`batch`](#batch) processor with the
`count` field instead.

## `compress`

``` yaml
type: compress
compress:
  algorithm: gzip
  level: -1
  parts: []
```

Compresses parts of a message according to the selected algorithm. Supported
compression types are: gzip, zlib, flate.

The 'level' field might not apply to all algorithms.

## `conditional`

``` yaml
type: conditional
conditional:
  condition:
    type: text
    text:
      arg: ""
      operator: equals_cs
      part: 0
  else_processors: []
  processors: []
```

Conditional is a processor that has a list of child `processors`,
`else_processors`, and a condition. For each message, if the condition
passes, the child `processors` will be applied, otherwise the
`else_processors` are applied. This processor is useful for applying
processors based on the content type of the message.

You can find a [full list of conditions here](../conditions).

## `decode`

``` yaml
type: decode
decode:
  parts: []
  scheme: base64
```

Decodes parts of a message according to the selected scheme. Supported available
schemes are: base64.

## `decompress`

``` yaml
type: decompress
decompress:
  algorithm: gzip
  parts: []
```

Decompresses message parts according to the selected algorithm. Supported
decompression types are: gzip, zlib, bzip2, flate.

Parts that fail to decompress (invalid format) will be removed from the message.
If the message results in zero parts it is skipped entirely.

## `dedupe`

``` yaml
type: dedupe
dedupe:
  cache: ""
  drop_on_err: true
  hash: none
  key: ""
  parts:
  - 0
```

Dedupes messages by caching selected (and optionally hashed) message parts,
dropping messages that are already cached. The hash type can be chosen from:
none or xxhash (more will come soon).

Optionally, the `key` field can be populated in order to hash on a
function interpolated string rather than the full contents of message parts.
This allows you to deduplicate based on dynamic fields within a message, such as
its metadata, JSON fields, etc. A full list of interpolation functions can be
found [here](../config_interpolation.md#functions).

For example, the following config would deduplicate based on the concatenated
values of the metadata field `kafka_key` and the value of the JSON
path `id` within the message contents:

``` yaml
dedupe:
  cache: foocache
  key: ${!metadata:kafka_key}-${!json_field:id}
```

Caches should be configured as a resource, for more information check out the
[documentation here](../caches).

### Delivery Guarantees

Performing a deduplication step on a payload in transit voids any at-least-once
guarantees that the payload previously had, as it's impossible to fully
guarantee that the message is propagated to the next destination. If the message
is reprocessed due to output failure or a service restart then it will be lost
due to failing the deduplication step on the second attempt.

You can avoid reprocessing payloads on failed sends by using either the
[`retry`](../outputs/README.md#retry) output type or the
[`broker`](../outputs/README.md#broker) output type using the 'try'
pattern. However, if the service is restarted between retry attempts then the
message can still be lost.

It is worth strongly considering the delivery guarantees that your pipeline is
meant to provide when using this processor.

## `encode`

``` yaml
type: encode
encode:
  parts: []
  scheme: base64
```

Encodes parts of a message according to the selected scheme. Supported schemes
are: base64.

## `filter`

``` yaml
type: filter
filter:
  type: text
  text:
    arg: ""
    operator: equals_cs
    part: 0
```

Tests each message against a condition, if the condition fails then the message
is dropped. You can find a [full list of conditions here](../conditions).

NOTE: If you are combining messages into batches using the
[`batch`](#batch) processor this filter will
apply to the _whole_ batch. If you instead wish to filter _individual_ parts of
the batch use the [`filter_parts`](#filter_parts) processor.

## `filter_parts`

``` yaml
type: filter_parts
filter_parts:
  type: text
  text:
    arg: ""
    operator: equals_cs
    part: 0
```

Tests each individual part of a message batch against a condition, if the
condition fails then the part is dropped. If the resulting batch is empty it
will be dropped. You can find a [full list of conditions here](../conditions),
in this case each condition will be applied to a part as if it were a single
part message.

This processor is useful if you are combining messages into batches using the
[`batch`](#batch) processor and wish to remove specific parts.

## `grok`

``` yaml
type: grok
grok:
  named_captures_only: true
  output_format: json
  parts: []
  patterns: []
  remove_empty_values: true
  use_default_patterns: true
```

Parses a payload by attempting to apply a list of Grok patterns, if a pattern
returns at least one value a resulting structured object is created according to
the chosen output format and will replace the payload. Currently only json is a
valid output format.

This processor respects type hints in the grok patterns, therefore with the
pattern `%{WORD:first},%{INT:second:int}` and a payload of `foo,1`
the resulting payload would be `{"first":"foo","second":1}`.

## `hash`

``` yaml
type: hash
hash:
  algorithm: sha256
  parts: []
```

Hashes parts of a message according to the selected algorithm. Supported
algorithms are: sha256, sha512, xxhash64.

This processor is mostly useful when combined with the
[`process_field`](#process_field) processor as it allows you to hash a
specific field of a document like this:

``` yaml
# Hash the contents of 'foo.bar'
process_field:
  path: foo.bar
  processors:
  - type: hash
    hash:
      algorithm: sha256
```

## `hash_sample`

``` yaml
type: hash_sample
hash_sample:
  parts:
  - 0
  retain_max: 10
  retain_min: 0
```

Retains a percentage of messages deterministically by hashing selected parts of
the message and checking the hash against a valid range, dropping all others.

For example, setting `retain_min` to `0.0` and `remain_max` to `50.0`
results in dropping half of the input stream, and setting `retain_min`
to `50.0` and `retain_max` to `100.1` will drop the _other_ half.

## `http`

``` yaml
type: http
http:
  max_parallel: 0
  parallel: false
  request:
    backoff_on:
    - 429
    basic_auth:
      enabled: false
      password: ""
      username: ""
    drop_on: []
    headers:
      Content-Type: application/octet-stream
    max_retry_backoff_ms: 300000
    oauth:
      access_token: ""
      access_token_secret: ""
      consumer_key: ""
      consumer_secret: ""
      enabled: false
      request_url: ""
    rate_limit: ""
    retries: 3
    retry_period_ms: 1000
    timeout_ms: 5000
    tls:
      client_certs: []
      enabled: false
      root_cas_file: ""
      skip_cert_verify: false
    url: http://localhost:4195/post
    verb: POST
```

Performs an HTTP request using a message batch as the request body, and replaces
the original message parts with the body of the response.

If the batch contains only a single message part then it will be sent as the
body of the request. If the batch contains multiple messages then they will be
sent as a multipart HTTP request using a `Content-Type: multipart`
header.

If you are sending batches and wish to avoid this behaviour then you can set the
`parallel` flag to `true` and the messages of a batch will
be sent as individual requests in parallel. You can also cap the max number of
parallel requests with `max_parallel`. Alternatively, you can use the
[`archive`](#archive) processor to create a single message
from the batch.

The `rate_limit` field can be used to specify a rate limit
[resource](../rate_limits/README.md) to cap the rate of requests across all
parallel components service wide.

The URL and header values of this type can be dynamically set using function
interpolations described [here](../config_interpolation.md#functions).

In order to map or encode the payload to a specific request body, and map the
response back into the original payload instead of replacing it entirely, you
can use the [`process_map`](#process_map) or
 [`process_field`](#process_field) processors.

## `insert_part`

``` yaml
type: insert_part
insert_part:
  content: ""
  index: -1
```

Insert a new message part at an index. If the specified index is greater than
the length of the existing parts it will be appended to the end.

The index can be negative, and if so the part will be inserted from the end
counting backwards starting from -1. E.g. if index = -1 then the new part will
become the last part of the message, if index = -2 then the new part will be
inserted before the last element, and so on. If the negative index is greater
than the length of the existing parts it will be inserted at the beginning.

This processor will interpolate functions within the 'content' field, you can
find a list of functions [here](../config_interpolation.md#functions).

## `jmespath`

``` yaml
type: jmespath
jmespath:
  parts: []
  query: ""
```

Parses a message part as a JSON blob and attempts to apply a JMESPath expression
to it, replacing the contents of the part with the result. Please refer to the
[JMESPath website](http://jmespath.org/) for information and tutorials regarding
the syntax of expressions.

For example, with the following config:

``` yaml
jmespath:
  parts: [ 0 ]
  query: locations[?state == 'WA'].name | sort(@) | {Cities: join(', ', @)}
```

If the initial contents of part 0 were:

``` json
{
  "locations": [
    {"name": "Seattle", "state": "WA"},
    {"name": "New York", "state": "NY"},
    {"name": "Bellevue", "state": "WA"},
    {"name": "Olympia", "state": "WA"}
  ]
}
```

Then the resulting contents of part 0 would be:

``` json
{"Cities": "Bellevue, Olympia, Seattle"}
```

It is possible to create boolean queries with JMESPath, in order to filter
messages with boolean queries please instead use the
[`jmespath`](../conditions/README.md#jmespath) condition.

## `json`

``` yaml
type: json
json:
  operator: get
  parts: []
  path: ""
  value: ""
```

Parses a message part as a JSON document, performs a mutation on the data, and
then overwrites the previous contents with the new value.

If the path is empty or "." the root of the data will be targeted.

This processor will interpolate functions within the 'value' field, you can find
a list of functions [here](../config_interpolation.md#functions).

### Operations

#### `append`

Appends a value to an array at a target dot path. If the path does not exist all
objects in the path are created (unless there is a collision).

If a non-array value already exists in the target path it will be replaced by an
array containing the original value as well as the new value.

If the value is an array the elements of the array are expanded into the new
array. E.g. if the target is an array `[0,1]` and the value is also an
array `[2,3]`, the result will be `[0,1,2,3]` as opposed to
`[0,1,[2,3]]`.

#### `clean`

Walks the JSON structure and deletes any fields where the value is:

- An empty array
- An empty object
- An empty string
- null

#### `copy`

Copies the value of a target dot path (if it exists) to a location. The
destination path is specified in the `value` field. If the destination
path does not exist all objects in the path are created (unless there is a
collision).

#### `delete`

Removes a key identified by the dot path. If the path does not exist this is a
no-op.

#### `move`

Moves the value of a target dot path (if it exists) to a new location. The
destination path is specified in the `value` field. If the destination
path does not exist all objects in the path are created (unless there is a
collision).

#### `select`

Reads the value found at a dot path and replaced the original contents entirely
by the new value.

#### `set`

Sets the value of a field at a dot path. If the path does not exist all objects
in the path are created (unless there is a collision).

The value can be any type, including objects and arrays. When using YAML
configuration files a YAML object will be converted into a JSON object, i.e.
with the config:

``` yaml
json:
  operator: set
  parts: [0]
  path: some.path
  value:
    foo:
      bar: 5
```

The value will be converted into '{"foo":{"bar":5}}'. If the YAML object
contains keys that aren't strings those fields will be ignored.

## `lambda`

``` yaml
type: lambda
lambda:
  credentials:
    id: ""
    role: ""
    secret: ""
    token: ""
  endpoint: ""
  function: ""
  parallel: false
  rate_limit: ""
  region: eu-west-1
  retries: 3
  timeout_ms: 5000
```

Invokes an AWS lambda for each message part of a batch. The contents of the
message part is the payload of the request, and the result of the invocation
will become the new contents of the message.

It is possible to perform requests per message of a batch in parallel by setting
the `parallel` flag to `true`. The `rate_limit`
field can be used to specify a rate limit [resource](../rate_limits/README.md)
to cap the rate of requests across parallel components service wide.

In order to map or encode the payload to a specific request body, and map the
response back into the original payload instead of replacing it entirely, you
can use the [`process_map`](#process_map) or
 [`process_field`](#process_field) processors.

## `log`

``` yaml
type: log
log:
  level: INFO
  message: ""
```

Log is a processor that prints a log event each time it processes a message. The
message is then sent onwards unchanged. The log message can be set using
function interpolations described [here](../config_interpolation.md#functions)
which allows you to log the contents and metadata of a message.

For example, if we wished to create a debug log event for each message in a
pipeline in order to expose the JSON field `foo.bar` as well as the
metadata field `kafka_partition` we can achieve that with the
following config:

``` yaml
type: log
log:
  level: DEBUG
  message: "field: ${!json_field:foo.bar}, part: ${!metadata:kafka_partition}"
```

The `level` field determines the log level of the printed events and
can be any of the following values: TRACE, DEBUG, INFO, WARN, ERROR.

## `merge_json`

``` yaml
type: merge_json
merge_json:
  parts: []
  retain_parts: false
```

Parses selected message parts as JSON documents, attempts to merge them into one
single JSON document and then writes it to a new message part at the end of the
message. Merged parts are removed unless `retain_parts` is set to
true. The new merged message part will contain the metadata of the first part to
be merged.

## `metadata`

``` yaml
type: metadata
metadata:
  key: example
  operator: set
  parts: []
  value: ${!hostname}
```

Performs operations on the metadata of a message. Metadata are key/value pairs
that are associated with message parts of a batch. Metadata values can be
referred to using configuration
[interpolation functions](../config_interpolation.md#metadata),
which allow you to set fields in certain outputs using these dynamic values.

This processor will interpolate functions within the `value` field,
you can find a list of functions [here](../config_interpolation.md#functions).
This allows you to set the contents of a metadata field using values taken from
the message payload.

### Operations

#### `set`

Sets the value of a metadata key.

#### `delete_all`

Removes all metadata values from the message.

#### `delete_prefix`

Removes all metadata values from the message where the key is prefixed with the
value provided.

## `metric`

``` yaml
type: metric
metric:
  labels: {}
  path: ""
  type: counter
  value: ""
```

Creates metrics by extracting values from messages.

The `path` field should be a dot separated path of the metric to be
set and will automatically be converted into the correct format of the
configured metric aggregator.

The `value` field can be set using function interpolations described
[here](../config_interpolation.md#functions) and is used according to the
specific type.

### Types

#### `counter`

Increments a counter by exactly 1, the contents of `value` are ignored
by this type.

#### `counter_parts`

Increments a counter by the number of parts within the message batch, the
contents of `value` are ignored by this type.

#### `counter_by`

If the contents of `value` can be parsed as a positive integer value
then the counter is incremented by this value.

For example, the following configuration will increment the value of the
`count.custom.field` metric by the contents of `field.some.value`:

``` yaml
type: metric
metric:
  type: counter_by
  path: count.custom.field
  value: ${!json_field:field.some.value}
```

#### `gauge`

If the contents of `value` can be parsed as a positive integer value
then the gauge is set to this value.

For example, the following configuration will set the value of the
`gauge.custom.field` metric to the contents of `field.some.value`:

``` yaml
type: metric
metric:
  type: gauge
  path: gauge.custom.field
  value: ${!json_field:field.some.value}
```

#### `timing`

Equivalent to `gauge` where instead the metric is a timing.

### Labels

Some metrics aggregators, such as Prometheus, support arbitrary labels, in which
case the `labels` field can be used in order to create them. Label
values can also be set using function interpolations in order to dynamically
populate them with context about the message.

## `noop`

``` yaml
type: noop
```

Noop is a no-op processor that does nothing, the message passes through
unchanged.

## `process_batch`

``` yaml
type: process_batch
process_batch: []
```

A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message. This is useful for forcing batch
wide processors such as [`dedupe`](#dedupe) to apply to individual
message parts of a batch instead.

Please note that most processors already process per message of a batch, and
this processor is not needed in those cases.

## `process_dag`

``` yaml
type: process_dag
process_dag: {}
```

A processor that manages a map of `process_map` processors and
calculates a Directed Acyclic Graph (DAG) of their dependencies by referring to
their postmap targets for provided fields and their premap targets for required
fields.

The DAG is then used to execute the children in the necessary order with the
maximum parallelism possible.

The field `dependencies` is an optional array of fields that a child
depends on. This is useful for when fields are required but don't appear within
a premap such as those used in conditions.

This processor is extremely useful for performing a complex mesh of enrichments
where network requests mean we desire maximum parallelism across those
enrichments.

For example, if we had three target HTTP services that we wished to enrich each
document with - foo, bar and baz - where baz relies on the result of both foo
and bar, we might express that relationship here like so:

``` yaml
type: process_dag
process_dag:
  foo:
    premap:
      .: .
    processors:
    - type: http
      http:
        request:
          url: http://foo/enrich
    postmap:
      foo_result: .
  bar:
    premap:
      .: msg.sub.path
    processors:
    - type: http
      http:
        request:
          url: http://bar/enrich
    postmap:
      bar_result: .
  baz:
    premap:
      foo_obj: foo_result
      bar_obj: bar_result
    processors:
    - type: http
      http:
        request:
          url: http://baz/enrich
    postmap:
      baz_obj: .
```

With this config the DAG would determine that the children foo and bar can be
executed in parallel, and once they are both finished we may proceed onto baz.

## `process_field`

``` yaml
type: process_field
process_field:
  parts: []
  path: ""
  processors: []
```

A processor that extracts the value of a field within payloads (currently only
JSON format is supported) then applies a list of processors to the extracted
value, and finally sets the field within the original payloads to the processed
result.

If the number of messages resulting from the processing steps does not match the
original count then this processor fails and the messages continue unchanged.
Therefore, you should avoid using batch and filter type processors in this list.

## `process_map`

``` yaml
type: process_map
process_map:
  conditions: []
  parts: []
  postmap: {}
  postmap_optional: {}
  premap: {}
  premap_optional: {}
  processors: []
```

A processor that extracts and maps fields from the original payload into new
objects, applies a list of processors to the newly constructed objects, and
finally maps the result back into the original payload.

This processor is useful for performing processors on subsections of a payload.
For example, you could extract sections of a JSON object in order to construct
a request object for an `http` processor, then map the result back
into a field within the original object.

The order of stages of this processor are as follows:

- Conditions are applied to each _individual_ message part in the batch,
  determining whether the part will be mapped. If the conditions are empty all
  message parts will be mapped. If the field `parts` is populated the
  message parts not in this list are also excluded from mapping.
- Message parts that are flagged for mapping are mapped according to the premap
  fields, creating a new object. If the premap stage fails (targets are not
  found) the message part will not be processed.
- Message parts that are mapped are processed as a batch. You may safely break
  the batch into individual parts during processing with the `split`
  processor.
- After all child processors are applied to the mapped messages they are mapped
  back into the original message parts they originated from as per your postmap.
  If the postmap stage fails the mapping is skipped and the message payload
  remains as it started.

Map paths are arbitrary dot paths, target path hierarchies are constructed if
they do not yet exist. Processing is skipped for message parts where the premap
targets aren't found, for optional premap targets use `premap_optional`.

If postmap targets are not found the merge is abandoned, for optional postmap
targets use `postmap_optional`.

If the premap is empty then the full payload is sent to the processors, if the
postmap is empty then the processed result replaces the original contents
entirely.

Maps can reference the root of objects either with an empty string or '.', for
example the maps:

``` yaml
premap:
  .: foo.bar
postmap:
  foo.bar: .
```

Would create a new object where the root is the value of `foo.bar` and
would map the full contents of the result back into `foo.bar`.

If the number of total message parts resulting from the processing steps does
not match the original count then this processor fails and the messages continue
unchanged. Therefore, you should avoid using batch and filter type processors in
this list.

### Batch Ordering

This processor supports batch messages. When message parts are post-mapped after
processing they will be correctly aligned with the original batch. However, the
ordering of premapped message parts as they are sent through processors are not
guaranteed to match the ordering of the original batch.

## `sample`

``` yaml
type: sample
sample:
  retain: 10
  seed: 0
```

Retains a randomly sampled percentage of messages (0 to 100) and drops all
others. The random seed is static in order to sample deterministically, but can
be set in config to allow parallel samples that are unique.

## `select_parts`

``` yaml
type: select_parts
select_parts:
  parts:
  - 0
```

Cherry pick a set of parts from messages by their index. Indexes larger than the
number of parts are simply ignored.

The selected parts are added to the new message in the same order as the
selection array. E.g. with 'parts' set to [ 2, 0, 1 ] and the message parts
[ '0', '1', '2', '3' ], the output will be [ '2', '0', '1' ].

If none of the selected parts exist in the input message (resulting in an empty
output message) the message is dropped entirely.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.

## `split`

``` yaml
type: split
split:
  size: 1
```

Breaks message batches (synonymous with multiple part messages) into smaller
batches, targeting a specific batch size of discrete message parts (default size
is 1 message.)

For each batch, if there is a remainder of parts after splitting a batch, the
remainder is also sent as a single batch. For example, if your target size was
10, and the processor received a batch of 95 message parts, the result would be
9 batches of 10 messages followed by a batch of 5 messages.

The split processor should *always* be positioned at the end of a list of
processors.

## `text`

``` yaml
type: text
text:
  arg: ""
  operator: trim_space
  parts: []
  value: ""
```

Performs text based mutations on payloads.

This processor will interpolate functions within the 'value' field, you can find
a list of functions [here](../config_interpolation.md#functions).

### Operations

#### `append`

Appends text to the end of the payload.

#### `prepend`

Prepends text to the beginning of the payload.

#### `replace`

Replaces all occurrences of the argument in a message with a value.

#### `replace_regexp`

Replaces all occurrences of the argument regular expression in a message with a
value.

#### `strip_html`

Removes all HTML tags from a message.

#### `trim_space`

Removes all leading and trailing whitespace from the payload.

#### `trim`

Removes all leading and trailing occurrences of characters within the arg field.

## `throttle`

``` yaml
type: throttle
throttle:
  period: 100us
```

Throttles the throughput of a pipeline to a maximum of one message batch per
period. This throttle is per processing pipeline, and therefore four threads
each with a throttle would result in four times the rate specified.

The period should be specified as a time duration string. For example, '1s'
would be 1 second, '10ms' would be 10 milliseconds, etc.

## `unarchive`

``` yaml
type: unarchive
unarchive:
  format: binary
  parts: []
```

Unarchives parts of a message according to the selected archive type into
multiple parts. Supported archive types are: tar, zip, binary, lines.

When a part is unarchived it is split into more message parts that replace the
original part. If you wish to split the archive into one message per file then
follow this with the 'split' processor.

Parts that are selected but fail to unarchive (invalid format) will be removed
from the message. If the message results in zero parts it is skipped entirely.

For the unarchivers that contain file information (tar, zip), a metadata field
is added to each part called `archive_filename` with the extracted filename.

[0]: ./examples.md
