Processors
==========

This document was generated with `benthos --list-processors`.

Benthos processors are functions applied to messages passing through a pipeline.
The function signature allows a processor to mutate or drop messages depending
on the content of the message.

Processors are set via config, and depending on where in the config they are
placed they will be run either immediately after a specific input (set in the
input section), on all messages (set in the pipeline section) or before a
specific output (set in the output section). Most processors apply to all
messages and can be placed in the pipeline section:

``` yaml
pipeline:
  threads: 1
  processors:
  - type: foo
    foo:
      bar: baz
```

The `threads` field in the pipeline section determines how many
parallel processing threads are created. You can read more about parallel
processing in the [pipeline guide][1].

By organising processors you can configure complex behaviours in your pipeline.
You can [find some examples here][0].

### Error Handling

Some processors have conditions whereby they might fail. Benthos has mechanisms
for detecting and recovering from these failures which can be read about
[here](../error_handling.md).

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
2. [`awk`](#awk)
3. [`batch`](#batch)
4. [`bounds_check`](#bounds_check)
5. [`cache`](#cache)
6. [`catch`](#catch)
7. [`compress`](#compress)
8. [`conditional`](#conditional)
9. [`decode`](#decode)
10. [`decompress`](#decompress)
11. [`dedupe`](#dedupe)
12. [`encode`](#encode)
13. [`filter`](#filter)
14. [`filter_parts`](#filter_parts)
15. [`grok`](#grok)
16. [`group_by`](#group_by)
17. [`group_by_value`](#group_by_value)
18. [`hash`](#hash)
19. [`hash_sample`](#hash_sample)
20. [`http`](#http)
21. [`insert_part`](#insert_part)
22. [`jmespath`](#jmespath)
23. [`json`](#json)
24. [`lambda`](#lambda)
25. [`log`](#log)
26. [`merge_json`](#merge_json)
27. [`metadata`](#metadata)
28. [`metric`](#metric)
29. [`noop`](#noop)
30. [`parallel`](#parallel)
31. [`process_batch`](#process_batch)
32. [`process_dag`](#process_dag)
33. [`process_field`](#process_field)
34. [`process_map`](#process_map)
35. [`sample`](#sample)
36. [`select_parts`](#select_parts)
37. [`sleep`](#sleep)
38. [`split`](#split)
39. [`subprocess`](#subprocess)
40. [`switch`](#switch)
41. [`text`](#text)
42. [`throttle`](#throttle)
43. [`try`](#try)
44. [`unarchive`](#unarchive)
45. [`while`](#while)

## `archive`

``` yaml
type: archive
archive:
  format: binary
  path: ${!count:files}-${!timestamp_unix_nano}.txt
```

Archives all the messages of a batch into a single message according to the
selected archive format. Supported archive formats are:
`tar`, `zip`, `binary`, `lines` and `json_array`.

Some archive formats (such as tar, zip) treat each archive item (message part)
as a file with a path. Since message parts only contain raw data a unique path
must be generated for each part. This can be done by using function
interpolations on the 'path' field as described
[here](../config_interpolation.md#functions). For types that aren't file based
(such as binary) the file field is ignored.

The `json_array` format attempts to JSON parse each message and append
the result to an array, which becomes the contents of the resulting message.

The resulting archived message adopts the metadata of the _first_ message part
of the batch.

## `awk`

``` yaml
type: awk
awk:
  codec: text
  parts: []
  program: BEGIN { x = 0 } { print $0, x; x++ }
```

Executes an AWK program on messages by feeding contents as the input based on a
codec and replaces the contents with the result. If the result is empty (nothing
is printed by the program) then the original message contents remain unchanged.

Comes with a wide range of [custom functions](./awk_functions.md) for accessing
message metadata, json fields, printing logs, etc. These functions can be
overridden by functions within the program.

### Codecs

A codec can be specified that determines how the contents of the message are fed
into the program. This does not change the custom functions.

`none`

An empty string is fed into the program. Functions can still be used in order to
extract and mutate metadata and message contents. This is useful for when your
program only uses functions and doesn't need the full text of the message to be
parsed by the program.

`text`

The full contents of the message are fed into the program as a string, allowing
you to reference tokenised segments of the message with variables ($0, $1, etc).
Custom functions can still be used with this codec.

This is the default codec as it behaves most similar to typical usage of the awk
command line tool.

`json`

No contents are fed into the program. Instead, variables are extracted from the
message by walking the flattened JSON structure. Each value is converted into a
variable by taking its full path, e.g. the object:

``` json
{
	"foo": {
		"bar": {
			"value": 10
		},
		"created_at": "2018-12-18T11:57:32"
	}
}
```

Would result in the following variable declarations:

```
foo_bar_value = 10
foo_created_at = "2018-12-18T11:57:32"
```

Custom functions can also still be used with this codec.

## `batch`

``` yaml
type: batch
batch:
  byte_size: 0
  condition:
    type: static
    static: false
  count: 0
  period: ""
```

Reads a number of discrete messages, buffering (but not acknowledging) the
message parts until either:

- The `byte_size` field is non-zero and the total size of the batch in
  bytes matches or exceeds it.
- The `count` field is non-zero and the total number of messages in
  the batch matches or exceeds it.
- A message added to the batch causes the condition to resolve `true`.
- The `period` field is non-empty and the time since the last batch
  exceeds its value.

Once one of these events trigger the parts are combined into a single batch of
messages and sent through the pipeline. After reaching a destination the
acknowledgment is sent out for all messages inside the batch at the same time,
preserving at-least-once delivery guarantees.

The `period` field - when non-empty - defines a period of time whereby
a batch is sent even if the `byte_size` has not yet been reached.
Batch parameters are only triggered when a message is added, meaning a pending
batch can last beyond this period if no messages are added since the period was
reached.

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

Checks whether each message batch fits within certain boundaries, and drops
batches that do not.

## `cache`

``` yaml
type: cache
cache:
  cache: ""
  key: ""
  operator: set
  parts: []
  value: ""
```

Performs operations against a [cache resource](../caches) for each message of a
batch, allowing you to store or retrieve data within message payloads.

This processor will interpolate functions within the `key` and `value`
fields individually for each message of the batch. This allows you to specify
dynamic keys and values based on the contents of the message payloads and
metadata. You can find a list of functions
[here](../config_interpolation.md#functions).

### Operators

#### `set`

Set a key in the cache to a value. If the key already exists the contents are
overridden.

#### `add`

Set a key in the cache to a value. If the key already exists the action fails
with a 'key already exists' error, which can be detected with
[processor error handling](../error_handling.md).

#### `get`

Retrieve the contents of a cached key and replace the original message payload
with the result. If the key does not exist the action fails with an error, which
can be detected with [processor error handling](../error_handling.md).

### Examples

The `cache` processor can be used in combination with other processors
in order to solve a variety of data stream problems.

#### Deduplication

Deduplication can be done using the add operator with a key extracted from the
message payload, since it fails when a key already exists we can remove the
duplicates using a
[`processor_failed`](../conditions/README.md#processor_failed)
condition:

``` yaml
- type: cache
  cache:
    cache: TODO
    operator: add
    key: "${!json_field:message.id}"
    value: "storeme"
- type: filter_parts
  filter_parts:
    type: processor_failed
```

#### Hydration

It's possible to enrich payloads with content previously stored in a cache by
using the [`process_dag`](#process_dag) processor:

``` yaml
- type: process_map
  process_map:
    processors:
    - type: cache
      cache:
        cache: TODO
        operator: get
        key: "${!json_field:message.document_id}"
    postmap:
      message.document: .
```

## `catch`

``` yaml
type: catch
catch: []
```

Behaves similarly to the [`process_batch`](#process_batch) processor,
where a list of child processors are applied to individual messages of a batch.
However, processors are only applied to messages that failed a processing step
prior to the catch.

For example, with the following config:

``` yaml
- type: foo
- type: catch
  catch:
  - type: bar
  - type: baz
```

If the processor `foo` fails for a particular message, that message
will be fed into the processors `bar` and `baz`. Messages that do not
fail for the processor `foo` will skip these processors.

When messages leave the catch block their fail flags are cleared. This processor
is useful for when it's possible to recover failed messages, or when special
actions (such as logging/metrics) are required before dropping them.

More information about error handing can be found [here](../error_handling.md).

## `compress`

``` yaml
type: compress
compress:
  algorithm: gzip
  level: -1
  parts: []
```

Compresses messages according to the selected algorithm. Supported compression
algorithms are: gzip, zlib, flate.

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
`else_processors`, and a condition. For each message batch, if the
condition passes, the child `processors` will be applied, otherwise
the `else_processors` are applied. This processor is useful for
applying processors based on the content of message batches.

In order to conditionally process each message of a batch individually use this
processor with the [`process_batch`](#process_batch) processor.

You can find a [full list of conditions here](../conditions).

## `decode`

``` yaml
type: decode
decode:
  parts: []
  scheme: base64
```

Decodes messages according to the selected scheme. Supported available schemes
are: base64.

## `decompress`

``` yaml
type: decompress
decompress:
  algorithm: gzip
  parts: []
```

Decompresses messages according to the selected algorithm. Supported
decompression types are: gzip, zlib, bzip2, flate.

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

Dedupes message batches by caching selected (and optionally hashed) messages,
dropping batches that are already cached. The hash type can be chosen from:
none or xxhash.

This processor acts across an entire batch, in order to deduplicate individual
messages within a batch use this processor with the
[`process_batch`](#process_batch) processor.

Optionally, the `key` field can be populated in order to hash on a
function interpolated string rather than the full contents of messages. This
allows you to deduplicate based on dynamic fields within a message, such as its
metadata, JSON fields, etc. A full list of interpolation functions can be found
[here](../config_interpolation.md#functions).

For example, the following config would deduplicate based on the concatenated
values of the metadata field `kafka_key` and the value of the JSON
path `id` within the message contents:

``` yaml
dedupe:
  cache: foocache
  key: ${!metadata:kafka_key}-${!json_field:id}
```

Caches should be configured as a resource, for more information check out the
[documentation here](../caches/README.md).

When using this processor with an output target that might fail you should
always wrap the output within a [`retry`](../outputs/README.md#retry)
block. This ensures that during outages your messages aren't reprocessed after
failures, which would result in messages being dropped.

### Delivery Guarantees

Performing deduplication on a stream using a distributed cache voids any
at-least-once guarantees that it previously had. This is because the cache will
preserve message signatures even if the message fails to leave the Benthos
pipeline, which would cause message loss in the event of an outage at the output
sink followed by a restart of the Benthos instance.

If you intend to preserve at-least-once delivery guarantees you can avoid this
problem by using a memory based cache. This is a compromise that can achieve
effective deduplication but parallel deployments of the pipeline as well as
service restarts increase the chances of duplicates passing undetected.

## `encode`

``` yaml
type: encode
encode:
  parts: []
  scheme: base64
```

Encodes messages according to the selected scheme. Supported schemes are:
base64.

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

Tests each message batch against a condition, if the condition fails then the
batch is dropped. You can find a [full list of conditions here](../conditions).

In order to filter individual messages of a batch use the
[`filter_parts`](#filter_parts) processor.

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

Tests each individual message of a batch against a condition, if the condition
fails then the message is dropped. If the resulting batch is empty it will be
dropped. You can find a [full list of conditions here](../conditions), in this
case each condition will be applied to a message as if it were a single message
batch.

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

Parses message payloads by attempting to apply a list of Grok patterns, if a
pattern returns at least one value a resulting structured object is created
according to the chosen output format and will replace the payload. Currently
only json is a valid output format.

This processor respects type hints in the grok patterns, therefore with the
pattern `%{WORD:first},%{INT:second:int}` and a payload of `foo,1`
the resulting payload would be `{"first":"foo","second":1}`.

## `group_by`

``` yaml
type: group_by
group_by: []
```

Splits a batch of messages into N batches, where each resulting batch contains a
group of messages determined by conditions that are applied per message of the
original batch. Once the groups are established a list of processors are applied
to their respective grouped batch, which can be used to label the batch as per
their grouping.

Each group is configured in a list with a condition and a list of processors:

``` yaml
type: group_by
group_by:
  - condition:
      type: static
      static: true
    processors:
      - type: noop
```

Messages are added to the first group that passes and can only belong to a
single group. Messages that do not pass the conditions of any group are placed
in a final batch with no processors applied.

For example, imagine we have a batch of messages that we wish to split into two
groups - the foos and the bars - which should be sent to different output
destinations based on those groupings. We also need to send the foos as a tar
gzip archive. For this purpose we can use the `group_by` processor
with a [`switch`](../outputs/README.md#switch) output:

``` yaml
pipeline:
  processors:
  - type: group_by
    group_by:
    - condition:
        type: text
        text:
          operator: contains
          arg: "this is a foo"
      processors:
      - type: archive
        archive:
          format: tar
      - type: compress
        compress:
          algorithm: gzip
      - type: metadata
        metadata:
          operator: set
          key: grouping
          value: foo
output:
  type: switch
  switch:
    outputs:
    - output:
        type: foo_output
      condition:
        type: metadata
        metadata:
          operator: equals
          key: grouping
          arg: foo
    - output:
        type: bar_output
```

Since any message that isn't a foo is a bar, and bars do not require their own
processing steps, we only need a single grouping configuration.

## `group_by_value`

``` yaml
type: group_by_value
group_by_value:
  value: ${!metadata:example}
```

Splits a batch of messages into N batches, where each resulting batch contains a
group of messages determined by a
[function interpolated string](../config_interpolation.md#functions) evaluated
per message. This allows you to group messages using arbitrary fields within
their content or metadata, process them individually, and send them to unique
locations as per their group.

For example, if we were consuming Kafka messages and needed to group them by
their key, archive the groups, and send them to S3 with the key as part of the
path we could achieve that with the following:

``` yaml
pipeline:
  processors:
  - type: group_by_value
    group_by_value:
      value: ${!metadata:kafka_key}
  - type: archive
    archive:
      format: tar
  - type: compress
    compress:
      algorithm: gzip
output:
  type: s3
  s3:
    bucket: TODO
    path: docs/${!metadata:kafka_key}/${!count:files}-${!timestamp_unix_nano}.tar.gz
```

## `hash`

``` yaml
type: hash
hash:
  algorithm: sha256
  parts: []
```

Hashes messages according to the selected algorithm. Supported algorithms are:
sha256, sha512, xxhash64.

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

Retains a percentage of message batches deterministically by hashing selected
messages and checking the hash against a valid range, dropping all others.

For example, setting `retain_min` to `0.0` and `remain_max` to `50.0`
results in dropping half of the input stream, and setting `retain_min`
to `50.0` and `retain_max` to `100.1` will drop the _other_ half.

In order to sample individual messages of a batch use this processor with the
[`process_batch`](#process_batch) processor.

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
    max_retry_backoff: 300s
    oauth:
      access_token: ""
      access_token_secret: ""
      consumer_key: ""
      consumer_secret: ""
      enabled: false
      request_url: ""
    rate_limit: ""
    retries: 3
    retry_period: 1s
    timeout: 5s
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
 
### Error Handling

When all retry attempts for a message are exhausted the processor cancels the
attempt. These failed messages will continue through the pipeline unchanged, but
can be dropped or placed in a dead letter queue according to your config, you
can read about these patterns [here](../error_handling.md).

## `insert_part`

``` yaml
type: insert_part
insert_part:
  content: ""
  index: -1
```

Insert a new message into a batch at an index. If the specified index is greater
than the length of the existing batch it will be appended to the end.

The index can be negative, and if so the message will be inserted from the end
counting backwards starting from -1. E.g. if index = -1 then the new message
will become the last of the batch, if index = -2 then the new message will be
inserted before the last message, and so on. If the negative index is greater
than the length of the existing batch it will be inserted at the beginning.

This processor will interpolate functions within the 'content' field, you can
find a list of functions [here](../config_interpolation.md#functions).

## `jmespath`

``` yaml
type: jmespath
jmespath:
  parts: []
  query: ""
```

Parses a message as a JSON document and attempts to apply a JMESPath expression
to it, replacing the contents of the part with the result. Please refer to the
[JMESPath website](http://jmespath.org/) for information and tutorials regarding
the syntax of expressions.

For example, with the following config:

``` yaml
jmespath:
  query: locations[?state == 'WA'].name | sort(@) | {Cities: join(', ', @)}
```

If the initial contents of a message were:

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

Then the resulting contents would be:

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

Parses messages as a JSON document, performs a mutation on the data, and then
overwrites the previous contents with the new value.

If the path is empty or "." the root of the data will be targeted.

This processor will interpolate functions within the 'value' field, you can find
a list of functions [here](../config_interpolation.md#functions).

### Operators

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
    role_external_id: ""
    secret: ""
    token: ""
  endpoint: ""
  function: ""
  parallel: false
  rate_limit: ""
  region: eu-west-1
  retries: 3
  timeout: 5s
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

### Error Handling

When all retry attempts for a message are exhausted the processor cancels the
attempt. These failed messages will continue through the pipeline unchanged, but
can be dropped or placed in a dead letter queue according to your config, you
can read about these patterns [here](../error_handling.md).

## `log`

``` yaml
type: log
log:
  fields: {}
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

### Structured Fields

It's also possible to output a map of structured fields, this only works when
the service log is set to output as JSON. The field values are function
interpolated, meaning it's possible to output structured fields containing
message contents and metadata, e.g.:

``` yaml
type: log
log:
  level: DEBUG
  message: "foo"
  fields:
    id: "${!json_field:id}"
    kafka_topic: "${!metadata:kafka_topic}"
```

## `merge_json`

``` yaml
type: merge_json
merge_json:
  parts: []
  retain_parts: false
```

Parses selected messages of a batch as JSON documents, attempts to merge them
into one single JSON document and then writes it to a new message at the end of
the batch. Merged parts are removed unless `retain_parts` is set to
true. The new merged message will contain the metadata of the first part to be
merged.

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

### Operators

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

## `parallel`

``` yaml
type: parallel
parallel:
  cap: 0
  processors: []
```

A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message (similar to the
[`process_batch`](#process_batch) processor), but where each message
is processed in parallel.

The field `cap`, if greater than zero, caps the maximum number of
parallel processing threads.

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

Map target paths that are parents of other map target paths will always be
mapped first, therefore it is possible to map subpath overrides.

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

Retains a randomly sampled percentage of message batches (0 to 100) and drops
all others. The random seed is static in order to sample deterministically, but
can be set in config to allow parallel samples that are unique.

## `select_parts`

``` yaml
type: select_parts
select_parts:
  parts:
  - 0
```

Cherry pick a set of messages from a batch by their index. Indexes larger than
the number of messages are simply ignored.

The selected parts are added to the new message batch in the same order as the
selection array. E.g. with 'parts' set to [ 2, 0, 1 ] and the message parts
[ '0', '1', '2', '3' ], the output will be [ '2', '0', '1' ].

If none of the selected parts exist in the input batch (resulting in an empty
output message) the batch is dropped entirely.

Message indexes can be negative, and if so the part will be selected from the
end counting backwards starting from -1. E.g. if index = -1 then the selected
part will be the last part of the message, if index = -2 then the part before
the last element with be selected, and so on.

## `sleep`

``` yaml
type: sleep
sleep:
  duration: 100us
```

Sleep for a period of time specified as a duration string. This processor will
interpolate functions within the `duration` field, you can find a list
of functions [here](../config_interpolation.md#functions).

## `split`

``` yaml
type: split
split:
  byte_size: 0
  size: 1
```

Breaks message batches (synonymous with multiple part messages) into smaller
batches. The size of the resulting batches are determined either by a discrete
size or, if the field `byte_size` is non-zero, then by total size in
bytes (which ever limit is reached first).

If there is a remainder of messages after splitting a batch the remainder is
also sent as a single batch. For example, if your target size was 10, and the
processor received a batch of 95 message parts, the result would be 9 batches of
10 messages followed by a batch of 5 messages.

## `subprocess`

``` yaml
type: subprocess
subprocess:
  args: []
  name: cat
  parts: []
```

Subprocess is a processor that runs a process in the background and, for each
message, will pipe its contents to the stdin stream of the process followed by a
newline.

The subprocess must then either return a line over stdout or stderr. If a
response is returned over stdout then its contents will replace the message. If
a response is instead returned from stderr will be logged and the message will
continue unchanged and will be marked as failed.

#### Subprocess requirements

It is required that subprocesses flush their stdout and stderr pipes for each
line.

Benthos will attempt to keep the process alive for as long as the pipeline is
running. If the process exits early it will be restarted.

#### Messages containing line breaks

If a message contains line breaks each line of the message is piped to the
subprocess and flushed, and a response is expected from the subprocess before
another line is fed in.

## `switch`

``` yaml
type: switch
switch: []
```

Switch is a processor that lists child case objects each containing a condition
and processors. Each batch of messages is tested against the condition of each
child case until a condition passes, whereby the processors of that case will be
executed on the batch.

Each case may specify a boolean `fallthrough` field indicating whether
the next case should be executed after it (the default is `false`.)

A case takes this form:

``` yaml
- condition:
    type: foo
  processors:
  - type: foo
  fallthrough: false
```

In order to switch each message of a batch individually use this processor with
the [`process_batch`](#process_batch) processor.

You can find a [full list of conditions here](../conditions).

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

This processor will interpolate functions within the `value` field,
you can find a list of functions [here](../config_interpolation.md#functions).

### Operators

#### `append`

Appends text to the end of the payload.

#### `escape_url_query`

Escapes text so that it is safe to place within the query section of a URL.

#### `unescape_url_query`

Unescapes text that has been url escaped.

#### `find_regexp`

Extract the matching section of the argument regular expression in a message.

#### `prepend`

Prepends text to the beginning of the payload.

#### `replace`

Replaces all occurrences of the argument in a message with a value.

#### `replace_regexp`

Replaces all occurrences of the argument regular expression in a message with a
value. Inside the value $ signs are interpreted as submatch expansions, e.g. $1
represents the text of the first submatch.

#### `set`

Replace the contents of a message entirely with a value.

#### `strip_html`

Removes all HTML tags from a message.

#### `to_lower`

Converts all text into lower case.

#### `to_upper`

Converts all text into upper case.

#### `trim`

Removes all leading and trailing occurrences of characters within the arg field.

#### `trim_space`

Removes all leading and trailing whitespace from the payload.

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

## `try`

``` yaml
type: try
try: []
```

Behaves similarly to the [`process_batch`](#process_batch) processor,
where a list of child processors are applied to individual messages of a batch.
However, if a processor fails for a message then that message will skip all
following processors.

For example, with the following config:

``` yaml
- type: try
  try:
  - type: foo
  - type: bar
  - type: baz
```

If the processor `foo` fails for a particular message, that message
will skip the processors `bar` and `baz`.

This processor is useful for when child processors depend on the successful
output of previous processors. This processor can be followed with a
[catch](#catch) processor for defining child processors to be applied
only to failed messages.

More information about error handing can be found [here](../error_handling.md).

## `unarchive`

``` yaml
type: unarchive
unarchive:
  format: binary
  parts: []
```

Unarchives messages according to the selected archive format into multiple
messages within a batch. Supported archive formats are:
`tar`, `zip`, `binary`, `lines`, `json_documents` and `json_array`.

When a message is unarchived the new messages replaces the original message in
the batch. Messages that are selected but fail to unarchive (invalid format)
will remain unchanged in the message batch but will be flagged as having failed.

The `json_documents` format attempts to parse the message as a stream
of concatenated JSON documents. Each parsed document is expanded into a new
message.

The `json_array` format attempts to parse the message as a JSON array
and for each element of the array expands its contents into a new message.

For the unarchive formats that contain file information (tar, zip), a metadata
field is added to each message called `archive_filename` with the
extracted filename.

## `while`

``` yaml
type: while
while:
  at_least_once: false
  condition:
    type: text
    text:
      arg: ""
      operator: equals_cs
      part: 0
  max_loops: 0
  processors: []
```

While is a processor that has a condition and a list of child processors. The
child processors are executed continously on a message batch for as long as the
child condition resolves to true.

The field `at_least_once`, if true, ensures that the child processors
are always executed at least one time (like a do .. while loop.)

The field `max_loops`, if greater than zero, caps the number of loops
for a message batch to this value.

If following a loop execution the number of messages in a batch is reduced to
zero the loop is exited regardless of the condition result. If following a loop
execution there are more than 1 message batches the condition is checked against
the first batch only.

You can find a [full list of conditions here](../conditions).

[0]: ../examples/README.md
[1]: ../pipeline.md
