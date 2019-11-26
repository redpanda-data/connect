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
In this case the [`for_each`](#for_each) processor can be used.

### Contents

1. [`archive`](#archive)
2. [`avro`](#avro)
3. [`awk`](#awk)
4. [`batch`](#batch)
5. [`bounds_check`](#bounds_check)
6. [`cache`](#cache)
7. [`catch`](#catch)
8. [`compress`](#compress)
9. [`conditional`](#conditional)
10. [`decode`](#decode)
11. [`decompress`](#decompress)
12. [`dedupe`](#dedupe)
13. [`encode`](#encode)
14. [`filter`](#filter)
15. [`filter_parts`](#filter_parts)
16. [`for_each`](#for_each)
17. [`grok`](#grok)
18. [`group_by`](#group_by)
19. [`group_by_value`](#group_by_value)
20. [`hash`](#hash)
21. [`hash_sample`](#hash_sample)
22. [`http`](#http)
23. [`insert_part`](#insert_part)
24. [`jmespath`](#jmespath)
25. [`json`](#json)
26. [`json_schema`](#json_schema)
27. [`lambda`](#lambda)
28. [`log`](#log)
29. [`merge_json`](#merge_json)
30. [`metadata`](#metadata)
31. [`metric`](#metric)
32. [`noop`](#noop)
33. [`number`](#number)
34. [`parallel`](#parallel)
35. [`process_batch`](#process_batch)
36. [`process_dag`](#process_dag)
37. [`process_field`](#process_field)
38. [`process_map`](#process_map)
39. [`rate_limit`](#rate_limit)
40. [`redis`](#redis)
41. [`resource`](#resource)
42. [`sample`](#sample)
43. [`select_parts`](#select_parts)
44. [`sleep`](#sleep)
45. [`split`](#split)
46. [`sql`](#sql)
47. [`subprocess`](#subprocess)
48. [`switch`](#switch)
49. [`text`](#text)
50. [`throttle`](#throttle)
51. [`try`](#try)
52. [`unarchive`](#unarchive)
53. [`while`](#while)
54. [`workflow`](#workflow)
55. [`xml`](#xml)

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

## `avro`

``` yaml
type: avro
avro:
  encoding: textual
  operator: to_json
  parts: []
  schema: ""
```

EXPERIMENTAL: This processor is considered experimental and is therefore subject
to change outside of major version releases.

Performs Avro based operations on messages based on a schema. Supported encoding
types are textual, binary and single.

### Operators

#### `to_json`

Converts Avro documents into a JSON structure. This makes it easier to
manipulate the contents of the document within Benthos. The encoding field
specifies how the source documents are encoded.

#### `from_json`

Attempts to convert JSON documents into Avro documents according to the
specified encoding.

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

DEPRECATED: This processor is no longer supported and has been replaced with
improved batching mechanisms. For more information about batching in Benthos
please check out [this document](../batching.md).

This processor is scheduled to be removed in Benthos V4

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
- cache:
    cache: TODO
    operator: add
    key: "${!json_field:message.id}"
    value: "storeme"
- filter_parts:
    type: processor_failed
```

#### Hydration

It's possible to enrich payloads with content previously stored in a cache by
using the [`process_map`](#process_map) processor:

``` yaml
- process_map:
    processors:
    - cache:
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

Behaves similarly to the [`for_each`](#for_each) processor, where a
list of child processors are applied to individual messages of a batch. However,
processors are only applied to messages that failed a processing step prior to
the catch.

For example, with the following config:

``` yaml
- type: foo
- catch:
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
processor with the [`for_each`](#for_each) processor.

You can find a [full list of conditions here](../conditions).

## `decode`

``` yaml
type: decode
decode:
  parts: []
  scheme: base64
```

Decodes messages according to the selected scheme. Supported available schemes
are: hex, base64.

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
[`for_each`](#for_each) processor.

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
hex, base64.

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

## `for_each`

``` yaml
type: for_each
for_each: []
```

A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message. This is useful for forcing batch
wide processors such as [`dedupe`](#dedupe) or interpolations such as
the `value` field of the `metadata` processor to execute on
individual message parts of a batch instead.

Please note that most processors already process per message of a batch, and
this processor is not needed in those cases.

## `grok`

``` yaml
type: grok
grok:
  named_captures_only: true
  output_format: json
  parts: []
  pattern_definitions: {}
  patterns: []
  remove_empty_values: true
  use_default_patterns: true
```

Parses message payloads by attempting to apply a list of Grok patterns, if a
pattern returns at least one value a resulting structured object is created
according to the chosen output format and will replace the payload. Currently
only json is a valid output format.

Type hints within patterns are respected, therefore with the pattern
`%{WORD:first},%{INT:second:int}` and a payload of `foo,1`
the resulting payload would be `{"first":"foo","second":1}`.

### Performance

This processor currently uses the [Go RE2](https://golang.org/s/re2syntax)
regular expression engine, which is guaranteed to run in time linear to the size
of the input. However, this property often makes it less performant than pcre
based implementations of grok. For more information see
[https://swtch.com/~rsc/regexp/regexp1.html](https://swtch.com/~rsc/regexp/regexp1.html).

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
group_by:
- condition:
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
  - group_by:
    - condition:
        text:
          operator: contains
          arg: "this is a foo"
      processors:
      - archive:
          format: tar
      - compress:
          algorithm: gzip
      - metadata:
          operator: set
          key: grouping
          value: foo
output:
  switch:
    outputs:
    - output:
        type: foo_output
      condition:
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
  - group_by_value:
      value: ${!metadata:kafka_key}
  - archive:
      format: tar
  - compress:
      algorithm: gzip
output:
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
sha256, sha512, sha1, xxhash64.

This processor is mostly useful when combined with the
[`process_field`](#process_field) processor as it allows you to hash a
specific field of a document like this:

``` yaml
# Hash the contents of 'foo.bar'
process_field:
  path: foo.bar
  processors:
  - hash:
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
[`for_each`](#for_each) processor.

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
    copy_response_headers: false
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

### Metadata

If the request returns a response code this processor sets a metadata field
`http_status_code` on all resulting messages.

If the field `copy_response_headers` is set to `true` then any headers
in the response will also be set in the resulting message as metadata.
 
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

The new message will have metadata copied from the first pre-existing message of
the batch.

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
  operator: clean
  parts: []
  path: ""
  value: ""
```

Parses messages as a JSON document, performs a mutation on the data, and then
overwrites the previous contents with the new value.

The field `path` is a [dot separated path](../field_paths.md) which,
for most operators, determines the field within the payload to be targeted. If
the path is empty or "." the root of the data will be targeted.

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

#### `split`

Splits a string field by a value and replaces the original string with an array
containing the results of the split. This operator requires both the path value
and the contents of the `value` field to be strings.

## `json_schema`

``` yaml
type: json_schema
json_schema:
  parts: []
  schema: ""
  schema_path: ""
```

Checks messages against a provided JSONSchema definition but does not change the
payload under any circumstances. If a message does not match the schema it can
be caught using error handling methods outlined [here](../error_handling.md).

Please refer to the [JSON Schema website](https://json-schema.org/) for
information and tutorials regarding the syntax of the schema.

For example, with the following JSONSchema document:

```json
{
	"$id": "https://example.com/person.schema.json",
	"$schema": "http://json-schema.org/draft-07/schema#",
	"title": "Person",
	"type": "object",
	"properties": {
	  "firstName": {
		"type": "string",
		"description": "The person's first name."
	  },
	  "lastName": {
		"type": "string",
		"description": "The person's last name."
	  },
	  "age": {
		"description": "Age in years which must be equal to or greater than zero.",
		"type": "integer",
		"minimum": 0
	  }
	}
}
```

And the following Benthos configuration:

```yaml
pipeline:
  processors:
  - json_schema:
      schema_path: "file://path_to_schema.json"
  - catch:
    - log:
        level: ERROR
        message: "Schema validation failed due to: ${!error}"
```

If a payload being processed looked like:

```json
{"firstName":"John","lastName":"Doe","age":-21}
```

Then the payload would be unchanged but a log message would appear explaining
the fault. This gives you flexibility in how you may handle schema errors, but
for a simpler use case you might instead wish to use the
[`json_schema`](../conditions/README.md#json_schema) condition with a
[`filter`](#filter).

## `lambda`

``` yaml
type: lambda
lambda:
  credentials:
    id: ""
    profile: ""
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

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).

## `log`

``` yaml
type: log
log:
  fields: {}
  level: INFO
  message: ""
```

Log is a processor that prints a log event each time it processes a batch. The
batch is then sent onwards unchanged. The log message can be set using function
interpolations described [here](../config_interpolation.md#functions) which
allows you to log the contents and metadata of a messages within a batch.

In order to print a log message per message of a batch place it within a
[`for_each`](#for_each) processor.

For example, if we wished to create a debug log event for each message in a
pipeline in order to expose the JSON field `foo.bar` as well as the
metadata field `kafka_partition` we can achieve that with the
following config:

``` yaml
for_each:
- log:
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

This processor will interpolate functions within both the
`key` and `value` fields, you can find a list of functions
[here](../config_interpolation.md#functions). This allows you to set the
contents of a metadata field using values taken from the message payload.

Value interpolations are resolved once per batch. In order to resolve them per
message of a batch place it within a [`for_each`](#for_each)
processor:

``` yaml
for_each:
- metadata:
    operator: set
    key: foo
    value: ${!json_field:document.foo}
```

### Operators

#### `set`

Sets the value of a metadata key.

#### `delete`

Removes all metadata values from the message where the key matches the value
provided. If the value field is left empty the key value will instead be used.

#### `delete_all`

Removes all metadata values from the message.

#### `delete_prefix`

Removes all metadata values from the message where the key is prefixed with the
value provided. If the value field is left empty the key value will instead be
used as the prefix.

## `metric`

``` yaml
type: metric
metric:
  labels: {}
  path: ""
  type: counter
  value: ""
```

Expose custom metrics by extracting values from message batches. This processor
executes once per batch, in order to execute once per message place it within a
[`for_each`](#for_each) processor:

``` yaml
for_each:
- metric:
    type: counter_by
    path: count.custom.field
    value: ${!json_field:field.some.value}
```

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

## `number`

``` yaml
type: number
number:
  operator: add
  parts: []
  value: 0
```

Parses message contents into a 64-bit floating point number and performs an
operator on it. In order to execute this processor on a sub field of a document
use it with the [`process_field`](#process_field) processor.

The value field can either be a number or a string type. If it is a string type
then this processor will interpolate functions within it, you can find a list of
functions [here](../config_interpolation.md#functions).

For example, if we wanted to subtract the current unix timestamp from the field
'foo' of a JSON document `{"foo":1561219142}` we could use the
following config:

``` yaml
process_field:
  path: foo
  result_type: float
  processors:
  - number:
      operator: subtract
      value: "${!timestamp_unix}"
```

Value interpolations are resolved once per message batch, in order to resolve it
for each message of the batch place it within a
[`for_each`](#for_each) processor.

### Operators

#### `add`

Adds a value.

#### `subtract`

Subtracts a value.

## `parallel`

``` yaml
type: parallel
parallel:
  cap: 0
  processors: []
```

A processor that applies a list of child processors to messages of a batch as
though they were each a batch of one message (similar to the
[`for_each`](#for_each) processor), but where each message is
processed in parallel.

The field `cap`, if greater than zero, caps the maximum number of
parallel processing threads.

## `process_batch`

``` yaml
type: process_batch
process_batch: []
```

Alias for the [`for_each`](#for_each) processor, which should be used
instead.

## `process_dag`

``` yaml
type: process_dag
process_dag: {}
```

A processor that manages a map of `process_map` processors and
calculates a Directed Acyclic Graph (DAG) of their dependencies by referring to
their postmap targets for provided fields and their premap targets for required
fields.

The names of workflow stages may only contain alphanumeric, underscore and dash
characters (they must match the regular expression `[a-zA-Z0-9_-]+`).

The DAG is then used to execute the children in the necessary order with the
maximum parallelism possible. You can read more about workflows in Benthos
[in this document](../workflows.md).

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
process_dag:
  foo:
    premap:
      .: .
    processors:
    - http:
        request:
          url: http://foo/enrich
    postmap:
      foo_result: .
  bar:
    premap:
      .: msg.sub.path
    processors:
    - http:
        request:
          url: http://bar/enrich
    postmap:
      bar_result: .
  baz:
    premap:
      foo_obj: foo_result
      bar_obj: bar_result
    processors:
    - http:
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
  codec: json
  parts: []
  path: ""
  processors: []
  result_type: string
```

A processor that extracts the value of a field [dot path](../field_paths.md)
within payloads according to a specified codec, applies a list of processors to
the extracted value and finally sets the field within the original payloads to
the processed result.

### Codecs

#### `json` (default)

Parses the payload as a JSON document, extracts and sets the field using a dot
notation path.

The result, according to the config field `result_type`, can be
marshalled into any of the following types:
`string` (default), `int`, `float`, `bool`, `object` (including null),
 `array` and `discard`. The discard type is a special case that
discards the result of the processing steps entirely.

It's therefore possible to use this codec without any child processors as a way
of casting string values into other types. For example, with an input JSON
document `{"foo":"10"}` it's possible to cast the value of the
field foo to an integer type with:

```yaml
process_field:
  path: foo
  result_type: int
```

#### `metadata`

Extracts and sets a metadata value identified by the path field. If the field
`result_type` is set to `discard` then the result of the processing stages
is discarded and the original metadata value is left unchanged.

### Usage

For example, with an input JSON document `{"foo":"hello world"}`
it's possible to uppercase the value of the field 'foo' by using the JSON codec
and a [`text`](#text) child processor:

```yaml
process_field:
  codec: json
  path: foo
  processors:
  - text:
      operator: to_upper
```

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

A processor that extracts and maps fields identified via
[dot path](../field_paths.md) from the original payload into new objects,
applies a list of processors to the newly constructed objects, and finally maps
the result back into the original payload.

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

## `rate_limit`

``` yaml
type: rate_limit
rate_limit:
  resource: ""
```

Throttles the throughput of a pipeline according to a specified
[`rate_limit`](../rate_limits/README.md) resource. Rate limits are
shared across components and therefore apply globally to all processing
pipelines.

## `redis`

``` yaml
type: redis
redis:
  key: ""
  operator: scard
  parts: []
  retries: 3
  retry_period: 500ms
  url: tcp://localhost:6379
```

Performs actions against Redis that aren't possible using a
[`cache`](#cache) processor. Actions are performed for each message of
a batch, where the contents are replaced with the result.

The field `key` supports
[interpolation functions](../config_interpolation.md#functions) resolved
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

## `resource`

``` yaml
type: resource
resource: ""
```

Resource is a processor type that runs a processor resource by its name. This
processor allows you to run the same configured processor resource in multiple
places.

Resource processors also have the advantage of name based metrics and logging.
For example, the config:

``` yaml
pipeline:
  processors:
    - jmespath:
        query: foo
```

Is equivalent to:

``` yaml
pipeline:
  processors:
    - resource: foo_proc

resources:
  processors:
    foo_proc:
      jmespath:
        query: foo
```

But now the metrics path of the JMESPath processor will be
`resources.processors.foo_proc`, this way of flattening observability
labels becomes more useful as configs get larger and more nested.

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

This processor executes once per message batch. In order to execute once for
each message of a batch place it within a
[`for_each`](#for_each) processor:

``` yaml
for_each:
- sleep:
    duration: ${!metadata:sleep_for}
```

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

## `sql`

``` yaml
type: sql
sql:
  args: []
  driver: mysql
  dsn: ""
  query: ""
  result_codec: none
```

SQL is a processor that runs a query against a target database for each message
batch and, for queries that return rows, replaces the batch with the result.

If a query contains arguments they can be set as an array of strings supporting
[interpolation functions](../config_interpolation.md#functions) in the
`args` field.

In order to execute an SQL query for each message of the batch use this
processor within a [`for_each`](#for_each) processor:

``` yaml
for_each:
- sql:
    driver: mysql
    dsn: foouser:foopassword@tcp(localhost:3306)/foodb
    query: "INSERT INTO footable (foo, bar, baz) VALUES (?, ?, ?);"
    args:
    - ${!json_field:document.foo}
    - ${!json_field:document.bar}
    - ${!metadata:kafka_topic}
```

### Result Codecs

When a query returns rows they are serialised according to a chosen codec, and
the batch contents are replaced with the serialised result.

#### `none`

The result of the query is ignored and the message batch remains unchanged. If
your query does not return rows then this is the appropriate codec.

#### `json_array`

The resulting rows are serialised into an array of JSON objects, where each
object represents a row, where the key is the column name and the value is that
columns value in the row.

### Drivers

The following is a list of supported drivers and their respective DSN formats:

- `mysql`: `[username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]`
- `postgres`: `postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]`

Please note that the `postgres` driver enforces SSL by default, you
can override this with the parameter `sslmode=disable` if required.

## `subprocess`

``` yaml
type: subprocess
subprocess:
  args: []
  max_buffer: 65536
  name: cat
  parts: []
```

Subprocess is a processor that runs a process in the background and, for each
message, will pipe its contents to the stdin stream of the process followed by a
newline.

The subprocess must then either return a line over stdout or stderr. If a
response is returned over stdout then its contents will replace the message. If
a response is instead returned from stderr it will be logged and the message
will continue unchanged and will be [marked as failed](../error_handling.md).

The field `max_buffer` defines the maximum response size able to be
read from the subprocess. This value should be set significantly above the real
expected maximum response size.

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
the [`for_each`](#for_each) processor.

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

Value interpolations are resolved once per message batch, in order to resolve it
for each message of the batch place it within a
[`for_each`](#for_each) processor:

``` yaml
for_each:
- text:
    operator: set
    value: ${!json_field:document.content}
```

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

#### `quote`

Returns a doubled-quoted string, using escape sequences (\t, \n, \xFF, \u0100)
for control characters and other non-printable characters.

#### `regexp_expand`

Expands each matched occurrence of the argument regular expression according to
a template specified with the `value` field, and replaces the message
with the aggregated results.

Inside the template $ signs are interpreted as submatch expansions, e.g. $1
represents the text of the first submatch.

For example, given the following config:

```yaml
  - text:
      operator: regexp_expand
      arg: "(?m)(?P<key>\\w+):\\s+(?P<value>\\w+)$"
      value: "$key=$value\n"
```

And a message containing:

```text
option1: value1
# comment line
option2: value2
```

The resulting payload would be:

```text
option1=value1
option2=value2
```

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

#### `unquote`

Unquotes a single, double, or back-quoted string literal

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

Behaves similarly to the [`for_each`](#for_each) processor, where a
list of child processors are applied to individual messages of a batch. However,
if a processor fails for a message then that message will skip all following
processors.

For example, with the following config:

``` yaml
- try:
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
child processors are executed continuously on a message batch for as long as the
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

## `workflow`

``` yaml
type: workflow
workflow:
  meta_path: meta.workflow
  stages: {}
```

Performs the same workflow stages as the [`process_dag`](#process_dag)
processor, but uses a record of workflow statuses stored in the path specified
by the field `meta_path` in order to report which workflow stages
succeeded, were skipped, or failed for a document. The record takes this form:

```json
{
	"succeeded": [ "foo" ],
	"skipped": [ "bar" ],
	"failed": [ "baz" ]
}
```

If a document is consumed that already contains these records then they will be
used in order to only perform stages that haven't already succeeded or have been
skipped. For example, if a document received contained the above snippet then
the foo and bar stages would not be attempted. Before writing the new records to
the resulting payloads the old one will be moved into
`<meta_path>.previous`.

If a field `<meta_path>.apply` exists in the record for a document and
is an array then it will be used as a whitelist of stages to apply, all other
stages will be skipped.

You can read more about workflows in Benthos
[in this document](../workflows.md).

## `xml`

``` yaml
type: xml
xml:
  operator: to_json
  parts: []
```

EXPERIMENTAL: This processor is considered experimental and is therefore subject
to change outside of major version releases.

Parses messages as an XML document, performs a mutation on the data, and then
overwrites the previous contents with the new value.

### Operators

#### `to_json`

Converts an XML document into a JSON structure, where elements appear as keys of
an object according to the following rules:

- If an element contains attributes they are parsed by prefixing a hyphen,
  `-`, to the attribute label.
- If the element is a simple element and has attributes, the element value
  is given the key `#text`.
- XML comments, directives, and process instructions are ignored.
- When elements are repeated the resulting JSON value is an array.

For example, given the following XML:

```xml
<root>
  <title>This is a title</title>
  <description tone="boring">This is a description</description>
  <elements id="1">foo1</elements>
  <elements id="2">foo2</elements>
  <elements>foo3</elements>
</root>
```

The resulting JSON structure would look like this:

```json
{
  "root":{
    "title":"This is a title",
    "description":{
      "#text":"This is a description",
      "-tone":"boring"
    },
    "elements":[
      {"#text":"foo1","-id":"1"},
      {"#text":"foo2","-id":"2"},
      "foo3"
    ]
  }
}
```

[0]: ../examples/README.md
[1]: ../pipeline.md
