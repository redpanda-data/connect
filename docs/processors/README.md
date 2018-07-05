Processors
==========

This document was generated with `benthos --list-processors`.

Benthos has a concept of processors, these are functions that will be applied to
each message passing through a pipeline. The function signature allows a
processor to mutate or drop messages depending on the content of the message.

Processors are set via config, and depending on where in the config they are
placed they will be run either immediately after a specific input (set in the
input section), on all messages (set in the pipeline section) or before a
specific output (set in the output section).

By organising processors you can configure complex behaviours in your pipeline.
You can [find some examples here][0].

### Contents

1. [`archive`](#archive)
2. [`batch`](#batch)
3. [`bounds_check`](#bounds_check)
4. [`combine`](#combine)
5. [`compress`](#compress)
6. [`conditional`](#conditional)
7. [`decompress`](#decompress)
8. [`dedupe`](#dedupe)
9. [`filter`](#filter)
10. [`filter_parts`](#filter_parts)
11. [`grok`](#grok)
12. [`hash_sample`](#hash_sample)
13. [`http`](#http)
14. [`insert_part`](#insert_part)
15. [`jmespath`](#jmespath)
16. [`json`](#json)
17. [`merge_json`](#merge_json)
18. [`noop`](#noop)
19. [`sample`](#sample)
20. [`select_parts`](#select_parts)
21. [`split`](#split)
22. [`unarchive`](#unarchive)

## `archive`

``` yaml
type: archive
archive:
  format: binary
  path: ${!count:files}-${!timestamp_unix_nano}.txt
```

Archives all the parts of a message into a single part according to the selected
archive type. Supported archive types are: tar, binary (I'll add more later).

Some archive types (such as tar) treat each archive item (message part) as a
file with a path. Since message parts only contain raw data a unique path must
be generated for each part. This can be done by using function interpolations on
the 'path' field as described [here](../config_interpolation.md#functions). For
types that aren't file based (such as binary) the file field is ignored.

## `batch`

``` yaml
type: batch
batch:
  byte_size: 10000
```

Reads a number of discrete messages, buffering (but not acknowledging) the
message parts until the total size of the batch in bytes matches or exceeds the
configured byte size. Once the limit is reached the parts are combined into a
single batch of messages and sent through the pipeline. Once the combined batch
has reached a destination the acknowledgment is sent out for all messages inside
the batch, preserving at-least-once delivery guarantees.

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
that do not (log warning message and a metric).

## `combine`

``` yaml
type: combine
combine:
  parts: 2
```

If a message queue contains multiple part messages as individual parts it can
be useful to 'squash' them back into a single message. We can then push it
through a protocol that natively supports multiple part messages.

For example, if we started with N messages each containing M parts, pushed those
messages into Kafka by splitting the parts. We could now consume our N*M
messages from Kafka and squash them back into M part messages with the combine
processor, and then subsequently push them into something like ZMQ.

If a message received has more parts than the 'combine' amount it will be sent
unchanged with its original parts. This occurs even if there are cached parts
waiting to be combined, which will change the ordering of message parts through
the platform.

When a message part is received that increases the total cached number of parts
beyond the threshold it will have _all_ of its parts appended to the resuling
message. E.g. if you set the threshold at 4 and send a message of 2 parts
followed by a message of 3 parts then you will receive one output message of 5
parts.

## `compress`

``` yaml
type: compress
compress:
  algorithm: gzip
  level: -1
  parts: []
```

Compresses parts of a message according to the selected algorithm. Supported
compression types are: gzip (I'll add more later). If the list of target parts
is empty the compression will be applied to all message parts.

The 'level' field might not apply to all algorithms.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.

## `conditional`

``` yaml
type: conditional
conditional:
  condition:
    type: content
    content:
      arg: ""
      operator: equals_cs
      part: 0
  else_processors: []
  processors: []
```

Conditional is a processor that has a list of child 'processors',
'else_processors', and a condition. For each message if the condition passes the
child 'processors' will be applied, otherwise the 'else_processors' are applied.
This processor is useful for applying processors such as 'dedupe' based on the
content type of the message.

## `decompress`

``` yaml
type: decompress
decompress:
  algorithm: gzip
  parts: []
```

Decompresses the parts of a message according to the selected algorithm.
Supported decompression types are: gzip (I'll add more later). If the list of
target parts is empty the decompression will be applied to all message parts.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.

Parts that fail to decompress (invalid format) will be removed from the message.
If the message results in zero parts it is skipped entirely.

## `dedupe`

``` yaml
type: dedupe
dedupe:
  cache: ""
  drop_on_err: true
  hash: none
  json_paths: []
  parts:
  - 0
```

Dedupes messages by caching selected (and optionally hashed) parts, dropping
messages that are already cached. The hash type can be chosen from: none or
xxhash (more will come soon).

It's possible to dedupe based on JSON field data from message parts by setting
the value of `json_paths`, which is an array of JSON dot paths that
will be extracted from the message payload and concatenated. The result will
then be used to deduplicate. If the result is empty (i.e. none of the target
paths were found in the data) then this is considered an error, and the message
will be dropped or propagated based on the value of `drop_on_err`.

For example, if each message is a single part containing a JSON blob of the
following format:

``` json
{
	"id": "3274892374892374",
	"content": "hello world"
}
```

Then you could deduplicate using the raw contents of the 'id' field instead of
the whole body with the following config:

``` json
type: dedupe
dedupe:
  cache: foo_cache
  parts: [0]
  json_paths:
    - id
  hash: none
```

Caches should be configured as a resource, for more information check out the
[documentation here](../caches).

## `filter`

``` yaml
type: filter
filter:
  type: content
  content:
    arg: ""
    operator: equals_cs
    part: 0
```

Tests each message against a condition, if the condition fails then the message
is dropped. You can read a [full list of conditions here](../conditions).

NOTE: If you are combining messages into batches using the
[`combine`](#combine) or [`batch`](#batch) processors this filter will
apply to the _whole_ batch. If you instead wish to filter _individual_ parts of
the batch use the [`filter_parts`](#filter_parts) processor.

## `filter_parts`

``` yaml
type: filter_parts
filter_parts:
  and: []
  content:
    arg: ""
    operator: equals_cs
    part: 0
  count:
    arg: 100
  jmespath:
    part: 0
    query: ""
  not: {}
  or: []
  resource: ""
  static: true
  type: content
  xor: []
```

Tests each individual part of a message batch against a condition, if the
condition fails then the part is dropped. If the resulting batch is empty it
will be dropped. You can find a [full list of conditions here](../conditions),
in this case each condition will be applied to a part as if it were a single
part message.

This processor is useful if you are combining messages into batches using the
[`combine`](#combine) or [`batch`](#batch) processors and wish to
remove specific parts.

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

If the list of target parts is empty the query will be applied to all message
parts.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the
last element with be selected, and so on.

## `hash_sample`

``` yaml
type: hash_sample
hash_sample:
  parts:
  - 0
  retain_max: 10
  retain_min: 0
```

Passes on a percentage of messages deterministically by hashing selected parts
of the message and checking the hash against a valid range, dropping all others.

For example, a 'hash_sample' with 'retain_min' of 0.0 and 'remain_max' of 50.0
will receive half of the input stream, and a 'hash_sample' with 'retain_min' of
50.0 and 'retain_max' of 100.1 will receive the other half.

The part indexes can be negative, and if so the part will be selected from the
end counting backwards starting from -1. E.g. if index = -1 then the selected
part will be the last part of the message, if index = -2 then the part before
the last element with be selected, and so on.

## `http`

``` yaml
type: http
http:
  parts: []
  request:
    backoff_on:
    - 429
    basic_auth:
      enabled: false
      password: ""
      username: ""
    content_type: application/octet-stream
    drop_on: []
    max_retry_backoff_ms: 300000
    oauth:
      access_token: ""
      access_token_secret: ""
      consumer_key: ""
      consumer_secret: ""
      enabled: false
      request_url: ""
    retries: 3
    retry_period_ms: 1000
    skip_cert_verify: false
    timeout_ms: 5000
    url: http://localhost:4195/post
    verb: POST
  request_map: {}
  response_map: {}
  strict_request_map: true
```

Performs an HTTP request using a message part as the request body and either
replaces or augments the original message part with the body of the response.

By default the entire contents of the message part are sent and the response
entirely replaces the original contents. Alternatively, populating the
`request_map` and `response_map` fields with a map of destination to
source dot paths allows you to specify how the request payload is constructed,
and how the response is mapped to the original payload respectively.

When `strict_request_map` is set to `true` the processor is
skipped for any payloads where a map target is not found.

If the list of target parts is empty the processor will be applied to all
message parts.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the
last element with be selected, and so on.

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
the syntax of expressions. If the list of target parts is empty the query will
be applied to all message parts.

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

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if part = -1 then the selected part
will be the last part of the message, if part = -2 then the part before the
last element with be selected, and so on.

## `json`

``` yaml
type: json
json:
  operator: get
  parts: []
  path: ""
  value: ""
```

Parses a message part as a JSON blob, performs a mutation on the data, and then
overwrites the previous contents with the new value.

If the path is empty or "." the root of the data will be targeted.

If the list of target parts is empty the processor will be applied to all
message parts. Part indexes can be negative, and if so the part will be selected
from the end counting backwards starting from -1. E.g. if part = -1 then the
selected part will be the last part of the message, if part = -2 then the part
before the last element with be selected, and so on.

This processor will interpolate functions within the 'value' field, you can find
a list of functions [here](../config_interpolation.md#functions).

### Operations

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

#### `append`

Appends a value to an array at a target dot path. If the path does not exist all
objects in the path are created (unless there is a collision).

If a non-array value already exists in the target path it will be replaced by an
array containing the original value as well as the new value.

If the value is an array the elements of the array are expanded into the new
array. E.g. if the target is an array `[0,1]` and the value is also an
array `[2,3]`, the result will be `[0,1,2,3]` as opposed to
`[0,1,[2,3]]`.

#### `delete`

Removes a key identified by the dot path. If the path does not exist this is a
no-op.

#### `select`

Reads the value found at a dot path and replaced the original contents entirely
by the new value.

## `merge_json`

``` yaml
type: merge_json
merge_json:
  parts: []
  retain_parts: false
```

Parses selected message parts as JSON blobs, attempts to merge them into one
single JSON value and then writes it to a new message part at the end of the
message. Merged parts are removed unless `retain_parts` is set to
true.

If the list of target parts is empty the processor will be applied to all
message parts. Part indexes can be negative, and if so the part will be selected
from the end counting backwards starting from -1. E.g. if part = -1 then the
selected part will be the last part of the message, if part = -2 then the part
before the last element with be selected, and so on.

## `noop`

``` yaml
type: noop
noop: null
```

Noop is a no-op processor that does nothing, the message passes through
unchanged.

## `sample`

``` yaml
type: sample
sample:
  retain: 10
  seed: 0
```

Passes on a randomly sampled percentage of messages. The random seed is static
in order to sample deterministically, but can be set in config to allow parallel
samples that are unique.

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
split: {}
```

Extracts the individual parts of a multipart message and turns them each into a
unique message. It is NOT necessary to use the split processor when your output
only supports single part messages, since those message parts will automatically
be sent as individual messages.

Please note that when you split a message you will lose the coupling between the
acknowledgement from the output destination to the origin message at the input
source. If all but one part of a split message is successfully propagated to the
destination the source will still see an error and may attempt to resend the
entire message again.

The split operator is useful for breaking down messages containing a large
number of parts into smaller batches by using the split processor followed by
the combine processor. For example:

1 Message of 1000 parts -> Split -> Combine 10 -> 100 Messages of 10 parts.

## `unarchive`

``` yaml
type: unarchive
unarchive:
  format: binary
  parts: []
```

Unarchives parts of a message according to the selected archive type into
multiple parts. Supported archive types are: tar, binary. If the list of target
parts is empty the unarchive will be applied to all message parts.

When a part is unarchived it is split into more message parts that replace the
original part. If you wish to split the archive into one message per file then
follow this with the 'split' processor.

Part indexes can be negative, and if so the part will be selected from the end
counting backwards starting from -1. E.g. if index = -1 then the selected part
will be the last part of the message, if index = -2 then the part before the
last element with be selected, and so on.

Parts that are selected but fail to unarchive (invalid format) will be removed
from the message. If the message results in zero parts it is skipped entirely.

[0]: ./examples.md
