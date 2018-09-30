Outputs
=======

This document was generated with `benthos --list-outputs`

An output is a sink where we wish to send our consumed data after applying an
array of [processors](../processors). Only one output is configured at the root
of a Benthos config. However, the output can be a [broker](#broker) which
combines multiple outputs under a specific pattern.

### Multiplexing Outputs

It is possible to perform
[content based multiplexing](../concepts.md#content-based-multiplexing) of
messages to specific outputs either by using the `switch` output or a
broker with the `fan_out` pattern and a
[filter processor](../processors/README.md#filter) on each output, which
is a processor that drops messages if the condition does not pass. Conditions
are content aware logical operators that can be combined using boolean logic.

For more information regarding conditions, including a full list of available
conditions please [read the docs here](../conditions/README.md)

### Dead Letter Queues

It's possible to create fallback outputs for when an output target fails using
a [`broker`](#broker) output with the 'try' pattern.

### Contents

1. [`amqp`](#amqp)
2. [`broker`](#broker)
3. [`dynamic`](#dynamic)
4. [`elasticsearch`](#elasticsearch)
5. [`file`](#file)
6. [`files`](#files)
7. [`gcp_pubsub`](#gcp_pubsub)
8. [`hdfs`](#hdfs)
9. [`http_client`](#http_client)
10. [`http_server`](#http_server)
11. [`inproc`](#inproc)
12. [`kafka`](#kafka)
13. [`kinesis`](#kinesis)
14. [`mqtt`](#mqtt)
15. [`nanomsg`](#nanomsg)
16. [`nats`](#nats)
17. [`nats_stream`](#nats_stream)
18. [`nsq`](#nsq)
19. [`redis_list`](#redis_list)
20. [`redis_pubsub`](#redis_pubsub)
21. [`redis_streams`](#redis_streams)
22. [`retry`](#retry)
23. [`s3`](#s3)
24. [`sqs`](#sqs)
25. [`stdout`](#stdout)
26. [`switch`](#switch)
27. [`websocket`](#websocket)

## `amqp`

``` yaml
type: amqp
amqp:
  exchange: benthos-exchange
  exchange_declare:
    durable: true
    enabled: false
    type: direct
  immediate: false
  key: benthos-key
  mandatory: false
  persistent: false
  tls:
    client_certs: []
    enabled: false
    root_cas_file: ""
    skip_cert_verify: false
  url: amqp://guest:guest@localhost:5672/
```

Sends messages to an AMQP (0.91) exchange. AMQP is a messaging protocol used by
various message brokers, including RabbitMQ. The metadata from each message are
delivered as headers.

It's possible for this output type to create the target exchange by setting
`exchange_declare.enabled` to `true`, if the exchange already exists
then the declaration passively verifies that the settings match.

Exchange type options are: direct|fanout|topic|x-custom

TLS is automatic when connecting to an `amqps` URL, but custom
settings can be enabled in the `tls` section.

The field 'key' can be dynamically set using function interpolations described
[here](../config_interpolation.md#functions).

## `broker`

``` yaml
type: broker
broker:
  copies: 1
  outputs: []
  pattern: fan_out
```

The broker output type allows you to configure multiple output targets by
listing them:

``` yaml
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: foo
      foo:
        foo_field_1: value1
    - type: bar
      bar:
        bar_field_1: value2
        bar_field_2: value3
    - type: baz
      baz:
        baz_field_1: value4
      processors:
      - type: baz_processor
  processors:
  - type: some_processor
```

The broker pattern determines the way in which messages are allocated to outputs
and can be chosen from the following:

#### `fan_out`

With the fan out pattern all outputs will be sent every message that passes
through Benthos. If an output applies back pressure it will block all subsequent
messages, and if an output fails to send a message it will be retried
continuously until completion or service shut down.

#### `round_robin`

With the round robin pattern each message will be assigned a single output
following their order. If an output applies back pressure it will block all
subsequent messages. If an output fails to send a message then the message will
be re-attempted with the next input, and so on.

#### `greedy`

The greedy pattern results in higher output throughput at the cost of
potentially disproportionate message allocations to those outputs. Each message
is sent to a single output, which is determined by allowing outputs to claim
messages as soon as they are able to process them. This results in certain
faster outputs potentially processing more messages at the cost of slower
outputs.

#### `try`

The try pattern attempts to send each message to only one output, starting from
the first output on the list. If an output attempt fails then the broker
attempts to send to the next output in the list and so on.

This pattern is useful for triggering events in the case where certain output
targets have broken. For example, if you had an output type `http_client`
but wished to reroute messages whenever the endpoint becomes unreachable you
could use a try broker.

### Utilising More Outputs

When using brokered outputs with patterns such as round robin or greedy it is
possible to have multiple messages in-flight at the same time. In order to fully
utilise this you either need to have a greater number of input sources than
output sources [or use a buffer](../buffers/README.md).

### Processors

It is possible to configure [processors](../processors/README.md) at the broker
level, where they will be applied to _all_ child outputs, as well as on the
individual child outputs. If you have processors at both the broker level _and_
on child outputs then the broker processors will be applied _before_ the child
nodes processors.

## `dynamic`

``` yaml
type: dynamic
dynamic:
  outputs: {}
  prefix: ""
  timeout_ms: 5000
```

The dynamic type is a special broker type where the outputs are identified by
unique labels and can be created, changed and removed during runtime via a REST
HTTP interface. The broker pattern used is always `fan_out`, meaning
each message will be delivered to each dynamic output.

To GET a JSON map of output identifiers with their current uptimes use the
'/outputs' endpoint.

To perform CRUD actions on the outputs themselves use POST, DELETE, and GET
methods on the `/outputs/{output_id}` endpoint. When using POST the
body of the request should be a JSON configuration for the output, if the output
already exists it will be changed.

## `elasticsearch`

``` yaml
type: elasticsearch
elasticsearch:
  basic_auth:
    enabled: false
    password: ""
    username: ""
  id: ${!count:elastic_ids}-${!timestamp_unix}
  index: benthos_index
  pipeline: ""
  sniff: true
  timeout_ms: 5000
  type: doc
  urls:
  - http://localhost:9200
```

Publishes messages into an Elasticsearch index. This output currently does not
support creating the target index.

Both the `id` and `index` fields can be dynamically set using function
interpolations described [here](../config_interpolation.md#functions).

## `file`

``` yaml
type: file
file:
  delimiter: ""
  path: ""
```

The file output type simply appends all messages to an output file. Single part
messages are printed with a delimiter (defaults to '\n' if left empty).
Multipart messages are written with each part delimited, with the final part
followed by two delimiters, e.g. a multipart message [ "foo", "bar", "baz" ]
would be written as:

foo\n
bar\n
baz\n\n

## `files`

``` yaml
type: files
files:
  path: ${!count:files}-${!timestamp_unix_nano}.txt
```

Writes each individual part of each message to a new file.

Message parts only contain raw data, and therefore in order to create a unique
file for each part you need to generate unique file names. This can be done by
using function interpolations on the `path` field as described
[here](../config_interpolation.md#functions). When sending batched messages
these interpolations are performed per message part.

## `gcp_pubsub`

``` yaml
type: gcp_pubsub
gcp_pubsub:
  project: ""
  topic: ""
```

Sends messages to a GCP Cloud Pub/Sub topic. Metadata from messages are sent as
attributes.

## `hdfs`

``` yaml
type: hdfs
hdfs:
  directory: ""
  hosts:
  - localhost:9000
  path: ${!count:files}-${!timestamp_unix_nano}.txt
  user: benthos_hdfs
```

Sends message parts as files to a HDFS directory. Each file is written
with the path specified with the 'path' field, in order to have a different path
for each object you should use function interpolations described
[here](../config_interpolation.md#functions). When sending batched messages the
interpolations are performed per message part.

## `http_client`

``` yaml
type: http_client
http_client:
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

Sends messages to an HTTP server. The request will be retried for each message
whenever the response code is outside the range of 200 -> 299 inclusive. It is
possible to list codes outside of this range in the `drop_on` field in
order to prevent retry attempts.

The period of time between retries is linear by default. Response codes that are
within the `backoff_on` list will instead apply exponential backoff
between retry attempts.

When the number of retries expires the output will reject the message, the
behaviour after this will depend on the pipeline but usually this simply means
the send is attempted again until successful whilst applying back pressure.

The URL and header values of this type can be dynamically set using function
interpolations described [here](../config_interpolation.md#functions).

The body of the HTTP request is the raw contents of the message payload. If the
message has multiple parts the request will be sent according to
[RFC1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html)

## `http_server`

``` yaml
type: http_server
http_server:
  address: ""
  cert_file: ""
  key_file: ""
  path: /get
  stream_path: /get/stream
  timeout_ms: 5000
  ws_path: /get/ws
```

Sets up an HTTP server that will send messages over HTTP(S) GET requests. HTTP
2.0 is supported when using TLS, which is enabled when key and cert files are
specified.

You can leave the 'address' config field blank in order to use the default
service, but this will ignore TLS options.

You can receive a single, discrete message on the configured 'path' endpoint, or
receive a constant stream of line delimited messages on the configured
'stream_path' endpoint.

## `inproc`

``` yaml
type: inproc
inproc: ""
```

Sends data directly to Benthos inputs by connecting to a unique ID. This allows
you to hook up isolated streams whilst running Benthos in
[`--streams` mode](../streams/README.md) mode, it is NOT recommended
that you connect the inputs of a stream with an output of the same stream, as
feedback loops can lead to deadlocks in your message flow.

It is possible to connect multiple inputs to the same inproc ID, but only one
output can connect to an inproc ID, and will replace existing outputs if a
collision occurs.

## `kafka`

``` yaml
type: kafka
kafka:
  ack_replicas: false
  addresses:
  - localhost:9092
  client_id: benthos_kafka_output
  compression: none
  key: ""
  max_msg_bytes: 1e+06
  round_robin_partitions: false
  target_version: 1.0.0
  timeout_ms: 5000
  tls:
    client_certs: []
    enabled: false
    root_cas_file: ""
    skip_cert_verify: false
  topic: benthos_stream
```

The kafka output type writes messages to a kafka broker, these messages are
acknowledged, which is propagated back to the input. The config field
`ack_replicas` determines whether we wait for acknowledgement from all
replicas or just a single broker.

It is possible to specify a compression codec to use out of the following
options: none, snappy, lz4 and gzip.

If the field `key` is not empty then each message will be given its
contents as a key.

Both the `key` and `topic` fields can be dynamically set using
function interpolations described [here](../config_interpolation.md#functions).
When sending batched messages these interpolations are performed per message
part.

By default the paritioner will select partitions based on a hash of the key
value. If the key is empty then a partition is chosen at random. You can
alternatively force the partitioner to round-robin partitions with the field
`round_robin_partitions`.

### TLS

Custom TLS settings can be used to override system defaults. This includes
providing a collection of root certificate authorities, providing a list of
client certificates to use for client verification and skipping certificate
verification.

Client certificates can either be added by file or by raw contents:

``` yaml
enabled: true
client_certs:
  - cert_file: ./example.pem
    key_file: ./example.key
  - cert: foo
    key: bar
```

## `kinesis`

``` yaml
type: kinesis
kinesis:
  backoff:
    initial_interval: 1s
    max_elapsed_time: 30s
    max_interval: 5s
  credentials:
    id: ""
    role: ""
    secret: ""
    token: ""
  endpoint: ""
  hash_key: ""
  max_retries: 0
  partition_key: ""
  region: eu-west-1
  stream: ""
```

Sends messages to a Kinesis stream.

Both the `partition_key`(required) and `hash_key` (optional)
fields can be dynamically set using function interpolations described
[here](../config_interpolation.md#functions). When sending batched messages the
interpolations are performed per message part.

## `mqtt`

``` yaml
type: mqtt
mqtt:
  client_id: benthos_output
  qos: 1
  topic: benthos_topic
  urls:
  - tcp://localhost:1883
```

Pushes messages to an MQTT broker.

## `nanomsg`

``` yaml
type: nanomsg
nanomsg:
  bind: false
  poll_timeout_ms: 5000
  socket_type: PUSH
  urls:
  - tcp://localhost:5556
```

The scalability protocols are common communication patterns. This output should
be compatible with any implementation, but specifically targets Nanomsg.

Currently only PUSH and PUB sockets are supported.

## `nats`

``` yaml
type: nats
nats:
  subject: benthos_messages
  urls:
  - nats://localhost:4222
```

Publish to an NATS subject. NATS is at-most-once, so delivery is not guaranteed.
For at-least-once behaviour with NATS look at NATS Stream.

## `nats_stream`

``` yaml
type: nats_stream
nats_stream:
  client_id: benthos_client
  cluster_id: test-cluster
  subject: benthos_messages
  urls:
  - nats://localhost:4222
```

Publish to a NATS Stream subject.

## `nsq`

``` yaml
type: nsq
nsq:
  nsqd_tcp_address: localhost:4150
  topic: benthos_messages
  user_agent: benthos_producer
```

Publish to an NSQ topic. The `topic` field can be dynamically set
using function interpolations described
[here](../config_interpolation.md#functions). When sending batched messages
these interpolations are performed per message part.

## `redis_list`

``` yaml
type: redis_list
redis_list:
  key: benthos_list
  url: tcp://localhost:6379
```

Pushes messages onto the end of a Redis list (which is created if it doesn't
already exist) using the RPUSH command.

## `redis_pubsub`

``` yaml
type: redis_pubsub
redis_pubsub:
  channel: benthos_chan
  url: tcp://localhost:6379
```

Publishes messages through the Redis PubSub model. It is not possible to
guarantee that messages have been received.

## `redis_streams`

``` yaml
type: redis_streams
redis_streams:
  body_key: body
  max_length: 0
  stream: benthos_stream
  url: tcp://localhost:6379
```

Pushes messages to a Redis (v5.0+) Stream (which is created if it doesn't
already exist) using the XADD command. It's possible to specify a maximum length
of the target stream by setting it to a value greater than 0, in which case this
cap is applied only when Redis is able to remove a whole macro node, for
efficiency.

Redis stream entries are key/value pairs, as such it is necessary to specify the
key to be set to the body of the message. All metadata fields of the message
will also be set as key/value pairs, if there is a key collision between
a metadata item and the body then the body takes precedence.

## `retry`

``` yaml
type: retry
retry:
  backoff:
    initial_interval: 500ms
    max_elapsed_time: 0s
    max_interval: 3s
  max_retries: 0
  output: {}
```

Attempts to write messages to a child output and if the write fails for any
reason the message is retried either until success or, if the retries or max
elapsed time fields are non-zero, either is reached.

All messages in Benthos are always retried on an output error, but this would
usually involve propagating the error back to the source of the message, whereby
it would be reprocessed before reaching the output layer once again.

This output type is useful whenever we wish to avoid reprocessing a message on
the event of a failed send. We might, for example, have a dedupe processor that
we want to avoid reapplying to the same message more than once in the pipeline.

Rather than retrying the same output you may wish to retry the send using a
different output target (a dead letter queue). In which case you should instead
use the [`broker`](#broker) output type with the pattern 'try'.

## `s3`

``` yaml
type: s3
s3:
  bucket: ""
  credentials:
    id: ""
    role: ""
    secret: ""
    token: ""
  endpoint: ""
  path: ${!count:files}-${!timestamp_unix_nano}.txt
  region: eu-west-1
  timeout_s: 5
```

Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded
with the path specified with the 'path' field, in order to have a different path
for each object you should use function interpolations described
[here](../config_interpolation.md#functions).

## `sqs`

``` yaml
type: sqs
sqs:
  credentials:
    id: ""
    role: ""
    secret: ""
    token: ""
  endpoint: ""
  region: eu-west-1
  url: ""
```

Sends messages to an SQS queue.

## `stdout`

``` yaml
type: stdout
stdout:
  delimiter: ""
```

The stdout output type prints messages to stdout. Single part messages are
printed with a delimiter (defaults to '\n' if left empty). Multipart messages
are written with each part delimited, with the final part followed by two
delimiters, e.g. a multipart message [ "foo", "bar", "baz" ] would be written
as:

foo\n
bar\n
baz\n\n

## `switch`

``` yaml
type: switch
switch:
  outputs: []
```

The switch output type allows you to configure multiple conditional output
targets by listing child outputs paired with conditions. Conditional logic is
currently applied per whole message batch. In order to multiplex per message of
a batch use the [`broker`](#broker) output with the pattern
`fan_out`.

In the following example, messages containing "foo" will be sent to both the
`foo` and `baz` outputs. Messages containing "bar" will be
sent to both the `bar` and `baz` outputs. Messages
containing both "foo" and "bar" will be sent to all three outputs. And finally,
messages that do not contain "foo" or "bar" will be sent to the `baz`
output only.

``` yaml
output:
  type: switch
  switch:
    outputs:
    - output:
        type: foo
        foo:
          foo_field_1: value1
      condition:
        type: text
        text:
          operator: contains
          arg: foo
      fallthrough: true
    - output:
        type: bar
        bar:
          bar_field_1: value2
          bar_field_2: value3
      condition:
        type: text
        text:
          operator: contains
          arg: bar
      fallthrough: true
    - output:
        type: baz
        baz:
          baz_field_1: value4
        processors:
        - type: baz_processor
  processors:
  - type: some_processor
```

The switch output requires a minimum of two outputs. If no condition is defined
for an output, it behaves like a static `true` condition. If
`fallthrough` is set to `true`, the switch output will
continue evaluating additional outputs after finding a match. If an output
applies back pressure it will block all subsequent messages, and if an output
fails to send a message, it will be retried continously until completion or
service shut down. Messages that do not match any outputs will be dropped.

## `websocket`

``` yaml
type: websocket
websocket:
  basic_auth:
    enabled: false
    password: ""
    username: ""
  oauth:
    access_token: ""
    access_token_secret: ""
    consumer_key: ""
    consumer_secret: ""
    enabled: false
    request_url: ""
  url: ws://localhost:4195/post/ws
```

Sends messages to an HTTP server via a websocket connection.
