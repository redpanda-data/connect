Outputs
=======

This document was generated with `benthos --list-outputs`

An output is a sink where we wish to send our consumed data after applying an
array of [processors](../processors). Only one output is configured at the root
of a Benthos config. However, the output can be a [broker](#broker) which
combines multiple outputs under a specific pattern. For example, if we wanted
three outputs, a 'foo' a 'bar' and a 'baz', where each output received every
message we could use the 'broker' output type at our root:

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

Note that in this example we have specified a processor at the broker level
which will be applied to messages sent to _all_ outputs, and we also have a
processor at the baz level which is only applied to messages sent to the baz
output.

If we wanted each message to go to a single output then we could use the
'round_robin' broker pattern, or the 'greedy' broker pattern if we wanted to
maximize throughput. For more information regarding these patterns please read
[the broker section](#broker).

### Multiplexing Outputs

It is possible to perform content based multiplexing of messages to specific
outputs using a broker with the 'fan_out' pattern and a
[filter processor](../processors/README.md#filter) on each output, which
is a processor that drops messages if the condition does not pass. Conditions
are content aware logical operators that can be combined using boolean logic.

For example, say we have an output 'foo' that we only want to receive messages
that contain the word 'foo', and an output 'bar' that we wish to send everything
that 'foo' doesn't receive, we can achieve that with this config:

``` yaml
output:
  type: broker
  broker:
    pattern: fan_out
    outputs:
    - type: foo
      foo:
        foo_field_1: value1
      processors:
      - type: filter
        filter:
          type: content
          content:
            operator: contains
            part: 0
            arg: foo
    - type: bar
      bar:
        bar_field_1: value2
        bar_field_2: value3
      processors:
      - type: filter
        filter:
          type: not
          not:
            type: content
            content:
              operator: contains
              part: 0
              arg: foo
```

For more information regarding conditions, including a full list of available
conditions please [read the docs here](../conditions/README.md)

### Contents

1. [`amazon_s3`](#amazon_s3)
2. [`amazon_sqs`](#amazon_sqs)
3. [`amqp`](#amqp)
4. [`broker`](#broker)
5. [`dynamic`](#dynamic)
6. [`elasticsearch`](#elasticsearch)
7. [`file`](#file)
8. [`files`](#files)
9. [`http_client`](#http_client)
10. [`http_server`](#http_server)
11. [`kafka`](#kafka)
12. [`mqtt`](#mqtt)
13. [`nats`](#nats)
14. [`nats_stream`](#nats_stream)
15. [`nsq`](#nsq)
16. [`redis_list`](#redis_list)
17. [`redis_pubsub`](#redis_pubsub)
18. [`scalability_protocols`](#scalability_protocols)
19. [`stdout`](#stdout)
20. [`websocket`](#websocket)
21. [`zmq4`](#zmq4)

## `amazon_s3`

``` yaml
type: amazon_s3
amazon_s3:
  bucket: ""
  credentials:
    id: ""
    role: ""
    secret: ""
    token: ""
  path: ${!count:files}-${!timestamp_unix_nano}.txt
  region: eu-west-1
  timeout_s: 5
```

Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded
with the path specified with the 'path' field, in order to have a different path
for each object you should use function interpolations described
[here](../config_interpolation.md#functions).

## `amazon_sqs`

``` yaml
type: amazon_sqs
amazon_sqs:
  credentials:
    id: ""
    role: ""
    secret: ""
    token: ""
  region: eu-west-1
  url: ""
```

Sends messages to an SQS queue.

## `amqp`

``` yaml
type: amqp
amqp:
  exchange: benthos-exchange
  exchange_type: direct
  key: benthos-key
  url: amqp://guest:guest@localhost:5672/
```

AMQP (0.91) is the underlying messaging protocol that is used by various message
brokers, including RabbitMQ.

## `broker`

``` yaml
type: broker
broker:
  copies: 1
  outputs: []
  pattern: fan_out
```

The broker output type allows you to configure multiple output targets following
a broker pattern from this list:

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

### Utilising More Outputs

When using brokered outputs with patterns such as round robin or greedy it is
possible to have multiple messages in-flight at the same time. In order to fully
utilise this you either need to have a greater number of input sources than
output sources [or use a buffer](../buffers/README.md).

### Processors

It is possible to configure [processors](../processors/README.md) at the broker
level, where they will be applied to _all_ child outputs, as well as on the
individual child outputs. If you have processors at both the broker level _and_
on child outputs then the broker processors will be applied _after_ the child
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
HTTP interface. The broker pattern used is 'fan_out', meaning each message will
be delivered to each dynamic output.

To GET a JSON map of output identifiers with their current uptimes use the
'/outputs' endpoint.

To perform CRUD actions on the outputs themselves use POST, DELETE, and GET
methods on the '/output/{output_id}' endpoint. When using POST the body of the
request should be a JSON configuration for the output, if the output already
exists it will be changed.

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
  timeout_ms: 5000
  urls:
  - http://localhost:9200
```

Publishes messages into an Elasticsearch index as documents. This output
currently does not support creating the target index.

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
using function interpolations on the 'path' field as described
[here](../config_interpolation.md#functions).

## `http_client`

``` yaml
type: http_client
http_client:
  basic_auth:
    enabled: false
    password: ""
    username: ""
  content_type: application/octet-stream
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
```

The HTTP client output type connects to a server and sends POST requests for
each message. The body of the request is the raw message contents. The output
will apply back pressure until a 2XX response has been returned from the server.

For more information about sending HTTP messages, including details on sending
multipart, please read the 'docs/using_http.md' document.

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
  target_version: 0.8.2.0
  timeout_ms: 5000
  topic: benthos_stream
```

The kafka output type writes messages to a kafka broker, these messages are
acknowledged, which is propagated back to the input. The config field
'ack_replicas' determines whether we wait for acknowledgement from all replicas
or just a single broker.

It is possible to specify a compression codec to use out of the following
options: none, snappy, lz4 and gzip.

If the field 'key' is not empty then each message will be given its contents as
a key. This field can be dynamically set using function interpolations described
[here](../config_interpolation.md#functions).

By default the paritioner will select partitions based on a hash of the key
value. If the key is empty then a partition is chosen at random. You can
alternatively force the partitioner to round-robin partitions with the field
'round_robin_partitions'.

The target version by default will be the oldest supported, as it is expected
that the server will be backwards compatible. In order to support newer client
features you should increase this version up to the known version of the target
server.

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

Publish to a NATS Stream subject. NATS Streaming is at-least-once and therefore
this output is able to guarantee delivery on success.

## `nsq`

``` yaml
type: nsq
nsq:
  max_in_flight: 100
  nsqd_tcp_address: localhost:4150
  topic: benthos_messages
  user_agent: benthos_producer
```

Publish to an NSQ topic.

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

## `scalability_protocols`

``` yaml
type: scalability_protocols
scalability_protocols:
  bind: false
  poll_timeout_ms: 5000
  socket_type: PUSH
  urls:
  - tcp://localhost:5556
```

The scalability protocols are common communication patterns which will be
familiar to anyone accustomed to service messaging protocols.

This outnput type should be compatible with any implementation of these
protocols, but nanomsg (http://nanomsg.org/index.html) is the specific target of
this type.

Since scale proto messages are only single part we would need a binary format
for sending multi part messages. We can use the benthos binary format for this
purpose. However, this format may appear to be gibberish to other services. If
you want to use the binary format you can set 'benthos_multi' to true.

Currently only PUSH and PUB sockets are supported.

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

## `websocket`

``` yaml
type: websocket
websocket:
  url: ws://localhost:4195/post/ws
```

Sends messages to an HTTP server via a websocket connection.

## `zmq4`

``` yaml
type: zmq4
zmq4:
  bind: true
  high_water_mark: 0
  poll_timeout_ms: 5000
  socket_type: PUSH
  urls:
  - tcp://*:5556
```

The zmq4 output type attempts to send messages to a ZMQ4 port, currently only
PUSH and PUB sockets are supported.
