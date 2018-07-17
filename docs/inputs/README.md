Inputs
======

This document was generated with `benthos --list-inputs`

An input is a source of data piped through an array of
[processors](../processors). Only one input is configured at the root of a
Benthos config. However, the input can be a [broker](#broker) which combines
multiple inputs. For example, if we wanted three inputs, a 'foo' a 'bar' and a
'baz' we could use the 'broker' input type at our root:

``` yaml
input:
  type: broker
  broker:
    inputs:
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
which will be applied to _all_ inputs, and we also have a processor at the baz
level which is only applied to messages from the baz input.

### Contents

1. [`amazon_s3`](#amazon_s3)
2. [`amazon_sqs`](#amazon_sqs)
3. [`amqp`](#amqp)
4. [`broker`](#broker)
5. [`dynamic`](#dynamic)
6. [`file`](#file)
7. [`files`](#files)
8. [`http_client`](#http_client)
9. [`http_server`](#http_server)
10. [`inproc`](#inproc)
11. [`kafka`](#kafka)
12. [`kafka_balanced`](#kafka_balanced)
13. [`mqtt`](#mqtt)
14. [`nats`](#nats)
15. [`nats_stream`](#nats_stream)
16. [`nsq`](#nsq)
17. [`read_until`](#read_until)
18. [`redis_list`](#redis_list)
19. [`redis_pubsub`](#redis_pubsub)
20. [`scalability_protocols`](#scalability_protocols)
21. [`stdin`](#stdin)
22. [`websocket`](#websocket)
23. [`zmq4`](#zmq4)

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
  delete_objects: false
  prefix: ""
  region: eu-west-1
  sqs_body_path: Records.s3.object.key
  sqs_envelope_path: ""
  sqs_max_messages: 10
  sqs_url: ""
  timeout_s: 5
```

Downloads objects in an Amazon S3 bucket, optionally filtered by a prefix. If an
SQS queue has been configured then only object keys read from the queue will be
downloaded. Otherwise, the entire list of objects found when this input is
created will be downloaded. Note that the prefix configuration is only used when
downloading objects without SQS configured.

If your bucket is configured to send events directly to an SQS queue then you
need to set the 'sqs_body_path' field to where the object key is found in the
payload. However, it is also common practice to send bucket events to an SNS
topic which sends enveloped events to SQS, in which case you must also set the
'sqs_envelope_path' field to where the payload can be found.

Here is a guide for setting up an SQS queue that receives events for new S3
bucket objects:

https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html

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
  timeout_s: 5
  url: ""
```

Receive messages from an Amazon SQS URL, only the body is extracted into
messages.

## `amqp`

``` yaml
type: amqp
amqp:
  consumer_tag: benthos-consumer
  exchange: benthos-exchange
  exchange_type: direct
  key: benthos-key
  prefetch_count: 10
  prefetch_size: 0
  queue: benthos-queue
  url: amqp://guest:guest@localhost:5672/
```

AMQP (0.91) is the underlying messaging protocol that is used by various message
brokers, including RabbitMQ.

Exchange type options are: direct|fanout|topic|x-custom

## `broker`

``` yaml
type: broker
broker:
  copies: 1
  inputs: []
```

The broker type allows you to combine multiple inputs, where each input will be
read in parallel. A broker type is configured with its own list of input
configurations and a field to specify how many copies of the list of inputs
should be created.

Adding more input types allows you to merge streams from multiple sources into
one. For example, reading from both RabbitMQ and Kafka:

``` yaml
type: broker
broker:
  copies: 1
  inputs:
  - type: amqp
    amqp:
      url: amqp://guest:guest@localhost:5672/
      consumer_tag: benthos-consumer
      exchange: benthos-exchange
      exchange_type: direct
      key: benthos-key
      queue: benthos-queue
  - type: kafka
    kafka:
      addresses:
      - localhost:9092
      client_id: benthos_kafka_input
      consumer_group: benthos_consumer_group
      partition: 0
      topic: benthos_stream

```

If the number of copies is greater than zero the list will be copied that number
of times. For example, if your inputs were of type foo and bar, with 'copies'
set to '2', you would end up with two 'foo' inputs and two 'bar' inputs.

Sometimes you will want several inputs of similar configuration. For this
purpose you can use the special type 'ditto', which duplicates the previous
config and applies selective changes.

For example, if combining two Kafka inputs with mostly the same set up but
reading different partitions you can use this shortcut:

``` yaml
inputs:
- type: kafka
  kafka:
    addresses:
      - localhost:9092
    client_id: benthos_kafka_input
    consumer_group: benthos_consumer_group
    topic: benthos_stream
    partition: 0
- type: ditto
  kafka:
    partition: 1
```

Which will result in two inputs targeting the same Kafka brokers, on the same
consumer group etc, but consuming their own partitions.

Ditto can also be specified with a multiplier, which is useful if you want
multiple inputs that do not differ in config, like this:

``` yaml
inputs:
- type: kafka_balanced
  kafka:
    addresses:
      - localhost:9092
    client_id: benthos_kafka_input
    consumer_group: benthos_consumer_group
    topic: benthos_stream
- type: ditto_3
```

Which results in a total of four kafka_balanced inputs. Note that ditto_0 will
result in no duplicate configs, this might be useful if the config is generated
and there's a chance you won't want any duplicates.

### Processors

It is possible to configure [processors](../processors/README.md) at the broker
level, where they will be applied to _all_ child inputs, as well as on the
individual child inputs. If you have processors at both the broker level _and_
on child inputs then the broker processors will be applied _after_ the child
nodes processors.

## `dynamic`

``` yaml
type: dynamic
dynamic:
  inputs: {}
  prefix: ""
  timeout_ms: 5000
```

The dynamic type is a special broker type where the inputs are identified by
unique labels and can be created, changed and removed during runtime via a REST
HTTP interface.

To GET a JSON map of input identifiers with their current uptimes use the
'/inputs' endpoint.

To perform CRUD actions on the inputs themselves use POST, DELETE, and GET
methods on the '/input/{input_id}' endpoint. When using POST the body of the
request should be a JSON configuration for the input, if the input already
exists it will be changed.

## `file`

``` yaml
type: file
file:
  delimiter: ""
  max_buffer: 1e+06
  multipart: false
  path: ""
```

The file type reads input from a file. If multipart is set to false each line
is read as a separate message. If multipart is set to true each line is read as
a message part, and an empty line indicates the end of a message.

If the delimiter field is left empty then line feed (\n) is used.

## `files`

``` yaml
type: files
files:
  path: ""
```

Reads files from a path, where each discrete file will be consumed as a single
message payload. The path can either point to a single file (resulting in only a
single message) or a directory, in which case the directory will be walked and
each file found will become a message.

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
  payload: ""
  retries: 3
  retry_period_ms: 1000
  skip_cert_verify: false
  stream:
    delimiter: ""
    enabled: false
    max_buffer: 1e+06
    multipart: false
    reconnect: true
  timeout_ms: 5000
  url: http://localhost:4195/get
  verb: GET
```

The HTTP client input type connects to a server and continuously performs
requests for a single message.

You should set a sensible retry period and max backoff so as to not flood your
target server.

### Streaming

If you enable streaming then Benthos will consume the body of the response as a
line delimited list of message parts. Each part is read as an individual message
unless multipart is set to true, in which case an empty line indicates the end
of a message.

## `http_server`

``` yaml
type: http_server
http_server:
  address: ""
  cert_file: ""
  key_file: ""
  path: /post
  timeout_ms: 5000
  ws_path: /post/ws
```

Receive messages POSTed over HTTP(S). HTTP 2.0 is supported when using TLS,
which is enabled when key and cert files are specified.

You can leave the 'address' config field blank in order to use the instance wide
HTTP server.

## `inproc`

``` yaml
type: inproc
inproc: ""
```

Directly connect to an output within a Benthos process by referencing it by a
chosen ID. This allows you to hook up isolated streams whilst running Benthos in
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
  addresses:
  - localhost:9092
  client_id: benthos_kafka_input
  consumer_group: benthos_consumer_group
  partition: 0
  skip_cert_verify: false
  start_from_oldest: true
  target_version: 1.0.0
  tls_enable: false
  topic: benthos_stream
```

Connects to a kafka (0.8+) server. Offsets are managed within kafka as per the
consumer group (set via config). Only one partition per input is supported, if
you wish to balance partitions across a consumer group look at the
'kafka_balanced' input type instead.

The target version by default will be the oldest supported, as it is expected
that the server will be backwards compatible. In order to support newer client
features you should increase this version up to the known version of the target
server.

## `kafka_balanced`

``` yaml
type: kafka_balanced
kafka_balanced:
  addresses:
  - localhost:9092
  client_id: benthos_kafka_input
  consumer_group: benthos_consumer_group
  skip_cert_verify: false
  start_from_oldest: true
  tls_enable: false
  topics:
  - benthos_stream
```

Connects to a kafka (0.9+) server. Offsets are managed within kafka as per the
consumer group (set via config), and partitions are automatically balanced
across any members of the consumer group.

## `mqtt`

``` yaml
type: mqtt
mqtt:
  client_id: benthos_input
  qos: 1
  topics:
  - benthos_topic
  urls:
  - tcp://localhost:1883
```

Subscribe to topics on MQTT brokers

## `nats`

``` yaml
type: nats
nats:
  subject: benthos_messages
  urls:
  - nats://localhost:4222
```

Subscribe to a NATS subject. NATS is at-most-once, if you need at-least-once
behaviour then look at NATS Stream.

The urls can contain username/password semantics. e.g.
nats://derek:pass@localhost:4222

## `nats_stream`

``` yaml
type: nats_stream
nats_stream:
  client_id: benthos_client
  cluster_id: test-cluster
  durable_name: benthos_offset
  queue: benthos_queue
  start_from_oldest: true
  subject: benthos_messages
  urls:
  - nats://localhost:4222
```

Subscribe to a NATS Stream subject, which is at-least-once. Joining a queue is
optional and allows multiple clients of a subject to consume using queue
semantics.

Tracking and persisting offsets through a durable name is also optional and
works with or without a queue. If a durable name is not provided then subjects
are consumed from the most recently published message.

## `nsq`

``` yaml
type: nsq
nsq:
  channel: benthos_stream
  lookupd_http_addresses:
  - localhost:4161
  max_in_flight: 100
  nsqd_tcp_addresses:
  - localhost:4150
  topic: benthos_messages
  user_agent: benthos_consumer
```

Subscribe to an NSQ instance topic and channel.

## `read_until`

``` yaml
type: read_until
read_until:
  condition:
    type: text
    text:
      arg: ""
      operator: equals_cs
      part: 0
  input: {}
  restart_input: false
```

Reads from an input and tests a condition on each message. When the condition
returns true the message is sent out and the input is closed. Use this type to
define inputs where the stream should end once a certain message appears.

Sometimes inputs close themselves. For example, when the `file` input
type reaches the end of a file it will shut down. By default this type will also
shut down. If you wish for the input type to be restarted every time it shuts
down until the condition is met then set `restart_input` to `true`.

## `redis_list`

``` yaml
type: redis_list
redis_list:
  key: benthos_list
  timeout_ms: 5000
  url: tcp://localhost:6379
```

Pops messages from the beginning of a Redis list using the BLPop command.

## `redis_pubsub`

``` yaml
type: redis_pubsub
redis_pubsub:
  channels:
  - benthos_chan
  url: tcp://localhost:6379
```

Redis supports a publish/subscribe model, it's possible to subscribe to multiple
channels using this input.

## `scalability_protocols`

``` yaml
type: scalability_protocols
scalability_protocols:
  bind: true
  poll_timeout_ms: 5000
  reply_timeout_ms: 5000
  socket_type: PULL
  sub_filters: []
  urls:
  - tcp://*:5555
```

The scalability protocols are common communication patterns. This input should
be compatible with any implementation, but specifically targets Nanomsg.

Currently only PULL and SUB sockets are supported.

## `stdin`

``` yaml
type: stdin
stdin:
  delimiter: ""
  max_buffer: 1e+06
  multipart: false
```

The stdin input simply reads any data piped to stdin as messages. By default the
messages are assumed single part and are line delimited. If the multipart option
is set to true then lines are interpretted as message parts, and an empty line
indicates the end of the message.

If the delimiter field is left empty then line feed (\n) is used.

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
  open_message: ""
  url: ws://localhost:4195/get/ws
```

Sends messages to an HTTP server via a websocket connection.

## `zmq4`

``` yaml
type: zmq4
zmq4:
  bind: false
  high_water_mark: 0
  poll_timeout_ms: 5000
  socket_type: PULL
  sub_filters: []
  urls:
  - tcp://localhost:5555
```

ZMQ4 is supported but currently depends on C bindings. Since this is an
annoyance when building or using Benthos it is not compiled by default.

Build it into your project by getting CZMQ installed on your machine, then build
with the tag: 'go install -tags "ZMQ4" github.com/Jeffail/benthos/cmd/...'

ZMQ4 input supports PULL and SUB sockets only. If there is demand for other
socket types then they can be added easily.
