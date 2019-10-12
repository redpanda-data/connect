Inputs
======

This document was generated with `benthos --list-inputs`

An input is a source of data piped through an array of optional
[processors](../processors). Only one input is configured at the root of a
Benthos config. However, the root input can be a [broker](#broker) which
combines multiple inputs.

An input config section looks like this:

``` yaml
input:
  type: foo
  foo:
    bar: baz
  processors:
  - type: qux
```

### Contents

1. [`amqp`](#amqp)
2. [`amqp_0_9`](#amqp_0_9)
3. [`broker`](#broker)
4. [`dynamic`](#dynamic)
5. [`file`](#file)
6. [`files`](#files)
7. [`gcp_pubsub`](#gcp_pubsub)
8. [`hdfs`](#hdfs)
9. [`http_client`](#http_client)
10. [`http_server`](#http_server)
11. [`inproc`](#inproc)
12. [`kafka`](#kafka)
13. [`kafka_balanced`](#kafka_balanced)
14. [`kinesis`](#kinesis)
15. [`kinesis_balanced`](#kinesis_balanced)
16. [`mqtt`](#mqtt)
17. [`nanomsg`](#nanomsg)
18. [`nats`](#nats)
19. [`nats_stream`](#nats_stream)
20. [`nsq`](#nsq)
21. [`read_until`](#read_until)
22. [`redis_list`](#redis_list)
23. [`redis_pubsub`](#redis_pubsub)
24. [`redis_streams`](#redis_streams)
25. [`s3`](#s3)
26. [`sqs`](#sqs)
27. [`stdin`](#stdin)
28. [`tcp`](#tcp)
29. [`tcp_server`](#tcp_server)
30. [`udp_server`](#udp_server)
31. [`websocket`](#websocket)

## `amqp`

``` yaml
type: amqp
amqp:
  bindings_declare: []
  consumer_tag: benthos-consumer
  max_batch_count: 1
  prefetch_count: 10
  prefetch_size: 0
  queue: benthos-queue
  queue_declare:
    durable: true
    enabled: false
  tls:
    client_certs: []
    enabled: false
    root_cas_file: ""
    skip_cert_verify: false
  url: amqp://guest:guest@localhost:5672/
```

DEPRECATED: This input is deprecated and scheduled for removal in Benthos V4.
Please use [`amqp_0_9`](#amqp_0_9) instead.

## `amqp_0_9`

``` yaml
type: amqp_0_9
amqp_0_9:
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
  bindings_declare: []
  consumer_tag: benthos-consumer
  prefetch_count: 10
  prefetch_size: 0
  queue: benthos-queue
  queue_declare:
    durable: true
    enabled: false
  tls:
    client_certs: []
    enabled: false
    root_cas_file: ""
    skip_cert_verify: false
  url: amqp://guest:guest@localhost:5672/
```

Connects to an AMQP (0.91) queue. AMQP is a messaging protocol used by various
message brokers, including RabbitMQ.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

Use the `batching` fields to configure an optional
[batching policy](../batching.md#batch-policy).

It's possible for this input type to declare the target queue by setting
`queue_declare.enabled` to `true`, if the queue already exists then
the declaration passively verifies that they match the target fields.

Similarly, it is possible to declare queue bindings by adding objects to the
`bindings_declare` array. Binding declare objects take the form of:

``` yaml
{
  "exchange": "benthos-exchange",
  "key": "benthos-key"
}
```

TLS is automatic when connecting to an `amqps` URL, but custom
settings can be enabled in the `tls` section.

### Metadata

This input adds the following metadata fields to each message:

``` text
- amqp_content_type
- amqp_content_encoding
- amqp_delivery_mode
- amqp_priority
- amqp_correlation_id
- amqp_reply_to
- amqp_expiration
- amqp_message_id
- amqp_timestamp
- amqp_type
- amqp_user_id
- amqp_app_id
- amqp_consumer_tag
- amqp_delivery_tag
- amqp_redelivered
- amqp_exchange
- amqp_routing_key
- All existing message headers, including nested headers prefixed with the key
  of their respective parent.
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `broker`

``` yaml
type: broker
broker:
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
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
input:
  broker:
    copies: 1
    inputs:
    - amqp:
        url: amqp://guest:guest@localhost:5672/
        consumer_tag: benthos-consumer
        queue: benthos-queue
    - kafka:
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

### Batching

It's possible to configure a [batch policy](../batching.md#batch-policy) with a
broker using the `batching` fields. When doing this the feeds from all
child inputs are combined. Some inputs do not support broker based batching and
specify this in their documentation.

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
  timeout: 5s
```

The dynamic type is a special broker type where the inputs are identified by
unique labels and can be created, changed and removed during runtime via a REST
HTTP interface.

To GET a JSON map of input identifiers with their current uptimes use the
`/inputs` endpoint.

To perform CRUD actions on the inputs themselves use POST, DELETE, and GET
methods on the `/inputs/{input_id}` endpoint. When using POST the body
of the request should be a JSON configuration for the input, if the input
already exists it will be changed.

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

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

### Metadata

This input adds the following metadata fields to each message:

``` text
- path
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `gcp_pubsub`

``` yaml
type: gcp_pubsub
gcp_pubsub:
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
  max_batch_count: 1
  max_outstanding_bytes: 1000000000
  max_outstanding_messages: 1000
  project: ""
  subscription: ""
```

Consumes messages from a GCP Cloud Pub/Sub subscription.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

Use the `batching` fields to configure an optional
[batching policy](../batching.md#batch-policy).

### Metadata

This input adds the following metadata fields to each message:

``` text
- gcp_pubsub_publish_time_unix
- All message attributes
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `hdfs`

``` yaml
type: hdfs
hdfs:
  directory: ""
  hosts:
  - localhost:9000
  user: benthos_hdfs
```

Reads files from a HDFS directory, where each discrete file will be consumed as
a single message payload.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

### Metadata

This input adds the following metadata fields to each message:

``` text
- hdfs_name
- hdfs_path
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

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
  payload: ""
  rate_limit: ""
  retries: 3
  retry_period: 1s
  stream:
    delimiter: ""
    enabled: false
    max_buffer: 1e+06
    multipart: false
    reconnect: true
  timeout: 5s
  tls:
    client_certs: []
    enabled: false
    root_cas_file: ""
    skip_cert_verify: false
  url: http://localhost:4195/get
  verb: GET
```

The HTTP client input type connects to a server and continuously performs
requests for a single message.

You should set a sensible retry period and max backoff so as to not flood your
target server.

The URL and header values of this type can be dynamically set using function
interpolations described [here](../config_interpolation.md#functions).

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
  rate_limit: ""
  timeout: 5s
  ws_path: /post/ws
  ws_rate_limit_message: ""
  ws_welcome_message: ""
```

Receive messages POSTed over HTTP(S). HTTP 2.0 is supported when using TLS,
which is enabled when key and cert files are specified.

You can leave the 'address' config field blank in order to use the instance wide
HTTP server.

The field `rate_limit` allows you to specify an optional
[`rate_limit` resource](../rate_limits/README.md), which will be
applied to each HTTP request made and each websocket payload received.

When the rate limit is breached HTTP requests will have a 429 response returned
with a Retry-After header. Websocket payloads will be dropped and an optional
response payload will be sent as per `ws_rate_limit_message`.

### Responses

It's possible to return a response for each message received using
[synchronous responses](../sync_responses.md).

### Endpoints

The following fields specify endpoints that are registered for sending messages:

#### `path` (defaults to `/post`)

This endpoint expects POST requests where the entire request body is consumed as
a single message.

If the request contains a multipart `content-type` header as per
[rfc1341](https://www.w3.org/Protocols/rfc1341/7_2_Multipart.html) then the
multiple parts are consumed as a batch of messages, where each body part is a
message of the batch.

#### `ws_path` (defaults to `/post/ws`)

Creates a websocket connection, where payloads received on the socket are passed
through the pipeline as a batch of one message.

You may specify an optional `ws_welcome_message`, which is a static
payload to be sent to all clients once a websocket connection is first
established.

It's also possible to specify a `ws_rate_limit_message`, which is a
static payload to be sent to clients that have triggered the servers rate limit.

### Metadata

This input adds the following metadata fields to each message:

``` text
- http_server_user_agent
- All headers (only first values are taken)
- All query parameters
- All cookies
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

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
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
  client_id: benthos_kafka_input
  commit_period: 1s
  consumer_group: benthos_consumer_group
  fetch_buffer_cap: 256
  max_batch_count: 1
  max_processing_period: 100ms
  partition: 0
  sasl:
    enabled: false
    password: ""
    user: ""
  start_from_oldest: true
  target_version: 1.0.0
  tls:
    client_certs: []
    enabled: false
    root_cas_file: ""
    skip_cert_verify: false
  topic: benthos_stream
```

Connects to a kafka (0.8+) server. Offsets are managed within kafka as per the
consumer group (set via config). Only one partition per input is supported, if
you wish to balance partitions across a consumer group look at the
`kafka_balanced` input type instead.

Use the `batching` fields to configure an optional
[batching policy](../batching.md#batch-policy). It is not currently possible to
use [broker based batching](../batching.md#combined-batching) with this input
type.

This input currently provides a single continuous feed of data, and therefore
by default will only utilise a single processing thread and parallel output.
Take a look at the
[pipelines documentation](../pipeline.md#single-consumer-without-buffer) for
guides on how to work around this.

The field `max_processing_period` should be set above the maximum
estimated time taken to process a message.

The target version by default will be the oldest supported, as it is expected
that the server will be backwards compatible. In order to support newer client
features you should increase this version up to the known version of the target
server.

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

### Metadata

This input adds the following metadata fields to each message:

``` text
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_lag
- kafka_timestamp_unix
- All existing message headers (version 0.11+)
```

The field `kafka_lag` is the calculated difference between the high
water mark offset of the partition at the time of ingestion and the current
message offset.

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `kafka_balanced`

``` yaml
type: kafka_balanced
kafka_balanced:
  addresses:
  - localhost:9092
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
  client_id: benthos_kafka_input
  commit_period: 1s
  consumer_group: benthos_consumer_group
  fetch_buffer_cap: 256
  group:
    heartbeat_interval: 3s
    rebalance_timeout: 60s
    session_timeout: 10s
  max_batch_count: 1
  max_processing_period: 100ms
  sasl:
    enabled: false
    password: ""
    user: ""
  start_from_oldest: true
  target_version: 1.0.0
  tls:
    client_certs: []
    enabled: false
    root_cas_file: ""
    skip_cert_verify: false
  topics:
  - benthos_stream
```

Connects to a kafka (0.9+) server. Offsets are managed within kafka as per the
consumer group (set via config), and partitions are automatically balanced
across any members of the consumer group.

Partitions consumed by this input can be processed in parallel allowing it to
utilise <= N pipeline processing threads and parallel outputs where N is the
number of partitions allocated to this consumer.

The `batching` fields allow you to configure a
[batching policy](../batching.md#batch-policy) which will be applied per
partition. It is not currently possible to use
[broker based batching](../batching.md#combined-batching) with this input type.

The field `max_processing_period` should be set above the maximum
estimated time taken to process a message.

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

### Metadata

This input adds the following metadata fields to each message:

``` text
- kafka_key
- kafka_topic
- kafka_partition
- kafka_offset
- kafka_lag
- kafka_timestamp_unix
- All existing message headers (version 0.11+)
```

The field `kafka_lag` is the calculated difference between the high
water mark offset of the partition at the time of ingestion and the current
message offset.

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `kinesis`

``` yaml
type: kinesis
kinesis:
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
  client_id: benthos_consumer
  commit_period: 1s
  credentials:
    id: ""
    profile: ""
    role: ""
    role_external_id: ""
    secret: ""
    token: ""
  dynamodb_table: ""
  endpoint: ""
  limit: 100
  region: eu-west-1
  shard: "0"
  start_from_oldest: true
  stream: ""
  timeout: 5s
```

Receive messages from a Kinesis stream.

It's possible to use DynamoDB for persisting shard iterators by setting the
table name. Offsets will then be tracked per `client_id` per
`shard_id`. When using this mode you should create a table with
`namespace` as the primary key and `shard_id` as a sort key.

Use the `batching` fields to configure an optional
[batching policy](../batching.md#batch-policy). It is not currently possible to
use [broker based batching](../batching.md#combined-batching) with this input
type.

This input currently provides a single continuous feed of data, and therefore
by default will only utilise a single processing thread and parallel output.
Take a look at the
[pipelines documentation](../pipeline.md#single-consumer-without-buffer) for
guides on how to work around this.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).

## `kinesis_balanced`

``` yaml
type: kinesis_balanced
kinesis_balanced:
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
  credentials:
    id: ""
    profile: ""
    role: ""
    role_external_id: ""
    secret: ""
    token: ""
  dynamodb_billing_mode: ""
  dynamodb_read_provision: 0
  dynamodb_table: ""
  dynamodb_write_provision: 0
  endpoint: ""
  max_batch_count: 1
  region: eu-west-1
  start_from_oldest: true
  stream: ""
```

BETA: This input is a beta component and is subject to change outside of major
version releases.

Receives messages from a Kinesis stream and automatically balances shards across
consumers.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

Use the `batching` fields to configure an optional
[batching policy](../batching.md#batch-policy).

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).

### Metadata

This input adds the following metadata fields to each message:

```text
- kinesis_shard
- kinesis_partition_key
- kinesis_sequence_number
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `mqtt`

``` yaml
type: mqtt
mqtt:
  clean_session: true
  client_id: benthos_input
  password: ""
  qos: 1
  topics:
  - benthos_topic
  urls:
  - tcp://localhost:1883
  user: ""
```

Subscribe to topics on MQTT brokers.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

### Metadata

This input adds the following metadata fields to each message:

``` text
- mqtt_duplicate
- mqtt_qos
- mqtt_retained
- mqtt_topic
- mqtt_message_id
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `nanomsg`

``` yaml
type: nanomsg
nanomsg:
  bind: true
  poll_timeout: 5s
  reply_timeout: 5s
  socket_type: PULL
  sub_filters: []
  urls:
  - tcp://*:5555
```

The scalability protocols are common communication patterns. This input should
be compatible with any implementation, but specifically targets Nanomsg.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

Currently only PULL and SUB sockets are supported.

## `nats`

``` yaml
type: nats
nats:
  prefetch_count: 32
  queue: benthos_queue
  subject: benthos_messages
  urls:
  - nats://127.0.0.1:4222
```

Subscribe to a NATS subject. NATS is at-most-once, if you need at-least-once
behaviour then look at NATS Stream.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

The urls can contain username/password semantics. e.g.
nats://derek:pass@localhost:4222

### Metadata

This input adds the following metadata fields to each message:

``` text
- nats_subject
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `nats_stream`

``` yaml
type: nats_stream
nats_stream:
  ack_wait: 30s
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
  client_id: benthos_client
  cluster_id: test-cluster
  durable_name: benthos_offset
  max_inflight: 1024
  queue: benthos_queue
  start_from_oldest: true
  subject: benthos_messages
  unsubscribe_on_close: false
  urls:
  - nats://localhost:4222
```

Subscribe to a NATS Stream subject, which is at-least-once. Joining a queue is
optional and allows multiple clients of a subject to consume using queue
semantics.

Tracking and persisting offsets through a durable name is also optional and
works with or without a queue. If a durable name is not provided then subjects
are consumed from the most recently published message.

When a consumer closes its connection it unsubscribes, when all consumers of a
durable queue do this the offsets are deleted. In order to avoid this you can
stop the consumers from unsubscribing by setting the field
`unsubscribe_on_close` to `false`.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

Use the `batching` fields to configure an optional
[batching policy](../batching.md#batch-policy).

### Metadata

This input adds the following metadata fields to each message:

``` text
- nats_stream_subject
- nats_stream_sequence
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `nsq`

``` yaml
type: nsq
nsq:
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
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

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

Use the `batching` fields to configure an optional
[batching policy](../batching.md#batch-policy).

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

Reads from an input and tests a condition on each message. Messages are read
continuously while the condition returns false, when the condition returns true
the message that triggered the condition is sent out and the input is closed.
Use this type to define inputs where the stream should end once a certain
message appears.

Sometimes inputs close themselves. For example, when the `file` input
type reaches the end of a file it will shut down. By default this type will also
shut down. If you wish for the input type to be restarted every time it shuts
down until the condition is met then set `restart_input` to `true`.

### Metadata

A metadata key `benthos_read_until` containing the value `final` is
added to the first part of the message that triggers to input to stop.

## `redis_list`

``` yaml
type: redis_list
redis_list:
  key: benthos_list
  timeout: 5s
  url: tcp://localhost:6379
```

Pops messages from the beginning of a Redis list using the BLPop command.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

## `redis_pubsub`

``` yaml
type: redis_pubsub
redis_pubsub:
  channels:
  - benthos_chan
  url: tcp://localhost:6379
  use_patterns: false
```

Redis supports a publish/subscribe model, it's possible to subscribe to multiple
channels using this input.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

In order to subscribe to channels using the `PSUBSCRIBE` command set
the field `use_patterns` to `true`, then you can include glob-style
patterns in your channel names. For example:

- `h?llo` subscribes to hello, hallo and hxllo
- `h*llo` subscribes to hllo and heeeello
- `h[ae]llo` subscribes to hello and hallo, but not hillo

Use `\` to escape special characters if you want to match them
verbatim.

## `redis_streams`

``` yaml
type: redis_streams
redis_streams:
  batching:
    byte_size: 0
    condition:
      type: static
      static: false
    count: 1
    period: ""
  body_key: body
  client_id: benthos_consumer
  commit_period: 1s
  consumer_group: benthos_group
  limit: 10
  start_from_oldest: true
  streams:
  - benthos_stream
  timeout: 5s
  url: tcp://localhost:6379
```

Pulls messages from Redis (v5.0+) streams with the XREADGROUP command. The
`client_id` should be unique for each consumer of a group.

The field `limit` specifies the maximum number of records to be
received per request. When more than one record is returned they are batched and
can be split into individual messages with the `split` processor.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

Use the `batching` fields to configure an optional
[batching policy](../batching.md#batch-policy).

Redis stream entries are key/value pairs, as such it is necessary to specify the
key that contains the body of the message. All other keys/value pairs are saved
as metadata fields.

## `s3`

``` yaml
type: s3
s3:
  bucket: ""
  credentials:
    id: ""
    profile: ""
    role: ""
    role_external_id: ""
    secret: ""
    token: ""
  delete_objects: false
  download_manager:
    enabled: true
  endpoint: ""
  force_path_style_urls: false
  max_batch_count: 1
  prefix: ""
  region: eu-west-1
  retries: 3
  sqs_body_path: Records.*.s3.object.key
  sqs_bucket_path: ""
  sqs_endpoint: ""
  sqs_envelope_path: ""
  sqs_max_messages: 10
  sqs_url: ""
  timeout: 5s
```

Downloads objects in an Amazon S3 bucket, optionally filtered by a prefix. If an
SQS queue has been configured then only object keys read from the queue will be
downloaded. Otherwise, the entire list of objects found when this input is
created will be downloaded. Note that the prefix configuration is only used when
downloading objects without SQS configured.

If the download manager is enabled this can help speed up file downloads but
results in file metadata not being copied.

If your bucket is configured to send events directly to an SQS queue then you
need to set the `sqs_body_path` field to a
[dot path](../field_paths.md) where the object key is found in the payload.
However, it is also common practice to send bucket events to an SNS topic which
sends enveloped events to SQS, in which case you must also set the
`sqs_envelope_path` field to where the payload can be found.

When using SQS events it's also possible to extract target bucket names from the
events by specifying a path in the field `sqs_bucket_path`. For each
SQS event, if that path exists and contains a string it will used as the bucket
of the download instead of the `bucket` field.

Here is a guide for setting up an SQS queue that receives events for new S3
bucket objects:

https://docs.aws.amazon.com/AmazonS3/latest/dev/ways-to-add-notification-config-to-bucket.html

WARNING: When using SQS please make sure you have sensible values for
`sqs_max_messages` and also the visibility timeout of the queue
itself.

When Benthos consumes an S3 item as a result of receiving an SQS message the
message is not deleted until the S3 item has been sent onwards. This ensures
at-least-once crash resiliency, but also means that if the S3 item takes longer
to process than the visibility timeout of your queue then the same items might
be processed multiple times.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).

### Metadata

This input adds the following metadata fields to each message:

```
- s3_key
- s3_bucket
- s3_last_modified_unix*
- s3_last_modified (RFC3339)*
- s3_content_type*
- s3_content_encoding*
- All user defined metadata*

* Only added when NOT using download manager
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

## `sqs`

``` yaml
type: sqs
sqs:
  credentials:
    id: ""
    profile: ""
    role: ""
    role_external_id: ""
    secret: ""
    token: ""
  delete_message: true
  endpoint: ""
  max_number_of_messages: 1
  region: eu-west-1
  timeout: 5s
  url: ""
```

Receive messages from an Amazon SQS URL, only the body is extracted into
messages.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

### Credentials

By default Benthos will use a shared credentials file when connecting to AWS
services. It's also possible to set them explicitly at the component level,
allowing you to transfer data across accounts. You can find out more
[in this document](../aws.md).

### Metadata

This input adds the following metadata fields to each message:

```text
- sqs_message_id
- sqs_receipt_handle
- sqs_approximate_receive_count
- All message attributes
```

You can access these metadata fields using
[function interpolation](../config_interpolation.md#metadata).

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

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

If the delimiter field is left empty then line feed (\n) is used.

## `tcp`

``` yaml
type: tcp
tcp:
  address: localhost:4194
  delimiter: ""
  max_buffer: 1e+06
  multipart: false
```

Connects to a TCP server and consumes a continuous stream of messages.

If multipart is set to false each line of data is read as a separate message. If
multipart is set to true each line is read as a message part, and an empty line
indicates the end of a message.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

If the delimiter field is left empty then line feed (\n) is used.

## `tcp_server`

``` yaml
type: tcp_server
tcp_server:
  address: 127.0.0.1:0
  delimiter: ""
  max_buffer: 1e+06
  multipart: false
```

Creates a server that receives messages over TCP. Each connection is parsed as a
continuous stream of line delimited messages.

If multipart is set to false each line of data is read as a separate message. If
multipart is set to true each line is read as a message part, and an empty line
indicates the end of a message.

If the delimiter field is left empty then line feed (\n) is used.

The field `max_buffer` specifies the maximum amount of memory to
allocate _per connection_ for buffering lines of data. If a line of data from a
connection exceeds this value then the connection will be closed.

## `udp_server`

``` yaml
type: udp_server
udp_server:
  address: 127.0.0.1:0
  delimiter: ""
  max_buffer: 1e+06
```

Creates a server that receives messages over UDP as a continuous stream of data.
Each line is interpretted as an individual message, if the delimiter field is
left empty then line feed (\n) is used.

The field `max_buffer` specifies the maximum amount of memory to
allocate for buffering lines of data, this must exceed the largest expected
message size.

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

Connects to a websocket server and continuously receives messages.

Messages consumed by this input can be processed in parallel, meaning a single
instance of this input can utilise any number of threads within a
`pipeline` section of a config.

It is possible to configure an `open_message`, which when set to a
non-empty string will be sent to the websocket server each time a connection is
first established.
