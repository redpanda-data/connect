INPUTS
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

## `amazon_s3`

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

Receive messages from an Amazon SQS URL, only the body is extracted into
messages.

## `amqp`

AMQP (0.91) is the underlying messaging protocol that is used by various message
brokers, including RabbitMQ.

Exchange type options are: direct|fanout|topic|x-custom

## `broker`

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

The file type reads input from a file. If multipart is set to false each line
is read as a separate message. If multipart is set to true each line is read as
a message part, and an empty line indicates the end of a message.

Alternatively, a custom delimiter can be set that is used instead of line
breaks.

## `http_client`

The HTTP client input type connects to a server and continuously performs
requests for a single message.

You should set a sensible number of max retries and retry delays so as to not
stress your target server.

### Streaming

If you enable streaming then Benthos will consume the body of the response as a
line delimited list of message parts. Each part is read as an individual message
unless multipart is set to true, in which case an empty line indicates the end
of a message.

For more information about sending HTTP messages, including details on sending
multipart, please read the 'docs/using_http.md' document.

## `http_server`

Receive messages POSTed over HTTP(S). HTTP 2.0 is supported when using TLS,
which is enabled when key and cert files are specified.

You can leave the 'address' config field blank in order to use the default
service, but this will ignore TLS options.

## `kafka`

Connects to a kafka (0.8+) server. Offsets are managed within kafka as per the
consumer group (set via config). Only one partition per input is supported, if
you wish to balance partitions across a consumer group look at the
'kafka_balanced' input type instead.

The target version by default will be the oldest supported, as it is expected
that the server will be backwards compatible. In order to support newer client
features you should increase this version up to the known version of the target
server.

## `kafka_balanced`

Connects to a kafka (0.9+) server. Offsets are managed within kafka as per the
consumer group (set via config), and partitions are automatically balanced
across any members of the consumer group.

## `mqtt`

Subscribe to topics on MQTT brokers

## `nats`

Subscribe to a NATS subject. NATS is at-most-once, if you need at-least-once
behaviour then look at NATS Stream.

The urls can contain username/password semantics. e.g.
nats://derek:pass@localhost:4222

## `nats_stream`

Subscribe to a NATS Stream subject, which is at-least-once. Joining a queue is
optional and allows multiple clients of a subject to consume using queue
semantics.

Tracking and persisting offsets through a durable name is also optional and
works with or without a queue. If a durable name is not provided then subjects
are consumed from the most recently published message.

## `nsq`

Subscribe to an NSQ instance topic and channel.

## `read_until`

Reads from an input and tests a condition on each message. When the condition
returns true the message is sent out and the input is closed. Use this type to
define inputs where the stream should end once a certain message appears.

## `redis_list`

Pops messages from the beginning of a Redis list using the BLPop command.

## `redis_pubsub`

Redis supports a publish/subscribe model, it's possible to subscribe to multiple
channels using this input.

## `scalability_protocols`

The scalability protocols are common communication patterns which will be
familiar to anyone accustomed to service messaging protocols.

This input type should be compatible with any implementation of these protocols,
but nanomsg (http://nanomsg.org/index.html) is the specific target of this type.

Since scale proto messages are only single part we would need a binary format
for interpretting multi part messages. If the input is receiving messages from a
benthos output you can set both to use the benthos binary multipart format with
the 'benthos_multi' flag. Note, however, that this format may appear to be
gibberish to other services, and the input will be unable to read normal
messages with this setting.

Currently only PULL and SUB sockets are supported.

## `stdin`

The stdin input simply reads any data piped to stdin as messages. By default the
messages are assumed single part and are line delimited. If the multipart option
is set to true then lines are interpretted as message parts, and an empty line
indicates the end of the message.

Alternatively, a custom delimiter can be set that is used instead of line
breaks.

## `zmq4`

ZMQ4 is supported but currently depends on C bindings. Since this is an
annoyance when building or using Benthos it is not compiled by default.

Build it into your project by getting CZMQ installed on your machine, then build
with the tag: 'go install -tags "ZMQ4" github.com/Jeffail/benthos/cmd/...'

ZMQ4 input supports PULL and SUB sockets only. If there is demand for other
socket types then they can be added easily.
