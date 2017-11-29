INPUTS
======

This document has been generated with `benthos --list-inputs`.

## `amqp`

AMQP (0.91) is the underlying messaging protocol that is used by various message
brokers, including RabbitMQ.

Exchange type options are: direct|fanout|topic|x-custom

## `fan_in`

The fan in type allows you to combine multiple inputs. Each input will be read
in parallel. In order to configure a fan in type you simply add an array of
input configuration objects into the inputs field.

Adding more input types allows you to merge streams from multiple sources into
one. For example, having both a ZMQ4 PULL socket and a Nanomsg PULL socket:

``` yaml
type: fan_in
fan_in:
  inputs:
  -
    type: scalability_protocols
    scalability_protocols:
      address: tcp://nanoserver:3003
      bind_address: false
      socket_type: PULL
  -
    type: zmq4
    zmq4:
      addresses:
      - tcp://zmqserver:3004
      socket_type: PULL
```

Sometimes you will want several inputs of very similar configuration. You can
use the special type ditto in this case to duplicate the previous config and
apply selective changes.

For example, if combining two kafka inputs with the same set up, reading
different partitions you can use this shortcut:

``` yaml
inputs:
-
  type: kafka
  kafka:
    addresses:
      - localhost:9092
    client_id: benthos_kafka_input
    consumer_group: benthos_consumer_group
    topic: benthos_stream
    partition: 0
-
  type: ditto
  kafka:
    partition: 1
```

Which will result in two inputs targeting the same kafka brokers, on the same
consumer group etc, but consuming their own partitions. Ditto can also be
specified with a multiplier, which is useful if you want multiple inputs that do
not differ in config, like this:

``` yaml
inputs:
-
  type: kafka_balanced
  kafka:
    addresses:
      - localhost:9092
    client_id: benthos_kafka_input
    consumer_group: benthos_consumer_group
    topic: benthos_stream
-
  type: ditto_3
```

Which results in a total of four kafka_balanced inputs.

## `file`

The file type reads input from a file. If multipart is set to false each line
is read as a separate message. If multipart is set to true each line is read as
a message part, and an empty line indicates the end of a message.

Alternatively, a custom delimiter can be set that is used instead of line
breaks.

## `http_server`

In order to receive messages over HTTP Benthos hosts a server. Messages should
be sent as a POST request. HTTP 2.0 is supported when using TLS, which is
enabled when key and cert files are specified.

## `kafka`

Connects to a kafka (0.8+) server. Offsets are managed within kafka as per the
consumer group (set via config). Only one partition per input is supported, if
you wish to balance partitions across a consumer group look at the
'kafka_balanced' input type instead.

## `kafka_balanced`

Connects to a kafka (0.9+) server. Offsets are managed within kafka as per the
consumer group (set via config), and partitions are automatically balanced
across any members of the consumer group.

## `nats`

Subscribe to a NATS subject.

The url can contain username/password semantics. e.g.
nats://derek:pass@localhost:4222

Comma separated arrays are also supported, e.g. urlA, urlB.

## `nsq`

Subscribe to an NSQ instance topic and channel.

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

Currently only PULL, SUB, and REP sockets are supported.

When using REP sockets Benthos will respond to each request with a success or
error message. The content of these messages are set with the 'reply_success'
and 'reply_error' config options respectively. The 'reply_timeout_ms' option
decides how long Benthos will wait before giving up on the reply, which can
result in duplicate messages when triggered.

## `stdin`

The stdin input simply reads any data piped to stdin as messages. By default the
messages are assumed single part and are line delimited. If the multipart option
is set to true then lines are interpretted as message parts, and an empty line
indicates the end of the message.

Alternatively, a custom delimiter can be set that is used instead of line
breaks.
