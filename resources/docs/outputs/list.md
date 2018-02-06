OUTPUTS
=======

This document has been generated with `benthos --list-outputs`.

## `amqp`

AMQP (0.91) is the underlying messaging protocol that is used by various message
brokers, including RabbitMQ.

## `fan_out`

The fan out output type allows you to configure multiple output targets. With
the fan out model all outputs will be sent every message that passes through
benthos.

This process is blocking, meaning if any output applies backpressure then it
will block all outputs from receiving messages. If an output fails to guarantee
receipt of a message it will be tried again until success.

If Benthos is stopped during a fan out send it is possible that when started
again it will send a duplicate message to some outputs.

## `file`

The file output type simply appends all messages to an output file. Single part
messages are printed with a line separator '\n'. Multipart messages are written
with each part line separated, with the final part followed by two line
separators, e.g. a multipart message [ "foo", "bar", "baz" ] would be written
as:

foo\n
bar\n
baz\n\n

You can alternatively specify a custom delimiter that will follow the same rules
as '\n' above.

## `http_client`

The HTTP client output type connects to a server and sends POST requests for
each message. The body of the request is the raw message contents. The output
will apply back pressure until a 2XX response has been returned from the server.

For more information about sending HTTP messages, including details on sending
multipart, please read the 'docs/using_http.md' document.

## `http_server`

Sets up an HTTP server that will send messages over HTTP(S) GET requests. HTTP
2.0 is supported when using TLS, which is enabled when key and cert files are
specified.

You can leave the 'address' config field blank in order to use the default
service, but this will ignore TLS options.

You can receive a single, discrete message on the configured 'path' endpoint, or
receive a constant stream of line delimited messages on the configured
'stream_path' endpoint.

## `kafka`

The kafka output type writes messages to a kafka broker, these messages are
acknowledged, which is propagated back to the input. The config field
'ack_replicas' determines whether we wait for acknowledgement from all replicas
or just a single broker.

## `nats`

Publish to an NATS subject. NATS is at-most-once, so delivery is not guaranteed.
For at-least-once behaviour with NATS look at NATS Stream.

## `nats_stream`

Publish to a NATS Stream subject. NATS Streaming is at-least-once and therefore
this output is able to guarantee delivery on success.

## `nsq`

Publish to an NSQ topic.

## `redis_list`

Pushes messages onto the end of a Redis list (which is created if it doesn't
already exist) using the RPUSH command.

## `redis_pubsub`

Publishes messages through the Redis PubSub model. It is not possible to
guarantee that messages have been received.

## `round_robin`

The round robin output type allows you to send messages across multiple outputs,
where each message is sent to exactly one output following a strict order.

If an output applies back pressure this will also block other outputs from
receiving content.

## `scalability_protocols`

The scalability protocols are common communication patterns which will be
familiar to anyone accustomed to service messaging protocols.

This outnput type should be compatible with any implementation of these
protocols, but nanomsg (http://nanomsg.org/index.html) is the specific target of
this type.

Since scale proto messages are only single part we would need a binary format
for sending multi part messages. We can use the benthos binary format for this
purpose. However, this format may appear to be gibberish to other services. If
you want to use the binary format you can set 'benthos_multi' to true.

Currently only PUSH, PUB and REQ sockets are supported.

When using REQ sockets Benthos will expect acknowledgement from the consumer
that the message has been successfully propagated downstream. This comes in the
form of an expected response which is set by the 'reply_success' configuration
field.

If the reply from a REQ message is either not returned within the
'reply_timeout_ms' period, or if the reply does not match our 'reply_success'
string, then the message is considered lost and will be sent again.

## `stdout`

The stdout output type prints messages to stdout. Single part messages are
printed with a line separator '\n'. Multipart messages are written with each
part line separated, with the final part followed by two line separators, e.g.
a multipart message [ "foo", "bar", "baz" ] would be written as:

foo\n
bar\n
baz\n\n

You can alternatively specify a custom delimiter that will follow the same rules
as '\n' above.

## `zmq4`

The zmq4 output type attempts to send messages to a ZMQ4 port, currently only
PUSH and PUB sockets are supported.
