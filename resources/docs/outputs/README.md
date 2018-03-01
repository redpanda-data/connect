OUTPUTS
=======

This document has been generated with `benthos --list-outputs`.

## `amazon_s3`

Sends message parts as objects to an Amazon S3 bucket. Each object is uploaded
with the path specified with the 'path' field, in order to have a different path
for each object you should use function interpolations described
[here](../config_interpolation.md#functions).

## `amazon_sqs`

Sends messages to an SQS queue.

## `amqp`

AMQP (0.91) is the underlying messaging protocol that is used by various message
brokers, including RabbitMQ.

## `broker`

The broker output type allows you to configure multiple output targets following
a broker pattern from this list:

## `fan_out`

With the fan out pattern all outputs will be sent every message that passes
through Benthos. If an output applies back pressure it will block all subsequent
messages, and if an output fails to send a message it will be retried
continuously until completion or service shut down.

## `round_robin`

With the round robin pattern each message will be assigned a single output
following their order. If an output applies back pressure it will block all
subsequent messages. If an output fails to send a message then the message will
be re-attempted with the next input, and so on.

## `greedy`

The greedy pattern results in higher output throughput at the cost of
disproportionate message allocations to those outputs. Each message is sent to a
single output, and the output chosen is randomly selected only from outputs
ready to process a message. It is therefore possible for certain outputs to
receive a disproportionate number of messages depending on their throughput.

## `dynamic`

The dynamic type is a special broker type where the outputs are identified by
unique labels and can be created, changed and removed during runtime via a REST
HTTP interface.

To GET a JSON map of output identifiers with their current uptimes use the
'/outputs' endpoint.

To perform CRUD actions on the outputs themselves use POST, DELETE, and GET
methods on the '/output/{output_id}' endpoint. When using POST the body of the
request should be a JSON configuration for the output, if the output already
exists it will be changed.

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

## `files`

Writes each individual part of each message to a new file.

Message parts only contain raw data, and therefore in order to create a unique
file for each part you need to generate unique file names. This can be done by
using function interpolations on the 'path' field as described
[here](../config_interpolation.md#functions).

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

If the field 'key' is not empty then each message will be given its contents as
a key. This field can be dynamically set using function interpolations described
[here](../config_interpolation.md#functions).

By default the paritioner will select partitions based on a hash of the key
value. If the key is empty then a partition is chosen at random. You can
alternatively force the partitioner to round-robin partitions with the field
'round_robin_partitions'.

## `mqtt`

Pushes messages to an MQTT broker.

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

Currently only PUSH and PUB sockets are supported.

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
