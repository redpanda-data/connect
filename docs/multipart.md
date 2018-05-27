Multipart Messaging In Benthos
==============================

Benthos natively supports multipart messages, meaning they can be read,
processed, buffered, and written seamlessly. However, some inputs and outputs do
not support multipart and can therefore cause confusion.

Inputs that do not support multipart are easy, as they are simply read as
multipart messages with one part. Outputs, however, are more tricky.

An output that only supports single part messages (such as Kafka), which
receives a multipart message (perhaps from ZMQ), will output a unique message
per part. These parts are 'at-least-once', but if any part is unsuccessfully
sent it will mean the whole set of parts will be resent as message delivery can
only be guaranteed for the whole batch. We could potentially read those
individual parts and use the `combine` processor to 'squash' them back into a
multipart message, but such a system can be brittle.

Alternatively we can use the `archive` and `unarchive` processors, which squash
multipart messages into a single part and then expand them back out into
multiple parts respectively. This allows us to archive before sending on the
single part protocol, then unarchive back into the native multiple part
representation further in the pipeline.

The archive processor supports multiple algorithms including a binary blob
format and the `.tar` format (where message parts are treated as individual
files, read the type documentation for more information). In the following
examples whenever we use the archive processor we will be using the binary
format.

## Example

Let's consider a platform where our services `foo` and `bar` communicate through
ZMQ, but we want to introduce Kafka in the middle in order to occasionally
replay the stream. We start off our prototype platform by bridging the services
with Benthos.

If `foo` and `bar` expect to use multipart messages then by default our new
pipeline is wrong:

![multipart demo 1][multipart_demo_1]

As you can see above, the `bar` service on the right will see each message part
as an individual message.

Our first solution could be to leave the first Benthos unchanged, but set the
second Benthos to combine N messages from Kafka into N part messages, where N is
the static number of parts we expect from each message (three in this example).

![multipart demo 2][multipart_demo_2]

The config file of the second Benthos might look something like this:

``` yaml
input:
  type: kafka
  kafka:
    addresses:
    - localhost:9092
    consumer_group: benthos_consumer_group
    topic: benthos_stream
    partition: 0
  processors:
  - type: combine
    combine:
      parts: 3
  - type: bounds_check
output:
  type: zmq4
  zmq4:
    addresses:
    - tcp://*:5556
    bind: true
    socket_type: PUSH
```

The above is brittle, as any communication errors or crashes could result in
duplicated message parts that will break the ordering of message parts. This
solution would also not be feasible if the number of parts per message is
dynamic.

Another option would be to configure both Benthos instances to use archive
processors.

![multipart demo 3][multipart_demo_3]

In this example we can fluidly propagate messages with dynamic numbers of parts.
We are also safe from communication errors and crashes, since in the worst case
this would only introduce a duplicate message and would not otherwise break the
stream.

For the above example our first Benthos config might look like this:

``` yaml
input:
  type: zmq4
  zmq4:
    addresses:
    - tcp://localhost:5555
    socket_type: PULL
  processors:
  - type: bounds_check
  - type: archive
output:
  type: kafka
  kafka:
    addresses:
    - localhost:9092
    client_id: benthos_kafka_output
    topic: benthos_stream
```

And the second config might look like this:

``` yaml
input:
  type: kafka
  kafka:
    addresses:
    - localhost:9092
    consumer_group: benthos_consumer_group
    topic: benthos_stream
    partition: 0
  processors:
  - type: unarchive
  - type: bounds_check
output:
  type: zmq4
  zmq4:
    addresses:
    - tcp://*:5556
    bind: true
    socket_type: PUSH
```

We could even combine single and multiple part communication protocols by
specifying these processors per input or output type. For example, if we wished
to allow the second Benthos to also read directly from a `foo` type and combine
the streams we could change the config to this:

``` yaml
input:
  type: broker
  broker:
    inputs:
    - type: kafka
      kafka:
        addresses:
        - localhost:9092
        consumer_group: benthos_consumer_group
        topic: benthos_stream
        partition: 0
      processors:
      - type: unarchive
    - type: zmq4
      zmq4:
        addresses:
        - tcp://localhost:5555
        socket_type: PULL
  processors:
  - type: bounds_check
output:
  type: zmq4
  zmq4:
    addresses:
    - tcp://*:5556
    bind: true
    socket_type: PUSH
```

Where the `unarchive` processor is only applied to Kafka, but the `bounds_check`
is correctly applied to all inputs.

Solved.

[multipart_demo_1]: ../resources/img/docs/multipart_demo_1.png
[multipart_demo_2]: ../resources/img/docs/multipart_demo_2.png
[multipart_demo_3]: ../resources/img/docs/multipart_demo_3.png
