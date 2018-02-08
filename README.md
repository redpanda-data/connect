![Benthos](icon.png "Benthos")

[![godoc for Jeffail/benthos][1]][2]
[![goreportcard for Jeffail/benthos][3]][4]
[![Build Status][travis-badge]][travis-url]

Benthos is a service that bridges message queues in ways that can simplify your
platform and reduce development time. It also offers a variety of
[configurable message processors][10] that can be chained together for solving
common streaming problems such as filtering, modifying, batching, splitting,
(de)compressing, (un)archiving, etc.

A range of optional buffer strategies are available, allowing you to select a
balance between latency, protection against back pressure and file based
persistence, or nothing at all (direct bridge).

## Supported Protocols

![Arch Diagram](resources/img/inputs_doodle.png "Benthos Inputs")

Currently supported input/output targets:

- [Amazon (S3, SQS)][amazons3]
- File
- HTTP(S)
- [Kafka][kafka]
- [Nanomsg][nanomsg]
- [NATS][nats]
- [NATS Streaming][natsstreaming]
- [NSQ][nsq]
- [RabbitMQ (AMQP 0.91)][rabbitmq]
- [Redis][redis]
- Stdin/Stdout
- [ZMQ4][zmq]

Setting up multiple outputs or inputs is done by choosing a routing strategy
(fan-in, fan-out, round-robin, etc.)

For a full and up to date list of all inputs, buffer options, processors, and
outputs [you can find them in the docs][7], or print them from the binary:

```
# Print inputs, buffers and output options
benthos --list-inputs --list-buffers --list-outputs --list-processors | less
```

Mixing multiple part message protocols with single part can be done in different
ways, for more guidance [check out this doc.][5]

## Install

Build with Go:

``` shell
go get github.com/Jeffail/benthos/cmd/benthos
```

Or, pull the docker image:

``` shell
docker pull jeffail/benthos
```

Or, [download from here.](https://github.com/Jeffail/benthos/releases)

## Run

``` shell
benthos -c ./config.yaml
```

Or, with docker:

``` shell
# Send HTTP /POST data to Kafka:
docker run --rm \
	-e "BENTHOS_INPUT=http_server" \
	-e "BENTHOS_OUTPUT=kafka" \
	-e "KAFKA_OUTPUT_BROKER_ADDRESSES=kafka-server:9092" \
	-e "KAFKA_OUTPUT_TOPIC=benthos_topic" \
	-p 4195:4195 \
	jeffail/benthos

# Using your own config file:
docker run --rm -v /path/to/your/config.yaml:/benthos.yaml jeffail/benthos
```

## Config

Benthos has inputs, optional processors, an optional buffer, and outputs, which
are all set in a single config file.

Check out the samples in [./config](config), or create a fully populated default
configuration file:

``` shell
benthos --print-yaml > config.yaml
benthos --print-json > config.json
```

The configuration file should contain a section for an input, output, and a
buffer. For example, if we wanted to output to a ZMQ4 push socket our output
section in a YAML config might look like this:

``` yaml
output:
  type: zmq4
  zmq4:
    addresses:
      - tcp://*:1234
    socket_type: PUSH
```

There are also configuration sections for logging and metrics, if you print an
example config you will see the available options.

For a list of metrics within Benthos [check out this spec][6].

### Environment Variables

[You can use environment variables][8] to replace fields in your config files.

## ZMQ4 Support

Benthos supports ZMQ4 for both data input and output. To add this you need to
install libzmq4 and use the compile time flag when building Benthos:

``` shell
go install -tags "ZMQ4" ./cmd/...
```

## Vendoring

Benthos uses [dep][dep] for managing dependencies. To get started make sure you
have dep installed:

`go get -u github.com/golang/dep/cmd/dep`

And then run `dep ensure`. You can decrease the size of vendor by only storing
needed files with `dep prune`.

## Docker

There's a multi-stage `Dockerfile` for creating a Benthos docker image which
results in a minimal image from scratch. You can build it with:

``` shell
make docker
```

Then use the image:

``` shell
docker run --rm \
	-v /path/to/your/benthos.yaml:/config.yaml \
	-v /tmp/data:/data \
	-p 4195:4195 \
	benthos -c /config.yaml
```

There are a [few examples here][9] that show you some ways of setting up Benthos
containers using `docker-compose`.

[1]: https://godoc.org/github.com/Jeffail/benthos?status.svg
[2]: https://godoc.org/github.com/Jeffail/benthos/lib/input
[3]: https://goreportcard.com/badge/github.com/Jeffail/benthos
[4]: https://goreportcard.com/report/Jeffail/benthos
[5]: resources/docs/multipart.md
[6]: resources/docs/metrics.md
[7]: resources/docs
[8]: resources/docs/config_interpolation.md
[9]: resources/docker/compose_examples
[10]: resources/docs/processors/list.md
[travis-badge]: https://travis-ci.org/Jeffail/benthos.svg?branch=master
[travis-url]: https://travis-ci.org/Jeffail/benthos
[dep]: https://github.com/golang/dep
[amazons3]: https://aws.amazon.com/s3/
[zmq]: http://zeromq.org/
[nanomsg]: http://nanomsg.org/
[rabbitmq]: https://www.rabbitmq.com/
[nsq]: http://nsq.io/
[nats]: http://nats.io/
[natsstreaming]: https://nats.io/documentation/streaming/nats-streaming-intro/
[redis]: https://redis.io/
[kafka]: https://kafka.apache.org/
