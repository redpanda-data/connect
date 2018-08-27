![Benthos](icon.png "Benthos")

[![godoc for Jeffail/benthos][godoc-badge]][godoc-url]
[![goreportcard for Jeffail/benthos][goreport-badge]][goreport-url]
[![Build Status][travis-badge]][travis-url]

Benthos is a high performance and resilient message streaming service, able to
connect various sources and sinks and perform arbitrary
[actions, transformations and filters][processors] on payloads. It is easy to
deploy and monitor, and ready to drop into your pipeline either as a static
binary or a docker image. It can also be used as a [framework][godoc-url] for
building your own resilient stream processors in Go.

A Benthos stream consists of four layers: [inputs][inputs], optional
[buffer][buffers], [processor][processors] workers and [outputs][outputs].
Inputs and outputs can be combined in a range of broker patterns. It is possible
to run multiple isolated streams within a single Benthos instance using
[`--streams` mode][streams-mode], and perform CRUD operations on the running
streams via [REST endpoints][streams-api].

### Delivery Guarantees

Benthos is crash resilient by default. When connecting to at-least-once sources
and sinks without a buffer it guarantees at-least-once delivery without needing
to persist messages during transit.

When running a Benthos stream with a [buffer][buffers] there are various options
for choosing a level of resiliency that meets your needs.

## Supported Sources & Sinks

- [AWS (S3, SQS, Kinesis)][aws]
- [Elasticsearch][elasticsearch] (output only)
- File
- HTTP(S)
- [Kafka][kafka]
- [MQTT][mqtt]
- [Nanomsg][nanomsg]
- [NATS][nats]
- [NATS Streaming][natsstreaming]
- [NSQ][nsq]
- [RabbitMQ (AMQP 0.91)][rabbitmq]
- [Redis (streams, list, pubsub)][redis]
- Stdin/Stdout
- Websocket
- [ZMQ4][zmq]

## Documentation

Documentation for Benthos components, concepts and recommendations can be found
in the [docs directory.][general-docs]

For building your own stream processors using Benthos as a framework check out
the [stream package][godoc-url], which also includes some examples.

For some applied examples of Benthos such as streaming and deduplicating the
Twitter firehose to Kafka [check out the cookbook section][cookbook-docs].

## Run

``` shell
benthos -c ./config.yaml
```

Or, with docker:

``` shell
# Send HTTP /POST data to Kafka:
docker run --rm \
	-e "INPUT_TYPE=http_server" \
	-e "OUTPUT_TYPE=kafka" \
	-e "OUTPUT_KAFKA_ADDRESSES=kafka-server:9092" \
	-e "OUTPUT_KAFKA_TOPIC=benthos_topic" \
	-p 4195:4195 \
	jeffail/benthos

# Using your own config file:
docker run --rm -v /path/to/your/config.yaml:/benthos.yaml jeffail/benthos
```

### Metrics

Benthos [exposes lots of metrics][metrics] either to Statsd, Prometheus or for
debugging purposes an HTTP endpoint that returns a JSON formatted object.

## Configuration

The configuration file for a Benthos stream is made up of four main sections;
input, buffer, pipeline, output. If we were to pipe stdin directly to Kafka it
would look like this:

``` yaml
input:
  type: stdin
buffer:
  type: none
pipeline:
  threads: 1
  processors: []
output:
  type: kafka
  kafka:
    addresses:
    - localhost:9092
    topic: benthos_stream
```

There are also sections for setting logging, metrics and HTTP server options.

Benthos provides lots of tools for making configuration discovery and debugging
easy. You can read about them [here][config-doc].

You can also find runnable example configs demonstrating each input, output,
buffer and processor option [here](config).

### Environment Variables

It is possible to select fields inside a configuration file to be set via
[environment variables][config-interp]. The docker image, for example, is built
with [a config file][env-config] where _all_ common fields can be set this way.

## Install

Build with Go (1.11 or later):

``` shell
make deps
make
```

Or, pull the docker image:

``` shell
docker pull jeffail/benthos
```

Or, [grab a binary for your OS from here.][releases]

### Docker Builds

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

There are a [few examples here][compose-examples] that show you some ways of
setting up Benthos containers using `docker-compose`.

### ZMQ4 Support

Benthos supports ZMQ4 for both data input and output. To add this you need to
install libzmq4 and use the compile time flag when building Benthos:

``` shell
make TAGS=ZMQ4
```

## Contributing

Contributions are welcome, please [read the guidelines](CONTRIBUTING.md).

[inputs]: docs/inputs/README.md
[buffers]: docs/buffers/README.md
[processors]: docs/processors/README.md
[outputs]: docs/outputs/README.md

[metrics]: docs/metrics.md
[config-interp]: docs/config_interpolation.md
[compose-examples]: resources/docker/compose_examples
[streams-api]: docs/api/streams.md
[streams-mode]: docs/streams/README.md
[general-docs]: docs/README.md
[cookbook-docs]: docs/cookbook/README.md
[env-config]: config/env/README.md
[config-doc]: docs/configuration.md

[releases]: https://github.com/Jeffail/benthos/releases

[godoc-badge]: https://godoc.org/github.com/Jeffail/benthos/lib/stream?status.svg
[godoc-url]: https://godoc.org/github.com/Jeffail/benthos/lib/stream
[goreport-badge]: https://goreportcard.com/badge/github.com/Jeffail/benthos
[goreport-url]: https://goreportcard.com/report/Jeffail/benthos
[travis-badge]: https://travis-ci.org/Jeffail/benthos.svg?branch=master
[travis-url]: https://travis-ci.org/Jeffail/benthos

[aws]: https://aws.amazon.com/
[zmq]: http://zeromq.org/
[nanomsg]: http://nanomsg.org/
[rabbitmq]: https://www.rabbitmq.com/
[mqtt]: http://mqtt.org/
[nsq]: http://nsq.io/
[nats]: http://nats.io/
[natsstreaming]: https://nats.io/documentation/streaming/nats-streaming-intro/
[redis]: https://redis.io/
[kafka]: https://kafka.apache.org/
[elasticsearch]: https://www.elastic.co/
