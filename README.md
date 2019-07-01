![Benthos](icon.png "Benthos")

[![godoc for Jeffail/benthos](https://godoc.org/github.com/Jeffail/benthos/lib/stream?status.svg)](https://godoc.org/github.com/Jeffail/benthos/lib/stream) [![goreportcard for Jeffail/benthos](https://goreportcard.com/badge/github.com/Jeffail/benthos)](https://goreportcard.com/report/Jeffail/benthos) [![Build Status](https://cloud.drone.io/api/badges/Jeffail/benthos/status.svg)](https://cloud.drone.io/Jeffail/benthos)

Benthos is a high performance and resilient message streaming service, able to connect various sources and sinks and perform arbitrary [actions, transformations and filters](https://docs.benthos.dev/processors/) on payloads. It is easy to deploy and monitor, and ready to drop into your pipeline either as a static binary or a docker image. It can also be used as a [framework](https://godoc.org/github.com/Jeffail/benthos/lib/stream) for building your own resilient stream processors in Go.

A Benthos stream consists of four layers: [inputs](https://docs.benthos.dev/inputs/), optional [buffer](https://docs.benthos.dev/buffers/), [processor](https://docs.benthos.dev/processors/) workers and [outputs](https://docs.benthos.dev/outputs/). Inputs and outputs can be combined in a range of broker patterns. It is possible to run multiple isolated streams within a single Benthos instance using [`--streams` mode](https://docs.benthos.dev/streams/), and perform CRUD operations on the running streams via [REST endpoints](https://docs.benthos.dev/api/streams/).

### Delivery Guarantees

Benthos is crash resilient by default. When connecting to at-least-once sources and sinks without a buffer it guarantees at-least-once delivery without needing to persist messages during transit.

When running a Benthos stream with a [buffer](https://docs.benthos.dev/buffers/) there are various options for choosing a level of resiliency that meets your needs.

### Serverless

There are [specialised distributions](https://docs.benthos.dev/serverless/) of Benthos for serverless deployment.

Supported Sources & Sinks
-------------------------

- [AWS (DynamoDB, Kinesis, S3, SQS)](https://aws.amazon.com/)
- [Elasticsearch](https://www.elastic.co/) (output only)
- File
- [GCP (pub/sub)](https://cloud.google.com/)
- [HDFS](https://hadoop.apache.org/)
- HTTP(S)
- [Kafka](https://kafka.apache.org/)
- [Memcached](https://memcached.org/) (output only)
- [MQTT](http://mqtt.org/)
- [Nanomsg](http://nanomsg.org/)
- [NATS](http://nats.io/)
- [NATS Streaming](https://nats.io/documentation/streaming/nats-streaming-intro/)
- [NSQ](http://nsq.io/)
- [RabbitMQ (AMQP 0.91)](https://www.rabbitmq.com/)
- [Redis (streams, list, pubsub)](https://redis.io/)
- Stdin/Stdout
- Websocket
- [ZMQ4](http://zeromq.org/)

Documentation
-------------

Documentation for Benthos components, concepts and recommendations can be found on the [documentation site](https://docs.benthos.dev), or within the repo at the [docs directory](docs/README.md).

For guidance on how to configure more advanced stream processing concepts such as stream joins, enrichment workflows, etc, check out the [cookbooks section.](https://docs.benthos.dev/cookbooks/)

For guidance on building your own custom plugins check out [this example repo.](https://github.com/benthosdev/benthos-plugin-example)

Run
---

```shell
benthos -c ./config.yaml
```

Or, with docker:

```shell
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

Monitoring
----------

### Health Checks

Benthos serves two HTTP endpoints for health checks:

- `/ping` can be used as a liveness probe as it always returns a 200.
- `/ready` can be used as a readiness probe as it serves a 200 only when both the input and output are connected, otherwise a 503 is returned.

### Metrics

Benthos [exposes lots of metrics](https://docs.benthos.dev/metrics/) either to Statsd, Prometheus or for debugging purposes an HTTP endpoint that returns a JSON formatted object. The target can be specified [via config](config/metrics).

### Tracing

Benthos also [emits opentracing events](https://docs.benthos.dev/tracers/) to a tracer of your choice (currently only [Jaeger](https://www.jaegertracing.io/) is supported) which can be used to visualise the processors within a pipeline.

Configuration
-------------

The configuration file for a Benthos stream is made up of four main sections; input, buffer, pipeline, output. If we were to pipe stdin directly to Kafka it would look like this:

```yaml
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

Benthos provides lots of tools for making configuration discovery, debugging and organisation easy. You can [read about them here](https://docs.benthos.dev/configuration/).

You can also find runnable example configs demonstrating each input, output, buffer and processor option [here](config).

### Environment Variables

It is possible to select fields inside a configuration file to be set via [environment variables](https://docs.benthos.dev/config_interpolation/). The docker image, for example, is built with [a config file](config/env/README.md) where *all* common fields can be set this way.

Install
-------

Grab a binary for your OS from [here.](https://github.com/Jeffail/benthos/releases)

Or pull the docker image:

```shell
docker pull jeffail/benthos
```

Build with Go (1.11 or later):

```shell
git clone git@github.com:Jeffail/benthos
cd benthos
make
```

### Plugins

It's pretty easy to write your own custom plugins for Benthos, take a look at [this repo](https://github.com/benthosdev/benthos-plugin-example) for examples and build instructions.

### Docker Builds

There's a multi-stage `Dockerfile` for creating a Benthos docker image which results in a minimal image from scratch. You can build it with:

```shell
make docker
```

Then use the image:

```shell
docker run --rm \
	-v /path/to/your/benthos.yaml:/config.yaml \
	-v /tmp/data:/data \
	-p 4195:4195 \
	benthos -c /config.yaml
```

There are a [few examples here](resources/docker/compose_examples) that show you some ways of setting up Benthos containers using `docker-compose`.

### ZMQ4 Support

Benthos supports ZMQ4 for both data input and output. To add this you need to install libzmq4 and use the compile time flag when building Benthos:

```shell
make TAGS=ZMQ4
```

Or to build a docker image using CGO, which includes ZMQ:

```shell
make docker-cgo
```

Contributing
------------

Contributions are welcome, please [read the guidelines](CONTRIBUTING.md).
