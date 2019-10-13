![Benthos](icon.png "Benthos")

[![godoc for Jeffail/benthos][godoc-badge]][godoc-url]
[![goreportcard for Jeffail/benthos][goreport-badge]][goreport-url]
[![Build Status][drone-badge]][drone-url]

Benthos is a high performance and resilient stream processor, able to connect
various [sources][inputs] and [sinks][outputs] and perform arbitrary
[actions, transformations and filters][processors] on payloads. It is easy to
deploy and monitor, and ready to drop into your pipeline either as a static
binary or a docker image.

Stream pipelines are defined in a single config file, allowing you to declare
connectors and a list of processing stages:

```yaml
input:
  kafka_balanced:
    addresses: [ TODO ]
    topics: [ foo, bar ]
    consumer_group: foogroup

pipeline:
  threads: 4
  processors:
  - jmespath:
      query: '{ message: @, meta: { link_count: length(links) } }'

output:
  s3:
    bucket: TODO
    path: "${!metadata:kafka_topic}/${!json_field:message.id}.json"
```

### Delivery Guarantees

Benthos implements transaction based resiliency with back pressure. When
connecting to at-least-once sources and sinks it guarantees at-least-once
delivery without needing to persist messages during transit.

When running a Benthos stream with a [buffer][buffers] there are various options
for choosing a level of resiliency that meets your needs.

### Serverless

There are also [specialised distributions][serverless] of Benthos for serverless
deployment.

## Supported Sources & Sinks

- [AWS (DynamoDB, Kinesis, S3, SQS, SNS)][aws]
- [Elasticsearch][elasticsearch] (output only)
- File
- [GCP (pub/sub)][gcp]
- [HDFS][hdfs]
- HTTP(S)
- [Kafka][kafka]
- [Memcached][memcached] (output only)
- [MQTT][mqtt]
- [Nanomsg][nanomsg]
- [NATS][nats]
- [NATS Streaming][natsstreaming]
- [NSQ][nsq]
- [RabbitMQ (AMQP 0.91)][rabbitmq]
- [Redis (streams, list, pubsub, hashes)][redis]
- Stdin/Stdout
- TCP & UDP
- Websocket
- [ZMQ4][zmq]

## Documentation

Documentation for Benthos components, concepts and recommendations can be found
on the [documentation site][general-docs], or within the repo at the
[docs directory][docs-dir].

For guidance on how to configure more advanced stream processing concepts such
as stream joins, enrichment workflows, etc, check out the
[cookbooks section.][cookbooks]

For guidance on building your own custom plugins check out
[this example repo.][plugin-repo]

## Run

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

## Monitoring

### Health Checks

Benthos serves two HTTP endpoints for health checks:
- `/ping` can be used as a liveness probe as it always returns a 200.
- `/ready` can be used as a readiness probe as it serves a 200 only when both
  the input and output are connected, otherwise a 503 is returned.

### Metrics

Benthos [exposes lots of metrics][metrics] either to Statsd, Prometheus or for
debugging purposes an HTTP endpoint that returns a JSON formatted object. The
target can be specified [via config][metrics-config].

### Tracing

Benthos also [emits opentracing events][tracers] to a tracer of your choice
(currently only [Jaeger][jaeger] is supported) which can be used to visualise
the processors within a pipeline.

## Configuration

Benthos provides lots of tools for making configuration discovery, debugging and
organisation easy. You can [read about them here][config-doc].

You can also find runnable example configs demonstrating each input, output,
buffer and processor option [here](config).

### Environment Variables

It is possible to select fields inside a configuration file to be set via
[environment variables][config-interp]. The docker image, for example, is built
with [a config file][env-config] where _all_ common fields can be set this way.

## Install

Grab a binary for your OS from [here.][releases]

Or pull the docker image:

```shell
docker pull jeffail/benthos
```

On macOS, Benthos can be installed via Homebrew:

```shell
brew install benthos
```

Build with Go (1.11 or later):

```shell
git clone git@github.com:Jeffail/benthos
cd benthos
make
```

### Plugins

It's pretty easy to write your own custom plugins for Benthos, take a look at
[this repo][plugin-repo] for examples and build instructions.


### Docker Builds

There's a multi-stage `Dockerfile` for creating a Benthos docker image which
results in a minimal image from scratch. You can build it with:

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

There are a [few examples here][compose-examples] that show you some ways of
setting up Benthos containers using `docker-compose`.

### ZMQ4 Support

Benthos supports ZMQ4 for both data input and output. To add this you need to
install libzmq4 and use the compile time flag when building Benthos:

```shell
make TAGS=ZMQ4
```

Or to build a docker image using CGO, which includes ZMQ:

```shell
make docker-cgo
```

## Contributing

Contributions are welcome, please [read the guidelines](CONTRIBUTING.md), come
and chat in the [#benthos Gophers slack channel][benthos-slack-chan]
([get an invite][gophers-slack-invite]), and watch your back.

[inputs]: https://docs.benthos.dev/inputs/
[buffers]: https://docs.benthos.dev/buffers/
[processors]: https://docs.benthos.dev/processors/
[outputs]: https://docs.benthos.dev/outputs/

[metrics]: https://docs.benthos.dev/metrics/
[tracers]: https://docs.benthos.dev/tracers/
[metrics-config]: config/metrics
[config-interp]: https://docs.benthos.dev/config_interpolation/
[compose-examples]: resources/docker/compose_examples
[streams-api]: https://docs.benthos.dev/api/streams/
[streams-mode]: https://docs.benthos.dev/streams/
[general-docs]: https://docs.benthos.dev
[examples-docs]: https://docs.benthos.dev/examples/
[env-config]: config/env/README.md
[config-doc]: https://docs.benthos.dev/configuration/
[serverless]: https://docs.benthos.dev/serverless/
[cookbooks]: https://docs.benthos.dev/cookbooks/
[docs-dir]: docs/README.md

[releases]: https://github.com/Jeffail/benthos/releases
[plugin-repo]: https://github.com/benthosdev/benthos-plugin-example

[godoc-badge]: https://godoc.org/github.com/Jeffail/benthos/lib/stream?status.svg
[godoc-url]: https://godoc.org/github.com/Jeffail/benthos/lib/stream
[goreport-badge]: https://goreportcard.com/badge/github.com/Jeffail/benthos
[goreport-url]: https://goreportcard.com/report/Jeffail/benthos
[drone-badge]: https://cloud.drone.io/api/badges/Jeffail/benthos/status.svg
[drone-url]: https://cloud.drone.io/Jeffail/benthos

[benthos-slack-chan]: https://app.slack.com/client/T029RQSE6/CLWCBK7FY
[gophers-slack-invite]: https://gophersinvite.herokuapp.com/

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
[hdfs]: https://hadoop.apache.org/
[gcp]: https://cloud.google.com/
[memcached]: https://memcached.org/
[jaeger]: https://www.jaegertracing.io/
