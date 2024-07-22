Redpanda Connect
================

[![Build Status][actions-badge]][actions-url]

API for Apache V2 builds: [![godoc for redpanda-data/connect ASL][godoc-badge]][godoc-url-apache]

API for Enterprise builds: [![godoc for redpanda-data/connect RCL][godoc-badge]][godoc-url-enterprise]

Redpanda Connect is a high performance and resilient stream processor, able to connect various [sources][inputs] and [sinks][outputs] in a range of brokering patterns and perform [hydration, enrichments, transformations and filters][processors] on payloads.

It comes with a [powerful mapping language][bloblang-about], is easy to deploy and monitor, and ready to drop into your pipeline either as a static binary or docker image, making it cloud native as heck.

Redpanda Connect is declarative, with stream pipelines defined in as few as a single config file, allowing you to specify connectors and a list of processing stages:

```yaml
input:
  gcp_pubsub:
    project: foo
    subscription: bar

pipeline:
  processors:
    - mapping: |
        root.message = this
        root.meta.link_count = this.links.length()
        root.user.age = this.user.age.number()

output:
  redis_streams:
    url: tcp://TODO:6379
    stream: baz
    max_in_flight: 20
```

### Delivery Guarantees

Delivery guarantees [can be a dodgy subject](https://youtu.be/QmpBOCvY8mY). Redpanda Connect processes and acknowledges messages using an in-process transaction model with no need for any disk persisted state, so when connecting to at-least-once sources and sinks it's able to guarantee at-least-once delivery even in the event of crashes, disk corruption, or other unexpected server faults.

This behaviour is the default and free of caveats, which also makes deploying and scaling Redpanda Connect much simpler.

## Supported Sources & Sinks

AWS (DynamoDB, Kinesis, S3, SQS, SNS), Azure (Blob storage, Queue storage, Table storage), GCP (Pub/Sub, Cloud storage, Big query), Kafka, NATS (JetStream, Streaming), NSQ, MQTT, AMQP 0.91 (RabbitMQ), AMQP 1, Redis (streams, list, pubsub, hashes), Cassandra, Elasticsearch, HDFS, HTTP (server and client, including websockets), MongoDB, SQL (MySQL, PostgreSQL, Clickhouse, MSSQL), and [you know what just click here to see them all, they don't fit in a README][about-categories].

## Documentation

If you want to dive fully into Redpanda Connect then don't waste your time in this dump, check out the [documentation site][general-docs].

For guidance on building your own custom plugins in Go check out [the public APIs](https://pkg.go.dev/github.com/redpanda-data/benthos/v4/public/service).

## Install

Install on Linux:

```shell
curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip
unzip rpk-linux-amd64.zip -d ~/.local/bin/
```

Or use Homebrew:

```shell
brew install redpanda-data/tap/redpanda
```

Or pull the docker image:

```shell
docker pull docker.redpanda.com/redpandadata/connect
```

For more information check out the [getting started guide][getting-started].

## Run

```shell
rpk connect run ./config.yaml
```

Or, with docker:

```shell
# Using a config file
docker run --rm -v /path/to/your/config.yaml:/connect.yaml docker.redpanda.com/redpandadata/connect run

# Using a series of -s flags
docker run --rm -p 4195:4195 docker.redpanda.com/redpandadata/connect run \
  -s "input.type=http_server" \
  -s "output.type=kafka" \
  -s "output.kafka.addresses=kafka-server:9092" \
  -s "output.kafka.topic=redpanda_topic"
```

## Monitoring

### Health Checks

Redpanda Connect serves two HTTP endpoints for health checks:
- `/ping` can be used as a liveness probe as it always returns a 200.
- `/ready` can be used as a readiness probe as it serves a 200 only when both the input and output are connected, otherwise a 503 is returned.

### Metrics

Redpanda Connect [exposes lots of metrics][metrics] either to Statsd, Prometheus, a JSON HTTP endpoint, [and more][metrics].

### Tracing

Redpanda Connect also [emits open telemetry tracing events][tracers], which can be used to visualise the processors within a pipeline.

## Configuration

Redpanda Connect provides lots of tools for making configuration discovery, debugging and organisation easy. You can [read about them here][config-doc].

## Build

Build with Go (any [currently supported version](https://go.dev/dl/)):

```shell
git clone git@github.com:redpanda-data/connect
cd connect
make
```

## Lint

Redpanda Connect uses [golangci-lint][golangci-lint] for linting, which you can install with:

```shell
curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin
```

And then run it with `make lint`.

## Plugins

It's pretty easy to write your own custom plugins for Redpanda Connect in Go, for information check out [the API docs][godoc-url], and for inspiration there's an [example repo][plugin-repo] demonstrating a variety of plugin implementations.

## Extra Plugins

By default Redpanda Connect does not build with components that require linking to external libraries, such as the `zmq4` input and outputs. If you wish to build Redpanda Connect locally with these dependencies then set the build tag `x_benthos_extra`:

```shell
# With go
go install -tags "x_benthos_extra" github.com/redpanda-data/connect/v4/cmd/redpanda-connect@latest

# Using make
make TAGS=x_benthos_extra
```

Note that this tag may change or be broken out into granular tags for individual components outside of major version releases. If you attempt a build and these dependencies are not present you'll see error messages such as `ld: library not found for -lzmq`.

## Docker Builds

There's a multi-stage `Dockerfile` for creating a Redpanda Connect docker image which results in a minimal image from scratch. You can build it with:

```shell
make docker
```

Then use the image:

```shell
docker run --rm \
	-v /path/to/your/benthos.yaml:/config.yaml \
	-v /tmp/data:/data \
	-p 4195:4195 \
	redpandadata/connect -c /config.yaml
```

## Contributing

Contributions are welcome, please [read the guidelines](CONTRIBUTING.md).

[inputs]: https://docs.redpanda.com/redpanda-connect/components/inputs/about
[about-categories]: https://docs.redpanda.com/redpanda-connect/about#components
[processors]: https://docs.redpanda.com/redpanda-connect/components/processors/about
[outputs]: https://docs.redpanda.com/redpanda-connect/components/outputs/about
[metrics]: https://docs.redpanda.com/redpanda-connect/components/metrics/about
[tracers]: https://docs.redpanda.com/redpanda-connect/components/tracers/about
[config-interp]: https://docs.redpanda.com/redpanda-connect/configuration/interpolation
[streams-api]: https://docs.redpanda.com/redpanda-connect/guides/streams_mode/streams_api
[streams-mode]: https://docs.redpanda.com/redpanda-connect/guides/streams_mode/about
[general-docs]: https://docs.redpanda.com/redpanda-connect/about
[bloblang-about]: https://docs.redpanda.com/redpanda-connect/guides/bloblang/about
[config-doc]: https://docs.redpanda.com/redpanda-connect/configuration/about
[releases]: https://github.com/redpanda-data/connect/releases
[plugin-repo]: https://github.com/benthosdev/benthos-plugin-example
[getting-started]: https://docs.redpanda.com/redpanda-connect/guides/getting_started
[benthos-studio]: https://studio.benthos.dev

[godoc-badge]: https://pkg.go.dev/badge/github.com/redpanda-data/benthos/v4/public
[godoc-url-apache]: https://pkg.go.dev/github.com/redpanda-data/connect/public/bundle/free/v4
[godoc-url-enterprise]: https://pkg.go.dev/github.com/redpanda-data/connect/public/bundle/enterprise/v4
[actions-badge]: https://github.com/redpanda-data/connect/actions/workflows/test.yml/badge.svg
[actions-url]: https://github.com/redpanda-data/connect/actions/workflows/test.yml

[golangci-lint]: https://golangci-lint.run/
[jaeger]: https://www.jaegertracing.io/
