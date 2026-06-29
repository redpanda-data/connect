# Redpanda Connect

[![Build Status][actions-badge]][actions-url]
[![Apache V2 API][godoc-badge]][godoc-url-apache]
[![Enterprise API][godoc-badge]][godoc-url-enterprise]

Redpanda Connect is a stream processor that moves data between a wide range of [sources][inputs] and [sinks][outputs], with support for [hydration, enrichment, transformation, and filtering][processors] along the way.

That includes a rich set of change-data-capture (CDC) connectors — for Postgres, MySQL, MongoDB, Oracle, MSSQL, and more — so database changes can flow through your pipelines as first-class events.

It uses [Bloblang][bloblang-about] for mapping, runs as a single static binary or container image, and is easy to operate and monitor.

## Highlights

- **Declarative pipelines** — a stream topology fits in a single YAML file.
- **At-least-once delivery by default** — in-process transactions, no disk state required.
- **A large connector catalog** — cloud services, message brokers, databases, HTTP, and more.
- **First-class CDC** — change-data-capture connectors for Postgres, MySQL, MongoDB, Oracle, and MSSQL.
- **Bloblang** — a mapping language designed for stream data.
- **Cloud-friendly** — stateless and horizontally scalable, with metrics and tracing built in.

## Example

Stream Postgres changes into Apache Iceberg tables on S3, one Iceberg table per source table:

```yaml
input:
  postgres_cdc:
    dsn: postgres://user:pass@db.example.com:5432/app?sslmode=require
    schema: public
    tables: [ orders, customers ]
    stream_snapshot: true

output:
  iceberg:
    catalog:
      url: https://glue.us-east-1.amazonaws.com/iceberg
      warehouse: "123456789012"
      auth:
        aws_sigv4:
          region: us-east-1
          service: glue
    namespace: cdc
    table: ${! meta("table") }
    storage:
      aws_s3:
        bucket: my-iceberg-warehouse
        region: us-east-1
    schema_evolution:
      enabled: true
      table_location: s3://my-iceberg-warehouse/cdc/
```

## Quickstart

### Install

Linux:

```shell
curl -LO https://github.com/redpanda-data/redpanda/releases/latest/download/rpk-linux-amd64.zip
unzip rpk-linux-amd64.zip -d ~/.local/bin/
```

macOS (Homebrew):

```shell
brew install redpanda-data/tap/redpanda
```

Docker:

```shell
docker pull docker.redpanda.com/redpandadata/connect
```

See the [getting started guide][getting-started] for more options.

### Run

```shell
rpk connect run ./config.yaml
```

With Docker:

```shell
# From a config file
docker run --rm -v /path/to/your/config.yaml:/connect.yaml docker.redpanda.com/redpandadata/connect run

# With inline overrides
docker run --rm -p 4195:4195 docker.redpanda.com/redpandadata/connect run \
  -s "input.type=http_server" \
  -s "output.type=kafka" \
  -s "output.kafka.addresses=kafka-server:9092" \
  -s "output.kafka.topic=redpanda_topic"
```

## Connectors

The catalog includes AWS (DynamoDB, Kinesis, S3, SQS, SNS), Azure (Blob, Queue, Table), GCP (Pub/Sub, Cloud Storage, BigQuery), Kafka, NATS (JetStream, Streaming), NSQ, MQTT, AMQP 0.91 (RabbitMQ), AMQP 1, Redis, Cassandra, Elasticsearch, HDFS, HTTP (server, client, websockets), MongoDB, and SQL (MySQL, PostgreSQL, ClickHouse, MSSQL) — and a lot more in the [components documentation][about-categories].

## Delivery guarantees

Delivery guarantees [can be a tricky subject](https://youtu.be/QmpBOCvY8mY). Redpanda Connect processes and acknowledges messages using an in-process transaction model with no disk-persisted state, so when it's connecting at-least-once sources and sinks it can guarantee at-least-once delivery — even through crashes, disk corruption, or other server faults.

That's the default, with no caveats, which keeps deployment and scaling straightforward.

## Observability

### Health checks

Two HTTP endpoints are exposed for orchestration probes:

- `/ping` — liveness probe; always returns 200.
- `/ready` — readiness probe; returns 200 once both input and output are connected, otherwise 503.

### Metrics

Redpanda Connect [exposes metrics][metrics] to Statsd, Prometheus, a JSON HTTP endpoint, and [other backends][metrics].

### Tracing

OpenTelemetry traces are [emitted natively][tracers], so you can visualize what's happening inside a pipeline end-to-end.

## Configuration

Redpanda Connect ships with tooling for configuration discovery, debugging, and organization — see the [configuration guide][config-doc].

## Documentation

- [General documentation][general-docs]
- [Bloblang language guide][bloblang-about]
- [Public Go APIs](https://pkg.go.dev/github.com/redpanda-data/benthos/v4/public/service) for building custom plugins

## Build from source

Requires a [currently supported Go version](https://go.dev/dl/):

```shell
git clone git@github.com:redpanda-data/connect
cd connect
task build:all
```

### Plugins with external dependencies

Components that link against external C libraries (for example `zmq4`) aren't included by default. To pull them in, set the `x_benthos_extra` build tag:

```shell
# With go
go install -tags "x_benthos_extra" github.com/redpanda-data/connect/v4/cmd/redpanda-connect@latest

# With task
TAGS=x_benthos_extra task build:all
```

This tag may change or be split into more granular tags in future releases. If the required system libraries aren't installed, the build will fail with an error like `ld: library not found for -lzmq`.

### Docker image

A multi-stage `Dockerfile` builds a minimal scratch-based image:

```shell
task docker:all
```

```shell
docker run --rm \
    -v /path/to/your/config.yaml:/config.yaml \
    -v /tmp/data:/data \
    -p 4195:4195 \
    docker.redpanda.com/redpandadata/connect run /config.yaml
```

## Custom plugins

Writing your own plugins in Go is straightforward — check out the [API docs][godoc-url] and the [example plugin repository][plugin-repo] for reference implementations.

## Development

Redpanda Connect uses [golangci-lint][golangci-lint] for linting and `gofumpt` for formatting. You can configure your editor to use `gofumpt` automatically — instructions are [here](https://github.com/mvdan/gofumpt#installation).

```shell
task fmt    # format the codebase
task lint   # lint the codebase
task test   # unit and template tests
```

## Contributing

Contributions are welcome. Before opening a pull request, please make sure it has been:

- Unit tested with `task test`
- Linted with `task lint`
- Formatted with `task fmt`

Most integration tests spin up Docker containers, so they're skipped by `task test`. You can run them individually with:

```shell
go test -run "^Test.*Integration.*$" ./internal/impl/<connector directory>/...
```

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
[plugin-repo]: https://github.com/redpanda-data/redpanda-connect-plugin-example
[getting-started]: https://docs.redpanda.com/redpanda-connect/guides/getting_started

[godoc-badge]: https://pkg.go.dev/badge/github.com/redpanda-data/benthos/v4/public
[godoc-url]: https://pkg.go.dev/github.com/redpanda-data/benthos/v4/public
[godoc-url-apache]: https://pkg.go.dev/github.com/redpanda-data/connect/public/bundle/free/v4
[godoc-url-enterprise]: https://pkg.go.dev/github.com/redpanda-data/connect/public/bundle/enterprise/v4
[actions-badge]: https://github.com/redpanda-data/connect/actions/workflows/test.yml/badge.svg
[actions-url]: https://github.com/redpanda-data/connect/actions/workflows/test.yml

[golangci-lint]: https://golangci-lint.run/
[jaeger]: https://www.jaegertracing.io/
