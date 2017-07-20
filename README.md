![Benthos](icon.png "Benthos")

[![godoc for jeffail/benthos][1]][2]
[![goreportcard for jeffail/benthos][3]][4]

Benthos is a messaging bridge service that supports a wide and growing list of
input and output protocols.

A range of internal buffer strategies are available, allowing you to select a
balance between latency, protection against back pressure and file based
persistence, or nothing at all (direct bridge).

## Design

Benthos has inputs, an optional buffer, and outputs, which are all set in a
single config file.

```
+--------------------------------------+
|            Input Stream              |
| ( ZMQ, NSQ, AMQP, Kafka, HTTP, ... ) |--+
+--------------------------------------+  |
                                          v
             +--------------------------------------------+
             |                   Buffer                   |
             | ( Memory-Mapped Files, Memory, None, etc ) |
             +--------------------------------------------+
                             |
                             |  +--------------------------------------+
                             +->|             Output Stream            |
                                | ( ZMQ, NSQ, AMQP, Kafka, HTTP, ... ) |
                                +--------------------------------------+
```

## Supported Protocols

Currently supported input/output targets:

- [ZMQ4 (PUSH, PULL, SUB, PUB)][zmq]
- [Nanomsg/Scalability Protocols (PUSH, PULL, SUB, PUB)][nanomsg]
- [RabbitMQ (AMQP)][rabbitmq]
- [NSQ][nsq]
- [NATS][nats]
- [Kafka][kafka]
- HTTP 1.1 POST/GET
- STDIN/STDOUT
- File

You can also have multiple outputs or inputs by choosing a routing strategy
(fan in, fan out, round robin, etc.)

For a full and up to date list you can print them from the binary:

```
# Print inputs, buffers and output options
benthos --print-inputs --print-buffers --print-outputs | less
```

## Install

Build with Go:

``` shell
go get github.com/jeffail/benthos/cmd/...
```

Or, [download from here.](https://github.com/Jeffail/benthos/releases)

## Run

``` shell
benthos -c ./config.yaml
```

## Config

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

## Speed and Benchmarks

Benthos isn't doing much, so it's reasonable to expect low latencies and high
throughput. Here's a table of benchmarks from an 4-core (2.4ghz) machine,
bridging messages of 5000 bytes through a 500MB memory buffer via various
protocols:

|       | Avg. Latency (ms) | 99th P. Latency (ms) |    Msg/s |    Mb/s |
|------:|------------------:|---------------------:|---------:|--------:|
| ZMQ   |             5.940 |               67.268 |    31357 | 157.383 |
| Nano  |             3.312 |               20.020 |    22894 | 114.907 |
| HTTP  |             0.549 |                8.751 |     4280 |  21.479 |

Take these results with a pinch of salt. I've added some benchmarking utilities
in `./cmd/test`, hopefully a third party can cook us up some more meaningful
figures from a better set up than I have.

## ZMQ4 Support

Benthos supports ZMQ4 for both data input and output. To add this you need to
install libzmq4 and use the compile time flag when building benthos:

``` shell
go install -tags "ZMQ4" ./cmd/...
```

## Vendoring

Versions of go above 1.6 should automatically `go get` all vendored libraries.
Otherwise, while cloning use `--recursive`:

`git clone https://github.com/jeffail/benthos --recursive`

Or, if the repo is already cloned, get the latest libraries with:

`git submodule update --init`

To add new libraries simply run:

``` shell
PACKAGE=github.com/jeffail/util
git submodule add https://$PACKAGE vendor/$PACKAGE"
```

It might be handy to set yourself a function for this in your `.bashrc`:

``` bash
function go-add-vendor {
	git submodule add https://$1 vendor/$1
}
```

## Docker

There's a `Dockerfile` for creating a benthos docker image. This is built from
scratch and so you'll need to build without CGO (`CGO_ENABLED=0`) for your
benthos build to run within it. Create it like this:

``` shell
CGO_ENABLED=0 make docker
docker run --rm benthos
```

Then use the image:

``` shell
docker run --rm -v ~/benthos.yaml:/config.yaml -v /tmp/data:/data -p 8080:8080 \
	benthos -c /config.yaml
```

[1]: https://godoc.org/github.com/jeffail/benthos?status.svg
[2]: http://godoc.org/github.com/jeffail/benthos
[3]: https://goreportcard.com/badge/github.com/jeffail/benthos
[4]: https://goreportcard.com/report/jeffail/benthos
[zmq]: http://zeromq.org/
[nanomsg]: http://nanomsg.org/
[rabbitmq]: https://www.rabbitmq.com/
[nsq]: http://nsq.io/
[nats]: http://nats.io/
[kafka]: https://kafka.apache.org/
